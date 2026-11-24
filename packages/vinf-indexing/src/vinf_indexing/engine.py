"""Build one disposable, independent index with bounded external sorting."""

import math
import os
import shutil
import tempfile
from collections.abc import Sequence
from contextlib import ExitStack
from itertools import groupby
from pathlib import Path

from vinf_core.artifacts import fingerprint, parsed_catalog, parsed_items
from vinf_core.files import (
    artifact_lock,
    atomic_model,
    contained_path,
    file_hash,
    read_lock,
    read_model,
    separate_roots,
    sync_dir,
)
from vinf_core.index_format import FILES
from vinf_core.index_io import IndexReader
from vinf_core.indexes import (
    INDEX_VERSION,
    DocumentInfo,
    IndexConfig,
    IndexFile,
    IndexManifest,
    IndexResult,
)
from vinf_core.models import VinfError
from vinf_core.postings import TermCursor
from vinf_core.records import ParsedPage, SpeciesRecord
from vinf_core.sorting import ExternalSorter
from vinf_core.text import canonical_text, occurrences, tfidf_idf

from vinf_indexing.records import (
    NormRow,
    SourceRef,
    TokenRow,
    decode_norm,
    decode_token,
    encode_norm,
    encode_token,
)
from vinf_indexing.storage import (
    DocumentWriter,
    PostingsWriter,
    VocabularyWriter,
    write_norms,
)


def search_text(record: SpeciesRecord) -> str:
    parts = (
        record.scientific_name,
        record.common_name,
        *(s.text for s in record.sections),
    )
    return "\n".join(canonical_text(part) for part in parts if part and part.strip())


def _result(
    output: Path,
    manifest: IndexManifest,
    *,
    reused: bool,
    runs: int = 0,
    passes: int = 0,
) -> IndexResult:
    return IndexResult(
        output=str(output),
        generation=manifest.generation,
        documents=manifest.documents,
        terms=manifest.terms,
        tokens=manifest.tokens,
        duplicate_records=manifest.duplicate_records,
        reused=reused,
        sort_runs=runs,
        merge_passes=passes,
    )


def build_index(
    parsed: Path | Sequence[Path],
    output: Path,
    config: IndexConfig | None = None,
    *,
    rebuild: bool = False,
    memory_limit: int = 64 * 1024 * 1024,
    merge_fan_in: int = 16,
) -> IndexResult:
    output = output.resolve()
    inputs = (parsed,) if isinstance(parsed, Path) else tuple(parsed)
    if not inputs or len({p.resolve() for p in inputs}) != len(inputs):
        raise VinfError("Provide at least one distinct parsed directory.")
    if memory_limit < 1024 or not 2 <= merge_fan_in <= 64:
        raise VinfError(
            "Use a sort buffer of at least 1024 bytes and merge fan-in 2..64."
        )
    for path in inputs:
        separate_roots(path, output)
    settings = config or IndexConfig()
    with ExitStack() as locks:
        for path in sorted(inputs):
            locks.enter_context(read_lock(path / ".lock"))
        locks.enter_context(artifact_lock(output))
        catalogs = sorted(
            ((path, parsed_catalog(path)[1]) for path in inputs),
            key=lambda item: item[1].records_sha256,
        )
        if any(manifest.errors for _, manifest in catalogs):
            raise VinfError("Parsed data contains errors. Inspect its page records.")
        input_hash = fingerprint(
            "parsed-collections/2", *(m.records_sha256 for _, m in catalogs)
        )
        full_key = fingerprint(INDEX_VERSION, input_hash, settings.model_dump_json())
        if not rebuild and (output / "manifest.json").exists():
            saved = read_model(output / "manifest.json", IndexManifest)
            if saved.build_fingerprint == full_key:
                with IndexReader(output, acquire_lock=False) as reader:
                    reader.verify()
                return _result(output, saved, reused=True)
        with tempfile.TemporaryDirectory(prefix=".building-", dir=output) as temporary:
            workspace = Path(temporary)
            staged = workspace / "index"
            staged.mkdir()

            def source_sorter(name, key):
                return ExternalSorter[SourceRef](
                    workspace / name,
                    key=key,
                    encode=lambda row: row.model_dump_json().encode(),
                    decode=SourceRef.model_validate_json,
                    memory_bytes=memory_limit,
                    fan_in=merge_fan_in,
                )

            sources = source_sorter(
                "sources",
                lambda row: (
                    row.source_name,
                    row.source_id,
                    -row.timestamp,
                    str(row.doc_id),
                    row.source_fingerprint,
                    row.record_fingerprint,
                    row.path,
                ),
            )
            selected = source_sorter(
                "selected", lambda row: (str(row.doc_id), row.identity)
            )
            tokens = ExternalSorter[TokenRow](
                workspace / "tokens",
                key=lambda row: (row.term, row.doc_id, row.occurrence.position),
                encode=encode_token,
                decode=decode_token,
                memory_bytes=memory_limit,
                fan_in=merge_fan_in,
            )
            norms = ExternalSorter[NormRow](
                workspace / "norms",
                key=lambda row: (row.doc_id, row.term_id),
                encode=encode_norm,
                decode=decode_norm,
                memory_bytes=memory_limit,
                fan_in=merge_fan_in,
            )
            accepted = count = token_count = total_length = 0
            stop = set(settings.stop_words)
            for root, _ in catalogs:
                for entry, page in parsed_items(root):
                    if page.record is None:
                        continue
                    record = page.record
                    sources.add(
                        SourceRef(
                            source_name=record.source_name,
                            source_id=record.identifier(record.source_name)
                            or str(page.source.doc_id),
                            doc_id=page.source.doc_id,
                            timestamp=page.source.fetched_at.timestamp(),
                            path=str(contained_path(root, entry.path)),
                            source_fingerprint=fingerprint(
                                page.source.model_dump_json()
                            ),
                            record_fingerprint=fingerprint(record.model_dump_json()),
                        )
                    )
                    accepted += 1
            for _, group in groupby(sources.values(), key=lambda row: row.identity):
                chosen = None
                for other in group:
                    if chosen is None:
                        chosen = other
                    elif (
                        other.doc_id == chosen.doc_id
                        and other.source_fingerprint == chosen.source_fingerprint
                        and other.record_fingerprint != chosen.record_fingerprint
                    ):
                        raise VinfError(
                            f"Conflicting parsed records for {chosen.doc_id}."
                        )
                selected.add(chosen)
                count += 1
            if not count:
                raise VinfError(
                    "No extracted species records. Parse saved pages first."
                )
            with DocumentWriter(staged, count) as documents:
                for identifier, source in enumerate(selected.values()):
                    page = read_model(Path(source.path), ParsedPage)
                    record = page.record
                    text = search_text(record)
                    length = 0
                    for term, value in occurrences(text):
                        length += 1
                        if term not in stop:
                            tokens.add(TokenRow(term, identifier, value))
                            token_count += 1
                    documents.add(
                        source.doc_id,
                        DocumentInfo(
                            source_name=record.source_name,
                            source_id=source.source_id,
                            scientific_name=record.scientific_name,
                            common_name=record.common_name,
                            url=page.source.url,
                        ),
                        text,
                        length,
                    )
                    total_length += length
            with (
                VocabularyWriter(staged) as vocabulary,
                PostingsWriter(staged, workspace) as postings,
            ):
                for term_id, (term, rows) in enumerate(
                    groupby(tokens.values(), key=lambda row: row.term)
                ):
                    writer = postings.term(term_id)
                    for identifier, group in groupby(rows, key=lambda row: row.doc_id):
                        writer.add(identifier, (row.occurrence for row in group))
                    info = writer.finish(term)
                    vocabulary.add(info)
                    idf = tfidf_idf(count, info.df)
                    with TermCursor(staged, info, count) as cursor:
                        for posting in cursor:
                            norms.add(
                                NormRow(
                                    posting.doc_id,
                                    term_id,
                                    ((1 + math.log(posting.tf)) * idf) ** 2,
                                )
                            )
            write_norms(
                staged / "documents.idx",
                groupby(norms.values(), key=lambda row: row.doc_id),
            )
            manifest = IndexManifest(
                generation=full_key[:16],
                build_fingerprint=full_key,
                input_fingerprint=input_hash,
                config=settings,
                documents=count,
                terms=vocabulary.count,
                tokens=token_count,
                duplicate_records=accepted - count,
                average_length=total_length / count,
                files=tuple(
                    IndexFile(
                        path=name,
                        size_bytes=(staged / name).stat().st_size,
                        sha256=file_hash(staged / name),
                    )
                    for name, _ in FILES
                ),
            )
            atomic_model(staged / "manifest.json", manifest)
            # One current index, no history, migration branches or rollback copies.
            for path in output.iterdir():
                if path.name == ".lock" or path == workspace:
                    continue
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            for path in staged.iterdir():
                if path.name != "manifest.json":
                    os.replace(path, output / path.name)
            os.replace(staged / "manifest.json", output / "manifest.json")
            sync_dir(output)
            sorters = (sources, selected, tokens, norms)
            return _result(
                output,
                manifest,
                reused=False,
                runs=sum(s.runs for s in sorters),
                passes=sum(s.passes for s in sorters),
            )
