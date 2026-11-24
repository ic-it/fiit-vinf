"""Extract one canonical result per raw page, without HTTP or search processing."""

from collections.abc import Iterator
from pathlib import Path

from vinf_core.artifacts import (
    crawl_documents,
    fingerprint,
    read_body,
    source_document,
    write_models,
)
from vinf_core.files import (
    artifact_lock,
    atomic_model,
    file_hash,
    read_lock,
    read_model,
    separate_roots,
)
from vinf_core.models import Compression, VinfError
from vinf_core.records import (
    ParsedEntry,
    ParsedManifest,
    ParsedPage,
    ParseResult,
    ParseStatus,
)

from vinf_parsing.profile import ExtractionProfile


def parse_documents(
    crawled: Path,
    parsed: Path,
    *,
    profile: ExtractionProfile,
    compression: Compression | None = None,
) -> ParseResult:
    separate_roots(crawled, parsed)
    with read_lock(crawled / "crawl/lock"), artifact_lock(parsed):
        if compression is None:
            saved = parsed / "manifest.json"
            compression = (
                Compression.GZIP
                if saved.exists()
                and read_model(saved, ParsedManifest).records_path.endswith(".gz")
                else Compression.NONE
            )
        if compression not in {Compression.NONE, Compression.GZIP}:
            raise VinfError("Parsed JSON supports compression none or gzip.")
        suffix = ".json.gz" if compression == Compression.GZIP else ".json"
        catalog_hash, documents = crawl_documents(crawled)
        catalog = parsed / (
            "catalog.jsonl.gz" if compression == Compression.GZIP else "catalog.jsonl"
        )
        counts = {status: 0 for status in ParseStatus}
        written = reused = 0
        obsolete: list[Path] = []

        def entries() -> Iterator[ParsedEntry]:
            nonlocal written, reused
            for document in documents:
                source = source_document(document)
                key = fingerprint(profile.version, source.model_dump_json())
                path = parsed / "pages" / f"{source.doc_id}{suffix}"
                alternate = (
                    path.with_suffix("")
                    if path.suffix == ".gz"
                    else path.with_suffix(".json.gz")
                )
                cached = None
                for candidate in (path, alternate):
                    if candidate.exists():
                        value = read_model(candidate, ParsedPage)
                        if (
                            value.fingerprint == key
                            and value.status != ParseStatus.ERROR
                        ):
                            cached = value
                            if candidate != path:
                                atomic_model(path, value)
                            break
                if cached is not None:
                    page = cached
                    reused += 1
                else:
                    body = read_body(crawled, source)
                    decision = profile.select(body)
                    record = None
                    error = None
                    status = ParseStatus.REJECTED
                    if decision.selected:
                        try:
                            record = profile.extract(body, source)
                            status = ParseStatus.ACCEPTED
                        except ValueError as exc:
                            error, status = str(exc), ParseStatus.ERROR
                    page = ParsedPage(
                        source=source,
                        fingerprint=key,
                        status=status,
                        reason=decision.reason,
                        record=record,
                        error=error,
                    )
                    atomic_model(path, page)
                    written += 1
                counts[page.status] += 1
                obsolete.append(alternate)
                yield ParsedEntry(
                    doc_id=source.doc_id,
                    path=path.relative_to(parsed).as_posix(),
                    sha256=file_hash(path),
                    status=page.status,
                )

        write_models(catalog, entries())
        atomic_model(
            parsed / "manifest.json",
            ParsedManifest(
                producer=profile.version,
                input_fingerprint=fingerprint(profile.version, catalog_hash),
                record_count=len(documents),
                accepted=counts[ParseStatus.ACCEPTED],
                rejected=counts[ParseStatus.REJECTED],
                errors=counts[ParseStatus.ERROR],
                records_path=catalog.name,
                records_sha256=file_hash(catalog),
            ),
        )
        # Only discard redundant representations after publishing the new catalog.
        for path in obsolete:
            path.unlink(missing_ok=True)
        other_catalog = (
            catalog.with_suffix("")
            if catalog.suffix == ".gz"
            else catalog.with_suffix(".jsonl.gz")
        )
        other_catalog.unlink(missing_ok=True)
    return ParseResult(
        output=str(parsed),
        documents=len(documents),
        accepted=counts[ParseStatus.ACCEPTED],
        rejected=counts[ParseStatus.REJECTED],
        errors=counts[ParseStatus.ERROR],
        written=written,
        reused=reused,
    )
