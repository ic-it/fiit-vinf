"""Build one standalone, native Lucene FSDirectory from completed extractions."""

import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from vinf_core.files import (
    artifact_lock,
    atomic_model,
    read_lock,
    read_model,
    separate_roots,
)
from vinf_core.models import VinfError
from vinf_core.text import canonical_text
from vinf_spark.models import FishBaseSpecies, WikiPage

from vinf_lucene.inputs import input_artifacts, records
from vinf_lucene.models import BuildResult, Manifest
from vinf_lucene.runtime import analyzer, initialize
from vinf_lucene.schema import FIELDS


def document(fish: FishBaseSpecies, wiki: WikiPage | None):
    from org.apache.lucene.document import (
        Document,
        Field,
        StoredField,
        StringField,
        TextField,
    )

    result, record = Document(), fish.record

    def add(name, value, kind):
        if value is not None and str(value).strip():
            result.add(kind(name, str(value), Field.Store.YES))

    add("identity", f"{record.source_name}:{fish.source_id}", StringField)
    add("source", record.source_name, StringField)
    add("source_id", fish.source_id, StringField)
    result.add(StoredField("doc_id", str(fish.source.doc_id)))
    result.add(StoredField("url", fish.source.url))
    add("scientific_name", record.scientific_name, TextField)
    result.add(StringField("scientific_exact", record.species_key, Field.Store.NO))
    add("common_name", record.common_name, TextField)
    add(
        "fishbase_text",
        "\n".join(canonical_text(s.text) for s in record.sections),
        TextField,
    )
    add("family", record.taxonomy.family, StringField)
    add("order", record.taxonomy.order, StringField)
    add("iucn", record.iucn_code, StringField)
    for value in record.environments:
        add("environment", value.value, StringField)
    add("linked", str(wiki is not None).lower(), StringField)
    if wiki:
        add("wiki_text", wiki.description, TextField)
        add("wiki_categories", "\n".join(wiki.categories), TextField)
        add("wiki_authority", wiki.authority, TextField)
        add("wiki_status", wiki.status, StringField)
        add("wiki_page_id", wiki.page_id, StringField)
        for name, value in (
            ("wiki_url", wiki.url),
            ("wiki_revision_id", wiki.revision_id),
            ("wiki_status_system", wiki.status_system),
            ("wiki_wikitext_sha256", wiki.wikitext_sha256),
        ):
            if value is not None:
                result.add(StoredField(name, str(value)))
    return result


def build(fishbase: Path, linked: Path, output: Path, *, rebuild=False):
    fishbase, linked, output = fishbase.resolve(), linked.resolve(), output.resolve()
    for source in (fishbase, linked):
        separate_roots(source, output)
    started = time.monotonic()
    with (
        read_lock(fishbase / ".lock"),
        read_lock(linked / ".lock"),
        artifact_lock(output),
    ):
        fish_report, join_report, inputs, signature = input_artifacts(fishbase, linked)
        existing = {p.name for p in output.iterdir()} - {".lock"}
        previous = None
        if existing:
            if existing - {"manifest.json", "index"} or "manifest.json" not in existing:
                raise VinfError(
                    "Output contains files not owned by the Lucene indexer."
                )
            previous = read_model(output / "manifest.json", Manifest)
            if previous.producer != "vinf-lucene/1":
                raise VinfError("Output belongs to a different producer.")
            if previous.input_fingerprint == signature and not rebuild:
                if not (output / "index").is_dir():
                    raise VinfError("Index directory is missing. Rebuild the index.")
                return BuildResult(
                    output=str(output),
                    documents=previous.documents,
                    linked_documents=previous.linked_documents,
                    index_bytes=previous.index_bytes,
                    reused=True,
                    elapsed_seconds=round(time.monotonic() - started, 3),
                )
            if not rebuild:
                raise VinfError(
                    "Index inputs changed. Pass --rebuild to regenerate it."
                )
        lucene = initialize()
        from java.lang import System
        from java.nio.file import Paths
        from org.apache.lucene.index import (
            DirectoryReader,
            IndexWriter,
            IndexWriterConfig,
        )
        from org.apache.lucene.search.similarities import BM25Similarity
        from org.apache.lucene.store import FSDirectory
        from org.apache.lucene.util import Version

        with tempfile.TemporaryDirectory(prefix=".building-", dir=output) as temporary:
            work = Path(temporary)
            directory = FSDirectory.open(Paths.get(str(work / "index")))
            analysis = analyzer()
            writer = None
            try:
                config = IndexWriterConfig(analysis)
                config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                config.setSimilarity(BM25Similarity(1.2, 0.75))
                config.setRAMBufferSizeMB(64.0)
                writer = IndexWriter(directory, config)
                total = enriched = 0
                for fish, wiki in records(fishbase, linked):
                    writer.addDocument(document(fish, wiki))
                    total += 1
                    enriched += int(wiki is not None)
                if (
                    total != fish_report.counts["unique_species"]
                    or enriched != join_report.rows
                ):
                    raise VinfError(
                        "Indexed document counts differ from the Spark reports."
                    )
                writer.commit()
                writer.close()
                writer = None
                reader = DirectoryReader.open(directory)
                try:
                    if reader.numDocs() != total:
                        raise VinfError(
                            "Lucene reader count differs from the input count."
                        )
                finally:
                    reader.close()
            finally:
                if writer is not None:
                    writer.rollback()
                analysis.close()
                directory.close()
            size = sum(
                p.stat().st_size for p in (work / "index").iterdir() if p.is_file()
            )
            value = Manifest(
                created_at=datetime.now(UTC),
                pylucene_version=lucene.VERSION,
                lucene_version=str(Version.LATEST),
                java_version=System.getProperty("java.version"),
                documents=total,
                linked_documents=enriched,
                input_fingerprint=signature,
                inputs=inputs,
                fields=FIELDS,
                index_bytes=size,
                elapsed_seconds=round(time.monotonic() - started, 3),
            )
            atomic_model(work / "manifest.json", value)
            for name in existing:
                path = output / name
                shutil.rmtree(path) if path.is_dir() else path.unlink()
            (work / "index").replace(output / "index")
            (work / "manifest.json").replace(output / "manifest.json")
    return BuildResult(
        output=str(output),
        documents=value.documents,
        linked_documents=value.linked_documents,
        index_bytes=size,
        reused=False,
        elapsed_seconds=value.elapsed_seconds,
    )
