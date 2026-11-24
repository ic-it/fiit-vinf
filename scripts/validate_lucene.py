"""Check every stored document against source snapshots and Lucene CheckIndex."""

from datetime import UTC, datetime
from pathlib import Path

from vinf_core.files import atomic_model, file_hash, read_lock, read_model
from vinf_core.index_io import IndexReader
from vinf_core.indexes import IndexManifest
from vinf_core.text import canonical_text
from vinf_lucene.inputs import input_artifacts
from vinf_lucene.models import IndexAudit, Manifest
from vinf_lucene.runtime import initialize
from vinf_spark.models import FishBaseSpecies
from vinf_spark.selection import json_rows

ROOT = Path(__file__).resolve().parents[1]


def main():
    initialize()
    from java.nio.file import Paths
    from org.apache.lucene.index import CheckIndex, DirectoryReader
    from org.apache.lucene.store import FSDirectory

    base = ROOT / "data/lucene/indexed"
    fishbase, linked = ROOT / "data/spark/fishbase", ROOT / "data/linked"
    with (
        read_lock(base / ".lock"),
        read_lock(fishbase / ".lock"),
        read_lock(linked / ".lock"),
    ):
        manifest = read_model(base / "manifest.json", Manifest)
        fish_report, join_report, _, signature = input_artifacts(fishbase, linked)
        assert signature == manifest.input_fingerprint, "Stale input fingerprint"
        directory = FSDirectory.open(Paths.get(str(base / "index")))
        reader = DirectoryReader.open(directory)
        try:
            stored, identities, wiki_ids = reader.storedFields(), {}, set()
            for i in range(reader.maxDoc()):
                doc = stored.document(i)
                key = doc.get("identity")
                assert key not in identities, f"Duplicate {key}"
                identities[key] = i
                if value := doc.get("wiki_page_id"):
                    wiki_ids.add(value)
            source_keys = set()
            for row in json_rows(fishbase / "species"):
                fish = FishBaseSpecies.model_validate(row)
                key = f"{fish.record.source_name}:{fish.source_id}"
                source_keys.add(key)
                doc = stored.document(identities[key])
                for name, expected in (
                    ("source", fish.record.source_name),
                    ("source_id", fish.source_id),
                    ("doc_id", str(fish.source.doc_id)),
                    ("url", fish.source.url),
                    ("scientific_name", fish.record.scientific_name),
                    ("common_name", fish.record.common_name),
                    (
                        "fishbase_text",
                        "\n".join(canonical_text(s.text) for s in fish.record.sections),
                    ),
                    ("family", fish.record.taxonomy.family),
                    ("order", fish.record.taxonomy.order),
                    ("iucn", fish.record.iucn_code),
                ):
                    assert doc.get(name) == (expected or None), (key, name)
                assert set(doc.getValues("environment")) == {
                    v.value for v in fish.record.environments
                }, key
            assert source_keys == identities.keys(), "Index/source identities differ"
            enriched = set()
            for row in json_rows(linked / "records"):
                fish, wiki = row["fishbase"], row["wikipedia"]
                key = f"{fish['record']['source_name']}:{fish['source_id']}"
                assert key not in enriched and row["decision"] == "linked", key
                enriched.add(key)
                doc = stored.document(identities[key])
                for name, expected in (
                    ("linked", "true"),
                    ("wiki_url", wiki["url"]),
                    ("wiki_page_id", str(wiki["page_id"])),
                    (
                        "wiki_revision_id",
                        str(wiki["revision_id"])
                        if wiki["revision_id"] is not None
                        else None,
                    ),
                    ("wiki_text", wiki["description"]),
                    ("wiki_categories", "\n".join(wiki["categories"])),
                    ("wiki_authority", wiki["authority"]),
                    ("wiki_status", wiki["status"]),
                    ("wiki_status_system", wiki["status_system"]),
                    ("wiki_wikitext_sha256", wiki["wikitext_sha256"]),
                ):
                    assert doc.get(name) == (expected or None), (key, name)
            for key in source_keys - enriched:
                doc = stored.document(identities[key])
                assert doc.get("linked") == "false" and doc.get("wiki_url") is None, key
            assert (
                reader.numDocs()
                == manifest.documents
                == fish_report.counts["unique_species"]
            )
            assert len(enriched) == manifest.linked_documents == join_report.rows
        finally:
            reader.close()
        checker = CheckIndex(directory)
        try:
            clean = checker.checkIndex().clean
            assert clean, "Lucene CheckIndex found corruption"
        finally:
            checker.close()
            directory.close()
    old_manifest = read_model(
        ROOT / "data/fishbase/indexed/manifest.json", IndexManifest
    )
    with IndexReader(ROOT / "data/fishbase/indexed") as old:
        old_keys = {f"{doc.source_name}:{doc.source_id}" for doc in old.documents()}
    assert old_keys == source_keys, "Old/new source-qualified identities differ"
    old_bytes = sum(p.size_bytes for p in old_manifest.files)
    measured = sum(p.stat().st_size for p in (base / "index").iterdir() if p.is_file())
    assert measured == manifest.index_bytes, "Index payload size changed"
    value = IndexAudit(
        created_at=datetime.now(UTC),
        documents=len(identities),
        identical_fishbase_records=len(source_keys),
        identical_wikipedia_records=len(enriched),
        unique_wikipedia_pages=len(wiki_ids),
        old_same_identities=True,
        lucene_check_index_clean=clean,
        input_fingerprint=signature,
        lucene_manifest_sha256=file_hash(base / "manifest.json"),
        old_manifest_sha256=file_hash(ROOT / "data/fishbase/indexed/manifest.json"),
        old_index_payload_bytes=old_bytes,
        lucene_index_payload_bytes=measured,
    )
    atomic_model(ROOT / "reports/consultation4/audit.json", value)
    print(value.model_dump_json())


if __name__ == "__main__":
    main()
