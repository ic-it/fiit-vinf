"""Package a small, explicitly labeled subset of actual source packets/XML pages."""

import bz2
import json
import shutil
from pathlib import Path

from validate_spark import XMLScan, json_rows, xml_pages
from vinf_core.artifacts import fingerprint, write_models
from vinf_core.content import packet_json_path
from vinf_core.files import atomic_model, file_hash, read_model
from vinf_core.models import CatalogManifest, DocumentMetadata
from vinf_spark.models import FishBaseSpecies, StageReport

from vinf.project import Project, ProjectConfig, ProjectSource

ROOT = Path(__file__).resolve().parents[1]


def main():
    output = ROOT / "dist/spark-demo"
    output.mkdir(parents=True, exist_ok=False)
    report = read_model(ROOT / "data/linked/manifest.json", StageReport)
    wanted = {example.source_id for example in report.examples[:3]}
    extra_wiki = next(
        row
        for row in json_rows(ROOT / "data/wikipedia/parsed/pages")
        if not row.get("species_key")
    )
    wiki_ids = {example.wiki_page_id for example in report.examples[:3]} | {
        extra_wiki["page_id"]
    }
    unmatched = next(json_rows(ROOT / "data/linked/unmatched"))["source_id"]
    wanted.add(unmatched)
    documents = []
    raw = ROOT / "data/fishbase/crawled"
    for row in json_rows(ROOT / "data/spark/fishbase/species"):
        species = FishBaseSpecies.model_validate(row)
        if species.source_id not in wanted:
            continue
        packet = raw / Path(species.source.raw_path).parent
        shutil.copytree(packet, output / "crawled" / packet.relative_to(raw))
        documents.append(
            read_model(packet_json_path(packet, "metadata.json"), DocumentMetadata)
        )
    if len(documents) != 4:
        raise ValueError("The demo must contain four real FishBase packets.")
    catalog = output / "crawled/catalog/documents.jsonl"
    write_models(catalog, sorted(documents, key=lambda item: str(item.doc_id)))
    atomic_model(
        catalog.parent / "manifest.json",
        CatalogManifest(
            producer="vinf-spark-demo/subset",
            record_count=len(documents),
            catalog_sha256=file_hash(catalog),
            input_fingerprint=fingerprint(
                "read-only subset", *(str(item.doc_id) for item in documents)
            ),
            journal_sequence=0,
        ),
    )
    (output / "crawled/crawl").mkdir()
    (output / "crawled/crawl/lock").touch()
    Project(
        output,
        ProjectConfig(
            name="fishbase-wikipedia-offline-demo",
            sources=(
                ProjectSource(
                    name="fishbase", crawled=Path("crawled"), parsed=Path("parsed")
                ),
            ),
        ),
    ).save()
    target = output / "data/wikipedia/selected/demo.xml.bz2"
    target.parent.mkdir(parents=True)
    import re

    found = set()
    with bz2.open(target, "wb") as stream:
        stream.write(b"<mediawiki>\n")
        wiki_manifest = read_model(
            ROOT / "data/wikipedia/parsed/manifest.json", StageReport
        )
        for path in sorted(map(Path, wiki_manifest.inputs)):
            for xml in xml_pages(path, XMLScan()):
                identifier = int(re.search(r"<id>(\d+)</id>", xml)[1])
                if identifier in wiki_ids:
                    stream.write(xml.encode("utf-8"))
                    found.add(identifier)
                    if found == wiki_ids:
                        break
            if found == wiki_ids:
                break
        stream.write(b"</mediawiki>\n")
    if found != wiki_ids:
        raise ValueError("A selected original Wikipedia page was not found.")
    (output / "expected.json").write_text(
        json.dumps(
            {
                "fishbase_pages": 4,
                "fishbase_species": 4,
                "wikipedia_pages": 4,
                "linked_species": 3,
                "unique_wikipedia_pages": 3,
                "unmatched_species": 1,
                "linked_scientific_names": [
                    example.scientific_name for example in report.examples[:3]
                ],
                "wiki_page_ids": sorted(wiki_ids),
            },
            indent=2,
        )
        + "\n"
    )
    (output / "README.txt").write_text(
        "Offline Spark mini-demo: a read-only subset, not the full corpus.\n"
        "Four original FishBase packets and four original Wikipedia XML page records.\n"
        "The XML pages come from the 20261101 official dump; parent-file URLs and hashes\n"
        "are in the accompanying report/download manifests. Wikipedia contributors\n"
        "retain CC BY-SA attribution and revision provenance inside each page.\n"
        "The crawler catalog is a subset and has no resumable frontier.\n\n"
        "Copy this directory beside the source archive's pyproject.toml as demo.\n"
        "From the source directory, run each independent stage:\n\n"
        "uv sync --locked --extra spark\n"
        "uv run --extra spark vinf --project demo spark fishbase\n"
        "uv run --extra spark vinf --project demo spark wiki\n"
        "uv run --extra spark vinf --project demo spark join\n"
        "uv run --extra spark vinf --project demo spark status\n"
        "uv run --extra spark vinf --project demo spark examples --limit 3\n\n"
        "Expected results are in expected.json. No HTTP is needed after dependency\n"
        "installation. Add --overwrite to regenerate an existing Spark stage.\n"
    )
    print(output)


if __name__ == "__main__":
    main()
