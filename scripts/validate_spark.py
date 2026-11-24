"""Independently audit published Spark outputs against saved source artifacts."""

import bz2
import re
from dataclasses import dataclass
from pathlib import Path

from vinf_core.artifacts import parsed_items
from vinf_core.files import atomic_model, file_hash, read_model
from vinf_core.models import Model
from vinf_core.records import ParsedPage
from vinf_spark.models import FishBaseSpecies, StageReport, WikiPage
from vinf_spark.selection import RangeDownload, Selection, json_rows

ROOT = Path(__file__).resolve().parents[1]


@dataclass
class XMLScan:
    uncompressed_bytes: int = 0


def xml_pages(path, scan):
    """Independent sequential framing, not the Spark/Hadoop record reader."""
    with bz2.open(path, "rb") as stream:
        page = []
        inside = False
        for line in stream:
            scan.uncompressed_bytes += len(line)
            if b"<page>" in line:
                inside = True
                page = []
            if inside:
                page.append(line)
            if inside and b"</page>" in line:
                yield b"".join(page).decode("utf-8")
                inside = False
        if inside:
            raise ValueError(f"Incomplete XML page in {path}")


class Audit(Model):
    original_species: int
    spark_species: int
    identical_fishbase_records: int
    xml_pages: int
    xml_original_bytes: int
    xml_bzip2_bytes: int
    xml_main_namespace_pages: int
    spark_wikipedia_pages: int
    linked_records: int
    unique_linked_wikipedia_pages: int
    unmatched_records: int
    fishbase_manifest_sha256: str
    wikipedia_manifest_sha256: str
    join_manifest_sha256: str
    selected_index_titles: int
    errors: tuple[str, ...] = ()


def main():
    parsed = ROOT / "data/fishbase/parsed"
    chosen = {}
    for entry, page in parsed_items(parsed):
        if page.record is None:
            continue
        identity = page.record.identifier("fishbase") or str(page.source.doc_id)
        rank = (-page.source.fetched_at.timestamp(), str(page.source.doc_id))
        if identity not in chosen or rank < chosen[identity][0]:
            chosen[identity] = (rank, parsed / entry.path)
    spark_ids = set()
    for row in json_rows(ROOT / "data/spark/fishbase/species"):
        species = FishBaseSpecies.model_validate(row)
        if species.source_id in spark_ids:
            raise ValueError(f"Duplicate Spark species: {species.source_id}")
        reference = read_model(chosen[species.source_id][1], ParsedPage)
        if reference.record != species.record or reference.source != species.source:
            raise ValueError(
                f"Sequential/Spark extraction differs: {species.source_id}"
            )
        spark_ids.add(species.source_id)
    if spark_ids != chosen.keys():
        raise ValueError("Sequential/Spark species identities differ.")

    wiki_ids = set()
    for row in json_rows(ROOT / "data/wikipedia/parsed/pages"):
        page = WikiPage.model_validate(row)
        if page.page_id in wiki_ids:
            raise ValueError(f"Duplicate Wikipedia page: {page.page_id}")
        wiki_ids.add(page.page_id)
    original_ids = set()
    xml_count = 0
    scan = XMLScan()
    manifest = read_model(ROOT / "data/wikipedia/parsed/manifest.json", StageReport)
    inputs = tuple(map(Path, manifest.inputs))
    selection_path = ROOT / "data/wikipedia/selection.json"
    selection = read_model(selection_path, Selection)
    expected_inputs = {
        str((ROOT / "data/wikipedia/selected" / block.filename).resolve())
        for block in selection.blocks
    }
    if set(manifest.inputs) != expected_inputs:
        raise ValueError("Wikipedia output does not match the current selection plan.")
    blocks_by_path = {
        (ROOT / "data/wikipedia/selected" / block.filename).resolve(): block
        for block in selection.blocks
    }
    if len(manifest.input_sha256) != len(inputs):
        raise ValueError("Wikipedia input checksums are incomplete.")
    for path, expected_hash in zip(inputs, manifest.input_sha256, strict=True):
        metadata = read_model(
            path.with_name(path.name + ".metadata.json"), RangeDownload
        )
        block = blocks_by_path[path]
        if (
            metadata.url != selection.dump_url
            or (metadata.start, metadata.end) != (block.start, block.end)
            or metadata.dump_bytes != selection.dump_bytes
            or metadata.bytes != path.stat().st_size
            or metadata.sha256 != file_hash(path)
            or metadata.sha256 != expected_hash
        ):
            raise ValueError(f"Range provenance/checksum mismatch: {path}")
        for xml in xml_pages(path, scan):
            xml_count += 1
            if re.search(r"<ns>0</ns>", xml):
                identifier = int(re.search(r"<id>(\d+)</id>", xml)[1])
                if identifier in original_ids:
                    raise ValueError(
                        f"Overlapping source ranges: duplicate page {identifier}"
                    )
                original_ids.add(identifier)
    if wiki_ids != original_ids:
        raise ValueError(
            f"Spark XML framing differs: missing {len(original_ids - wiki_ids)}, unexpected {len(wiki_ids - original_ids)}"
        )
    if not {hit.page_id for hit in selection.hits}.issubset(original_ids):
        raise ValueError(
            "A selected title from the global index is absent from the saved XML."
        )

    linked_fish, linked_wiki = set(), set()
    for row in json_rows(ROOT / "data/linked/records"):
        fish = FishBaseSpecies.model_validate(row["fishbase"])
        wiki = WikiPage.model_validate(
            {k: v for k, v in row["wikipedia"].items() if k != "candidate_count"}
        )
        if row["decision"] != "linked" or fish.record.species_key != wiki.species_key:
            raise ValueError("Invalid scientific-name link.")
        if fish.source_id in linked_fish:
            raise ValueError(
                "More than one accepted Wikipedia link for a FishBase record."
            )
        if wiki.redirect or wiki.disambiguation or row["family_check"] == "conflict":
            raise ValueError("Unsafe candidate was accepted.")
        linked_fish.add(fish.source_id)
        linked_wiki.add(wiki.page_id)
    unmatched = {row["source_id"] for row in json_rows(ROOT / "data/linked/unmatched")}
    if linked_fish & unmatched or linked_fish | unmatched != spark_ids:
        raise ValueError(
            "Linked/unmatched records do not partition the FishBase corpus."
        )
    audit = Audit(
        original_species=len(chosen),
        spark_species=len(spark_ids),
        identical_fishbase_records=len(spark_ids),
        xml_pages=xml_count,
        xml_original_bytes=scan.uncompressed_bytes,
        xml_bzip2_bytes=sum(path.stat().st_size for path in inputs),
        xml_main_namespace_pages=len(original_ids),
        spark_wikipedia_pages=len(wiki_ids),
        linked_records=len(linked_fish),
        unique_linked_wikipedia_pages=len(linked_wiki),
        unmatched_records=len(unmatched),
        fishbase_manifest_sha256=file_hash(ROOT / "data/spark/fishbase/manifest.json"),
        wikipedia_manifest_sha256=file_hash(
            ROOT / "data/wikipedia/parsed/manifest.json"
        ),
        join_manifest_sha256=file_hash(ROOT / "data/linked/manifest.json"),
        selected_index_titles=len(selection.hits),
    )
    atomic_model(ROOT / "reports/consultation3/audit.json", audit)
    print(audit.model_dump_json())


if __name__ == "__main__":
    main()
