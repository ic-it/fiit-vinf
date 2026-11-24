"""Stream completed Spark artifacts; keep only compact identity checks in memory."""

from pathlib import Path

from vinf_core.artifacts import fingerprint
from vinf_core.files import file_hash, read_model
from vinf_core.models import VinfError
from vinf_spark.models import FishBaseSpecies, StageReport, WikiPage
from vinf_spark.selection import json_rows

from vinf_lucene.models import InputArtifact


def input_artifacts(fishbase: Path, linked: Path):
    fish = read_model(fishbase / "manifest.json", StageReport)
    join = read_model(linked / "manifest.json", StageReport)
    if fish.stage != "fishbase" or join.stage != "join":
        raise VinfError("Expected completed Spark FishBase and join artifacts.")
    if join.input_sha256[0] != file_hash(fishbase / "manifest.json"):
        raise VinfError("The saved join predates the FishBase extraction.")
    files = [fishbase / "manifest.json", linked / "manifest.json"]
    for directory in (fishbase / "species", linked / "records"):
        files.extend(sorted(directory.glob("part-*.json*")))
    artifacts = tuple(InputArtifact(path=str(p), sha256=file_hash(p)) for p in files)
    return fish, join, artifacts, fingerprint(*(p.sha256 for p in artifacts))


def records(fishbase: Path, linked: Path):
    identities = {}
    for row in json_rows(fishbase / "species"):
        fish = FishBaseSpecies.model_validate(row)
        if fish.source_id in identities:
            raise VinfError(f"Duplicate FishBase identity: {fish.source_id}")
        identities[fish.source_id] = fingerprint(fish.model_dump_json())
    seen = set()
    for row in json_rows(linked / "records"):
        fish = FishBaseSpecies.model_validate(row["fishbase"])
        if (
            row["decision"] != "linked"
            or fish.source_id in seen
            or identities.get(fish.source_id) != fingerprint(fish.model_dump_json())
        ):
            raise VinfError(f"Inconsistent accepted link: {fish.source_id}")
        values = row["wikipedia"]
        wiki = WikiPage.model_validate({k: values[k] for k in WikiPage.model_fields})
        if (
            wiki.redirect
            or wiki.disambiguation
            or wiki.species_key != fish.record.species_key
        ):
            raise VinfError(f"Invalid Wikipedia identity for {fish.source_id}")
        seen.add(fish.source_id)
        yield fish, wiki
    for row in json_rows(fishbase / "species"):
        fish = FishBaseSpecies.model_validate(row)
        if fish.source_id not in seen:
            yield fish, None
