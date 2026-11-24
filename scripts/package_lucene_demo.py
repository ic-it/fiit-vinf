"""Copy a labeled four-record subset of actual extracted data into an offline demo."""

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

from vinf_core.files import atomic_model, file_hash
from vinf_spark.models import StageReport

from vinf.project import LucenePaths, Project, ProjectConfig, ProjectSource

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/lucene-sample.json"


def write_sample(output: Path):
    values = json.loads(FIXTURE.read_text())["pairs"]
    fishbase, linked = output / "data/spark/fishbase", output / "data/linked"
    for directory, name, rows in (
        (fishbase, "species", [v["fishbase"] for v in values]),
        (
            linked,
            "records",
            [dict(v, decision="linked") for v in values if v["wikipedia"]],
        ),
    ):
        (directory / name).mkdir(parents=True, exist_ok=True)
        (directory / ".lock").touch()
        with gzip.open(
            directory / name / "part-sample.json.gz", "wt", encoding="utf-8"
        ) as stream:
            for row in rows:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    fish = StageReport(
        stage="fishbase",
        producer="vinf-lucene-demo/subset",
        created_at=datetime.now(UTC),
        spark_version="3.5.9",
        master="saved subset",
        partitions=1,
        inputs=(str(FIXTURE),),
        input_sha256=(file_hash(FIXTURE),),
        output=str(fishbase),
        rows=4,
        counts={"unique_species": 4},
        elapsed_seconds=0,
    )
    atomic_model(fishbase / "manifest.json", fish)
    join = fish.model_copy(
        update={
            "stage": "join",
            "output": str(linked),
            "rows": 3,
            "inputs": (str(fishbase),),
            "input_sha256": (file_hash(fishbase / "manifest.json"),),
            "counts": {
                "fishbase_species": 4,
                "linked_species": 3,
                "unmatched_species": 1,
            },
        }
    )
    atomic_model(linked / "manifest.json", join)
    Project(
        output,
        ProjectConfig(
            name="lucene-offline-demo",
            sources=(
                ProjectSource(
                    name="fishbase", crawled=Path("crawled"), parsed=Path("parsed")
                ),
            ),
            lucene=LucenePaths(),
        ),
    ).save()
    return fishbase, linked


def main():
    output = ROOT / "dist/lucene-demo"
    output.mkdir(parents=True, exist_ok=False)
    write_sample(output)
    (output / "README.txt").write_text(
        "Offline Lucene demo: four real extracted FishBase records, three accepted Wiki links.\n"
        "This is a copied subset, not a Spark re-execution or full-corpus benchmark.\n"
        "Source provenance is in each record and tests/fixtures/lucene-sample.json.\n\n"
        "Copy this directory into the unpacked source checkout as demo.\n"
        "After uv sync --locked and uv run python scripts/install_pylucene.py:\n\n"
        "uv run vinf --project demo lucene build\n"
        "uv run vinf --project demo lucene status\n"
        "uv run vinf --project demo lucene query 'Aaptosyax AND grypus'\n"
        "uv run vinf --project demo lucene query '\"Aaptosyax grypus\"'\n"
        "uv run vinf --project demo lucene query 'Aaptosyaz~1' --explain\n\n"
        "Expected: four indexed species, three with Wikipedia; each sample query finds Aaptosyax grypus.\n"
        "Indexing/search need no network after native dependencies are installed.\n"
    )
    print(output)


if __name__ == "__main__":
    main()
