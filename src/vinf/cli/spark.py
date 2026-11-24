"""Independent Spark commands; no command invokes the following stage."""

import json
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

import typer
from rich.table import Table
from vinf_core.files import atomic_model, file_hash, read_model
from vinf_core.models import VinfError
from vinf_spark.models import SparkSettings, StageReport

from vinf.cli.common import (
    active_project,
    configure_project,
    console,
    display_model,
    errors,
    guarded,
    source_path,
)

app = typer.Typer(
    no_args_is_help=True, help="Spark REGEX extraction and Wikipedia linking."
)


@app.callback()
def main(
    ctx: typer.Context,
    project: Annotated[Path | None, typer.Option("--project")] = None,
):
    if project is not None:
        configure_project(ctx, project)


def _root():
    project = active_project()
    return project.root if project else Path.cwd()


def _settings(master: str, partitions: int, compression: str):
    return SparkSettings(master=master, partitions=partitions, compression=compression)


@app.command("download")
@guarded
def download(
    url: str,
    output: Annotated[
        Path | None,
        typer.Option(help="Raw dump path; defaults to data/wikipedia/raw/<filename>."),
    ] = None,
    sha1: Annotated[
        str | None, typer.Option(help="Optional publisher SHA-1 checksum.")
    ] = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Download one explicitly chosen HTTPS dump; record its source and checksums."""
    from vinf_spark.downloads import download_dump

    target = output or _root() / "data/wikipedia/raw" / Path(urlsplit(url).path).name
    display_model(download_dump(url, target, sha1), json_output=json_output)


@app.command("plan")
@guarded
def plan(
    index: Annotated[Path | None, typer.Option()] = None,
    output: Annotated[Path | None, typer.Option()] = None,
    follow_redirects: Annotated[bool, typer.Option()] = True,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Locate corpus scientific/common names throughout the complete dump index."""
    from vinf_spark.models import Download
    from vinf_spark.selection import make_selection

    root = _root()
    indexes = sorted(
        (root / "data/wikipedia/index").glob("*-multistream-index.txt.bz2")
    )
    if index is None and len(indexes) != 1:
        raise VinfError("Download one complete multistream index or select --index.")
    index = index or indexes[0]
    metadata = read_model(index.with_name(index.name + ".metadata.json"), Download)
    dump_url = metadata.url.replace(
        "-multistream-index.txt.bz2", "-multistream.xml.bz2"
    )
    publisher = json.loads((index.parent / "dumpstatus.json").read_text())
    info = next(
        (
            info
            for job in publisher["jobs"].values()
            for name, info in job.get("files", {}).items()
            if name == Path(urlsplit(dump_url).path).name
        ),
        None,
    )
    if info is None:
        raise VinfError(
            "Dated publisher manifest does not list the corresponding XML dump."
        )
    target = output or root / "data/wikipedia/selection.json"
    value = make_selection(
        index,
        root / "data/spark/fishbase",
        dump_url,
        info["size"],
        target,
        parsed=root / "data/wikipedia/parsed" if follow_redirects else None,
    )
    summary = {
        "plan": str(target),
        "scientific_names": value.scientific_names,
        "scientific_titles_found": value.scientific_titles_found,
        "common_titles_found": value.common_titles_found,
        "blocks": len(value.blocks),
        "selected_bytes": value.selected_bytes,
        "unfound_scientific_titles": len(value.missing_scientific_names),
    }
    typer.echo(json.dumps(summary, indent=None if json_output else 2))


@app.command("fetch")
@guarded
def fetch(
    plan: Annotated[Path | None, typer.Option()] = None,
    output: Annotated[Path | None, typer.Option()] = None,
    workers: Annotated[int, typer.Option(min=1, max=4)] = 2,
    interval: Annotated[float, typer.Option(min=0.1)] = 0.25,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Fetch original bzip2 blocks by exact HTTP ranges; reuse verified blocks."""
    from vinf_spark.selection import Selection, fetch_selection

    root = _root()
    selected = read_model(plan or root / "data/wikipedia/selection.json", Selection)
    target = output or root / "data/wikipedia/selected"
    result = fetch_selection(
        selected,
        target,
        workers=workers,
        interval=interval,
        progress=lambda done, total, failed: (errors if json_output else console).print(
            f"Blocks {done}/{total}; failed {failed}", markup=False
        ),
    )
    atomic_model(root / "data/wikipedia/fetch.json", result)
    display_model(result, json_output=json_output)
    if result.failures:
        raise typer.Exit(1)


@app.command("fishbase")
@guarded
def fishbase(
    input_path: Annotated[Path | None, typer.Option("--input")] = None,
    output: Annotated[Path | None, typer.Option()] = None,
    source: Annotated[str | None, typer.Option()] = None,
    master: Annotated[str, typer.Option()] = "local[2]",
    partitions: Annotated[int, typer.Option(min=1, max=1024)] = 4,
    compression: Annotated[Literal["gzip", "none"], typer.Option()] = "gzip",
    overwrite: Annotated[bool, typer.Option()] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Extract saved raw HTML on Spark workers, then deduplicate by FishBase ID."""
    from vinf_spark.engine import parse_fishbase

    value = parse_fishbase(
        source_path(input_path, "crawled", source=source),
        output or _root() / "data/spark/fishbase",
        _settings(master, partitions, compression),
        overwrite=overwrite,
    )
    display_model(value, json_output=json_output)


@app.command("wiki")
@guarded
def wiki(
    inputs: Annotated[
        list[Path] | None,
        typer.Option("--input", help="Repeat for multiple .xml/.bz2/.gz dump files."),
    ] = None,
    output: Annotated[Path | None, typer.Option()] = None,
    master: Annotated[str, typer.Option()] = "local[2]",
    partitions: Annotated[int, typer.Option(min=1, max=1024)] = 4,
    compression: Annotated[Literal["gzip", "none"], typer.Option()] = "gzip",
    overwrite: Annotated[bool, typer.Option()] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Read XML page records with Spark; REGEX-extract names, lead text and facts."""
    from vinf_spark.engine import parse_wikipedia

    selected_root = _root() / "data/wikipedia/selected"
    selection = _root() / "data/wikipedia/selection.json"
    if inputs is None and selection.exists():
        from vinf_spark.selection import Selection

        selected = read_model(selection, Selection)
        paths = tuple(selected_root / block.filename for block in selected.blocks)
        if any(not path.exists() for path in paths):
            raise VinfError(
                "Selected blocks are missing. Complete 'vinf spark fetch' first."
            )
    else:
        paths = tuple(inputs or sorted(selected_root.glob("*.xml*.bz2")))
    if not paths:
        raise VinfError(
            "No Wikipedia input. Run 'vinf spark plan' and 'vinf spark fetch', or pass --input."
        )
    display_model(
        parse_wikipedia(
            paths,
            output or _root() / "data/wikipedia/parsed",
            _settings(master, partitions, compression),
            overwrite=overwrite,
        ),
        json_output=json_output,
    )


@app.command("join")
@guarded
def join(
    fishbase: Annotated[Path | None, typer.Option()] = None,
    wikipedia: Annotated[Path | None, typer.Option()] = None,
    output: Annotated[Path | None, typer.Option()] = None,
    master: Annotated[str, typer.Option()] = "local[2]",
    partitions: Annotated[int, typer.Option(min=1, max=1024)] = 4,
    compression: Annotated[Literal["gzip", "none"], typer.Option()] = "gzip",
    overwrite: Annotated[bool, typer.Option()] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Link unique scientific names; retain conflicts and unmatched records."""
    from vinf_spark.engine import link_species

    root = _root()
    display_model(
        link_species(
            fishbase or root / "data/spark/fishbase",
            wikipedia or root / "data/wikipedia/parsed",
            output or root / "data/linked",
            _settings(master, partitions, compression),
            overwrite=overwrite,
        ),
        json_output=json_output,
    )


@app.command("status")
@guarded
def status(json_output: Annotated[bool, typer.Option("--json")] = False):
    """Read published Spark manifests without starting Java or scanning datasets."""
    table = Table(title="Saved Spark stages")
    table.add_column("Stage")
    table.add_column("Rows", justify="right")
    table.add_column("Counts", overflow="fold")
    table.add_column("Seconds", justify="right")
    for path in ("data/spark/fishbase", "data/wikipedia/parsed", "data/linked"):
        manifest = _root() / path / "manifest.json"
        if manifest.exists():
            report = read_model(manifest, StageReport)
            if json_output:
                typer.echo(report.model_dump_json())
            else:
                table.add_row(
                    report.stage,
                    f"{report.rows:,}",
                    "\n".join(
                        f"{key.replace('_', ' ')}: {value:,}"
                        for key, value in report.counts.items()
                    ),
                    f"{report.elapsed_seconds:.3f}",
                )
    if not json_output:
        console.print(table)


@app.command("examples")
@guarded
def examples(
    limit: Annotated[int, typer.Option(min=1, max=8)] = 5,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Show saved real join examples without starting Spark."""
    report = read_model(_root() / "data/linked/manifest.json", StageReport)
    for number, example in enumerate(report.examples[:limit], 1):
        if json_output:
            typer.echo(example.model_dump_json())
            continue
        console.print(
            f"{number}. {example.scientific_name}", style="bold", markup=False
        )
        console.print(f"FishBase {example.source_id}: {example.url}", markup=False)
        console.print(
            f"Wikipedia {example.wiki_page_id}: {example.wiki_title} — {example.wiki_url}",
            markup=False,
        )
        console.print(
            f"Family check: {example.family_check}; source statuses: {example.fishbase_status or 'unknown'} / {example.wiki_status or 'unknown'}",
            markup=False,
        )
        console.print(
            example.wiki_description or "No lead paragraph extracted.", markup=False
        )
        console.print()


@app.command("report")
@guarded
def report(output: Annotated[Path | None, typer.Option()] = None):
    """Render the consultation wiki source from completed runs and their audit."""
    from vinf.spark_report import build_report

    root = _root()
    path = build_report(root, output or root / "wiki/konzultacia3.dokuwiki.txt")
    console.print(str(path), markup=False)


@app.command("coverage")
@guarded
def coverage(json_output: Annotated[bool, typer.Option("--json")] = False):
    """Explain linked coverage and separate reasons for unmatched species."""
    from vinf_spark.coverage import analyze_coverage
    from vinf_spark.selection import Selection

    root = _root()
    current = read_model(root / "data/wikipedia/selection.json", Selection)
    wiki = read_model(root / "data/wikipedia/parsed/manifest.json", StageReport)
    joined = read_model(root / "data/linked/manifest.json", StageReport)
    paths = {
        str((root / "data/wikipedia/selected" / block.filename).resolve())
        for block in current.blocks
    }
    fishbase_hash = file_hash(root / "data/spark/fishbase/manifest.json")
    if current.fishbase_manifest_sha256 != fishbase_hash:
        raise VinfError("Selection predates FishBase extraction. Run plan first.")
    if set(wiki.inputs) != paths or joined.input_sha256 != (
        fishbase_hash,
        file_hash(root / "data/wikipedia/parsed/manifest.json"),
    ):
        raise VinfError(
            "Saved extraction/join predates current inputs. Run wiki and join first."
        )
    value = analyze_coverage(
        root / "data/spark/fishbase",
        root / "data/wikipedia/parsed",
        root / "data/linked",
        current,
    )
    atomic_model(root / "reports/consultation3/coverage.json", value)
    if json_output:
        typer.echo(value.model_dump_json())
    else:
        console.print(
            f"Linked {value.linked_species:,}/{value.fishbase_species:,} ({value.coverage_percent:.2f}%)"
        )
        table = Table("Unmatched reason", "Species")
        for reason, count in value.unmatched_reasons.items():
            table.add_row(reason.replace("_", " "), str(count))
        console.print(table)
