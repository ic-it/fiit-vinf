"""Independent PyLucene commands, sharing the current project's saved paths."""

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table
from vinf_core.files import atomic_model, read_model
from vinf_core.models import VinfError
from vinf_lucene.models import Manifest, Scope

from vinf.cli.common import (
    active_project,
    configure_project,
    console,
    display_model,
    guarded,
    index_path,
)
from vinf.project import LucenePaths

app = typer.Typer(
    no_args_is_help=True,
    help="Build, search and compare the independent PyLucene index.",
)


@app.callback()
def main(
    ctx: typer.Context,
    project: Annotated[Path | None, typer.Option("--project")] = None,
):
    if project is not None:
        configure_project(ctx, project)


def _path(name: str):
    project = active_project()
    return (
        (project.root / getattr(project.config.lucene, name))
        if project
        else Path.cwd() / getattr(LucenePaths(), name)
    )


@app.command("build")
@guarded
def build(
    fishbase: Annotated[Path | None, typer.Option()] = None,
    linked: Annotated[Path | None, typer.Option()] = None,
    output: Annotated[Path | None, typer.Option()] = None,
    rebuild: Annotated[bool, typer.Option()] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Index all FishBase species and enrich only accepted Wikipedia links."""
    from vinf_lucene.indexing import build as build_index

    value = build_index(
        fishbase or _path("fishbase"),
        linked or _path("linked"),
        output or _path("indexed"),
        rebuild=rebuild,
    )
    display_model(value, json_output=json_output)


@app.command("status")
@guarded
def status(
    index: Annotated[Path | None, typer.Option()] = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Read the saved manifest without starting Java or scanning the index."""
    value = read_model((index or _path("indexed")) / "manifest.json", Manifest)
    if json_output:
        typer.echo(value.model_dump_json())
        return
    table = Table("Metric", "Value", title="Saved PyLucene index")
    for key in (
        "documents",
        "linked_documents",
        "index_bytes",
        "pylucene_version",
        "lucene_version",
        "java_version",
        "analyzer",
        "similarity",
        "elapsed_seconds",
    ):
        table.add_row(key.replace("_", " "), str(getattr(value, key)))
    console.print(table)


@app.command("schema")
def schema(json_output: Annotated[bool, typer.Option("--json")] = False):
    """Explain every indexed and stored field without starting Java."""
    from vinf_lucene.schema import FIELDS

    if json_output:
        typer.echo(json.dumps([field.model_dump() for field in FIELDS]))
        return
    table = Table("Field", "Type", "Stored", "Purpose")
    for field in FIELDS:
        table.add_row(field.name, field.kind, str(field.stored), field.reason)
    console.print(table)


@app.command("query")
@guarded
def query(
    text: Annotated[
        str,
        typer.Argument(
            help='Lucene syntax: marine (Pacific OR Atlantic), "coral reefs", Aaptosyaz~1, family:Cyprinidae.'
        ),
    ],
    index: Annotated[Path | None, typer.Option()] = None,
    scope: Annotated[
        Scope, typer.Option(help="Default text fields: all, fishbase, or wikipedia.")
    ] = Scope.ALL,
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 10,
    explain: Annotated[bool, typer.Option()] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Search using native Lucene query parsing, positional phrases and BM25."""
    from vinf_lucene.search import search

    result = search(
        index or _path("indexed"), text, scope=scope, limit=limit, explain=explain
    )
    if json_output:
        typer.echo(result.model_dump_json())
        return
    console.print(
        f"{result.total_matches:,} matches; {result.documents:,} documents; {result.elapsed_ms:.3f} ms",
        markup=False,
    )
    for hit in result.hits:
        console.print(
            f"{hit.rank}. {hit.scientific_name}"
            + (f" — {hit.common_name}" if hit.common_name else ""),
            style="bold",
            markup=False,
        )
        console.print(
            f"BM25 {hit.score:.5f} | FishBase {hit.source_id}: {hit.url}", markup=False
        )
        if hit.wiki_url:
            console.print(f"Wikipedia: {hit.wiki_url}", markup=False)
        console.print(hit.snippet, markup=False)
        if hit.explanation:
            console.print(hit.explanation, markup=False)
        console.print()
    if explain:
        console.print(f"Parsed query: {result.parsed_query}", markup=False)


@app.command("compare")
@guarded
def compare(
    queries: Annotated[Path | None, typer.Option()] = None,
    old_index: Annotated[Path | None, typer.Option()] = None,
    index: Annotated[Path | None, typer.Option()] = None,
    output: Annotated[Path | None, typer.Option()] = None,
    top_k: Annotated[int, typer.Option(min=1, max=100)] = 5,
    repetitions: Annotated[int, typer.Option(min=1, max=20)] = 5,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Compare the old BM25, Lucene FishBase-only and enriched Lucene on identical queries."""
    from vinf_lucene.comparison import compare as run_comparison
    from vinf_search.models import QuerySet

    project = active_project()
    if queries is None:
        if project is None or project.config.queries is None:
            raise VinfError("Set the project query file or pass --queries.")
        queries = project.resolve(project.config.queries)
    value = run_comparison(
        index_path(old_index),
        index or _path("indexed"),
        read_model(queries, QuerySet),
        top_k=top_k,
        repetitions=repetitions,
    )
    target = (
        output
        or (project.root if project else Path.cwd())
        / "reports/consultation4/comparison.json"
    )
    atomic_model(target, value)
    if json_output:
        typer.echo(value.model_dump_json())
        return
    for case in value.cases:
        console.print(case.question, style="bold", markup=False)
        console.print(case.query, markup=False)
        table = Table("Engine", "Matches", "Median ms", f"Top {top_k}")
        for run in case.runs:
            table.add_row(
                run.engine,
                str(run.total_matches),
                f"{run.median_ms:.3f}",
                "; ".join(run.top_names),
            )
        console.print(table)
    console.print(value.note, markup=False)
    console.print(str(target), markup=False)


@app.command("report")
@guarded
def report(output: Annotated[Path | None, typer.Option()] = None):
    """Render the consultation page from current audited measurements."""
    from vinf.lucene_report import build_report

    project = active_project()
    root = project.root if project else Path.cwd()
    console.print(
        str(
            build_report(
                root,
                output or root / "wiki/konzultacia4.dokuwiki.txt",
                _path("indexed"),
            )
        ),
        markup=False,
    )
