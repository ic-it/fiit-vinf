from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from vinf_core.files import atomic_model, read_model
from vinf_core.models import VinfError
from vinf_search import compare as compare_queries
from vinf_search import search
from vinf_search.language import parse_query
from vinf_search.models import (
    QuerySet,
    RankMethod,
)

from vinf.cli.common import active_project, console, guarded, index_path
from vinf.search_output import display_hits
from vinf.search_records import detailed_result

app = typer.Typer(
    no_args_is_help=True,
    help="Search an existing index or compare its ranking methods.",
)


@app.command()
@guarded
def query(
    text: Annotated[
        str,
        typer.Argument(help="Query language, e.g. marine (China OR Japan) -reef."),
    ],
    index: Annotated[
        Path | None, typer.Option("--index", exists=True, file_okay=False)
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    method: Annotated[RankMethod, typer.Option()] = RankMethod.BM25,
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 10,
    text_view: Annotated[
        bool,
        typer.Option("--text", help="Show each hit followed by its full indexed text."),
    ] = False,
    detailed: Annotated[
        bool, typer.Option(help="Show the saved parsed page for each hit.")
    ] = False,
    highlight: Annotated[
        bool,
        typer.Option(
            help="Highlight verified matches in indexed text; implies --text. Incompatible with --detailed."
        ),
    ] = False,
    parsed: Annotated[
        list[Path] | None,
        typer.Option(
            exists=True,
            file_okay=False,
            help="Parsed input for --detailed; repeat for multiple sources. Defaults to project inputs.",
        ),
    ] = None,
    explain: Annotated[bool, typer.Option()] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Evaluate the query language and rank with BM25, TF-IDF cosine or TF-IDF sum."""
    if highlight and detailed:
        raise VinfError(
            "--highlight cannot be combined with --detailed. Use --text for highlighted output."
        )
    if text_view and detailed:
        raise VinfError("Choose --text or --detailed.")
    if parsed and not detailed:
        raise VinfError("--parsed is used with --detailed.")
    roots = tuple(parsed or ())
    if detailed and not roots:
        current = active_project(project, required=True)
        roots = tuple(
            current.resolve(source.parsed) for source in current.config.sources
        )
    result = search(
        index_path(index, project),
        text,
        method=method,
        limit=limit,
        explain=explain,
        include_text=text_view,
        capture_matches=highlight,
    )
    if detailed:
        result = detailed_result(result, roots)
    if json_output:
        typer.echo(result.model_dump_json())
    else:
        display_hits(
            result,
            console,
            feed=text_view or detailed or highlight,
            detailed=detailed,
            highlight=highlight,
            explain=explain,
        )


@app.command()
@guarded
def compare(
    queries: Annotated[
        Path | None, typer.Option(help="JSON QuerySet; default: project queries file.")
    ] = None,
    index: Annotated[
        Path | None, typer.Option("--index", exists=True, file_okay=False)
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    output: Annotated[
        Path | None, typer.Option(help="Optional JSON report destination.")
    ] = None,
    top_k: Annotated[int, typer.Option(min=1, max=100)] = 5,
    json_output: Annotated[bool, typer.Option("--json")] = False,
):
    """Compare all three rankers on identical queries; save term explanations."""
    if queries is None:
        current = active_project(project)
        if current is None or current.config.queries is None:
            raise VinfError("Provide --queries or set queries in vinf.project.json.")
        queries = current.resolve(current.config.queries)
    result = compare_queries(
        index_path(index, project), read_model(queries, QuerySet), top_k=top_k
    )
    current = active_project(project)
    if output is None and current is not None:
        output = current.root / "reports/comparison.json"
    if output:
        atomic_model(output, result)
    if json_output:
        typer.echo(result.model_dump_json())
    else:
        for comparison in result.comparisons:
            console.print(comparison.query.question, style="bold cyan", markup=False)
            console.print(comparison.query.description, markup=False)
            for ranked in comparison.results:
                display_hits(ranked, console, explain=True)
        console.print(result.note)


@app.command("syntax")
@guarded
def syntax(text: Annotated[str, typer.Argument()]) -> None:
    """Show the parsed query expression without opening an index."""
    console.print_json(parse_query(text).model_dump_json(indent=2))
