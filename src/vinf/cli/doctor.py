from pathlib import Path
from typing import Annotated

import typer
from vinf_core.models import ArtifactCheck
from vinf_crawler.checks import check_dataset
from vinf_crawler.models import DoctorReport
from vinf_parsing import get_profile
from vinf_parsing.checks import check_parsed
from vinf_search.checks import check_index

from vinf.cli.common import (
    active_project,
    display_model,
    guarded,
    index_path,
    source_path,
)

app = typer.Typer(
    no_args_is_help=True, help="Validate individual stage snapshots without HTTP."
)


def show(result: ArtifactCheck | DoctorReport, json_output: bool) -> None:
    display_model(result, json_output=json_output)
    if not result.ok:
        raise typer.Exit(1)


@app.command()
@guarded
def crawl(
    dataset: Annotated[
        Path | None, typer.Option("--dataset", "-d", exists=True, file_okay=False)
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
    deep: Annotated[
        bool, typer.Option(help="Decode content and verify original SHA-256.")
    ] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    show(
        check_dataset(source_path(dataset, "crawled", project, source), deep=deep),
        json_output,
    )


@app.command()
@guarded
def parsed(
    input_dir: Annotated[
        Path | None, typer.Option("--input", exists=True, file_okay=False)
    ] = None,
    crawled: Annotated[
        Path | None,
        typer.Option(
            file_okay=False, help="Optional raw input for re-extraction checks."
        ),
    ] = None,
    profile: Annotated[str | None, typer.Option()] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
    deep: Annotated[
        bool, typer.Option(help="Re-extract from the project's raw input.")
    ] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    current = active_project(project)
    if deep and crawled is None:
        crawled = source_path(None, "crawled", project, source)
    adapter = profile or (
        current.source(source).profile
        if current and (source or input_dir is None)
        else "fishbase"
    )
    show(
        check_parsed(
            source_path(input_dir, "parsed", project, source),
            crawled=crawled,
            profile=get_profile(adapter) if crawled else None,
        ),
        json_output,
    )


@app.command()
@guarded
def index(
    input_dir: Annotated[
        Path | None, typer.Option("--input", exists=True, file_okay=False)
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    deep: Annotated[
        bool, typer.Option(help="Verify all postings, TF/DF, lengths and norms.")
    ] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    show(check_index(index_path(input_dir, project), deep=deep), json_output)
