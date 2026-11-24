from pathlib import Path
from typing import Annotated, Literal

import typer
from vinf_core.models import Compression
from vinf_parsing import get_profile, parse_documents
from vinf_parsing.profiles import PROFILES

from vinf.cli.common import active_project, console, display_model, guarded, source_path

app = typer.Typer(
    no_args_is_help=True,
    help="Parse saved HTML into a separate JSON/JSONL snapshot, optionally gzip.",
)


@app.command()
def profiles() -> None:
    """List implemented source adapters."""
    for profile in PROFILES:
        console.print(f"{profile.name}: {profile.version}", markup=False)


@app.command()
@guarded
def run(
    input_dir: Annotated[
        Path | None, typer.Option("--input", exists=True, file_okay=False)
    ] = None,
    output: Annotated[Path | None, typer.Option("--output", file_okay=False)] = None,
    profile: Annotated[
        str | None, typer.Option(help="Source adapter; see parse profiles.")
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
    compression: Annotated[
        Literal["none", "gzip"] | None,
        typer.Option(
            help="Parsed storage: none or gzip; retain existing format by default."
        ),
    ] = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Select and extract locally. No crawling or indexing is started."""
    current = active_project(project)
    if profile is None:
        profile = (
            current.source(source).profile
            if current and (source or input_dir is None or output is None)
            else "fishbase"
        )
    result = parse_documents(
        source_path(input_dir, "crawled", project, source),
        source_path(output, "parsed", project, source),
        profile=get_profile(profile),
        compression=Compression(compression) if compression is not None else None,
    )
    display_model(result, json_output=json_output)
    if result.errors:
        raise typer.Exit(1)
