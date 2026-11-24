from pathlib import Path
from typing import Annotated

import typer
from vinf_core.models import VinfError

from vinf.cli.common import active_project, console, guarded
from vinf.project import PROJECT_FILE, Project, ProjectConfig, make_source

app = typer.Typer(
    no_args_is_help=True, help="Create and inspect local project configurations."
)


@app.command()
@guarded
def init(
    directory: Annotated[
        Path,
        typer.Argument(
            help="Project root; stored paths are relative to this directory."
        ),
    ] = Path("."),
    name: Annotated[str | None, typer.Option()] = None,
    source: Annotated[
        str, typer.Option(help="Name of the initial source.")
    ] = "fishbase",
    profile: Annotated[str, typer.Option()] = "fishbase",
    seed: Annotated[list[str] | None, typer.Option("--seed")] = None,
    crawl_config: Annotated[
        Path | None, typer.Option(exists=True, dir_okay=False)
    ] = None,
    crawled: Annotated[Path | None, typer.Option()] = None,
    parsed: Annotated[Path | None, typer.Option()] = None,
    indexed: Annotated[Path, typer.Option()] = Path("data/indexed"),
    queries: Annotated[Path | None, typer.Option()] = None,
) -> None:
    """Create configuration only. Existing datasets can be attached without copying."""
    root = directory.resolve()
    if (root / PROJECT_FILE).exists():
        raise VinfError(f"Project already exists: {root / PROJECT_FILE}")
    first = make_source(
        root, source, profile, crawled, parsed, tuple(seed or ()), crawl_config
    )
    if queries is None and (root / "queries.example.json").is_file():
        queries = Path("queries.example.json")
    project = Project(
        root,
        ProjectConfig(
            name=name or root.name, sources=(first,), indexed=indexed, queries=queries
        ),
    )
    project.save()
    console.print(f"Created {root / PROJECT_FILE}", markup=False)


@app.command()
@guarded
def show(project: Annotated[Path | None, typer.Option("--project")] = None) -> None:
    """Show configuration and the resolved paths used by each stage."""
    current = active_project(project, required=True)
    console.print_json(current.config.model_dump_json(indent=2))
    for source in current.config.sources:
        console.print(
            f"{source.name}:\n  crawled: {current.resolve(source.crawled)}\n  parsed:  {current.resolve(source.parsed)}",
            markup=False,
        )
    console.print(f"indexed: {current.resolve(current.config.indexed)}", markup=False)


@app.command()
@guarded
def add_source(
    name: Annotated[str, typer.Argument()],
    project: Annotated[Path | None, typer.Option("--project")] = None,
    profile: Annotated[str, typer.Option()] = "fishbase",
    seed: Annotated[list[str] | None, typer.Option("--seed")] = None,
    crawl_config: Annotated[
        Path | None, typer.Option(exists=True, dir_okay=False)
    ] = None,
    crawled: Annotated[Path | None, typer.Option()] = None,
    parsed: Annotated[Path | None, typer.Option()] = None,
) -> None:
    """Register another source. No network requests or stage execution."""
    current = active_project(project, required=True)
    source = make_source(
        current.root, name, profile, crawled, parsed, tuple(seed or ()), crawl_config
    )
    config = ProjectConfig.model_validate(
        current.config.model_copy(
            update={"sources": (*current.config.sources, source)}
        ).model_dump()
    )
    Project(current.root, config).save()
    console.print(f"Added source {name}", markup=False)
