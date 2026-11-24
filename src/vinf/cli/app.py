from pathlib import Path
from typing import Annotated

import typer

from vinf import __version__
from vinf.cli.common import configure_project
from vinf.cli.crawl import app as crawl_app
from vinf.cli.doctor import app as doctor_app
from vinf.cli.index import app as index_app
from vinf.cli.lucene import app as lucene_app
from vinf.cli.parse import app as parse_app
from vinf.cli.project import app as project_app
from vinf.cli.report import app as report_app
from vinf.cli.search import app as search_app
from vinf.cli.spark import app as spark_app

app = typer.Typer(
    no_args_is_help=True, help="Independent file-based information retrieval stages."
)
for name, command in (
    ("project", project_app),
    ("crawl", crawl_app),
    ("parse", parse_app),
    ("index", index_app),
    ("search", search_app),
    ("doctor", doctor_app),
    ("report", report_app),
    ("spark", spark_app),
    ("lucene", lucene_app),
):
    app.add_typer(command, name=name)


@app.command()
def version() -> None:
    """Print the application version."""
    typer.echo(__version__)


@app.callback()
def main(
    ctx: typer.Context,
    project: Annotated[
        Path | None,
        typer.Option(
            "--project",
            help="Project directory or vinf.project.json; default: discover from the current directory.",
        ),
    ] = None,
) -> None:
    configure_project(ctx, project)
