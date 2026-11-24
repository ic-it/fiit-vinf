from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
from typing import Literal

import typer
from pydantic import BaseModel, ValidationError
from rich.console import Console
from rich.table import Table
from vinf_core.models import VinfError

from vinf.project import Project, load_project

console = Console()
errors = Console(stderr=True)


def display_model(value: BaseModel, *, json_output: bool = False) -> None:
    if json_output:
        typer.echo(value.model_dump_json())
        return
    table = Table(title=type(value).__name__)
    table.add_column("Metric")
    table.add_column("Value", overflow="fold")
    for name in type(value).model_fields:
        table.add_row(name.replace("_", " "), str(getattr(value, name)))
    console.print(table)


def guarded(function: Callable) -> Callable:
    @wraps(function)
    def wrapped(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (VinfError, OSError, ValidationError) as exc:
            errors.print(str(exc), style="red", markup=False)
            raise typer.Exit(1) from exc

    return wrapped


_PROJECT: ContextVar[Path | None] = ContextVar("vinf_project", default=None)


def configure_project(context: typer.Context, path: Path | None) -> None:
    token = _PROJECT.set(path)
    context.call_on_close(lambda: _PROJECT.reset(token))


def active_project(
    path: Path | None = None, *, required: bool = False
) -> Project | None:
    project = load_project(path if path is not None else _PROJECT.get())
    if required and project is None:
        raise VinfError("No project found. Run 'vinf project init' or pass --project.")
    return project


def source_path(
    value: Path | None,
    stage: Literal["crawled", "parsed"],
    project: Path | None = None,
    source: str | None = None,
) -> Path:
    if value is not None:
        return value
    current = active_project(project)
    if current is None:
        if stage == "crawled":
            return Path("data/crawl")
        raise VinfError(
            f"Provide a path or run 'vinf project init' to configure {stage}."
        )
    selected = current.source(source)
    return current.resolve(getattr(selected, stage))


def index_path(value: Path | None, project: Path | None = None) -> Path:
    if value is not None:
        return value
    current = active_project(project, required=True)
    return current.resolve(current.config.indexed)
