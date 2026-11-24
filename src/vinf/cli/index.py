from itertools import islice
from pathlib import Path
from typing import Annotated
from uuid import UUID

import typer
from rich.table import Table
from vinf_core.files import read_model
from vinf_core.index_io import IndexReader
from vinf_core.indexes import IndexConfig, Occurrence, TermInfo
from vinf_core.models import Model, VinfError
from vinf_core.text import words
from vinf_indexing import build_index

from vinf.cli.common import active_project, console, display_model, guarded, index_path

app = typer.Typer(
    no_args_is_help=True,
    help="Build or inspect a compact positional index from one or more parsed snapshots.",
)


@app.callback()
def main() -> None:
    """Build an index from independently published parsed snapshots."""


@app.command()
@guarded
def build(
    input_dirs: Annotated[
        list[Path] | None, typer.Option("--input", exists=True, file_okay=False)
    ] = None,
    output: Annotated[Path | None, typer.Option("--output", file_okay=False)] = None,
    config: Annotated[
        Path | None, typer.Option(help="Optional JSON IndexConfig.")
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[
        str | None,
        typer.Option(
            "--source", help="Index one project source; default: all project sources."
        ),
    ] = None,
    rebuild: Annotated[
        bool,
        typer.Option(help="Rebuild this generation."),
    ] = False,
    memory_mib: Annotated[
        int,
        typer.Option(
            min=1,
            help="Sort-buffer budget in MiB; excludes the current document and I/O buffers.",
        ),
    ] = 64,
    merge_fan_in: Annotated[
        int, typer.Option(min=2, max=64, help="Maximum temporary runs merged at once.")
    ] = 16,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Read parsed DTOs only. Explicit paths override project defaults."""
    current = active_project(project)
    if input_dirs is None:
        if current is None:
            raise VinfError(
                "Provide --input or create a project with 'vinf project init'."
            )
        sources = (current.source(source),) if source else current.config.sources
        input_dirs = [current.resolve(item.parsed) for item in sources]
    settings = (
        read_model(config, IndexConfig)
        if config
        else current.config.index
        if current
        else None
    )
    result = build_index(
        input_dirs,
        index_path(output, project),
        settings,
        rebuild=rebuild,
        memory_limit=memory_mib * 1024 * 1024,
        merge_fan_in=merge_fan_in,
    )
    display_model(result, json_output=json_output)


class InspectedPosting(Model):
    local_id: int
    doc_id: UUID
    scientific_name: str
    tf: int
    occurrences: tuple[Occurrence, ...]


class TermInspection(Model):
    term: str
    location: TermInfo | None
    postings: tuple[InspectedPosting, ...]


@app.command()
@guarded
def inspect(
    term: Annotated[str, typer.Argument(help="One vocabulary term; case insensitive.")],
    index: Annotated[Path | None, typer.Option("--index", file_okay=False)] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    limit: Annotated[int, typer.Option(min=1, help="Maximum documents shown.")] = 10,
    occurrences: Annotated[
        int, typer.Option(min=1, help="Maximum occurrences shown per document.")
    ] = 5,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Decode a term's binary postings without reading parsed or raw files."""
    tokens = words(term)
    if len(tokens) != 1:
        raise VinfError("Inspect accepts one term. Use search for a query.")
    term = tokens[0]
    path = index_path(index, project)
    with IndexReader(path) as reader:
        location = reader.find(term)
        shown = []
        if location:
            with reader.cursor(location) as cursor:
                for posting in islice(cursor, limit):
                    document = reader.document(posting.doc_id)
                    with cursor.positions(posting) as positions:
                        values = tuple(islice(positions, occurrences))
                    shown.append(
                        InspectedPosting(
                            local_id=posting.doc_id,
                            doc_id=document.doc_id,
                            scientific_name=document.scientific_name,
                            tf=posting.tf,
                            occurrences=values,
                        )
                    )
        result = TermInspection(term=term, location=location, postings=tuple(shown))
    if json_output:
        typer.echo(result.model_dump_json())
        return
    if location is None:
        console.print(f"No indexed term: {term}", markup=False)
        return
    console.print(
        f"{term}: DF={location.df}, {location.size_bytes} bytes, {location.blocks} blocks; "
        f"showing {len(shown)} documents. Offsets are UTF-8 bytes within each text.",
        markup=False,
    )
    table = Table()
    for column in ("ID", "Document / UUID", "TF", "position: start–end / section"):
        table.add_column(column, overflow="fold")
    for posting in result.postings:
        table.add_row(
            str(posting.local_id),
            f"{posting.scientific_name}\n{posting.doc_id}",
            str(posting.tf),
            "\n".join(
                f"{o.position}: {o.start}–{o.end} / {o.segment}"
                for o in posting.occurrences
            ),
        )
    console.print(table)
