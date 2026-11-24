from pathlib import Path
from typing import Annotated

import typer
from vinf_core.models import VinfError

from vinf.cli.common import active_project, console, guarded, index_path, source_path
from vinf.reporting import make_report
from vinf.statistics import measure_corpus

app = typer.Typer(
    no_args_is_help=True,
    help="Render an English coursework report from saved stage outputs.",
)


@app.command()
@guarded
def stats(
    output: Annotated[Path | None, typer.Option()] = None,
    encoding: Annotated[
        str, typer.Option(help="Named tiktoken BPE encoding.")
    ] = "cl100k_base",
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
) -> None:
    """Measure saved stages and BPE tokens without producing new stage outputs."""
    current = active_project(project, required=True)
    output = output or current.root / "reports/statistics.json"
    result = measure_corpus(
        source_path(None, "crawled", project, source),
        source_path(None, "parsed", project, source),
        index_path(None, project),
        output,
        encoding_name=encoding,
        progress=lambda message: console.print(message, markup=False),
    )
    console.print(f"Saved {output}; HTML BPE tokens: {result.html_bpe_tokens:,}")


@app.command()
@guarded
def build(
    crawled: Annotated[Path | None, typer.Option(exists=True, file_okay=False)] = None,
    parsed: Annotated[Path | None, typer.Option(exists=True, file_okay=False)] = None,
    index: Annotated[Path | None, typer.Option(exists=True, file_okay=False)] = None,
    comparison: Annotated[
        Path | None, typer.Option(exists=True, dir_okay=False)
    ] = None,
    statistics: Annotated[
        Path | None, typer.Option(exists=True, dir_okay=False)
    ] = None,
    output: Annotated[
        Path | None, typer.Option(help="Destination .dokuwiki.txt file.")
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
) -> None:
    """Use existing artifacts; no crawling, parsing or index building is invoked."""
    current = active_project(project)
    if current:
        comparison = comparison or current.root / "reports/comparison.json"
        statistics = statistics or current.root / "reports/statistics.json"
        output = output or current.root / "reports/submission.dokuwiki.txt"
    if comparison is None or output is None or statistics is None:
        raise VinfError(
            "Provide --comparison/--statistics/--output or create a project."
        )
    console.print(
        make_report(
            source_path(crawled, "crawled", project, source),
            source_path(parsed, "parsed", project, source),
            index_path(index, project),
            comparison,
            output,
            statistics,
        ),
        markup=False,
    )
