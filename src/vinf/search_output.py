"""Rich search views. Rendering consumes saved scores and match spans only."""

from collections.abc import Iterable

from rich import box
from rich.console import Console, RenderableType
from rich.padding import Padding
from rich.table import Table
from rich.text import Text
from vinf_core.models import VinfError
from vinf_search.models import MatchSpan, RankMethod, SearchHit, SearchResult

from vinf.search_records import DetailedHit


def indented(value: RenderableType) -> Padding:
    return Padding(value, (0, 0, 0, 2), expand=False)


def highlighted_text(
    value: str, spans: tuple[MatchSpan, ...], *, offset: int = 0
) -> Text:
    """Translate saved UTF-8 byte boundaries to Rich character boundaries."""
    body = value.encode("utf-8")
    selected = [
        span for span in spans if offset <= span.start < span.end <= offset + len(body)
    ]
    boundaries = sorted(
        {boundary - offset for span in selected for boundary in (span.start, span.end)}
    )
    positions = {}
    previous = character = 0
    try:
        for boundary in boundaries:
            character += len(body[previous:boundary].decode("utf-8"))
            positions[boundary] = character
            previous = boundary
    except UnicodeError as exc:
        raise VinfError("Match offsets split a UTF-8 character.") from exc
    text = Text(value)
    for span in selected:
        text.stylize(
            "yellow", positions[span.start - offset], positions[span.end - offset]
        )
    return text


def display_explanation(console: Console, result: SearchResult, hit: SearchHit) -> None:
    console.print()
    console.print(
        Text(
            f"Explanation · {result.method.upper()} · score={hit.score:.6f}",
            style="bold magenta",
        )
    )
    context = result.ranking
    metrics = f"tokens={hit.document_tokens} · documents={context.documents}"
    if result.method == RankMethod.TFIDF:
        metrics += f" · document norm={hit.document_norm:.6f} · query norm={context.query_norm:.6f}"
    elif result.method == RankMethod.BM25:
        metrics += (
            f" · avg length={context.average_length:.6f}"
            f" · k1={context.config.k1:g} · b={context.config.b:g}"
        )
    else:
        metrics += " · sum(TF × ln(N/DF)); positive query terms counted once"
    if not hit.explanation:
        console.print(
            indented(
                Text("No positive indexed terms contributed to the score.", style="dim")
            )
        )
    else:
        table = Table.grid(padding=(0, 2))
        table.add_column(style="cyan", overflow="fold")
        table.add_column(style="green", justify="right")
        for _ in range(3):
            table.add_column(style="dim")
        for term in hit.explanation:
            table.add_row(
                Text(term.term),
                f"{term.contribution:.6f}",
                f"tf={term.tf}",
                f"df={term.df}",
                f"idf={term.idf:.6f}",
            )
        console.print(indented(table))
    console.print(indented(Text(metrics, style="dim")))


def display_fields(
    console: Console, rows: Iterable[tuple[str, str | None]], *, dim: bool = False
) -> None:
    table = Table.grid(padding=(0, 2))
    table.add_column(style="dim" if dim else "cyan", no_wrap=True)
    table.add_column(style="dim" if dim else "none", overflow="fold")
    for name, value in rows:
        if value:
            table.add_row(Text(name), Text(value))
    if table.row_count:
        console.print(indented(table))


def indexed_segments(hit: SearchHit) -> list[tuple[int, str]]:
    """Read stored line boundaries without tokenizing or matching text."""
    if hit.fulltext is None:
        return []
    segments: list[tuple[int, str]] = []
    offset = 0
    for line in hit.fulltext.split("\n"):
        segments.append((offset, line))
        offset += len(line.encode("utf-8")) + 1
    return segments


def name_count(hit: SearchHit) -> int:
    return sum(
        bool(value and value.strip())
        for value in (hit.scientific_name, hit.common_name)
    )


def display_header(
    console: Console,
    result: SearchResult,
    hit: SearchHit,
    segments: list[tuple[int, str]],
    *,
    highlight: bool,
) -> None:
    title = Text(f"{hit.rank}. ", style="bold cyan")
    title.stylize("magenta", 0, len(title))
    name = (
        segments[0][1]
        if segments and hit.scientific_name.strip()
        else hit.scientific_name
    )
    title.append_text(highlighted_text(name, hit.matches) if highlight else Text(name))
    score = Text(f"{result.method.upper()} ", style="dim")
    score.append(f"{hit.score:.6f}", style="not dim bold green")
    header = Table.grid(expand=True, padding=(0, 2))
    header.add_column(ratio=1, overflow="fold")
    header.add_column(justify="right", no_wrap=True)
    header.add_row(title, score)
    console.print(header)
    if hit.common_name and hit.common_name.strip():
        name = Text(hit.common_name, style="italic")
        if segments:
            offset, value = segments[name_count(hit) - 1]
            name = (
                highlighted_text(value, hit.matches, offset=offset)
                if highlight
                else Text(value)
            )
            name.stylize("italic")
        console.print(indented(name))
    source = Text(f"{hit.source_name} · {hit.source_id} · ", style="dim")
    source.append(hit.url, style="not dim bright_blue underline")
    console.print(indented(source))


def display_record(console: Console, hit: DetailedHit) -> None:
    record = hit.parsed.record
    taxonomy = record.taxonomy
    length = record.max_length
    measurement = None
    if length is not None:
        measurement = " ".join(
            part
            for part in (
                f"{length.value:g} {length.unit}",
                length.measurement,
                length.sex,
            )
            if part
        )
        if length.unit != "cm" or length.value != length.centimetres:
            measurement += f" · {length.centimetres:g} cm"
    console.print()
    display_fields(
        console,
        (
            (
                "Taxonomy",
                " › ".join(
                    part
                    for part in (taxonomy.class_name, taxonomy.order, taxonomy.family)
                    if part
                ),
            ),
            (
                "Habitat",
                " · ".join(
                    part for part in (*record.environments, record.climate) if part
                ),
            ),
            ("Max length", measurement),
            ("IUCN", record.iucn_code),
            (
                "Other IDs",
                " · ".join(
                    f"{item.namespace}:{item.value}"
                    for item in record.identifiers
                    if (item.namespace, item.value) != (hit.source_name, hit.source_id)
                ),
            ),
        ),
    )
    for warning in record.warnings:
        console.print(indented(Text(f"Warning: {warning}", style="bold red")))
    for section in record.sections:
        if not section.text.strip():
            continue
        console.print()
        console.print(Text(section.name, style="bold cyan"))
        console.print(indented(Text(section.text)))
    page, source = hit.parsed, hit.parsed.source
    console.print()
    console.print(Text("Details", style="dim"))
    display_fields(
        console,
        (
            ("UUID", str(hit.doc_id)),
            ("Fetched", f"{source.fetched_at.isoformat()} · {source.encoding}"),
            (
                "Requested",
                source.requested_url if source.requested_url != hit.url else None,
            ),
            ("Raw file", source.raw_path),
            ("SHA-256", source.sha256),
            ("Parser", f"{record.profile} · {page.status} · {page.reason}"),
            ("Fingerprint", page.fingerprint),
            ("Species key", record.species_key),
            ("Schemas", f"page={page.schema_version} · record={record.schema_version}"),
            (
                "HTML chars",
                "; ".join(
                    f"{section.name}: {section.raw_start}:{section.raw_end} ({section.rule})"
                    for section in record.sections
                ),
            ),
        ),
        dim=True,
    )


def display_text(
    console: Console,
    hit: SearchHit,
    segments: list[tuple[int, str]],
    *,
    highlight: bool,
) -> None:
    if hit.fulltext is None:
        raise VinfError("Text output needs saved indexed text.")
    console.print(Text("Full text", style="dim"))
    for index, (offset, value) in enumerate(segments[name_count(hit) :]):
        if index:
            console.print()
        body = (
            highlighted_text(value, hit.matches, offset=offset)
            if highlight
            else Text(value)
        )
        console.print(indented(body))


def display_hits(
    result: SearchResult,
    console: Console,
    *,
    feed: bool = False,
    detailed: bool = False,
    highlight: bool = False,
    explain: bool = False,
) -> None:
    if detailed and highlight:
        raise VinfError("--highlight cannot be combined with --detailed.")
    heading = Text("Search ", style="bold cyan")
    heading.append(f"· {result.method.upper()} · ", style="dim")
    heading.append(result.query, style="bold")
    console.print(heading)
    summary = Text(f"Matched: {result.total_matches}", style="green")
    summary.append(
        f" · Showing: {len(result.hits)} · Index: {result.generation}", style="dim"
    )
    if result.unknown_tokens:
        summary.append(f" · Unknown: {', '.join(result.unknown_tokens)}", style="red")
    console.print(summary)
    if not feed:
        table = Table(box=box.SIMPLE_HEAD, header_style="bold cyan", border_style="dim")
        table.add_column("Rank", style="magenta", justify="right")
        table.add_column("Source", style="dim", overflow="fold")
        table.add_column("Species", style="bold cyan", overflow="fold")
        table.add_column("Score", style="green", justify="right")
        table.add_column("URL", style="bright_blue", overflow="fold")
        for hit in result.hits:
            table.add_row(
                str(hit.rank),
                Text(hit.source_name),
                Text(hit.scientific_name),
                f"{hit.score:.6f}",
                Text(hit.url),
            )
        console.print(table)
    for hit in result.hits:
        if feed:
            console.print()
            console.rule(style="dim cyan")
            segments = [] if detailed else indexed_segments(hit)
            display_header(console, result, hit, segments, highlight=highlight)
            if detailed:
                if not isinstance(hit, DetailedHit):
                    raise VinfError("Detailed output needs saved parsed records.")
                display_record(console, hit)
            else:
                console.print()
                display_text(console, hit, segments, highlight=highlight)
        if explain:
            if not feed:
                console.rule(
                    Text(f"{hit.rank}. {hit.scientific_name}", style="bold cyan"),
                    style="dim cyan",
                )
            display_explanation(console, result, hit)
