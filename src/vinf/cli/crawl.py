from __future__ import annotations

from pathlib import Path
from typing import Annotated
from uuid import UUID

import typer
from pydantic import TypeAdapter
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from vinf_core.files import dataset_lock, read_model
from vinf_core.models import Compression, RobotsStrategy, VinfError
from vinf_crawler.config import (
    CrawlConfig,
    FailureAction,
    FailurePolicy,
    ParallelConfig,
    ProxyConfig,
    ProxyPoolConfig,
    RateLimitScope,
    RobotsConfig,
    StorageConfig,
    TraversalConfig,
)
from vinf_crawler.engine import Crawler
from vinf_crawler.http import CrawlPaused
from vinf_crawler.models import TaskStatus, TransportStatus
from vinf_crawler.store import CrawlStore

from vinf.cli.common import (
    active_project,
    console,
    display_model,
    errors,
    guarded,
    source_path,
)

app = typer.Typer(
    no_args_is_help=True,
    help="Download raw HTML from arbitrary seeds; resume the saved FIFO queue.",
)
Dataset = Annotated[Path | None, typer.Option("--dataset", "-d", file_okay=False)]


@app.command("export")
@guarded
def export_saved(
    output: Annotated[Path, typer.Option("--output", file_okay=False)],
    dataset: Dataset = None,
    compression: Annotated[Compression, typer.Option()] = Compression.GZIP,
    json_output: Annotated[bool, typer.Option("--json")] = False,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
) -> None:
    """Copy saved raw packets and frontier into a compressed crawl-only directory. No HTTP."""
    dataset = source_path(dataset, "crawled", project, source)
    from vinf_crawler.export import export_crawl

    display_model(export_crawl(dataset, output, compression), json_output=json_output)


@app.command("repack")
@guarded
def repack_saved(
    compression: Annotated[Compression, typer.Option()],
    keep_backup: Annotated[
        bool, typer.Option(help="Keep the previous verified storage representation.")
    ] = True,
    dataset: Dataset = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
) -> None:
    """Convert saved raw storage locally, keeping a backup. Then rerun parse/index."""
    from vinf_crawler.export import repack_crawl

    dataset = source_path(dataset, "crawled", project, source)
    result = repack_crawl(dataset, compression, keep_backup=keep_backup)
    current = active_project(project)
    if current:
        sources = tuple(
            item.model_copy(update={"crawl": CrawlStore(dataset).config})
            if current.resolve(item.crawled) == dataset.resolve()
            else item
            for item in current.config.sources
        )
        type(current)(
            current.root, current.config.model_copy(update={"sources": sources})
        ).save()
    display_model(result, json_output=json_output)
    if not json_output:
        console.print("Storage changed. Run parse run, then index build.")


@app.command()
@guarded
def init(
    dataset: Dataset = None,
    config: Annotated[
        Path | None, typer.Option(help="JSON CrawlConfig, schema 2.")
    ] = None,
    seed: Annotated[
        list[str] | None,
        typer.Option("--seed", help="Repeat for multiple starting URLs."),
    ] = None,
    max_depth: Annotated[int | None, typer.Option(min=0)] = None,
    workers: Annotated[int | None, typer.Option(min=1, max=32)] = None,
    user_agent: Annotated[
        str | None, typer.Option(help="An honest crawler identity.")
    ] = None,
    compression: Annotated[
        Compression | None,
        typer.Option(help="Raw HTML storage: none (default), gzip or xz."),
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
):
    """Create a dataset. Supply seeds or a configuration; no site is built in."""
    dataset = source_path(dataset, "crawled", project, source)
    current = active_project(project)
    settings = None
    if config:
        settings = read_model(config, CrawlConfig)
    elif seed:
        settings = CrawlConfig(seeds=tuple(seed))
    elif current:
        settings = current.source(source).crawl
    if settings is None:
        raise VinfError(
            "Supply --seed/--config or configure crawl seeds in the project."
        )
    traversal = (
        settings.traversal.model_copy(update={"max_depth": max_depth})
        if max_depth is not None
        else settings.traversal
    )
    http = (
        settings.http.model_copy(update={"user_agent": user_agent})
        if user_agent is not None
        else settings.http
    )
    settings = CrawlConfig(
        seeds=tuple(seed) if seed else settings.seeds,
        scope=settings.scope,
        traversal=traversal,
        http=type(http).model_validate(http.model_dump()),
        robots=settings.robots,
        failures=settings.failures,
        proxies=settings.proxies,
        proxy_pool=settings.proxy_pool,
        parallel=ParallelConfig(workers=workers)
        if workers is not None
        else settings.parallel,
        storage=settings.storage.model_copy(update={"compression": compression})
        if compression is not None
        else settings.storage,
    )
    with dataset_lock(dataset):
        store = CrawlStore.initialize(dataset, settings)
    display_model(store.statistics())


@app.command()
@guarded
def run(
    dataset: Dataset = None,
    limit: Annotated[
        int,
        typer.Option(
            min=0, help="Committed documents in this run; 0 means no run limit."
        ),
    ] = 0,
    workers: Annotated[
        int | None,
        typer.Option(min=1, max=32, help="Save the number of concurrent HTTP workers."),
    ] = None,
    rate_limit_scope: Annotated[
        RateLimitScope | None,
        typer.Option(
            help="site: shared pacing; proxy: independent pacing per transport/origin. Saves the policy."
        ),
    ] = None,
    resume_paused: Annotated[
        bool, typer.Option(help="Explicitly resume an inspected pause.")
    ] = False,
    resume_proxy: Annotated[
        list[str] | None,
        typer.Option(
            "--resume-proxy",
            help="Resume only this proxy's pause; cooldown deadlines remain enforced.",
        ),
    ] = None,
    quiet: Annotated[
        bool, typer.Option(help="Suppress progress and HTTP messages.")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="JSON RunResult on stdout.")
    ] = False,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
):
    """Continue ready tasks. Cooling-down and manual-retry tasks remain saved."""
    dataset = source_path(dataset, "crawled", project, source)
    with dataset_lock(dataset):
        store = CrawlStore(dataset, recover=True)
        settings = store.config.model_copy(
            update={
                "parallel": ParallelConfig(workers=workers)
                if workers is not None
                else store.config.parallel,
                "http": store.config.http.model_copy(
                    update={"rate_limit_scope": rate_limit_scope}
                )
                if rate_limit_scope is not None
                else store.config.http,
            }
        )
        if settings != store.config:
            store.change_config(settings)

        progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            TimeElapsedColumn(),
            console=errors,
            disable=quiet or json_output or not errors.is_terminal,
        )
        progress_id = progress.add_task("Starting crawler", total=None)

        def report(message: str) -> None:
            if quiet or json_output:
                return
            if errors.is_terminal:
                progress.update(progress_id, description=message)
            else:
                errors.print(message, markup=False)

        if not quiet and not json_output:
            errors.print(
                f"User-Agent: {store.config.http.user_agent}\n"
                f"Robots: {store.config.robots.strategy}; configured interval: {store.config.http.delay_seconds}s; rate limit scope: {store.config.http.rate_limit_scope}",
                markup=False,
            )
        try:
            with progress:
                result = Crawler(store, report).run(
                    limit,
                    resume_paused=resume_paused,
                    resume_proxies=tuple(resume_proxy or ()),
                )
        except KeyboardInterrupt:
            errors.print(
                "Interrupted. Saved documents and frontier are preserved.", markup=False
            )
            display_model(store.statistics(), json_output=json_output)
            raise typer.Exit(130)
        except CrawlPaused as exc:
            errors.print(str(exc), style="yellow", markup=False)
            display_model(store.statistics(), json_output=json_output)
            raise typer.Exit(2)
        if json_output:
            typer.echo(result.model_dump_json())
        else:
            display_model(result.statistics)
            console.print(
                f"Saved: {result.saved_this_run}; recovered: {result.recovered_this_run}; "
                f"failed attempts: {result.failed_attempts_this_run}; stop: {result.stop_reason}",
                markup=False,
            )


@app.command()
@guarded
def status(
    dataset: Dataset = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
):
    """Read saved progress without HTTP requests."""
    dataset = source_path(dataset, "crawled", project, source)
    display_model(CrawlStore(dataset).statistics(), json_output=json_output)


@app.command()
@guarded
def tasks(
    dataset: Dataset = None,
    status: Annotated[list[TaskStatus] | None, typer.Option("--status")] = None,
    reason: Annotated[str | None, typer.Option()] = None,
    limit: Annotated[
        int, typer.Option(min=0, help="Displayed records; 0 means all.")
    ] = 50,
    json_output: Annotated[
        bool, typer.Option("--json", help="One TaskRecord JSON per line.")
    ] = False,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
):
    """Inspect task UUIDs, failures and retry times. No HTTP requests."""
    dataset = source_path(dataset, "crawled", project, source)
    store = CrawlStore(dataset)
    table = Table(title="Saved crawler tasks")
    for column in (
        "UUID",
        "Status",
        "Proxy",
        "Reason",
        "Attempts",
        "Retry at (Unix UTC)",
        "URL",
    ):
        table.add_column(column, overflow="fold")
    shown = 0
    for record in store.iter_tasks(set(status) if status else None):
        if reason and record.reason != reason:
            continue
        if json_output:
            typer.echo(record.model_dump_json())
        else:
            table.add_row(
                str(record.task.doc_id),
                record.status,
                record.proxy_id or "unassigned",
                record.reason or "",
                str(record.attempts),
                str(record.retry_at or ""),
                record.task.url,
            )
        shown += 1
        if limit and shown >= limit:
            break
    if not json_output:
        console.print(table)


@app.command()
@guarded
def retry(
    dataset: Dataset = None,
    doc_id: Annotated[
        UUID | None, typer.Option("--id", help="Retry a specific UUID.")
    ] = None,
    reason: Annotated[
        str | None, typer.Option(help="For example rate-limit or timeout.")
    ] = None,
    status: Annotated[
        list[TaskStatus] | None, typer.Option("--status", help="Default: deferred.")
    ] = None,
    proxy_id: Annotated[
        str | None,
        typer.Option(
            "--proxy", help="Explicitly reassign the selected retained tasks."
        ),
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
):
    """Return retained tasks to pending. Cooldowns survive; downloaded files are reused."""
    dataset = source_path(dataset, "crawled", project, source)
    with dataset_lock(dataset):
        store = CrawlStore(dataset, recover=True)
        count = store.retry(
            doc_id=doc_id,
            reason=reason,
            statuses=tuple(status or (TaskStatus.DEFERRED,)),
            proxy_id=proxy_id,
        )
        store.publish_catalog()
    console.print(
        f"Returned to pending: {count}. Cooldowns and attempt history are preserved."
    )


@app.command()
@guarded
def skip(
    doc_id: Annotated[UUID, typer.Option("--id")],
    reason: Annotated[str, typer.Option(help="Why this task is being skipped.")],
    dataset: Dataset = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
):
    """Explicitly skip a task while retaining its URL and history for a later retry."""
    dataset = source_path(dataset, "crawled", project, source)
    with dataset_lock(dataset):
        store = CrawlStore(dataset, recover=True)
        store.skip(doc_id, reason)
        store.publish_catalog()
    console.print(f"Skipped and retained: {doc_id}")


@app.command()
@guarded
def policy(
    dataset: Dataset = None,
    config: Annotated[
        Path | None,
        typer.Option(help="Apply a complete updated config, keeping seeds."),
    ] = None,
    proxy_id: Annotated[
        str | None,
        typer.Option(
            "--proxy", help="Update one proxy's policy; omit for global defaults."
        ),
    ] = None,
    workers: Annotated[int | None, typer.Option(min=1, max=32)] = None,
    proxy_file: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            dir_okay=False,
            help="Public HTTP proxy list; failing pool routes are disabled, URLs retained.",
        ),
    ] = None,
    pool_size: Annotated[int, typer.Option(min=1, max=32)] = 16,
    prefer_url: Annotated[
        list[str] | None,
        typer.Option(
            "--prefer-url",
            help="Repeat URL regex to fetch before ordinary queued links.",
        ),
    ] = None,
    robots_strategy: Annotated[
        RobotsStrategy | None,
        typer.Option(
            help="config-first keeps robots URL rules but uses configured timing; respect is the default."
        ),
    ] = None,
    delay: Annotated[float | None, typer.Option(min=0)] = None,
    rate_limit_scope: Annotated[
        RateLimitScope | None,
        typer.Option(
            help="site (default) shares pacing; proxy paces each transport/origin independently. Cooldowns stay shared."
        ),
    ] = None,
    rate_limit_action: Annotated[FailureAction | None, typer.Option()] = None,
    temporary_action: Annotated[FailureAction | None, typer.Option()] = None,
    other_action: Annotated[FailureAction | None, typer.Option()] = None,
    compression: Annotated[
        Compression | None,
        typer.Option(
            help="Storage for future downloads; existing packets stay unchanged."
        ),
    ] = None,
    membership_cache_mib: Annotated[
        int | None,
        typer.Option(
            min=0, max=1024, help="UUID membership cache budget; 0 disables it."
        ),
    ] = None,
    catalog_interval: Annotated[
        int | None,
        typer.Option(
            min=0, help="Publish every N saved pages; 0 publishes only on exit."
        ),
    ] = None,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
):
    """Inspect/update saved policy. Revisit saved links when scope or traversal changes."""
    dataset = source_path(dataset, "crawled", project, source)
    with dataset_lock(dataset):
        store = CrawlStore(dataset, recover=True)
        settings = read_model(config, CrawlConfig) if config else store.config
        if proxy_id is not None and rate_limit_scope is not None:
            raise VinfError("--rate-limit-scope applies to the dataset; omit --proxy.")

        def failure_settings(base: FailurePolicy) -> FailurePolicy:
            return FailurePolicy(
                rate_limit=rate_limit_action or base.rate_limit,
                temporary=temporary_action or base.temporary,
                other=other_action or base.other,
                max_attempts=base.max_attempts,
                retry_delay_seconds=base.retry_delay_seconds,
                max_retry_delay_seconds=base.max_retry_delay_seconds,
            )

        proxies = settings.proxies
        if proxy_id is not None:
            if proxy_id not in {proxy.id for proxy in proxies}:
                raise VinfError(f"Proxy {proxy_id} is not configured.")
            proxies = tuple(
                ProxyConfig(
                    id=proxy.id,
                    url_env=proxy.url_env,
                    delay_seconds=delay if delay is not None else proxy.delay_seconds,
                    robots_strategy=robots_strategy or proxy.robots_strategy,
                    failures=failure_settings(proxy.failures or settings.failures)
                    if any((rate_limit_action, temporary_action, other_action))
                    else proxy.failures,
                )
                if proxy.id == proxy_id
                else proxy
                for proxy in proxies
            )
        settings = CrawlConfig(
            seeds=settings.seeds,
            scope=settings.scope,
            traversal=TraversalConfig.model_validate(
                {**settings.traversal.model_dump(), "prefer_url_patterns": prefer_url}
            )
            if prefer_url is not None
            else settings.traversal,
            http=settings.http.model_copy(
                update={
                    "delay_seconds": delay
                    if delay is not None and proxy_id is None
                    else settings.http.delay_seconds,
                    "rate_limit_scope": rate_limit_scope
                    if rate_limit_scope is not None
                    else settings.http.rate_limit_scope,
                }
            ),
            robots=RobotsConfig(strategy=robots_strategy or settings.robots.strategy)
            if proxy_id is None
            else settings.robots,
            failures=failure_settings(settings.failures)
            if proxy_id is None
            else settings.failures,
            proxies=proxies,
            proxy_pool=ProxyPoolConfig(file=str(proxy_file.resolve()), size=pool_size)
            if proxy_file is not None
            else settings.proxy_pool,
            parallel=ParallelConfig(workers=workers)
            if workers is not None
            else settings.parallel,
            storage=StorageConfig(
                compression=compression
                if compression is not None
                else settings.storage.compression,
                membership_cache_mib=membership_cache_mib
                if membership_cache_mib is not None
                else settings.storage.membership_cache_mib,
                catalog_interval_pages=catalog_interval
                if catalog_interval is not None
                else settings.storage.catalog_interval_pages,
            ),
        )
        if settings != store.config:
            store.change_config(settings)
            store.publish_catalog()
        console.print_json(settings.model_dump_json())


@app.command()
@guarded
def proxy_check(
    input_file: Annotated[Path, typer.Option("--input", exists=True, dir_okay=False)],
    output: Annotated[Path, typer.Option(dir_okay=False)],
    workers: Annotated[int, typer.Option(min=1, max=32)] = 16,
    timeout: Annotated[float, typer.Option(min=0.1, max=30)] = 3,
    target: Annotated[str, typer.Option()] = "https://example.org/",
) -> None:
    """Check HTTPS through public proxy endpoints; save working URLs and a JSON report."""
    from vinf_crawler.proxies import check_proxies

    result = check_proxies(
        input_file, output, workers=workers, timeout=timeout, target=target
    )
    console.print(
        f"Checked: {result.candidates}; working: {result.working}; invalid: {result.invalid_lines}."
    )
    console.print(
        f"Working list: {output}; report: {output.with_suffix('.report.json')}"
    )


@app.command()
@guarded
def transports(
    dataset: Dataset = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
    project: Annotated[Path | None, typer.Option("--project")] = None,
    source: Annotated[str | None, typer.Option("--source")] = None,
):
    """Inspect saved per-proxy strategy/cooldowns without resolving secrets or making HTTP requests."""
    store = CrawlStore(source_path(dataset, "crawled", project, source))
    values = store.statistics().transports
    if json_output:
        typer.echo(TypeAdapter(tuple[TransportStatus, ...]).dump_json(values).decode())
        return
    table = Table(title="Crawler transports (site-wide limits also apply)")
    for column in (
        "ID",
        "URL environment variable",
        "Robots",
        "Delay",
        "Cooldown until",
        "Pause",
        "Disabled",
    ):
        table.add_column(column, overflow="fold")
    for value in values:
        table.add_row(
            value.proxy_id,
            value.url_env or "direct connection",
            value.robots_strategy,
            str(value.minimum_delay_seconds),
            str(value.cooldown_until or ""),
            str(value.pause or ""),
            value.disabled or "",
        )
    console.print(table)
