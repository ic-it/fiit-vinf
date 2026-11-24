"""Bounded parallel HTTP with one writer and a plain FIFO queue."""

from __future__ import annotations

import hashlib
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from vinf_core.content import content_name
from vinf_core.files import contained_path
from vinf_core.models import DocumentLinks, DocumentMetadata, PauseState, VinfError

from vinf_crawler.config import CrawlConfig, FailureAction
from vinf_crawler.discovery import discover_links
from vinf_crawler.http import (
    CrawlPaused,
    FetchResult,
    HttpClient,
    HttpFailure,
    PolicyBlocked,
    Reporter,
)
from vinf_crawler.models import (
    CrawlTask,
    FailureReason,
    FetchFailed,
    Paused,
    Resumed,
    RunResult,
    TaskAssigned,
    TaskBlocked,
    TaskRecord,
    TaskStatus,
    TaskWaiting,
    utc_now,
)
from vinf_crawler.network import (
    FetchCancelled,
    NetworkPolicy,
    RequestDeferred,
    transports,
)
from vinf_crawler.store import CrawlStore


@dataclass
class Job:
    task: CrawlTask
    proxy_id: str
    config: CrawlConfig
    attempt: int
    handled: bool = False


@dataclass(frozen=True)
class Recovered:
    document: DocumentMetadata
    links: DocumentLinks


@dataclass(frozen=True)
class Failed:
    error: HttpFailure
    retry_at: float | None
    automatic: bool
    action: FailureAction
    shared: bool


type Outcome = (
    FetchResult | Recovered | Failed | PolicyBlocked | RequestDeferred | FetchCancelled
)


class Crawler:
    def __init__(
        self, store: CrawlStore, report: Reporter, client: HttpClient | None = None
    ):
        self.store, self.report, self.custom_client = store, report, client
        self.routes = transports(store.config, store.root)
        self.by_id = {route.id: route for route in self.routes}
        self.policy = NetworkPolicy(store.root, store.config, self.routes, report)
        self.clients: dict[tuple[int, str], HttpClient] = {}
        self.pending: list[tuple[Job, Future[Outcome]]] = []
        self.saved = self.recovered = self.failed = 0
        self.total_bytes = store.progress.html_bytes
        self.stop_reason = "queue-exhausted"

    def _proxy(self, state: TaskRecord) -> str:
        if self.store.config.proxy_pool is not None:
            selected = self.by_id.get(state.proxy_id)
            if selected and not self.policy.state.transports[selected.id].disabled:
                return selected.id
            available = [
                route
                for route in self.routes
                if not self.policy.state.transports[route.id].disabled
            ]
            if available:
                return available[state.task.sequence % len(available)].id
        return state.proxy_id or self.routes[state.task.sequence % len(self.routes)].id

    def _select(self, count: int) -> list[Job]:
        if all(
            self.policy.state.transports[route.id].disabled for route in self.routes
        ):
            self.stop_reason = "no-healthy-transports"
            return []

        def ready(state: TaskRecord) -> bool:
            now = time.time()
            eligible = self.store.eligible_at(state)
            if eligible is None or eligible > now:
                return False
            proxy_id = self._proxy(state)
            if proxy_id not in self.by_id:
                return True
            eligible = self.policy.ready_at(proxy_id, state.task.url)
            return eligible is not None and eligible <= now

        states = self.store.queue.take(
            count,
            ready,
            on_scan=lambda scanned, selected: self.report(
                f"Scanning queue: {scanned:,} entries; {selected}/{count} tasks ready"
            ),
        )
        return [
            Job(
                state.task,
                self._proxy(state),
                self.by_id[self._proxy(state)].config
                if self._proxy(state) in self.by_id
                else self.store.config,
                state.cycle_attempts + 1,
            )
            for state in states
        ]

    def _failed(self, job: Job, error: HttpFailure) -> Failed:
        policy = job.config.failures
        delay = min(
            policy.max_retry_delay_seconds,
            policy.retry_delay_seconds * 2 ** min(job.attempt - 1, 20),
        )
        delay = max(delay, error.retry_after)
        rate_limit = error.reason == FailureReason.RATE_LIMIT
        if rate_limit:
            delay = max(60, delay)
        retry_at = (
            time.time() + delay
            if error.automatic
            or error.retry_after
            or error.reason == FailureReason.PROXY_ERROR
            else None
        )
        action = (
            policy.rate_limit
            if rate_limit
            else policy.temporary
            if error.automatic
            else policy.other
        )
        shared = rate_limit or error.retry_after > 0
        pause = (
            PauseState(reason=str(error), until=retry_at)
            if action == FailureAction.PAUSE
            else None
        )
        self.policy.defer(
            job.proxy_id, error.url, until=retry_at, pause=pause, shared=shared
        )
        return Failed(
            error,
            retry_at,
            error.automatic and job.attempt < policy.max_attempts,
            action,
            shared,
        )

    def _fetch(self, job: Job, client: HttpClient) -> Outcome:
        try:
            return client.fetch(job.task.url)
        except PolicyBlocked as exc:
            return exc
        except (RequestDeferred, FetchCancelled) as exc:
            return exc
        except HttpFailure as exc:
            return self._failed(job, exc)

    def _dispatch(self, executor: ThreadPoolExecutor, jobs: list[Job]) -> None:
        recovered = [(job, self.store.recover_packet(job.task)) for job in jobs]
        # Commit already saved packets before reserving any additional HTTP work.
        if any(packet is not None for _, packet in recovered):
            self.pending = []
            for job, packet in recovered:
                if packet:
                    future = Future()
                    future.set_result(Recovered(*packet))
                    self.pending.append((job, future))
            return
        self.pending = []
        for slot, job in enumerate(jobs):
            if job.proxy_id not in self.by_id:
                future = Future()
                future.set_result(
                    PolicyBlocked(
                        "proxy-unconfigured",
                        f"Proxy {job.proxy_id} is no longer configured. Restore it or explicitly reassign with crawl retry --proxy.",
                    )
                )
            elif (
                job.config.traversal.max_depth is not None
                and job.task.depth > job.config.traversal.max_depth
            ):
                future = Future()
                future.set_result(
                    PolicyBlocked(
                        "depth-limit",
                        f"Task depth {job.task.depth} exceeds current max_depth {job.config.traversal.max_depth}.",
                    )
                )
            else:
                if self.store.task(job.task.doc_id).proxy_id != job.proxy_id:
                    self.store.apply(
                        TaskAssigned(
                            seq=self.store.next_sequence,
                            doc_id=job.task.doc_id,
                            proxy_id=job.proxy_id,
                        )
                    )
                client = self.custom_client
                if client is None:
                    key = (slot, job.proxy_id)
                    if key not in self.clients:
                        self.clients[key] = HttpClient(
                            self.store.root,
                            self.by_id[job.proxy_id],
                            self.policy,
                            self.report,
                        )
                    client = self.clients[key]
                self.report(
                    f"{job.proxy_id}: depth {job.task.depth}; task #{job.task.sequence}: {job.task.url}"
                )
                future = executor.submit(self._fetch, job, client)
            self.pending.append((job, future))

    def _packet(
        self, job: Job, response: FetchResult
    ) -> tuple[DocumentMetadata, DocumentLinks]:
        encoding = response.encoding
        try:
            text = response.body.decode(encoding, errors="replace")
        except LookupError:
            encoding, text = "utf-8", response.body.decode("utf-8", errors="replace")
        links = discover_links(
            text, response.final_url, job.task.doc_id, job.task.depth, job.config
        )
        document = DocumentMetadata(
            doc_id=job.task.doc_id,
            requested_url=job.task.url,
            final_url=response.final_url,
            raw_path=f"raw/{job.task.doc_id}/{content_name(job.config.storage.compression)}",
            compression=job.config.storage.compression,
            fetched_at=utc_now(),
            http_status=response.status,
            content_type=response.headers.get("content-type", ""),
            encoding=encoding,
            size_bytes=len(response.body),
            sha256=hashlib.sha256(response.body).hexdigest(),
            depth=job.task.depth,
            discovered_from=job.task.parent_id,
            user_agent=job.config.http.user_agent,
            robots_strategy=job.config.robots.strategy,
            elapsed_seconds=round(response.elapsed, 6),
            response_headers=response.headers,
            proxy_id=job.proxy_id,
        )
        return self.store.save_packet(job.task, document, response.body, links), links

    def _accept(self, job: Job, outcome: Outcome, *, raise_pause: bool = True) -> None:
        task = job.task
        if isinstance(outcome, (RequestDeferred, FetchCancelled)):
            if isinstance(outcome, RequestDeferred):
                self.store.apply(
                    TaskWaiting(
                        seq=self.store.next_sequence,
                        doc_id=task.doc_id,
                        retry_at=outcome.retry_at,
                        error=str(outcome),
                    )
                )
            self.report(f"{job.proxy_id}: pending and retained: {task.url}")
            return
        if isinstance(outcome, PolicyBlocked):
            self.store.apply(
                TaskBlocked(
                    seq=self.store.next_sequence,
                    doc_id=task.doc_id,
                    reason=outcome.reason,
                    error=str(outcome),
                )
            )
            self.report(f"Blocked (retained): {outcome}")
            return
        if isinstance(outcome, (FetchResult, Recovered)):
            size = (
                len(outcome.body)
                if isinstance(outcome, FetchResult)
                else outcome.document.size_bytes
            )
            budget = job.config.traversal.max_total_bytes
            if budget is not None and self.total_bytes + size > budget:
                outcome = self._failed(
                    job,
                    HttpFailure(
                        "Response would exceed total byte budget; raise it and retry.",
                        FailureReason.BYTE_BUDGET,
                        url=task.url,
                        status=200,
                    ),
                )
            else:
                if isinstance(outcome, Recovered):
                    document, links = outcome.document, outcome.links
                    self.recovered += 1
                else:
                    document, links = self._packet(job, outcome)
                self.total_bytes += size
                self.store.commit_page(task, document, links)
                self.saved += 1
                self.report(
                    f"Saved {task.doc_id} via {document.proxy_id}: {size:,} bytes"
                )
                interval = self.store.config.storage.catalog_interval_pages
                if interval and self.saved % interval == 0:
                    self.report("Publishing catalog")
                    self.store.publish_catalog()
                return
        self.failed += 1
        error = outcome.error
        route = self.by_id.get(job.proxy_id)
        if (
            route
            and route.pooled
            and error.status is None
            and error.reason
            in {
                FailureReason.PROXY_ERROR,
                FailureReason.TIMEOUT,
                FailureReason.NETWORK_ERROR,
            }
        ):
            self.policy.disable(job.proxy_id, str(error))
            self.store.apply(
                TaskWaiting(
                    seq=self.store.next_sequence,
                    doc_id=task.doc_id,
                    retry_at=None,
                    error=f"Pool proxy disabled: {error}",
                )
            )
            self.report(
                f"{job.proxy_id}: disabled; task retained for another transport."
            )
            return
        self.store.apply(
            FetchFailed(
                seq=self.store.next_sequence,
                doc_id=task.doc_id,
                proxy_id=job.proxy_id,
                reason=error.reason,
                error=str(error),
                http_status=error.status,
                retry_at=outcome.retry_at,
                automatic_retry=outcome.automatic,
            )
        )
        self.report(
            f"{job.proxy_id}: deferred {task.doc_id} ({error.reason}, attempt {job.attempt}/{job.config.failures.max_attempts}): {error}"
        )
        if error.reason == FailureReason.BYTE_BUDGET:
            self.stop_reason = "byte-budget"
        if outcome.action == FailureAction.PAUSE and job.proxy_id == "direct":
            self.store.apply(
                Paused(
                    seq=self.store.next_sequence,
                    pause=PauseState(reason=str(error), until=outcome.retry_at),
                )
            )
            if raise_pause:
                raise CrawlPaused(f"{error}; task retained, direct transport paused.")

    def run(
        self,
        limit: int = 0,
        *,
        resume_paused: bool = False,
        resume_proxies: tuple[str, ...] = (),
    ) -> RunResult:
        config = self.store.config
        workers = 1 if self.custom_client is not None else config.parallel.workers
        executor = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="vinf-http"
        )
        try:
            if pause := self.store.progress.pause:
                expired = pause.until is not None and pause.until <= time.time()
                if not resume_paused and not expired:
                    raise CrawlPaused(
                        f"Dataset paused: {pause.reason}. Inspect and use --resume-paused."
                    )
                self.store.apply(Resumed(seq=self.store.next_sequence))
            if resume_paused:
                self.policy.resume()
            elif resume_proxies:
                self.policy.resume(resume_proxies)
            self.store.resume_discovery()
            self.store.queue.reprioritize()
            for document in self.store.iter_documents():
                if not contained_path(self.store.root, document.raw_path).is_file():
                    raise VinfError(
                        "A committed raw file is missing. Run doctor crawl."
                    )
            while not limit or self.saved < limit:
                count = workers if not limit else min(workers, limit - self.saved)
                if config.traversal.max_pages is not None:
                    count = min(
                        count,
                        config.traversal.max_pages
                        - self.store.progress.counts[TaskStatus.DOWNLOADED],
                    )
                    if count <= 0:
                        self.stop_reason = "page-budget"
                        break
                if config.traversal.max_total_bytes is not None:
                    remaining = config.traversal.max_total_bytes - self.total_bytes
                    if remaining <= 0:
                        self.stop_reason = "byte-budget"
                        break
                    count = min(
                        count, max(1, remaining // config.http.max_response_bytes)
                    )
                self.report("Selecting next tasks")
                jobs = self._select(count)
                if not jobs:
                    stats = self.store.statistics()
                    self.stop_reason = (
                        self.stop_reason
                        if self.stop_reason == "no-healthy-transports"
                        else (
                            "waiting-or-needs-retry"
                            if stats.pending or stats.deferred
                            else "queue-exhausted"
                        )
                    )
                    break
                self._dispatch(executor, jobs)
                for job, future in self.pending:
                    outcome = future.result()
                    job.handled = True
                    self._accept(job, outcome)
                self.pending = []
                if self.stop_reason == "byte-budget":
                    break
            else:
                self.stop_reason = "run-limit"
        finally:
            self.policy.cancelled.set()
            executor.shutdown(wait=True, cancel_futures=True)
            try:
                # Preserve completed downloads/failures after an interruption too.
                for job, future in self.pending:
                    if (
                        not job.handled
                        and not future.cancelled()
                        and future.exception() is None
                    ):
                        job.handled = True
                        self._accept(job, future.result(), raise_pause=False)
            finally:
                for client in self.clients.values():
                    client.close()
                if self.custom_client is not None:
                    self.custom_client.close()
                self.report("Publishing catalog")
                self.store.publish_catalog()
        return RunResult(
            statistics=self.store.statistics(),
            saved_this_run=self.saved,
            recovered_this_run=self.recovered,
            failed_attempts_this_run=self.failed,
            stop_reason=self.stop_reason,
        )
