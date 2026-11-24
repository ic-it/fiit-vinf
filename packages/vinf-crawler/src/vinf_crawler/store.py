"""Raw packets, JSON task buckets and a FIFO. No full collection in RAM."""

from __future__ import annotations

import hashlib
import os
import tempfile
import time
from collections.abc import Iterable, Iterator
from itertools import batched
from pathlib import Path
from uuid import UUID

from vinf_core.artifacts import write_models
from vinf_core.content import (
    content_name,
    encode_content,
    packet_json_path,
    read_content,
)
from vinf_core.files import (
    atomic_bytes,
    atomic_model,
    contained_path,
    file_hash,
    read_model,
    sync_dir,
)
from vinf_core.models import (
    CatalogManifest,
    DocumentLinks,
    DocumentMetadata,
    LinkDecision,
    VinfError,
)

from vinf_crawler.config import CrawlConfig
from vinf_crawler.discovery import (
    Scope,
    classify_links,
    document_id,
    normalize_url,
)
from vinf_crawler.models import (
    ConfigChanged,
    CrawlStats,
    CrawlTask,
    FetchFailed,
    Operation,
    PageCommitted,
    Paused,
    Resumed,
    TaskAssigned,
    TaskBlocked,
    TaskRecord,
    TaskSkipped,
    TasksRetried,
    TaskStatus,
    TaskWaiting,
    TransportStatus,
)
from vinf_crawler.network import NetworkState, TransportState
from vinf_crawler.queue import BATCH_SIZE, FileQueue


class CrawlStore:
    def __init__(self, root: Path, *, recover: bool = False):
        self.root = root.resolve()
        self.queue = FileQueue(self.root / "crawl", repair=recover)

    @classmethod
    def initialize(cls, root: Path, config: CrawlConfig) -> CrawlStore:
        seeds = [normalize_url(seed) for seed in config.seeds]
        if any(seed is None for seed in seeds):
            raise VinfError(
                "Every seed must be a valid HTTP(S) URL without credentials."
            )
        config = config.model_copy(update={"seeds": tuple(sorted(set(seeds)))})
        scope = Scope(config)
        for seed in config.seeds:
            if reason := scope.reason(seed):
                raise VinfError(
                    f"Seed is outside the configured scope ({reason}): {seed}"
                )
        if (root / "crawl/state.json").exists():
            raise VinfError("Dataset already exists. Use crawl run to continue it.")
        FileQueue.create(root / "crawl", config)
        store = cls(root)
        store.import_records(
            TaskRecord(
                task=CrawlTask(doc_id=document_id(url), url=url, depth=0, sequence=seq)
            )
            for seq, url in enumerate(config.seeds)
        )
        store.publish_catalog()
        return store

    @property
    def progress(self):
        return self.queue.head

    @property
    def config(self) -> CrawlConfig:
        return self.progress.config

    @property
    def next_sequence(self) -> int:
        return self.progress.last_sequence + 1

    def task(self, identifier: UUID) -> TaskRecord:
        return self.queue.get(identifier)

    def iter_tasks(
        self, statuses: set[TaskStatus] | None = None
    ) -> Iterator[TaskRecord]:
        yield from self.queue.records(statuses)

    def document(self, identifier: UUID) -> DocumentMetadata:
        record = self.task(identifier)
        if record.status != TaskStatus.DOWNLOADED:
            raise VinfError(f"Task has no committed document: {identifier}")
        return read_model(
            packet_json_path(self.packet_path(record.task), "metadata.json"),
            DocumentMetadata,
        )

    def iter_documents(self) -> Iterator[DocumentMetadata]:
        """Visit committed packets in bucket order, loading each bucket once."""
        if not self.progress.counts[TaskStatus.DOWNLOADED]:
            return
        for record in self.iter_tasks({TaskStatus.DOWNLOADED}):
            yield read_model(
                packet_json_path(self.packet_path(record.task), "metadata.json"),
                DocumentMetadata,
            )

    def eligible_at(self, record: TaskRecord) -> float | None:
        if (
            record.status == TaskStatus.PENDING
            or record.status == TaskStatus.DEFERRED
            and record.automatic_retry
        ):
            return record.retry_at or 0
        return None

    def next_task(self, now: float | None = None) -> CrawlTask | None:
        now = time.time() if now is None else now
        for record in self.queue.candidates(now):
            if self.eligible_at(record) <= now:
                return record.task
        return None

    def import_records(self, records: Iterable[TaskRecord]) -> None:
        """Stream canonical task records into an empty or extending queue."""
        for group in batched(records, BATCH_SIZE):
            documents = tuple(
                read_model(
                    packet_json_path(self.packet_path(record.task), "metadata.json"),
                    DocumentMetadata,
                )
                for record in group
                if record.status == TaskStatus.DOWNLOADED
            )
            self.queue.commit(
                records=group,
                documents=documents,
            )

    def apply(self, event: Operation) -> None:
        records, documents, changes = (), (), {}
        clear_limited = False
        if isinstance(
            event,
            (
                TaskAssigned,
                TaskWaiting,
                TaskBlocked,
                TaskSkipped,
                FetchFailed,
                PageCommitted,
            ),
        ):
            record = self.task(event.doc_id)
            if record.status == TaskStatus.DOWNLOADED:
                raise VinfError("Cannot modify an already committed document.")
            match event:
                case TaskAssigned():
                    record = record.model_copy(update={"proxy_id": event.proxy_id})
                case TaskWaiting():
                    record = record.model_copy(
                        update={
                            "status": TaskStatus.PENDING,
                            "reason": "cooldown",
                            "last_error": event.error,
                            "retry_at": max(record.retry_at or 0, event.retry_at or 0)
                            or None,
                        }
                    )
                case TaskBlocked():
                    record = record.model_copy(
                        update={
                            "status": TaskStatus.BLOCKED,
                            "reason": event.reason,
                            "last_error": event.error,
                            "automatic_retry": False,
                        }
                    )
                case TaskSkipped():
                    record = record.model_copy(
                        update={
                            "status": TaskStatus.SKIPPED,
                            "reason": event.reason,
                            "automatic_retry": False,
                        }
                    )
                case FetchFailed():
                    record = record.model_copy(
                        update={
                            "status": TaskStatus.DEFERRED,
                            "proxy_id": event.proxy_id,
                            "attempts": record.attempts + 1,
                            "cycle_attempts": record.cycle_attempts + 1,
                            "reason": str(event.reason),
                            "last_error": event.error,
                            "http_status": event.http_status,
                            "retry_at": event.retry_at,
                            "automatic_retry": event.automatic_retry,
                        }
                    )
                case PageCommitted():
                    document = read_model(
                        packet_json_path(
                            self.packet_path(record.task), "metadata.json"
                        ),
                        DocumentMetadata,
                    )
                    record = record.model_copy(
                        update={
                            "status": TaskStatus.DOWNLOADED,
                            "proxy_id": document.proxy_id,
                            "attempts": record.attempts + 1,
                            "cycle_attempts": record.cycle_attempts + 1,
                            "retry_at": None,
                            "automatic_retry": False,
                        }
                    )
                    documents = (document,)
            records = (record,)
        elif isinstance(event, TasksRetried):
            records = tuple(
                self.task(identifier).model_copy(
                    update={
                        "status": TaskStatus.PENDING,
                        "cycle_attempts": 0,
                        "automatic_retry": False,
                        **(
                            {"proxy_id": event.proxy_id}
                            if event.proxy_id is not None
                            else {}
                        ),
                    }
                )
                for identifier in event.doc_ids
            )
            if any(
                self.task(identifier).status == TaskStatus.DOWNLOADED
                for identifier in event.doc_ids
            ):
                raise VinfError("Cannot retry an already committed document.")
        elif isinstance(event, Paused):
            changes["pause"] = event.pause
        elif isinstance(event, Resumed):
            changes["pause"] = None
        elif isinstance(event, ConfigChanged):
            changes["config"] = event.config
            clear_limited = (
                self.config.scope != event.config.scope
                or self.config.traversal.model_dump(exclude={"prefer_url_patterns"})
                != event.config.traversal.model_dump(exclude={"prefer_url_patterns"})
            )
            if clear_limited:
                changes["rescan_pending"] = True
        self.queue.commit(
            event,
            records,
            documents=documents,
            changes=changes,
            clear_limited=clear_limited,
            finish=tuple(record.task.doc_id for record in records)
            if isinstance(
                event,
                (TaskWaiting, TaskBlocked, TaskSkipped, FetchFailed, PageCommitted),
            )
            else (),
        )

    def packet_path(self, task: CrawlTask) -> Path:
        return self.root / "raw" / str(task.doc_id)

    def save_packet(
        self,
        task: CrawlTask,
        document: DocumentMetadata,
        body: bytes,
        links: DocumentLinks,
    ) -> DocumentMetadata:
        destination = self.packet_path(task)
        if destination.exists():
            raise VinfError(f"Refusing to overwrite a saved packet: {destination}")
        temporary_root = self.root / "tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix="packet-", dir=temporary_root))
        payload = encode_content(body, document.compression)
        if document.compression != "none":
            document = document.model_copy(update={"stored_size_bytes": len(payload)})
        if (
            document.raw_path
            != f"raw/{task.doc_id}/{content_name(document.compression)}"
        ):
            raise VinfError("Raw path does not match its compression format.")
        atomic_bytes(temporary / content_name(document.compression), payload)
        json_suffix = ".gz" if document.compression != "none" else ""
        atomic_model(temporary / f"metadata.json{json_suffix}", document)
        atomic_model(temporary / f"links.json{json_suffix}", links)
        sync_dir(temporary)
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temporary, destination)
        sync_dir(destination.parent)
        sync_dir(self.root)
        return document

    def read_packet(
        self, task: CrawlTask, *, deep: bool = True
    ) -> tuple[DocumentMetadata, DocumentLinks]:
        directory = self.packet_path(task)
        document = read_model(
            packet_json_path(directory, "metadata.json"), DocumentMetadata
        )
        links = read_model(packet_json_path(directory, "links.json"), DocumentLinks)
        raw = contained_path(self.root, document.raw_path)
        if (
            document.doc_id != task.doc_id
            or links.doc_id != task.doc_id
            or document.requested_url != task.url
            or raw != directory / content_name(document.compression)
        ):
            raise VinfError(f"Packet identity mismatch: {directory}")
        expected_size = (
            document.stored_size_bytes
            if document.stored_size_bytes is not None
            else (document.size_bytes if document.compression == "none" else None)
        )
        if expected_size is not None and raw.stat().st_size != expected_size:
            raise VinfError(f"Raw packet checksum mismatch: {directory}")
        if deep:
            read_content(self.root, document)
        for link in links.links:
            if link.url and (
                normalize_url(link.url) != link.url
                or document_id(link.url) != link.doc_id
            ):
                raise VinfError(f"Invalid saved link identity: {link.url}")
        return document, links

    def recover_packet(
        self, task: CrawlTask
    ) -> tuple[DocumentMetadata, DocumentLinks] | None:
        return self.read_packet(task) if self.packet_path(task).exists() else None

    def schedule_links(self, task: CrawlTask, links: DocumentLinks) -> None:
        records, limited, seen = [], [], set()

        def flush():
            if records or limited:
                self.queue.commit(
                    records=records,
                    limited=limited,
                )
                records.clear()
                limited.clear()
                seen.clear()

        for group in batched(
            classify_links(links.links, task.depth, self.config), BATCH_SIZE
        ):
            known = self.queue.existing(
                link.doc_id for link in group if link.doc_id is not None
            )
            for link in group:
                if link.doc_id is None or link.doc_id in seen or link.doc_id in known:
                    continue
                if link.decision == LinkDecision.DEFERRED:
                    limited.append(link.doc_id)
                    seen.add(link.doc_id)
                elif link.decision == LinkDecision.ALLOWED:
                    if link.url is None:
                        raise VinfError("An allowed link must have a normalized URL.")
                    records.append(
                        TaskRecord(
                            task=CrawlTask(
                                doc_id=link.doc_id,
                                url=link.url,
                                depth=task.depth + 1,
                                sequence=self.progress.count + len(records),
                                parent_id=task.doc_id,
                            )
                        )
                    )
                    seen.add(link.doc_id)
                known.add(link.doc_id)
                if len(records) == BATCH_SIZE or len(limited) == BATCH_SIZE:
                    flush()
        flush()

    def commit_page(
        self, task: CrawlTask, document: DocumentMetadata, links: DocumentLinks
    ) -> None:
        # Persist discoveries before marking the parent done. An interrupted parent
        # still has its raw packet, so recovery can finish deduplicated discovery.
        self.schedule_links(task, links)
        self.apply(PageCommitted(seq=self.next_sequence, doc_id=document.doc_id))

    def change_config(self, config: CrawlConfig) -> None:
        if (
            tuple(sorted(normalize_url(seed) or "" for seed in config.seeds))
            != self.config.seeds
        ):
            raise VinfError(
                "Seeds are fixed for this dataset. Create a new dataset for different seeds."
            )
        config = config.model_copy(update={"seeds": self.config.seeds})
        for seed in config.seeds:
            if reason := Scope(config).reason(seed):
                raise VinfError(f"Updated scope excludes seed ({reason}): {seed}")
        self.apply(ConfigChanged(seq=self.next_sequence, config=config))
        self.resume_discovery()
        self.queue.reprioritize()

    def resume_discovery(self) -> None:
        if not self.progress.rescan_pending:
            return
        for document in self.iter_documents():
            record = self.task(document.doc_id)
            _, links = self.read_packet(record.task, deep=False)
            self.schedule_links(record.task, links)
        self.queue.commit(changes={"rescan_pending": False})

    def retry(
        self,
        *,
        doc_id: UUID | None = None,
        reason: str | None = None,
        statuses: tuple[TaskStatus, ...] = (TaskStatus.DEFERRED,),
        proxy_id: str | None = None,
    ) -> int:
        allowed = {proxy.id for proxy in self.config.proxies} or {"direct"}
        if proxy_id is not None and proxy_id not in allowed:
            raise VinfError(f"Proxy {proxy_id} is not configured.")
        candidates = (
            (self.task(doc_id),)
            if doc_id is not None
            else self.iter_tasks(set(statuses))
        )
        identifiers, count = [], 0
        for record in candidates:
            if (
                record.status == TaskStatus.DOWNLOADED
                or reason is not None
                and record.reason != reason
            ):
                continue
            identifiers.append(record.task.doc_id)
            count += 1
            if len(identifiers) == BATCH_SIZE:
                self.apply(
                    TasksRetried(
                        seq=self.next_sequence,
                        doc_ids=tuple(identifiers),
                        proxy_id=proxy_id,
                    )
                )
                identifiers.clear()
        if identifiers:
            self.apply(
                TasksRetried(
                    seq=self.next_sequence,
                    doc_ids=tuple(identifiers),
                    proxy_id=proxy_id,
                )
            )
        return count

    def skip(self, doc_id: UUID, reason: str) -> None:
        self.apply(TaskSkipped(seq=self.next_sequence, doc_id=doc_id, reason=reason))

    def publish_catalog(self, *, force: bool = False) -> None:
        catalog = self.root / "catalog/documents.jsonl"
        manifest_path = self.root / "catalog/manifest.json"
        if not force and catalog.is_file() and manifest_path.is_file():
            manifest = read_model(manifest_path, CatalogManifest)
            # Committed packets are immutable. Queue-only operations cannot
            # change their catalog, so only advance its queue checkpoint.
            if manifest.record_count == self.progress.counts[TaskStatus.DOWNLOADED]:
                if manifest.journal_sequence != self.progress.last_sequence:
                    atomic_model(
                        manifest_path,
                        manifest.model_copy(
                            update={"journal_sequence": self.progress.last_sequence}
                        ),
                    )
                return
        digest, count = hashlib.sha256(), 0

        def documents():
            nonlocal count
            for document in self.iter_documents():
                if count:
                    digest.update(b"\n")
                digest.update(f"{document.doc_id}:{document.sha256}".encode())
                count += 1
                yield document

        write_models(catalog, documents())
        atomic_model(
            manifest_path,
            CatalogManifest(
                record_count=count,
                catalog_sha256=file_hash(catalog),
                input_fingerprint=digest.hexdigest(),
                journal_sequence=self.progress.last_sequence,
            ),
        )

    def statistics(self) -> CrawlStats:
        head, now = self.progress, time.time()
        network_path = self.root / "crawl/network.json"
        network = (
            read_model(network_path, NetworkState)
            if network_path.exists()
            else NetworkState()
        )
        summaries = []
        # HTTP retry waits are persisted in the network policy. Reading status
        # must not visit queue entries or task files, even for a large frontier.
        deadline = None
        if head.counts[TaskStatus.PENDING] or head.counts[TaskStatus.DEFERRED]:
            for transport in network.transports.values():
                for clock in transport.origins.values():
                    if clock.cooldown_until > now:
                        deadline = (
                            min(deadline, clock.cooldown_until)
                            if deadline is not None
                            else clock.cooldown_until
                        )
        configured = {proxy.id: proxy for proxy in self.config.proxies}
        proxy_ids = set(configured)
        if self.config.proxy_pool:
            proxy_ids.update(
                identifier
                for identifier in network.transports
                if identifier.startswith("pool-")
            )
        transport_ids = set(proxy_ids)
        if (
            not transport_ids
            or self.config.proxy_pool
            and self.config.proxy_pool.fallback_direct
        ):
            transport_ids.add("direct")
        for proxy_id in sorted(transport_ids):
            proxy = configured.get(proxy_id)
            transport = network.transports.get(proxy_id, TransportState())
            future = [
                value.cooldown_until
                for value in (*network.origins.values(), *transport.origins.values())
                if value.cooldown_until > now
            ]
            if (
                transport.pause
                and transport.pause.until
                and transport.pause.until > now
            ):
                future.append(transport.pause.until)
            if future and (
                head.counts[TaskStatus.PENDING] or head.counts[TaskStatus.DEFERRED]
            ):
                deadline = (
                    min(deadline, *future) if deadline is not None else min(future)
                )
            summaries.append(
                TransportStatus(
                    proxy_id=proxy_id,
                    url_env=proxy.url_env if proxy else None,
                    robots_strategy=(proxy.robots_strategy if proxy else None)
                    or self.config.robots.strategy,
                    minimum_delay_seconds=proxy.delay_seconds
                    if proxy and proxy.delay_seconds is not None
                    else self.config.http.delay_seconds,
                    pause=transport.pause,
                    cooldown_until=max(future, default=None),
                    disabled=transport.disabled,
                )
            )
        return CrawlStats(
            dataset=str(self.root),
            discovered=head.count,
            downloaded=head.counts[TaskStatus.DOWNLOADED],
            pending=head.counts[TaskStatus.PENDING],
            deferred=head.counts[TaskStatus.DEFERRED],
            blocked=head.counts[TaskStatus.BLOCKED],
            skipped=head.counts[TaskStatus.SKIPPED],
            limited_links=head.limited_count,
            html_bytes=head.html_bytes,
            stored_html_bytes=head.stored_html_bytes,
            complete=head.counts[TaskStatus.DOWNLOADED] == head.count
            and not head.limited_count
            and head.pause is None,
            next_retry_at=deadline,
            robots_strategy=self.config.robots.strategy,
            minimum_delay_seconds=self.config.http.delay_seconds,
            rate_limit_scope=self.config.http.rate_limit_scope,
            pause=head.pause,
            operation_sequence=head.last_sequence,
            pending_write=self.queue.pending_path.exists(),
            workers=self.config.parallel.workers,
            proxy_ids=tuple(sorted(proxy_ids)),
            transports=tuple(summaries),
        )
