"""JSON task buckets with a bounded cache and an append-only FIFO of UUIDs."""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Sequence
from itertools import groupby
from pathlib import Path
from typing import Self
from uuid import UUID

from pydantic import AwareDatetime, Field, RootModel, model_validator
from vinf_core.files import atomic_bytes, atomic_model, read_model, sync_dir
from vinf_core.models import DocumentMetadata, Model, PauseState, VinfError

from vinf_crawler.config import CrawlConfig
from vinf_crawler.models import Operation, TaskRecord, TaskStatus, utc_now

BATCH_SIZE = 1024
CACHE_BUCKETS = 8
QUEUE_ENTRY_BYTES = 39  # JSON quotes, 36 ASCII UUID characters, and a newline.


class TaskBucket(RootModel[dict[UUID, TaskRecord]]):
    @model_validator(mode="after")
    def check_identity(self) -> Self:
        if any(
            identifier != record.task.doc_id for identifier, record in self.root.items()
        ):
            raise ValueError("Task bucket UUID does not match its record.")
        return self


class QueueState(Model):
    created_at: AwareDatetime = Field(default_factory=utc_now)
    config: CrawlConfig
    count: int = 0
    counts: dict[TaskStatus, int] = Field(
        default_factory=lambda: dict.fromkeys(TaskStatus, 0)
    )
    queue_bytes: int = 0
    cursor: int = 0
    priority_bytes: int = 0
    priority_cursor: int = 0
    ranked_patterns: tuple[str, ...] = ()
    inflight: tuple[UUID, ...] = ()
    last_sequence: int = 0
    html_bytes: int = 0
    stored_html_bytes: int = 0
    limited_count: int = 0
    pause: PauseState | None = None
    rescan_pending: bool = False


class PendingWrite(Model):
    state: QueueState
    records: tuple[TaskRecord, ...] = ()
    append_at: int
    enqueue: tuple[UUID, ...] = ()
    priority_append_at: int = 0
    priority_enqueue: tuple[UUID, ...] = ()
    replace_queues: bool = False
    limited_add: tuple[UUID, ...] = ()
    limited_remove: tuple[UUID, ...] = ()
    clear_limited: bool = False


def is_active(record: TaskRecord) -> bool:
    return (
        record.status == TaskStatus.PENDING
        or record.status == TaskStatus.DEFERRED
        and record.automatic_retry
    )


class FileQueue:
    def __init__(self, crawl: Path, *, repair: bool = False):
        self.root = crawl
        self.path = crawl / "queue.jsonl"
        self.priority_path = crawl / "priority.jsonl"
        self.state_path = crawl / "state.json"
        self.pending_path = crawl / "pending.json"
        self._cache: OrderedDict[str, TaskBucket] = OrderedDict()
        self._known: OrderedDict[str, tuple[frozenset[int], int]] = OrderedDict()
        self._known_bytes = 0
        self._known_limit = 0
        if repair and self.pending_path.exists():
            self._install(read_model(self.pending_path, PendingWrite))
        if repair and not self.pending_path.exists():
            for name in (
                "queue.reorder.jsonl",
                "priority.reorder.jsonl",
                ".queue.replace",
                ".priority.replace",
            ):
                (crawl / name).unlink(missing_ok=True)
        self.head = read_model(self.state_path, QueueState)
        self._resize_membership_cache()

    @classmethod
    def create(cls, crawl: Path, config: CrawlConfig) -> FileQueue:
        crawl.mkdir(parents=True, exist_ok=True)
        (crawl / "tasks").mkdir()
        for prefix in range(256):
            atomic_bytes(crawl / "tasks" / f"{prefix:02x}.json", b"{}\n")
        (crawl / "limited").mkdir()
        (crawl / "lock").touch()
        atomic_bytes(crawl / "queue.jsonl", b"")
        atomic_bytes(crawl / "priority.jsonl", b"")
        atomic_model(
            crawl / "state.json",
            QueueState(
                config=config, ranked_patterns=config.traversal.prefer_url_patterns
            ),
        )
        return cls(crawl)

    def bucket_path(self, prefix: str) -> Path:
        return self.root / "tasks" / f"{prefix}.json"

    def _remember(self, prefix: str, bucket: TaskBucket) -> TaskBucket:
        self._cache[prefix] = bucket
        self._cache.move_to_end(prefix)
        if len(self._cache) > CACHE_BUCKETS:
            self._cache.popitem(last=False)
        previous = self._known.pop(prefix, None)
        if previous is not None:
            self._known_bytes -= previous[1]
        if self._known_limit:
            identifiers = frozenset(identifier.int for identifier in bucket.root)
            size = (
                sys.getsizeof(identifiers)
                + sum(map(sys.getsizeof, identifiers))
                + sys.getsizeof(prefix)
                + 128  # Allow for the LRU entry and its size accounting.
            )
            if size <= self._known_limit:
                self._known[prefix] = identifiers, size
                self._known_bytes += size
                self._trim_membership_cache()
        return bucket

    def _trim_membership_cache(self) -> None:
        while self._known_bytes > self._known_limit:
            _, (_, size) = self._known.popitem(last=False)
            self._known_bytes -= size

    def _resize_membership_cache(self) -> None:
        self._known_limit = self.head.config.storage.membership_cache_mib * 1024**2
        self._trim_membership_cache()

    def _bucket(self, prefix: str) -> TaskBucket:
        if prefix in self._cache:
            self._cache.move_to_end(prefix)
            return self._cache[prefix]
        bucket = read_model(self.bucket_path(prefix), TaskBucket)
        if any(identifier.hex[:2] != prefix for identifier in bucket.root):
            raise VinfError(f"Task is stored in the wrong bucket: {prefix}")
        return self._remember(prefix, bucket)

    def limited_path(self, identifier: UUID) -> Path:
        return self.root / "limited" / identifier.hex[:2] / str(identifier)

    def contains(self, identifier: UUID) -> bool:
        prefix = identifier.hex[:2]
        if prefix in self._known:
            self._known.move_to_end(prefix)
            return identifier.int in self._known[prefix][0]
        return identifier in self._bucket(prefix).root

    def existing(self, identifiers: Iterable[UUID]) -> set[UUID]:
        """Check exact membership without loading task DTOs on cache hits."""
        return {
            identifier
            for identifier in sorted(set(identifiers), key=lambda item: item.hex[:2])
            if self.contains(identifier)
        }

    def lookup(self, identifiers: Iterable[UUID]) -> dict[UUID, TaskRecord]:
        """Look up a caller-bounded batch, opening each requested bucket once."""
        found = {}
        for prefix, group in groupby(
            sorted(set(identifiers), key=lambda identifier: identifier.hex[:2]),
            key=lambda identifier: identifier.hex[:2],
        ):
            bucket = self._bucket(prefix)
            for identifier in group:
                if identifier in bucket.root:
                    found[identifier] = bucket.root[identifier]
        return found

    def get(self, identifier: UUID) -> TaskRecord:
        try:
            return self._bucket(identifier.hex[:2]).root[identifier]
        except KeyError as exc:
            raise VinfError(f"Unknown crawl task: {identifier}") from exc

    def records(self, statuses: set[TaskStatus] | None = None) -> Iterator[TaskRecord]:
        for prefix in range(256):
            for record in self._bucket(f"{prefix:02x}").root.values():
                if statuses is None or record.status in statuses:
                    yield record

    def _items(
        self, cursor: int, end: int, *, priority: bool = False
    ) -> Iterator[tuple[UUID, int]]:
        with (self.priority_path if priority else self.path).open("rb") as stream:
            stream.seek(cursor)
            while stream.tell() < end:
                line = stream.readline()
                if not line.endswith(b"\n") or stream.tell() > end:
                    raise VinfError("Incomplete FIFO entry; recover with crawl run.")
                yield UUID(json.loads(line)), stream.tell()

    def entries(self) -> Iterator[UUID]:
        for priority in (True, False):
            cursor, end = (
                (self.head.priority_cursor, self.head.priority_bytes)
                if priority
                else (self.head.cursor, self.head.queue_bytes)
            )
            yield from (
                identifier
                for identifier, _ in self._items(cursor, end, priority=priority)
            )

    def reprioritize(self) -> None:
        """Repartition waiting entries after an explicit preference change."""
        patterns = self.head.config.traversal.prefer_url_patterns
        if self.head.ranked_patterns == patterns:
            return
        rules = tuple(re.compile(pattern) for pattern in patterns)
        preferred = (
            {
                record.task.doc_id
                for record in self.records()
                if any(rule.search(record.task.url) for rule in rules)
            }
            if rules
            else set()
        )
        normal_path, priority_path = (
            self.root / "queue.reorder.jsonl",
            self.root / "priority.reorder.jsonl",
        )
        with normal_path.open("wb") as normal, priority_path.open("wb") as important:
            for identifier in self.entries():
                target = important if identifier in preferred else normal
                target.write((json.dumps(str(identifier)) + "\n").encode())
            for stream in (normal, important):
                stream.flush()
                os.fsync(stream.fileno())
        state = self.head.model_copy(
            update={
                "cursor": 0,
                "priority_cursor": 0,
                "queue_bytes": normal_path.stat().st_size,
                "priority_bytes": priority_path.stat().st_size,
                "ranked_patterns": patterns,
                "last_sequence": self.head.last_sequence + 1,
            }
        )
        transaction = PendingWrite(state=state, append_at=0, replace_queues=True)
        sync_dir(self.root)
        atomic_model(self.pending_path, transaction)
        self._install(transaction)

    def candidates(self, now: float) -> Iterator[TaskRecord]:
        claimed = set(self.head.inflight)
        for identifier in self.head.inflight:
            record = self.get(identifier)
            if is_active(record) and (record.retry_at or 0) <= now:
                yield record
        for identifier in self.entries():
            if identifier not in claimed:
                record = self.get(identifier)
                if is_active(record) and (record.retry_at or 0) <= now:
                    yield record

    def take(
        self,
        count: int,
        ready: Callable[[TaskRecord], bool],
        *,
        on_scan: Callable[[int, int], None] | None = None,
    ) -> list[TaskRecord]:
        """Claim a bounded worker batch. Interrupted claims remain in state.json."""
        ends = (self.head.priority_bytes, self.head.queue_bytes)
        selected, keep, rotate = [], [], []
        inspected = 0
        for identifier in self.head.inflight:
            record = self.get(identifier)
            if is_active(record):
                if len(selected) < count and ready(record):
                    selected.append(record)
                    keep.append(identifier)
                elif len(selected) >= count:
                    keep.append(identifier)
                else:
                    rotate.append(identifier)
        if tuple(keep) != self.head.inflight:
            self.commit(enqueue=rotate, changes={"inflight": tuple(keep)})
        if len(selected) >= count:
            return selected
        claimed = set(keep)
        for priority, end in zip((True, False), ends, strict=True):
            key = "priority_cursor" if priority else "cursor"
            scanned, rotated = 0, []
            for identifier, cursor in self._items(
                getattr(self.head, key), end, priority=priority
            ):
                record = self.get(identifier)
                scanned += 1
                inspected += 1
                if identifier not in claimed and is_active(record):
                    if ready(record):
                        selected.append(record)
                        keep.append(identifier)
                        claimed.add(identifier)
                    else:
                        rotated.append(identifier)
                if scanned == BATCH_SIZE or len(selected) == count or cursor == end:
                    self.commit(
                        enqueue=rotated, changes={key: cursor, "inflight": tuple(keep)}
                    )
                    if on_scan is not None:
                        on_scan(inspected, len(selected))
                    scanned, rotated = 0, []
                if len(selected) == count:
                    return selected
        return selected

    def _install(self, transaction: PendingWrite) -> None:
        # The pending description is written first. Repeating these replacements
        # and writing the same queue suffix is safe after a process interruption.
        if transaction.replace_queues:
            for name, path in (("queue", self.path), ("priority", self.priority_path)):
                replacement = self.root / f".{name}.replace"
                replacement.unlink(missing_ok=True)
                os.link(self.root / f"{name}.reorder.jsonl", replacement)
                os.replace(replacement, path)
                sync_dir(self.root)
        if transaction.clear_limited:
            shutil.rmtree(self.root / "limited", ignore_errors=True)
            (self.root / "limited").mkdir()
        for prefix, records in groupby(
            sorted(transaction.records, key=lambda record: record.task.doc_id.hex[:2]),
            key=lambda record: record.task.doc_id.hex[:2],
        ):
            updated = self._bucket(prefix).model_copy(deep=False)
            updated.root = {
                **updated.root,
                **{record.task.doc_id: record for record in records},
            }
            body = updated.model_dump_json(exclude_defaults=True) + "\n"
            atomic_bytes(self.bucket_path(prefix), body.encode())
            self._remember(prefix, updated)
        for identifier in transaction.limited_add:
            atomic_bytes(self.limited_path(identifier), b"")
        for identifier in transaction.limited_remove:
            path = self.limited_path(identifier)
            path.unlink(missing_ok=True)
            sync_dir(path.parent)
        for path, offset, identifiers, size in (
            (
                self.path,
                transaction.append_at,
                transaction.enqueue,
                transaction.state.queue_bytes,
            ),
            (
                self.priority_path,
                transaction.priority_append_at,
                transaction.priority_enqueue,
                transaction.state.priority_bytes,
            ),
        ):
            if not identifiers:
                continue
            with path.open("r+b") as stream:
                stream.seek(offset)
                for identifier in identifiers:
                    stream.write((json.dumps(str(identifier)) + "\n").encode())
                stream.truncate(size)
                stream.flush()
                os.fsync(stream.fileno())
        # atomic_bytes already syncs each replaced bucket's directory.
        if transaction.clear_limited or transaction.limited_add:
            sync_dir(self.root / "limited")
        atomic_model(self.state_path, transaction.state)
        self.pending_path.unlink(missing_ok=True)
        if transaction.replace_queues:
            for name in ("queue.reorder.jsonl", "priority.reorder.jsonl"):
                (self.root / name).unlink(missing_ok=True)
        sync_dir(self.root)
        self.head = transaction.state
        self._resize_membership_cache()

    def commit(
        self,
        event: Operation | None = None,
        records: Sequence[TaskRecord] = (),
        *,
        documents: Sequence[DocumentMetadata] = (),
        limited: Sequence[UUID] = (),
        clear_limited: bool = False,
        changes: dict | None = None,
        finish: Sequence[UUID] = (),
        enqueue: Sequence[UUID] = (),
    ) -> None:
        if self.pending_path.exists():
            raise VinfError("Recover the interrupted queue write with crawl run first.")
        if event is not None and event.seq != self.head.last_sequence + 1:
            raise VinfError("Discontinuous queue operation sequence.")
        if max(len(records), len(limited), len(enqueue)) > BATCH_SIZE:
            raise VinfError("Queue write exceeds its bounded batch size.")
        state = self.head.model_copy(deep=True, update=changes or {})
        if clear_limited:
            state = state.model_copy(update={"limited_count": 0})
        previous_records = self.lookup(record.task.doc_id for record in records)
        appended, added, removed, seen = list(enqueue), [], [], set()
        for record in records:
            identifier = record.task.doc_id
            if identifier in seen:
                raise VinfError(f"Duplicate task in queue write: {identifier}")
            seen.add(identifier)
            previous = previous_records.get(identifier)
            if previous is None:
                if record.task.sequence != state.count:
                    raise VinfError("Discontinuous discovery order.")
                state = state.model_copy(update={"count": state.count + 1})
            elif previous.task != record.task:
                raise VinfError("Cannot change immutable task identity.")
            if previous is not None:
                state.counts[previous.status] -= 1
            state.counts[record.status] += 1
            if is_active(record) and (
                previous is None or not is_active(previous) or identifier in finish
            ):
                appended.append(identifier)
            if not clear_limited and self.limited_path(identifier).exists():
                removed.append(identifier)
                state = state.model_copy(
                    update={"limited_count": state.limited_count - 1}
                )
        for identifier in limited:
            if (
                identifier not in seen
                and not self.contains(identifier)
                and not self.limited_path(identifier).exists()
            ):
                added.append(identifier)
                seen.add(identifier)
                state = state.model_copy(
                    update={"limited_count": state.limited_count + 1}
                )
        for document in documents:
            state = state.model_copy(
                update={
                    "html_bytes": state.html_bytes + document.size_bytes,
                    "stored_html_bytes": state.stored_html_bytes
                    + (
                        document.stored_size_bytes
                        if document.stored_size_bytes is not None
                        else document.size_bytes
                    ),
                }
            )
        if finish:
            state = state.model_copy(
                update={
                    "inflight": tuple(
                        identifier
                        for identifier in state.inflight
                        if identifier not in finish
                    )
                }
            )
        rules = tuple(
            re.compile(pattern)
            for pattern in state.config.traversal.prefer_url_patterns
        )
        waiting = (
            {
                **self.lookup(appended),
                **{record.task.doc_id: record for record in records},
            }
            if rules
            else {}
        )
        normal, important = [], []
        for identifier in appended:
            target = (
                important
                if any(rule.search(waiting[identifier].task.url) for rule in rules)
                else normal
            )
            target.append(identifier)
        state = state.model_copy(
            update={
                "queue_bytes": state.queue_bytes + QUEUE_ENTRY_BYTES * len(normal),
                "priority_bytes": state.priority_bytes
                + QUEUE_ENTRY_BYTES * len(important),
                "last_sequence": self.head.last_sequence + 1,
            }
        )
        transaction = PendingWrite(
            state=state,
            records=tuple(records),
            append_at=self.head.queue_bytes,
            enqueue=tuple(normal),
            priority_append_at=self.head.priority_bytes,
            priority_enqueue=tuple(important),
            limited_add=tuple(added),
            limited_remove=tuple(removed),
            clear_limited=clear_limited,
        )
        atomic_model(self.pending_path, transaction)
        self._install(transaction)
