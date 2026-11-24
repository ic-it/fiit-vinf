"""Offline checks for JSON task buckets, FIFO references and raw packets."""

from collections import Counter
from itertools import chain, groupby
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

from vinf_core.artifacts import iter_models
from vinf_core.files import contained_path, file_hash, read_model
from vinf_core.models import CatalogManifest, DocumentMetadata, VinfError
from vinf_core.sorting import ExternalSorter

from vinf_crawler.discovery import document_id, normalize_url
from vinf_crawler.models import DoctorReport, TaskStatus
from vinf_crawler.queue import is_active
from vinf_crawler.store import CrawlStore


class Findings:
    def __init__(self):
        self.errors, self.warnings = [], []

    def add(self, message: str, *, warning: bool = False):
        target = self.warnings if warning else self.errors
        if len(target) < 25:
            target.append(message)
        elif len(target) == 25:
            target.append("Further diagnostics omitted.")


def check_frontier(store: CrawlStore, findings: Findings) -> None:
    """Merge sorted UUID references with one task bucket at a time."""
    with TemporaryDirectory(prefix="vinf-frontier-check-") as temporary:
        references = ExternalSorter[UUID](
            Path(temporary) / "references",
            key=lambda identifier: (identifier.int,),
            encode=lambda identifier: identifier.bytes,
            decode=lambda body: UUID(bytes=body),
            memory_bytes=16 * 1024 * 1024,
        )
        for identifier in chain(store.progress.inflight, store.queue.entries()):
            references.add(identifier)
        ordered = references.values()
        current = next(ordered, None)
        for _, bucket in groupby(
            store.iter_tasks(), key=lambda record: record.task.doc_id.hex[:2]
        ):
            for record in sorted(bucket, key=lambda record: record.task.doc_id):
                identifier = record.task.doc_id
                while current is not None and current < identifier:
                    findings.add("FIFO references an unknown task.")
                    current = next(ordered, None)
                present = current == identifier
                while current == identifier:
                    current = next(ordered, None)
                if is_active(record) and not present:
                    findings.add(
                        "Active tasks are missing from the FIFO and interrupted claims."
                    )
        if current is not None:
            findings.add("FIFO references an unknown task.")
        ordered.close()


def check_dataset(root: Path, *, deep: bool = False) -> DoctorReport:
    store = CrawlStore(root)
    findings, counts = Findings(), Counter()
    head = store.progress
    if store.queue.pending_path.exists():
        findings.add("Unfinished queue write; crawl run will recover it.", warning=True)
    html_bytes = stored_bytes = 0
    for record in store.iter_tasks():
        counts[record.status] += 1
        if (
            normalize_url(record.task.url) != record.task.url
            or document_id(record.task.url) != record.task.doc_id
        ):
            findings.add(f"{record.task.doc_id}: invalid task URL identity.")
        if record.status == TaskStatus.DOWNLOADED:
            try:
                document, _ = store.read_packet(record.task, deep=deep)
                html_bytes += document.size_bytes
                stored_bytes += (
                    document.stored_size_bytes
                    if document.stored_size_bytes is not None
                    else document.size_bytes
                )
            except (VinfError, OSError, ValueError) as exc:
                findings.add(f"{record.task.doc_id}: {exc}")
        elif store.packet_path(record.task).exists():
            try:
                store.recover_packet(record.task)
                findings.add(
                    f"{record.task.doc_id}: saved packet will be recovered without HTTP.",
                    warning=True,
                )
            except (VinfError, OSError) as exc:
                findings.add(f"{record.task.doc_id}: invalid recovery packet: {exc}")
    if (
        any(counts[status] != head.counts[status] for status in TaskStatus)
        or sum(counts.values()) != head.count
    ):
        findings.add("Queue counters do not match task buckets.")
    if (html_bytes, stored_bytes) != (head.html_bytes, head.stored_html_bytes):
        findings.add("Byte counters do not match raw packets.")
    try:
        check_frontier(store, findings)
        if store.queue.path.stat().st_size != head.queue_bytes:
            findings.add("FIFO length differs from its saved state.")
        if store.queue.priority_path.stat().st_size != head.priority_bytes:
            findings.add("Priority FIFO length differs from its saved state.")
        limited = sum(
            1
            for shard in (store.queue.root / "limited").iterdir()
            for entry in shard.iterdir()
            if entry.is_file()
        )
        if limited != head.limited_count:
            findings.add("Limited-link counter does not match saved markers.")
        manifest = read_model(store.root / "catalog/manifest.json", CatalogManifest)
        catalog = contained_path(store.root, manifest.catalog_path)
        if file_hash(catalog) != manifest.catalog_sha256:
            findings.add("Catalog checksum mismatch.")
        count = 0
        current = manifest.journal_sequence == head.last_sequence
        for actual in iter_models(catalog, DocumentMetadata):
            count += 1
            if current and actual != store.document(actual.doc_id):
                findings.add("Catalog differs from committed raw metadata.")
        if (
            count != manifest.record_count
            or current
            and count != head.counts[TaskStatus.DOWNLOADED]
        ):
            findings.add("Catalog record count mismatch.")
        if not current:
            findings.add(
                "Catalog is behind the queue; next publish will rebuild it.",
                warning=True,
            )
    except (VinfError, OSError, ValueError) as exc:
        findings.add(f"Queue/catalog: {exc}")
    temporary = store.root / "tmp"
    if temporary.exists() and any(temporary.iterdir()):
        findings.add(
            "Uncommitted temporary packets exist; they are excluded from catalog.",
            warning=True,
        )
    return DoctorReport(
        ok=not findings.errors,
        deep=deep,
        errors=tuple(findings.errors),
        warnings=tuple(findings.warnings),
        statistics=store.statistics(),
    )
