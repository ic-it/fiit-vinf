"""Lossless crawl storage conversion, with checksum verification and no HTTP."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from uuid import UUID, uuid4

from vinf_core.content import content_name, packet_json_path, read_content
from vinf_core.files import (
    atomic_model,
    dataset_lock,
    read_lock,
    separate_roots,
    sync_dir,
)
from vinf_core.models import Compression, Model, VinfError

from vinf_crawler.checks import check_dataset
from vinf_crawler.config import CrawlConfig
from vinf_crawler.models import (
    ConfigChanged,
    TaskStatus,
)
from vinf_crawler.store import CrawlStore


class CrawlExportResult(Model):
    output: str
    compression: Compression
    packets: int
    html_bytes: int
    stored_html_bytes: int
    content_verified: bool
    backup: str | None = None


def export_crawl(
    source: Path, output: Path, compression: Compression = Compression.GZIP
) -> CrawlExportResult:
    with read_lock(source / "crawl/lock"):
        return _export_crawl(source, output, compression)


def _export_crawl(
    source: Path, output: Path, compression: Compression
) -> CrawlExportResult:
    source, output = source.resolve(), output.resolve()
    separate_roots(source, output)
    if output.exists():
        raise VinfError("Export destination already exists; choose a new directory.")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".crawl-export-", dir=output.parent
    ) as temporary:
        store = CrawlStore(source)
        if store.queue.pending_path.exists():
            raise VinfError(
                "Recover the interrupted source queue with crawl run before exporting."
            )
        staged = Path(temporary) / "crawled"
        shutil.copytree(
            source / "crawl", staged / "crawl", ignore=shutil.ignore_patterns("lock")
        )
        (staged / "crawl/lock").touch()

        def settings(config: CrawlConfig) -> CrawlConfig:
            return config.model_copy(
                update={
                    "storage": config.storage.model_copy(
                        update={"compression": compression}
                    )
                }
            )

        writer = CrawlStore(staged)
        packets = html_bytes = stored_bytes = committed_stored_bytes = 0
        for packet in (source / "raw").glob("*"):
            record = store.task(UUID(packet.name))
            document, links = store.read_packet(record.task, deep=True)
            body = read_content(source, document)
            document = document.model_copy(
                update={
                    "raw_path": f"raw/{document.doc_id}/{content_name(compression)}",
                    "compression": compression,
                    "stored_size_bytes": None,
                }
            )
            document = writer.save_packet(record.task, document, body, links)
            size = (
                document.stored_size_bytes
                if document.stored_size_bytes is not None
                else document.size_bytes
            )
            packets += 1
            html_bytes += document.size_bytes
            stored_bytes += size
            if record.status == TaskStatus.DOWNLOADED:
                committed_stored_bytes += size
        config = settings(writer.config)
        writer.queue.commit(
            ConfigChanged(seq=writer.next_sequence, config=config),
            changes={"config": config, "stored_html_bytes": committed_stored_bytes},
        )
        exported = CrawlStore(staged)
        exported.publish_catalog()
        for name in ("sample-plan.json", "seed-selection.json"):
            if (source / name).is_file():
                shutil.copy2(source / name, staged / name)
        atomic_model(staged / "config.json", exported.config)
        check = check_dataset(staged, deep=True)
        if not check.ok:
            raise VinfError(f"Compressed export failed verification: {check.errors}")
        result = CrawlExportResult(
            output=str(output),
            compression=compression,
            packets=packets,
            html_bytes=html_bytes,
            stored_html_bytes=stored_bytes,
            content_verified=True,
        )
        atomic_model(staged / "export.json", result)
        if output.exists():
            raise VinfError(
                "Export destination appeared while copying; refusing to overwrite."
            )
        os.rename(staged, output)
        sync_dir(output.parent)
    return result


def repack_crawl(
    source: Path, compression: Compression, *, keep_backup: bool = True
) -> CrawlExportResult:
    """Replace verified storage and optionally retain the original representation."""
    source = source.resolve()
    if not (source / "crawl/state.json").is_file():
        raise VinfError(f"Not a crawl dataset: {source}")
    with dataset_lock(source):
        store = CrawlStore(source)
        # Refuse to lose user files inside raw packets during a directory swap.
        for packet in (source / "raw").glob("*"):
            try:
                record = store.task(UUID(packet.name))
            except (ValueError, VinfError) as exc:
                raise VinfError(f"Unknown raw packet: {packet}") from exc
            document, _ = store.read_packet(record.task, deep=False)
            expected = {
                packet / content_name(document.compression),
                packet_json_path(packet, "metadata.json"),
                packet_json_path(packet, "links.json"),
            }
            if set(packet.iterdir()) != expected:
                raise VinfError(f"Extra files in {packet}; use crawl export instead.")

        backup = source.with_name(f"{source.name}.backup-{uuid4().hex[:12]}")
        with tempfile.TemporaryDirectory(
            prefix=".crawl-repack-", dir=source.parent
        ) as temporary:
            staged = Path(temporary) / "crawled"
            result = _export_crawl(source, staged, compression).model_copy(
                update={
                    "output": str(source),
                    "backup": str(backup) if keep_backup else None,
                }
            )
            for entry in source.iterdir():
                if entry.name in {
                    "raw",
                    "crawl",
                    "catalog",
                    "config.json",
                    "export.json",
                }:
                    continue
                destination = staged / entry.name
                if entry.is_dir():
                    shutil.copytree(entry, destination, dirs_exist_ok=True)
                else:
                    shutil.copy2(entry, destination)
            for entry in (source / "catalog").iterdir():
                destination = staged / "catalog" / entry.name
                if not destination.exists():
                    if entry.is_dir():
                        shutil.copytree(entry, destination)
                    else:
                        shutil.copy2(entry, destination)
            atomic_model(staged / "export.json", result)
            # Both the old and the incoming dataset stay locked during the swap.
            with dataset_lock(staged):
                os.rename(source, backup)
                try:
                    os.rename(staged, source)
                except OSError:
                    os.rename(backup, source)
                    raise
                sync_dir(source.parent)
            if not keep_backup:
                shutil.rmtree(backup)
    return result
