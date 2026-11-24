"""A few checks for FIFO order, bounded task buckets and interrupted writes."""

import json
import os
import tempfile
import unittest
from itertools import chain, islice
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from vinf_core.files import atomic_bytes, atomic_model, dataset_lock
from vinf_crawler.checks import Findings, check_dataset, check_frontier
from vinf_crawler.config import CrawlConfig
from vinf_crawler.discovery import document_id
from vinf_crawler.models import CrawlTask, TaskRecord, TaskStatus
from vinf_crawler.queue import CACHE_BUCKETS
from vinf_crawler.store import CrawlStore


def task(sequence):
    url = f"https://example.test/{sequence}"
    return TaskRecord(
        task=CrawlTask(
            doc_id=document_id(url), url=url, depth=sequence % 3, sequence=sequence
        )
    )


class QueueChecks(unittest.TestCase):
    def test_exact_membership_cache_and_untouched_fifo_files(self):
        with tempfile.TemporaryDirectory() as temporary, dataset_lock(Path(temporary)):
            root = Path(temporary)
            store = CrawlStore.initialize(root, CrawlConfig(seeds=(task(0).task.url,)))
            store.import_records(task(sequence) for sequence in range(1, 200))
            identifiers = {task(sequence).task.doc_id for sequence in range(200)}
            new = task(200)
            self.assertFalse(store.queue.contains(new.task.doc_id))
            with patch(
                "vinf_crawler.queue.read_model",
                side_effect=AssertionError("Membership cache must avoid bucket reads."),
            ):
                self.assertEqual(store.queue.existing(identifiers), identifiers)
                self.assertFalse(store.queue.contains(new.task.doc_id))
            store.import_records((new,))
            self.assertTrue(store.queue.contains(new.task.doc_id))
            self.assertLessEqual(store.queue._known_bytes, store.queue._known_limit)
            actual_open = Path.open

            def reject_fifo_write(path, mode="r", *args, **kwargs):
                if path in (store.queue.path, store.queue.priority_path):
                    self.assertEqual(mode, "rb", "Unchanged FIFOs must not be written.")
                return actual_open(path, mode, *args, **kwargs)

            with patch.object(Path, "open", reject_fifo_write):
                self.assertEqual(len(store.queue.take(2, lambda _: True)), 2)
            store.change_config(
                store.config.model_copy(
                    update={
                        "storage": store.config.storage.model_copy(
                            update={"membership_cache_mib": 0}
                        )
                    }
                )
            )
            self.assertFalse(store.queue._known)
            self.assertTrue(store.queue.contains(new.task.doc_id))

    def test_preferred_fifo_reorders_existing_tasks_and_recovers_interruption(self):
        with tempfile.TemporaryDirectory() as temporary, dataset_lock(Path(temporary)):
            root = Path(temporary)
            store = CrawlStore.initialize(root, CrawlConfig(seeds=(task(0).task.url,)))
            store.import_records(task(sequence) for sequence in range(1, 13))
            config = store.config.model_copy(
                update={
                    "traversal": store.config.traversal.model_copy(
                        update={"prefer_url_patterns": (r"/(?:3|7|13)$",)}
                    )
                }
            )
            actual_replace = os.replace

            def interrupted(source, target):
                actual_replace(source, target)
                if Path(target) == store.queue.path:
                    raise KeyboardInterrupt

            with (
                patch("vinf_crawler.queue.os.replace", side_effect=interrupted),
                self.assertRaises(KeyboardInterrupt),
            ):
                store.change_config(config)
            store = CrawlStore(root, recover=True)
            store.import_records((task(13),))
            self.assertEqual(
                [
                    record.task.sequence
                    for record in islice(store.queue.candidates(float("inf")), 5)
                ],
                [3, 7, 13, 0, 1],
            )
            self.assertEqual(
                [
                    record.task.sequence
                    for record in store.queue.take(2, lambda _: True)
                ],
                [3, 7],
            )
            restarted = CrawlStore(root, recover=True)
            self.assertEqual(
                [
                    record.task.sequence
                    for record in restarted.queue.take(2, lambda _: True)
                ],
                [3, 7],
            )
            restarted.publish_catalog()
            with (
                patch.object(restarted.queue, "records", side_effect=AssertionError),
                patch.object(restarted.queue, "entries", side_effect=AssertionError),
            ):
                restarted.queue.reprioritize()
            self.assertTrue(check_dataset(root, deep=True).ok)
            self.assertFalse(list((root / "crawl").glob("*.reorder.jsonl")))

    def test_fifo_bucket_dedup_retry_and_constant_status_reads(self):
        with tempfile.TemporaryDirectory() as temporary, dataset_lock(Path(temporary)):
            root = Path(temporary)
            store = CrawlStore.initialize(root, CrawlConfig(seeds=(task(0).task.url,)))
            store.import_records(task(sequence) for sequence in range(1, 1100))
            self.assertEqual(
                [
                    r.task.sequence
                    for r in islice(store.queue.candidates(float("inf")), 4)
                ],
                [0, 1, 2, 3],
            )
            self.assertTrue(store.queue.contains(task(200).task.doc_id))
            self.assertFalse(store.queue.contains(task(1101).task.doc_id))
            identifier = task(200).task.doc_id
            body = json.loads(store.queue.bucket_path(identifier.hex[:2]).read_bytes())[
                str(identifier)
            ]
            self.assertNotIn("last_error", body)
            self.assertNotIn("proxy_id", body)
            self.assertEqual(len(list((root / "crawl/tasks").iterdir())), 256)
            self.assertLessEqual(len(store.queue._cache), CACHE_BUCKETS)
            selected = store.queue.take(2, lambda _: True)
            self.assertEqual([r.task.sequence for r in selected], [0, 1])
            self.assertEqual(
                [
                    r.task.sequence
                    for r in CrawlStore(root).queue.take(2, lambda _: True)
                ],
                [0, 1],
            )
            store = CrawlStore(root)
            store.skip(task(0).task.doc_id, "Review later")
            self.assertEqual(store.retry(doc_id=task(0).task.doc_id), 1)
            self.assertEqual(store.queue.take(1, lambda _: True)[0].task.sequence, 1)
            store.skip(task(1).task.doc_id, "Review later")
            self.assertEqual(store.queue.take(1, lambda _: True)[0].task.sequence, 2)
            actual_open = Path.open

            def deny_data(path, *args, **kwargs):
                if path.name == "queue.jsonl" or path.resolve().is_relative_to(
                    root.resolve() / "crawl/tasks"
                ):
                    raise AssertionError(
                        "Status must not read queue entries or task files."
                    )
                return actual_open(path, *args, **kwargs)

            with patch.object(Path, "open", deny_data):
                self.assertEqual(CrawlStore(root).statistics().discovered, 1100)
            store.publish_catalog()
            self.assertTrue(check_dataset(root, deep=True).ok)
            for entries, message in (
                (iter(()), "Active tasks are missing"),
                (
                    chain(store.queue.entries(), (UUID(int=(1 << 128) - 1),)),
                    "FIFO references an unknown task",
                ),
            ):
                findings = Findings()
                with patch.object(store.queue, "entries", return_value=entries):
                    check_frontier(store, findings)
                self.assertTrue(any(message in error for error in findings.errors))
            self.assertLessEqual(len(store.queue._cache), CACHE_BUCKETS)
            self.assertEqual(
                CrawlStore(root).task(task(1).task.doc_id).status, TaskStatus.SKIPPED
            )

    def test_recovery_after_task_write_and_after_state_write(self):
        for fault in ("task-file", "state-file"):
            with (
                self.subTest(fault=fault),
                tempfile.TemporaryDirectory() as temporary,
                dataset_lock(Path(temporary)),
            ):
                root = Path(temporary)
                store = CrawlStore.initialize(
                    root, CrawlConfig(seeds=(task(0).task.url,))
                )
                records = (task(1), task(2))
                if fault == "task-file":

                    def interrupted(path, body, root=root):
                        atomic_bytes(path, body)
                        if path.parent.resolve() == (root / "crawl/tasks").resolve():
                            raise KeyboardInterrupt

                    context = patch(
                        "vinf_crawler.queue.atomic_bytes", side_effect=interrupted
                    )
                else:

                    def interrupted(path, value, root=root):
                        atomic_model(path, value)
                        if path.resolve() == (root / "crawl/state.json").resolve():
                            raise KeyboardInterrupt

                    context = patch(
                        "vinf_crawler.queue.atomic_model", side_effect=interrupted
                    )
                with context, self.assertRaises(KeyboardInterrupt):
                    store.queue.commit(records=records)
                self.assertTrue(CrawlStore(root).statistics().pending_write)
                recovered = CrawlStore(root, recover=True)
                for record in (task(0), *records):
                    self.assertEqual(recovered.task(record.task.doc_id), record)
                self.assertEqual(recovered.progress.count, 3)
                self.assertEqual(recovered.progress.counts[TaskStatus.PENDING], 3)
                self.assertFalse(recovered.queue.pending_path.exists())
                self.assertEqual(
                    CrawlStore(root, recover=True).progress, recovered.progress
                )
                recovered.publish_catalog()
                self.assertTrue(check_dataset(root, deep=True).ok)
