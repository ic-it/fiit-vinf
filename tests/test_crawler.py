from __future__ import annotations

import tempfile
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError
from vinf_core.files import dataset_lock, file_hash, read_model
from vinf_core.models import LinkDecision, RobotsStrategy
from vinf_crawler.checks import check_dataset
from vinf_crawler.config import (
    CrawlConfig,
    FailurePolicy,
    HttpConfig,
    RobotsConfig,
    StorageConfig,
    TraversalConfig,
)
from vinf_crawler.discovery import discover_links, document_id, normalize_url
from vinf_crawler.engine import Crawler
from vinf_crawler.models import TaskStatus
from vinf_crawler.store import CrawlStore


@contextmanager
def fixture_site(pages):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            status, body, headers = pages.get(self.path, (404, b"", {}))
            self.send_response(status)
            self.send_header(
                "Content-Type",
                "text/plain"
                if self.path == "/robots.txt"
                else "text/html; charset=utf-8",
            )
            self.send_header("Content-Length", str(len(body)))
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class NoNetwork:
    def fetch(self, url):
        raise AssertionError("Recovery must not download a saved packet again.")

    def close(self):
        pass


class CrawlerChecks(unittest.TestCase):
    def test_selection_rechecks_cooldown_expiry_during_the_same_scan(self):
        with tempfile.TemporaryDirectory() as temporary, dataset_lock(Path(temporary)):
            root = Path(temporary)
            seeds = ("https://example.test/a", "https://example.test/b")
            store = CrawlStore.initialize(root, CrawlConfig(seeds=seeds))
            crawler = Crawler(store, lambda _: None)
            clock = [1000.0]
            original_get = store.queue.get

            def get(identifier):
                clock[0] = 1000 if identifier == document_id(seeds[0]) else 1002
                return original_get(identifier)

            with (
                patch("vinf_crawler.network.time.time", side_effect=lambda: clock[0]),
                patch.object(store.queue, "get", side_effect=get),
            ):
                crawler.policy.defer(
                    "direct", seeds[0], until=1001, pause=None, shared=False
                )
                jobs = crawler._select(1)
            self.assertEqual([job.task.url for job in jobs], [seeds[1]])
            self.assertEqual(store.progress.inflight, (document_id(seeds[1]),))
            self.assertIn(document_id(seeds[0]), tuple(store.queue.entries()))

    def test_config_first_keeps_cached_url_rules_and_server_cooldowns(self):
        pages = {
            "/robots.txt": (
                200,
                (
                    b"User-agent: *\nDisallow: /deny\nAllow: /deny/open\n"
                    b"Crawl-delay: 10\nRequest-rate: 1/30\n"
                ),
                {},
            ),
            "/start": (
                200,
                (
                    b'<a href="/deny">deny</a><a href="/deny/open">open</a>'
                    b'<a href="/limited">limited</a><a href="/next">next</a>'
                ),
                {},
            ),
            "/deny/open": (200, b"<html>allowed</html>", {}),
            "/limited": (429, b"", {"Retry-After": "60"}),
        }
        with (
            tempfile.TemporaryDirectory() as temporary,
            fixture_site(pages) as (base, requests),
            dataset_lock(Path(temporary)),
        ):
            root = Path(temporary)
            store = CrawlStore.initialize(
                root,
                CrawlConfig(
                    seeds=(base + "/start",),
                    http=HttpConfig(delay_seconds=0),
                    robots=RobotsConfig(strategy=RobotsStrategy.CONFIG_FIRST),
                ),
            )
            Crawler(store, lambda _: None).run(1)
            store = CrawlStore(root, recover=True)
            result = Crawler(store, lambda _: None).run()
            self.assertEqual(
                (
                    result.statistics.downloaded,
                    result.statistics.blocked,
                    result.statistics.deferred,
                    result.statistics.pending,
                ),
                (2, 1, 1, 1),
            )
            self.assertEqual(
                requests, ["/robots.txt", "/start", "/deny/open", "/limited"]
            )
            self.assertEqual(
                store.document(document_id(base + "/start")).robots_strategy,
                RobotsStrategy.CONFIG_FIRST,
            )
            Crawler(CrawlStore(root, recover=True), lambda _: None).run()
            self.assertEqual(len(requests), 4)
            self.assertTrue(check_dataset(root, deep=True).ok)

    def test_real_http_fifo_crash_recovery_robots_and_offline_expansion(self):
        pages = {
            "/robots.txt": (200, b"User-agent: *\nDisallow: /deny\n", {}),
            "/alpha": (
                200,
                (
                    b'<html><a href="/family">family</a><a href="/deny">deny</a>'
                    b'<a href="/Fish?lang=Russian&amp;x=1">fish</a></html>'
                ),
                {},
            ),
            "/family": (
                200,
                b'<html><a href="/category/unknown">category</a></html>',
                {},
            ),
            "/Fish?lang=Russian&x=1": (200, b"<html>fish</html>", {}),
            "/category/unknown": (200, b"<html>category</html>", {}),
            "/deny": (200, b"<html>manually allowed</html>", {}),
        }
        with (
            tempfile.TemporaryDirectory() as temporary,
            fixture_site(pages) as (base, requests),
        ):
            root = Path(temporary)
            with dataset_lock(root):
                store = CrawlStore.initialize(
                    root,
                    CrawlConfig(
                        seeds=(base + "/alpha",),
                        http=HttpConfig(delay_seconds=0),
                        traversal=TraversalConfig(max_depth=1),
                        storage=StorageConfig(catalog_interval_pages=1),
                    ),
                )
                task = store.next_task()
                with (
                    patch.object(store, "commit_page", side_effect=KeyboardInterrupt),
                    self.assertRaises(KeyboardInterrupt),
                ):
                    Crawler(store, lambda _: None).run(1)
                raw = root / store.read_packet(task)[0].raw_path
                original = (file_hash(raw), raw.stat().st_mtime_ns)
                request_count = len(requests)
                store = CrawlStore(root, recover=True)
                result = Crawler(store, lambda _: None, NoNetwork()).run(1)
                self.assertEqual(result.recovered_this_run, 1)
                self.assertEqual(len(requests), request_count)
                self.assertEqual((file_hash(raw), raw.stat().st_mtime_ns), original)
                store.change_config(
                    store.config.model_copy(
                        update={"traversal": TraversalConfig(max_depth=0)}
                    )
                )
                result = Crawler(store, lambda _: None, NoNetwork()).run(0)
                self.assertEqual(result.statistics.blocked, 3)
                self.assertEqual(len(requests), request_count)
                store = CrawlStore(root)
                store.change_config(
                    store.config.model_copy(
                        update={"traversal": TraversalConfig(max_depth=1)}
                    )
                )
                self.assertEqual(
                    store.retry(reason="depth-limit", statuses=(TaskStatus.BLOCKED,)), 3
                )
                result = Crawler(CrawlStore(root), lambda _: None).run(0)
                self.assertEqual(result.statistics.downloaded, 3)
                self.assertEqual(result.statistics.blocked, 1)
                self.assertEqual(result.statistics.limited_links, 1)
                self.assertNotIn("/deny", requests)
                # Manual retry visits UUID buckets, not original discovery order.
                self.assertCountEqual(
                    requests[2:], ["/Fish?lang=Russian&x=1", "/family"]
                )
                store = CrawlStore(root)
                store.change_config(
                    store.config.model_copy(
                        update={"traversal": TraversalConfig(max_depth=2)}
                    )
                )
                self.assertEqual(
                    len(requests), 4
                )  # Replanning uses saved links, no HTTP.
                result = Crawler(store, lambda _: None).run(0)
                self.assertEqual(result.statistics.downloaded, 4)
                store = CrawlStore(root)
                store.change_config(
                    store.config.model_copy(
                        update={"robots": RobotsConfig(strategy=RobotsStrategy.IGNORE)}
                    )
                )
                store.retry(statuses=(TaskStatus.BLOCKED,))
                result = Crawler(store, lambda _: None).run(0)
                self.assertEqual(result.statistics.downloaded, 5)
                self.assertTrue(result.statistics.complete)
                self.assertTrue(check_dataset(root, deep=True).ok)
                store = CrawlStore(root)
                with patch("vinf_crawler.queue.read_model", wraps=read_model) as reads:
                    self.assertEqual(len(list(store.iter_documents())), 5)
                buckets = [call.args[0] for call in reads.call_args_list]
                self.assertEqual(len(buckets), len(set(buckets)))
                self.assertLessEqual(len(buckets), 256)
                catalog = root / "catalog/documents.jsonl"
                modified = catalog.stat().st_mtime_ns
                with patch.object(store, "iter_documents", side_effect=AssertionError):
                    store.publish_catalog()
                self.assertEqual(catalog.stat().st_mtime_ns, modified)

    def test_real_rate_limit_retained_cooldown_manual_retry_and_skip(self):
        pages = {
            "/robots.txt": (200, b"User-agent: *\nDisallow:\n", {}),
            "/a-limited": (429, b"", {"Retry-After": "120"}),
            "/z-later": (404, b"", {}),
        }
        with (
            tempfile.TemporaryDirectory() as temporary,
            fixture_site(pages) as (base, requests),
        ):
            root = Path(temporary)
            with dataset_lock(root):
                store = CrawlStore.initialize(
                    root,
                    CrawlConfig(
                        seeds=(base + "/a-limited", base + "/z-later"),
                        http=HttpConfig(delay_seconds=0),
                        failures=FailurePolicy(max_attempts=1),
                    ),
                )
                result = Crawler(store, lambda _: None).run(0)
                self.assertEqual(
                    (result.statistics.deferred, result.statistics.pending), (1, 1)
                )
                self.assertEqual(requests, ["/robots.txt", "/a-limited"])
                store = CrawlStore(root)
                limited = store.task(document_id(base + "/a-limited"))
                later_id = document_id(base + "/z-later")
                self.assertFalse(limited.automatic_retry)
                ready = limited.retry_at
                store.skip(later_id, "later")
                self.assertEqual(
                    CrawlStore(root).task(later_id).status, TaskStatus.SKIPPED
                )
                store.retry(doc_id=later_id)
                self.assertEqual(store.retry(reason="rate-limit"), 1)
                self.assertEqual(
                    Crawler(store, lambda _: None).run(0).saved_this_run, 0
                )
                self.assertEqual(
                    len(requests), 2
                )  # Explicit retry must not bypass cooldown.
                pages["/a-limited"] = (200, b"<html>available again</html>", {})
                with patch("vinf_crawler.store.time.time", return_value=ready + 1):
                    result = Crawler(CrawlStore(root), lambda _: None).run(0)
                self.assertEqual(
                    (result.statistics.downloaded, result.statistics.deferred), (1, 1)
                )
                store = CrawlStore(root)
                self.assertEqual(store.task(later_id).reason, "http-error")
                self.assertEqual(store.retry(reason="http-error"), 1)
                pages["/z-later"] = (200, b"<html>available now</html>", {})
                with patch("vinf_crawler.store.time.time", return_value=ready + 2):
                    result = Crawler(store, lambda _: None).run(0)
                self.assertTrue(result.statistics.complete)
                self.assertEqual(CrawlStore(root).task(limited.task.doc_id).attempts, 2)
                self.assertEqual(CrawlStore(root).task(later_id).attempts, 2)
                self.assertTrue(check_dataset(root, deep=True).ok)

    def test_generic_urls_width_and_typed_configuration(self):
        config = CrawlConfig(
            seeds=("https://example.org/start",),
            traversal=TraversalConfig(max_links_per_page=1),
        )
        links = discover_links(
            """
            <base href="/arbitrary/">
            <a href="A?lang=Russian&amp;x=1#one">one</a>
            <a href="A?lang=Russian&amp;x=1#two">duplicate</a>
            <a href="B?x=1&amp;lang=Russian">two</a>
            <a href="https://external.example/page">external</a>
            <a href="mailto:someone@example.org">mail</a>
            <!-- <a href="hidden">comment</a> -->
            <script>const fake = '<a href="hidden-js">';</script>
            <div title='<a href="hidden-attribute">'>text</div>
            <a title="a > b" HREF='A?lang=Russian&amp;x=1'>same</a>
            <a href=A?lang=Russian&amp;x=1>unquoted query</a>
            <span title='broken'quote'> <a href="B?x=1&amp;lang=Russian">two</a>
        """,
            config.seeds[0],
            document_id(config.seeds[0]),
            0,
            config,
        )
        self.assertEqual(len(links.links), 4)
        allowed = [
            link for link in links.links if link.decision == LinkDecision.ALLOWED
        ]
        deferred = [
            link for link in links.links if link.decision == LinkDecision.DEFERRED
        ]
        self.assertEqual(
            allowed[0].url, "https://example.org/arbitrary/A?lang=Russian&x=1"
        )
        self.assertEqual(deferred[0].reason, "width-limit")
        self.assertEqual(
            normalize_url("HTTPS://EXAMPLE.ORG:443/A?b=2&a=1#x"),
            "https://example.org/A?b=2&a=1",
        )
        with self.assertRaises(ValidationError):
            CrawlConfig.model_validate_json(
                '{"seeds":["https://example.org"],"page_types":["species"]}'
            )


if __name__ == "__main__":
    unittest.main()
