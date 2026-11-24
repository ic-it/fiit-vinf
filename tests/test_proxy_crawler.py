"""A few real localhost integration checks: forwarding, CONNECT and restart policy."""

import base64
import http.client
import os
import select
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

from protego import Protego
from typer.testing import CliRunner
from vinf_core.files import atomic_model, dataset_lock, read_model
from vinf_core.models import RobotsStrategy
from vinf_crawler.checks import check_dataset
from vinf_crawler.config import (
    CrawlConfig,
    FailureAction,
    FailurePolicy,
    HttpConfig,
    ParallelConfig,
    ProxyConfig,
    ProxyPoolConfig,
    RateLimitScope,
    RobotsConfig,
    TraversalConfig,
)
from vinf_crawler.discovery import document_id, origin
from vinf_crawler.engine import Crawler
from vinf_crawler.http import FetchResult, HttpFailure
from vinf_crawler.models import FailureReason, TaskStatus
from vinf_crawler.network import (
    NetworkPolicy,
    NetworkState,
    OriginThrottle,
    RequestDeferred,
    Transport,
)
from vinf_crawler.store import CrawlStore

from vinf.cli.app import app


@dataclass
class Trace:
    calls: list = field(default_factory=list)
    tunnels: list = field(default_factory=list)
    active: int = 0
    peak: int = 0
    authorization_leaks: list = field(default_factory=list)
    guard: threading.Lock = field(default_factory=threading.Lock)


@contextmanager
def site(pages, *, tls=None, delay=0):
    trace = Trace()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            with trace.guard:
                trace.calls.append((self.path, time.monotonic()))
                trace.authorization_leaks.append(
                    self.headers.get("Proxy-Authorization")
                )
                trace.active += 1
                trace.peak = max(trace.peak, trace.active)
            try:
                if self.path != "/robots.txt":
                    time.sleep(delay)
                status, body, headers = pages.get(self.path, (404, b"", {}))
                self.send_response(status)
                self.send_header(
                    "Content-Type",
                    "text/plain" if self.path == "/robots.txt" else "text/html",
                )
                self.send_header("Content-Length", str(len(body)))
                for key, value in headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(body)
            finally:
                with trace.guard:
                    trace.active -= 1

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    if tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(*tls)
        server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"{'https' if tls else 'http'}://127.0.0.1:{server.server_port}", trace
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@contextmanager
def proxy(*, authenticated=False):
    trace = Trace()
    expected = "Basic " + base64.b64encode(b"fixture:fixture-secret").decode()

    class Handler(BaseHTTPRequestHandler):
        def authorized(self):
            if authenticated and self.headers.get("Proxy-Authorization") != expected:
                self.send_response(407)
                self.send_header("Proxy-Authenticate", 'Basic realm="fixture"')
                self.send_header("Content-Length", "0")
                self.end_headers()
                return False
            return True

        def do_GET(self):
            if not self.authorized():
                return
            trace.calls.append((self.path, time.monotonic()))
            url = urlsplit(self.path)
            headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower()
                not in {"proxy-authorization", "proxy-connection", "connection"}
            }
            with closing(
                http.client.HTTPConnection(url.hostname, url.port, timeout=5)
            ) as connection:
                connection.request(
                    "GET",
                    (url.path or "/") + ("?" + url.query if url.query else ""),
                    headers=headers,
                )
                response = connection.getresponse()
                body = response.read()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in {
                        "content-length",
                        "transfer-encoding",
                        "connection",
                    }:
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        def do_CONNECT(self):
            if not self.authorized():
                return
            trace.tunnels.append(self.path)
            hostname, port = self.path.rsplit(":", 1)
            with socket.create_connection((hostname, int(port)), timeout=5) as upstream:
                self.send_response(200)
                self.end_headers()
                while True:
                    ready, _, _ = select.select((self.connection, upstream), (), (), 5)
                    if not ready:
                        break
                    for source in ready:
                        body = source.recv(65536)
                        if not body:
                            return
                        (
                            upstream if source is self.connection else self.connection
                        ).sendall(body)
            self.close_connection = True

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        credentials = "fixture:fixture-secret@" if authenticated else ""
        yield f"http://{credentials}127.0.0.1:{server.server_port}", trace
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class ProxyCrawlerChecks(unittest.TestCase):
    def test_site_and_proxy_pacing_scopes_keep_durable_clocks_and_shared_cooldowns(
        self,
    ):
        for scope, strategy, expected in (
            (RateLimitScope.SITE, RobotsStrategy.RESPECT, (1000, 1010, 1020)),
            (RateLimitScope.PROXY, RobotsStrategy.RESPECT, (1000, 1000, 1010)),
            (RateLimitScope.SITE, RobotsStrategy.CONFIG_FIRST, (1000, 1001, 1002)),
            (RateLimitScope.PROXY, RobotsStrategy.CONFIG_FIRST, (1000, 1000, 1001)),
        ):
            with (
                self.subTest(scope=scope, strategy=strategy),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                clock = [1000.0]
                config = CrawlConfig(
                    seeds=("https://example.test/fish",),
                    http=HttpConfig(delay_seconds=1, rate_limit_scope=scope),
                    robots=RobotsConfig(strategy=strategy),
                )
                routes = (Transport("one", config), Transport("two", config))
                rules = Protego.parse(
                    "User-agent: *\nCrawl-delay: 10\nRequest-rate: 1/5\n"
                )
                policy = NetworkPolicy(root, config, routes, lambda _: None)
                for route in routes:
                    policy.register_rules(route.id, "https://example.test", rules)

                def wait(seconds, clock=clock):
                    clock[0] += seconds

                starts = []
                with (
                    patch(
                        "vinf_crawler.network.time.time",
                        side_effect=lambda clock=clock: clock[0],
                    ),
                    patch.object(policy.cancelled, "wait", side_effect=wait),
                ):
                    for identifier in ("one", "two", "one"):
                        policy.before_request(identifier, config.seeds[0], rules)
                        starts.append(clock[0])
                    self.assertEqual(tuple(starts), expected)
                    resumed = NetworkPolicy(root, config, routes, lambda _: None)
                    self.assertEqual(resumed.state, policy.state)
                    resumed.defer(
                        "one",
                        config.seeds[0],
                        until=clock[0] + 60,
                        pause=None,
                        shared=True,
                    )
                    with self.assertRaises(RequestDeferred):
                        resumed.before_request("two", config.seeds[0], rules)
                    self.assertFalse(resumed.cancelled.is_set())

    def test_pool_disables_connection_failures_but_retains_site_rate_limits(self):
        for rate_limit in (False, True):
            with (
                self.subTest(rate_limit=rate_limit),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                pool = root / "proxies.txt"
                pool.write_text(
                    "# public list\n1.1.1.1:8080\n127.0.0.1:8080\ninvalid\n"
                )
                config = CrawlConfig(
                    seeds=("https://example.test/fish",),
                    proxy_pool=ProxyPoolConfig(file=str(pool)),
                )
                store = CrawlStore.initialize(root, config)
                crawler = Crawler(store, lambda _: None)
                attempts = []

                def fetch(client, url, attempts=attempts, rate_limit=rate_limit):
                    attempts.append(client.proxy_id)
                    if client.pooled:
                        raise HttpFailure(
                            "fixture failure",
                            FailureReason.RATE_LIMIT
                            if rate_limit
                            else FailureReason.PROXY_ERROR,
                            url=url,
                            status=429 if rate_limit else None,
                            retry_after=60 if rate_limit else 0,
                            automatic=True,
                        )
                    return FetchResult(
                        body=b"<html>fish</html>",
                        final_url=url,
                        status=200,
                        headers={"content-type": "text/html"},
                        encoding="utf-8",
                        elapsed=0,
                    )

                with (
                    patch("vinf_crawler.http.HttpClient.fetch", new=fetch),
                    dataset_lock(root),
                ):
                    result = crawler.run(limit=1)
                pooled = next(route for route in crawler.routes if route.pooled)
                state = crawler.policy.state.transports[pooled.id]
                if rate_limit:
                    self.assertIsNone(state.disabled)
                    self.assertEqual(attempts, [pooled.id])
                    self.assertEqual(result.statistics.deferred, 1)
                else:
                    self.assertIsNotNone(state.disabled)
                    self.assertEqual(attempts, [pooled.id, "direct"])
                    self.assertEqual(result.statistics.downloaded, 1)
                self.assertEqual(result.statistics.skipped, 0)

    def test_real_proxy_parallelism_https_connect_and_secret_free_restart(self):
        pages = {"/robots.txt": (200, b"User-agent: *\nDisallow:\n", {})}
        pages.update(
            {f"/{letter}": (200, b"<html>fish</html>", {}) for letter in "abcdefgh"}
        )
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            cert, key = parent / "cert.pem", parent / "key.pem"
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=localhost",
                    "-addext",
                    "subjectAltName=DNS:localhost,IP:127.0.0.1",
                    "-keyout",
                    str(key),
                    "-out",
                    str(cert),
                ],
                check=True,
                capture_output=True,
            )
            with (
                site(pages, delay=0.15) as (http, http_trace),
                site(pages, tls=(cert, key), delay=0.15) as (https, tls_trace),
                proxy(authenticated=True) as (one, first),
                proxy() as (two, second),
                patch.dict(
                    os.environ,
                    {
                        "VINF_TEST_PROXY_ONE": one,
                        "VINF_TEST_PROXY_TWO": two,
                        "REQUESTS_CA_BUNDLE": str(cert),
                        "HTTP_PROXY": "http://127.0.0.1:1",
                        "HTTPS_PROXY": "http://127.0.0.1:1",
                        "NO_PROXY": "127.0.0.1",
                    },
                ),
            ):
                root = parent / "crawled"
                with dataset_lock(root):
                    store = CrawlStore.initialize(
                        root,
                        CrawlConfig(
                            seeds=tuple(http + "/" + letter for letter in "abcd")
                            + tuple(https + "/" + letter for letter in "efgh"),
                            http=HttpConfig(delay_seconds=0.035),
                            parallel=ParallelConfig(workers=4),
                            traversal=TraversalConfig(max_pages=8),
                            proxies=(
                                ProxyConfig(
                                    id="one",
                                    url_env="VINF_TEST_PROXY_ONE",
                                    delay_seconds=0.07,
                                ),
                                ProxyConfig(
                                    id="two",
                                    url_env="VINF_TEST_PROXY_TWO",
                                    delay_seconds=0.04,
                                    robots_strategy=RobotsStrategy.STRICT,
                                ),
                            ),
                        ),
                    )
                    result = Crawler(store, lambda _: None).run(8)
                    self.assertEqual(result.saved_this_run, 8)
                    self.assertGreaterEqual(max(http_trace.peak, tls_trace.peak), 2)
                    self.assertTrue(
                        first.calls
                        and second.calls
                        and first.tunnels
                        and second.tunnels
                    )
                    self.assertFalse(
                        any(
                            http_trace.authorization_leaks
                            + tls_trace.authorization_leaks
                        )
                    )
                    self.assertEqual(
                        [path for path, _ in http_trace.calls].count("/robots.txt"), 2
                    )
                    self.assertEqual(
                        [path for path, _ in tls_trace.calls].count("/robots.txt"), 2
                    )
                    for document in store.iter_documents():
                        identifier = document.doc_id
                        state = store.task(identifier)
                        self.assertEqual(
                            document.proxy_id, ("one", "two")[state.task.sequence % 2]
                        )
                        self.assertEqual(document.proxy_id, state.proxy_id)
                    policy = read_model(root / "crawl/network.json", NetworkState)
                    for history in policy.origins.values():
                        self.assertTrue(
                            all(
                                b - a >= 0.034
                                for a, b in zip(history.starts, history.starts[1:])
                            )
                        )
                    for proxy_id, delay in (("one", 0.07), ("two", 0.04)):
                        self.assertEqual(
                            len(
                                list(
                                    (
                                        root / "crawl/transports" / proxy_id / "robots"
                                    ).glob("*.json")
                                )
                            ),
                            2,
                        )
                        for history in policy.transports[proxy_id].origins.values():
                            self.assertTrue(
                                all(
                                    b - a >= delay - 0.001
                                    for a, b in zip(history.starts, history.starts[1:])
                                )
                            )
                    calls = (
                        len(first.calls),
                        len(second.calls),
                        len(first.tunnels),
                        len(second.tunnels),
                    )
                    self.assertEqual(
                        Crawler(CrawlStore(root), lambda _: None).run(0).stop_reason,
                        "page-budget",
                    )
                    self.assertEqual(
                        calls,
                        (
                            len(first.calls),
                            len(second.calls),
                            len(first.tunnels),
                            len(second.tunnels),
                        ),
                    )
                    self.assertEqual(
                        read_model(root / "crawl/network.json", NetworkState), policy
                    )
                    self.assertTrue(check_dataset(root, deep=True).ok)
                for path in root.rglob("*"):
                    if path.is_file():
                        self.assertNotIn(
                            b"fixture-secret", path.read_bytes(), str(path)
                        )

    def test_redirect_to_cooling_origin_retains_task_without_busy_retry(self):
        allowed = (200, b"User-agent: *\nDisallow:\n", {})
        with (
            tempfile.TemporaryDirectory() as temporary,
            site({"/robots.txt": allowed}) as (target, target_trace),
            site(
                {
                    "/robots.txt": allowed,
                    "/entry": (302, b"", {"Location": target + "/fish"}),
                }
            ) as (base, initial_trace),
        ):
            root = Path(temporary)
            with dataset_lock(root):
                store = CrawlStore.initialize(
                    root,
                    CrawlConfig(
                        seeds=(base + "/entry",),
                        http=HttpConfig(delay_seconds=0),
                        parallel=ParallelConfig(workers=2),
                    ),
                )
                until = time.time() + 120
                atomic_model(
                    root / "crawl/network.json",
                    NetworkState(
                        origins={origin(target): OriginThrottle(cooldown_until=until)}
                    ),
                )
                result = Crawler(store, lambda _: None).run(0)
                task = next(iter(store.iter_tasks()))
                self.assertEqual(result.failed_attempts_this_run, 0)
                self.assertEqual(
                    (task.status, task.attempts, task.retry_at),
                    (TaskStatus.PENDING, 0, until),
                )
                self.assertEqual(
                    [path for path, _ in initial_trace.calls], ["/robots.txt", "/entry"]
                )
                self.assertFalse(target_trace.calls)
                Crawler(CrawlStore(root), lambda _: None).run(0)
                self.assertEqual(len(initial_trace.calls), 2)
                self.assertTrue(check_dataset(root, deep=True).ok)

    def test_per_proxy_failure_policy_binding_cooldown_and_manual_overrides(self):
        pages = {
            "/robots.txt": (200, b"User-agent: *\nDisallow: /c-denied\n", {}),
            "/a-rate": (429, b"", {"Retry-After": "120"}),
            "/b-ok": (200, b"<html>fish</html>", {}),
            "/c-denied": (200, b"<html>manually allowed</html>", {}),
        }
        with (
            tempfile.TemporaryDirectory() as temporary,
            site(pages) as (base, trace),
            proxy() as (one, first),
            proxy() as (two, second),
            patch.dict(
                os.environ, {"VINF_TEST_PROXY_ONE": one, "VINF_TEST_PROXY_TWO": two}
            ),
        ):
            root = Path(temporary)
            with dataset_lock(root):
                store = CrawlStore.initialize(
                    root,
                    CrawlConfig(
                        seeds=tuple(
                            base + path for path in ("/a-rate", "/b-ok", "/c-denied")
                        ),
                        http=HttpConfig(delay_seconds=0),
                        proxies=(
                            ProxyConfig(
                                id="one",
                                url_env="VINF_TEST_PROXY_ONE",
                                failures=FailurePolicy(
                                    max_attempts=1, rate_limit=FailureAction.PAUSE
                                ),
                            ),
                            ProxyConfig(id="two", url_env="VINF_TEST_PROXY_TWO"),
                        ),
                    ),
                )
                result = Crawler(store, lambda _: None).run(0)
                limited_id, denied_id = (
                    document_id(base + "/a-rate"),
                    document_id(base + "/c-denied"),
                )
                self.assertEqual(
                    (result.statistics.deferred, result.statistics.pending), (1, 2)
                )
                self.assertFalse(store.task(limited_id).automatic_retry)
                self.assertEqual(store.task(limited_id).proxy_id, "one")
                self.assertFalse(second.calls)
                self.assertEqual(len(first.calls), 2)
                until = store.task(limited_id).retry_at
                self.assertGreaterEqual(until - time.time(), 119)
                self.assertIsNotNone(
                    read_model(root / "crawl/network.json", NetworkState)
                    .transports["one"]
                    .pause
                )
            runner = CliRunner()
            for args in (
                (
                    "policy",
                    "--proxy",
                    "two",
                    "--robots-strategy",
                    "strict",
                    "--workers",
                    "3",
                ),
                ("transports", "--json"),
            ):
                response = runner.invoke(app, ["crawl", *args, "--dataset", str(root)])
                self.assertEqual(
                    response.exit_code, 0, response.output + repr(response.exception)
                )
            with dataset_lock(root):
                store = CrawlStore(root)
                self.assertEqual(store.retry(doc_id=limited_id), 1)
                self.assertEqual(
                    Crawler(store, lambda _: None)
                    .run(0, resume_proxies=("one",))
                    .saved_this_run,
                    0,
                )
                self.assertFalse(
                    second.calls
                )  # Resume and retry keep the site-wide deadline.
                pages["/a-rate"] = (200, b"<html>available</html>", {})
                with patch("vinf_crawler.network.time.time", return_value=until + 1):
                    result = Crawler(CrawlStore(root), lambda _: None).run(0)
                store = CrawlStore(root)
                self.assertEqual(result.statistics.downloaded, 2)
                self.assertEqual(store.document(limited_id).proxy_id, "one")
                self.assertEqual(store.task(denied_id).status, TaskStatus.BLOCKED)
                self.assertEqual(store.task(denied_id).proxy_id, "one")
                self.assertEqual(store.task(limited_id).attempts, 2)
                self.assertTrue(second.calls)
                self.assertEqual(
                    store.config.proxies[1].robots_strategy, RobotsStrategy.STRICT
                )
                store.retry(doc_id=denied_id, proxy_id="two")
                self.assertEqual(CrawlStore(root).task(denied_id).proxy_id, "two")
                store.retry(doc_id=denied_id, proxy_id="one")
            response = runner.invoke(
                app,
                [
                    "crawl",
                    "policy",
                    "--dataset",
                    str(root),
                    "--proxy",
                    "one",
                    "--robots-strategy",
                    "ignore",
                ],
            )
            self.assertEqual(response.exit_code, 0, response.output)
            with (
                dataset_lock(root),
                patch("vinf_crawler.network.time.time", return_value=until + 2),
            ):
                result = Crawler(CrawlStore(root), lambda _: None).run(0)
                self.assertTrue(result.statistics.complete)
                store = CrawlStore(root)
                self.assertEqual(
                    store.document(denied_id).robots_strategy,
                    RobotsStrategy.IGNORE,
                )
                self.assertEqual(store.document(denied_id).proxy_id, "one")
                self.assertTrue(check_dataset(root, deep=True).ok)
                self.assertIn("/c-denied", [path for path, _ in trace.calls])
