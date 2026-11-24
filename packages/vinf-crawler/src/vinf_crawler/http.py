from __future__ import annotations

import hashlib
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from protego import Protego
from vinf_core.files import atomic_model, read_model
from vinf_core.models import Model, RobotsStrategy, VinfError

from vinf_crawler.discovery import Scope, normalize_url, origin
from vinf_crawler.models import FailureReason
from vinf_crawler.network import NetworkPolicy, RobotsSnapshot, Transport

Reporter = Callable[[str], None]


class PolicyBlocked(VinfError):
    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


class CrawlPaused(VinfError):
    pass


class HttpFailure(VinfError):
    def __init__(
        self,
        message: str,
        reason: FailureReason,
        *,
        url: str,
        status: int | None = None,
        retry_after: float = 0,
        automatic: bool = False,
    ):
        super().__init__(message)
        self.reason, self.url, self.status = reason, url, status
        self.retry_after, self.automatic = retry_after, automatic


class FetchResult(Model):
    body: bytes
    final_url: str
    status: int
    headers: dict[str, str]
    encoding: str
    elapsed: float


@dataclass(frozen=True)
class CachedRules:
    expires_at: float
    rules: Protego


def retry_after_seconds(value: str | None) -> float:
    if not value:
        return 0
    try:
        seconds = float(value)
        return max(0, seconds) if 0 <= seconds < float("inf") else 0
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=UTC)
            return max(0, date.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return 0


class HttpClient:
    def __init__(
        self, root: Path, transport: Transport, policy: NetworkPolicy, report: Reporter
    ):
        self.root, self.config, self.report = root, transport.config, report
        self.proxy_id, self.policy = transport.id, policy
        self.pooled = transport.pooled
        self.scope = Scope(self.config)
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.verify = (
            os.environ.get("REQUESTS_CA_BUNDLE")
            or os.environ.get("CURL_CA_BUNDLE")
            or True
        )
        if transport.proxy_url is not None:
            self.session.proxies = {
                "http": transport.proxy_url,
                "https": transport.proxy_url,
            }
        self.session.headers.update(
            {
                "User-Agent": self.config.http.user_agent,
                "Accept": "text/html, application/xhtml+xml",
            }
        )
        self.rules_cache: dict[str, CachedRules] = {}

    def _error(self, exc: Exception) -> str:
        # Proxy exceptions may contain credentials or endpoint URLs.
        return f"{type(exc).__name__} via {self.proxy_id}"

    def close(self) -> None:
        self.session.close()

    def _request(
        self, url: str, rules: Protego | None, *, robots: bool = False
    ) -> FetchResult:
        self.policy.before_request(self.proxy_id, url, rules)
        self.report(f"{self.proxy_id}: GET {url}")
        started = time.monotonic()
        try:
            with self.session.get(
                url,
                timeout=(
                    self.config.http.connect_timeout,
                    self.config.http.read_timeout,
                ),
                allow_redirects=False,
                stream=True,
                headers={"Accept": "text/plain"} if robots else None,
            ) as response:
                headers = {
                    key.lower(): value for key, value in response.headers.items()
                }
                body = b""
                if response.status_code == 200:
                    chunks, size = [], 0
                    bound = (
                        512 * 1024 if robots else self.config.http.max_response_bytes
                    )
                    for chunk in response.iter_content(chunk_size=65536):
                        size += len(chunk)
                        if size > bound:
                            raise HttpFailure(
                                "Response exceeded the configured byte limit.",
                                FailureReason.TOO_LARGE,
                                url=url,
                                status=200,
                            )
                        chunks.append(chunk)
                    body = b"".join(chunks)
                return FetchResult(
                    body=body,
                    final_url=url,
                    status=response.status_code,
                    headers=headers,
                    encoding=response.encoding or "utf-8",
                    elapsed=time.monotonic() - started,
                )
        except requests.exceptions.ProxyError as exc:
            raise HttpFailure(
                f"Proxy connection failed: {self._error(exc)}",
                FailureReason.PROXY_ERROR,
                url=url,
                automatic=True,
            ) from None
        except requests.Timeout as exc:
            raise HttpFailure(
                f"HTTP timeout: {self._error(exc)}",
                FailureReason.TIMEOUT,
                url=url,
                automatic=True,
            ) from exc
        except requests.RequestException as exc:
            raise HttpFailure(
                f"HTTP request failed: {self._error(exc)}",
                FailureReason.NETWORK_ERROR,
                url=url,
                automatic=True,
            ) from exc

    def _robots(self, url: str) -> Protego | None:
        if self.config.robots.strategy == RobotsStrategy.IGNORE:
            return None
        with self.policy.robots_lock(self.proxy_id, origin(url)):
            return self._robots_for_origin(url)

    def _robots_for_origin(self, url: str) -> Protego:
        host = origin(url)
        cached = self.rules_cache.get(host)
        if cached and cached.expires_at > time.time():
            return cached.rules
        name = hashlib.sha256(host.encode()).hexdigest()[:16]
        path = self.policy.robots_directory(self.proxy_id) / f"{name}.json"
        saved = read_model(path, RobotsSnapshot) if path.exists() else None
        if saved is None or saved.expires_at <= time.time():
            current = host + "/robots.txt"
            previous_rules = Protego.parse(saved.body) if saved else None
            try:
                for step in range(self.config.http.max_redirects + 1):
                    response = self._request(current, previous_rules, robots=True)
                    if response.status not in {301, 302, 303, 307, 308}:
                        break
                    destination = normalize_url(
                        urljoin(current, response.headers.get("location", ""))
                    )
                    if (
                        not response.headers.get("location")
                        or destination is None
                        or self.scope.host_reason(destination)
                    ):
                        raise HttpFailure(
                            "robots.txt redirect is outside allowed hosts.",
                            FailureReason.ROBOTS_UNAVAILABLE,
                            url=current,
                        )
                    if step == self.config.http.max_redirects:
                        raise HttpFailure(
                            "robots.txt exceeded the redirect limit.",
                            FailureReason.ROBOTS_UNAVAILABLE,
                            url=current,
                        )
                    current = destination
            except HttpFailure as exc:
                if exc.reason in {
                    FailureReason.RATE_LIMIT,
                    FailureReason.PROXY_ERROR,
                } or (
                    self.pooled
                    and exc.reason
                    in {FailureReason.TIMEOUT, FailureReason.NETWORK_ERROR}
                ):
                    raise
                raise HttpFailure(
                    f"Cannot obtain robots.txt: {exc}",
                    FailureReason.ROBOTS_UNAVAILABLE,
                    url=exc.url,
                    retry_after=exc.retry_after,
                    automatic=exc.automatic,
                ) from exc
            status = response.status
            wait = retry_after_seconds(response.headers.get("retry-after"))
            if status == 407:
                raise HttpFailure(
                    "Proxy authentication required.",
                    FailureReason.PROXY_ERROR,
                    url=current,
                    status=status,
                )
            if status == 429:
                raise HttpFailure(
                    "robots.txt returned HTTP 429.",
                    FailureReason.RATE_LIMIT,
                    url=current,
                    status=status,
                    retry_after=wait,
                    automatic=True,
                )
            if status >= 500 or status in {408, 425}:
                raise HttpFailure(
                    f"robots.txt returned HTTP {status}.",
                    FailureReason.ROBOTS_UNAVAILABLE,
                    url=current,
                    status=status,
                    retry_after=wait,
                    automatic=True,
                )
            if status == 200:
                text = response.body.decode("utf-8", errors="replace")
                if "text/html" in response.headers.get(
                    "content-type", ""
                ).lower() or re.search(
                    r"<(?:html|!doctype\s+html)\b", text, re.IGNORECASE
                ):
                    raise HttpFailure(
                        "robots.txt returned HTML; inspect before retrying.",
                        FailureReason.ROBOTS_UNAVAILABLE,
                        url=current,
                        status=status,
                    )
            elif status in {401, 403}:
                text = "User-agent: *\nDisallow: /\n"
            elif 400 <= status < 500:
                text = ""
            else:
                raise HttpFailure(
                    f"Unexpected robots.txt HTTP {status}.",
                    FailureReason.ROBOTS_UNAVAILABLE,
                    url=current,
                    status=status,
                )
            now, ttl = time.time(), 86400 if status == 200 else 3600
            control = response.headers.get("cache-control", "").lower()
            if max_age := re.search(r"\bmax-age=(\d+)", control):
                ttl = min(ttl, int(max_age[1]))
            if "no-cache" in control or "no-store" in control:
                ttl = 0
            saved = RobotsSnapshot(
                origin=host,
                url=current,
                status=status,
                body=text,
                fetched_at=now,
                expires_at=now + ttl,
            )
            atomic_model(path, saved)
        if self.config.robots.strategy == RobotsStrategy.STRICT and saved.status != 200:
            raise PolicyBlocked(
                "robots",
                f"Strict strategy requires robots.txt HTTP 200; received {saved.status}.",
            )
        rules = Protego.parse(saved.body)
        self.rules_cache[host] = CachedRules(saved.expires_at, rules)
        self.policy.register_rules(self.proxy_id, host, rules)
        self.report(
            f"robots.txt HTTP {saved.status}; strategy={self.config.robots.strategy}"
        )
        return rules

    def fetch(self, url: str) -> FetchResult:
        current, started = url, time.monotonic()
        for step in range(self.config.http.max_redirects + 1):
            if reason := self.scope.reason(current):
                raise PolicyBlocked(
                    "scope", f"Request is outside scope ({reason}): {current}"
                )
            rules = self._robots(current)
            if rules is not None and not rules.can_fetch(
                current, self.config.http.user_agent
            ):
                raise PolicyBlocked("robots", f"robots.txt disallows {current}")
            response = self._request(current, rules)
            if response.status in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                destination = normalize_url(urljoin(current, location or ""))
                if (
                    not location
                    or destination is None
                    or step == self.config.http.max_redirects
                ):
                    raise HttpFailure(
                        "Invalid redirect or exceeded redirect limit.",
                        FailureReason.REDIRECT_ERROR,
                        url=current,
                        status=response.status,
                    )
                current = destination
                continue
            if response.status != 200:
                reason = (
                    FailureReason.PROXY_ERROR
                    if response.status == 407
                    else FailureReason.RATE_LIMIT
                    if response.status == 429
                    else (
                        FailureReason.SERVER_ERROR
                        if response.status >= 500
                        else FailureReason.HTTP_ERROR
                    )
                )
                automatic = response.status in {408, 425, 429} or response.status >= 500
                raise HttpFailure(
                    f"HTTP {response.status} for {current}",
                    reason,
                    url=current,
                    status=response.status,
                    automatic=automatic,
                    retry_after=retry_after_seconds(
                        response.headers.get("retry-after")
                    ),
                )
            mime = (
                response.headers.get("content-type", "")
                .split(";", 1)[0]
                .strip()
                .lower()
            )
            if mime not in {"text/html", "application/xhtml+xml"}:
                raise HttpFailure(
                    f"Unsupported content type: {mime or '(missing)'}. Saved as a deferred task.",
                    FailureReason.CONTENT_TYPE,
                    url=current,
                    status=200,
                )
            return response.model_copy(
                update={"final_url": current, "elapsed": time.monotonic() - started}
            )
        raise AssertionError("Redirect loop must return or raise.")
