"""Public proxy-list parsing and bounded HTTPS checks, independent of crawling."""

import os
from concurrent.futures import ThreadPoolExecutor
from ipaddress import ip_address
from pathlib import Path
from time import monotonic
from urllib.parse import urlsplit

import requests
from vinf_core.files import atomic_bytes, atomic_model
from vinf_core.models import Model, VinfError


class ProxyCandidates(Model):
    urls: tuple[str, ...]
    invalid_lines: int = 0


class ProxyCheck(Model):
    url: str
    ok: bool
    seconds: float
    error: str | None = None


class ProxyReport(Model):
    source: str
    target: str
    candidates: int
    invalid_lines: int
    working: int
    checks: tuple[ProxyCheck, ...]


def read_proxies(path: Path) -> ProxyCandidates:
    urls, invalid = {}, 0
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise VinfError(f"Cannot read proxy list: {path}") from exc
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        value = line if "://" in line else f"http://{line}"
        try:
            parts = urlsplit(value)
            address = ip_address(parts.hostname or "")
            if (
                parts.scheme not in {"http", "https"}
                or not address.is_global
                or parts.username is not None
                or parts.password is not None
                or not parts.port
                or parts.path not in {"", "/"}
                or parts.query
                or parts.fragment
                or any(character.isspace() for character in value)
            ):
                raise ValueError
            host = f"[{address}]" if address.version == 6 else str(address)
            urls[f"{parts.scheme}://{host}:{parts.port}"] = None
        except ValueError:
            invalid += 1
    return ProxyCandidates(urls=tuple(urls), invalid_lines=invalid)


def check_proxy(url: str, target: str, timeout: float) -> ProxyCheck:
    started = monotonic()
    try:
        with requests.Session() as session:
            session.trust_env = False
            session.verify = (
                os.environ.get("REQUESTS_CA_BUNDLE")
                or os.environ.get("CURL_CA_BUNDLE")
                or True
            )
            session.proxies = {"http": url, "https": url}
            session.headers["User-Agent"] = (
                "FIITVinfCrawler/0.10 proxy connectivity check"
            )
            with session.get(
                target, timeout=(timeout, timeout), stream=True, allow_redirects=False
            ) as response:
                if response.status_code != 200:
                    raise VinfError(f"HTTP {response.status_code}")
                content = next(response.iter_content(2048), b"")
                if not content:
                    raise VinfError("Empty response")
        return ProxyCheck(url=url, ok=True, seconds=round(monotonic() - started, 3))
    except (requests.RequestException, VinfError) as exc:
        return ProxyCheck(
            url=url,
            ok=False,
            seconds=round(monotonic() - started, 3),
            error=str(exc) if isinstance(exc, VinfError) else type(exc).__name__,
        )


def check_proxies(
    source: Path,
    output: Path,
    *,
    workers: int = 16,
    timeout: float = 3,
    target: str = "https://example.org/",
) -> ProxyReport:
    parts = urlsplit(target)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username
        or parts.password
    ):
        raise VinfError("The proxy check target must be a public HTTPS URL.")
    candidates = read_proxies(source)
    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="vinf-probe"
    ) as executor:
        checks = tuple(
            executor.map(lambda url: check_proxy(url, target, timeout), candidates.urls)
        )
    working = sorted(
        (check for check in checks if check.ok), key=lambda check: check.seconds
    )
    report = ProxyReport(
        source=str(source),
        target=target,
        candidates=len(checks),
        invalid_lines=candidates.invalid_lines,
        working=len(working),
        checks=checks,
    )
    atomic_bytes(output, "".join(check.url + "\n" for check in working).encode())
    atomic_model(output.with_suffix(".report.json"), report)
    return report
