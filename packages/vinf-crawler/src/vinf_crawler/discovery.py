from __future__ import annotations

import re
from dataclasses import dataclass
from html import unescape
from urllib.parse import urljoin, urlsplit, urlunsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from vinf_core.models import DiscoveredLink, DocumentLinks, LinkDecision

from vinf_crawler.config import CrawlConfig


def normalize_url(url: str) -> str | None:
    """Normalize syntax only. Query order, language and path case remain meaningful."""
    try:
        parts = urlsplit(url.strip())
        scheme = parts.scheme.lower()
        if (
            scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username
            or parts.password
        ):
            return None
        host = parts.hostname.encode("idna").decode("ascii").lower()
        if ":" in host:
            host = f"[{host}]"
        port = parts.port
        if port and (scheme, port) not in {("http", 80), ("https", 443)}:
            host += f":{port}"
        return urlunsplit((scheme, host, parts.path or "/", parts.query, ""))
    except (ValueError, UnicodeError):
        return None


def document_id(url: str) -> UUID:
    return uuid5(NAMESPACE_URL, url)


def origin(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


class Scope:
    def __init__(self, config: CrawlConfig):
        self.config = config.scope
        self.hosts = self.config.allowed_hosts or tuple(
            sorted({urlsplit(url).hostname for url in config.seeds})
        )

    def host_reason(self, url: str) -> str | None:
        parts = urlsplit(url)
        if parts.scheme not in self.config.allowed_schemes:
            return "scheme-not-allowed"
        host = parts.hostname or ""
        if not any(
            host == allowed
            or (self.config.include_subdomains and host.endswith("." + allowed))
            for allowed in self.hosts
        ):
            return "host-not-allowed"
        return None

    def reason(self, url: str) -> str | None:
        if reason := self.host_reason(url):
            return reason
        if any(re.search(pattern, url) for pattern in self.config.deny_url_patterns):
            return "url-denied"
        if self.config.allow_url_patterns and not any(
            re.search(pattern, url) for pattern in self.config.allow_url_patterns
        ):
            return "url-not-included"
        return None


@dataclass
class Anchor:
    href: str
    text: str = ""


# Match complete tags so quoted '>' and link-looking text in other attributes
# cannot create links. This extracts HTML links, not JavaScript or form actions.
IGNORED = re.compile(
    r"<!--[\s\S]*?(?:-->|\Z)|<(script|style)\b[^>]*>[\s\S]*?</\1\s*>",
    re.IGNORECASE,
)
TAG = re.compile(
    r"<(?P<end>/)?(?P<tag>[a-z][\w:-]*)\b"
    r"(?P<attrs>(?:[^>=]|=\s*(?:\"[^\"]*\"|'[^']*'|[^\s'\">]*))*)>",
    re.IGNORECASE,
)
ATTRIBUTE = re.compile(
    r"(?P<name>[^\s=/'\"<>`]+)"
    r"(?:\s*=\s*(?:\"(?P<double>[^\"]*)\"|'(?P<single>[^']*)'"
    r"|(?P<bare>[^\s'\"<>`]+)))?"
)


def extract_anchors(body: str) -> tuple[str | None, list[Anchor]]:
    body = IGNORED.sub("", body)
    base_href = None
    anchors: list[Anchor] = []
    active: Anchor | None = None
    position = 0
    for match in TAG.finditer(body):
        if active is not None:
            active.text += unescape(body[position : match.start()])
        position = match.end()
        tag = match["tag"].lower()
        if match["end"]:
            if tag == "a":
                active = None
            continue
        if tag not in {"a", "area", "base", "link"}:
            continue
        attributes: dict[str, str | None] = {}
        for attribute in ATTRIBUTE.finditer(match["attrs"]):
            value = next((v for v in attribute.groups()[1:] if v is not None), None)
            attributes[attribute["name"].lower()] = (
                unescape(value) if value is not None else None
            )
        href = attributes.get("href")
        if href is None:
            continue
        if tag == "base" and base_href is None:
            base_href = href
        elif tag in {"a", "area"}:
            anchor = Anchor(href, attributes.get("alt") or "")
            anchors.append(anchor)
            active = anchor if tag == "a" else None
        elif tag == "link" and set((attributes.get("rel") or "").lower().split()) & {
            "next",
            "prev",
            "alternate",
        }:
            anchors.append(Anchor(href))
    if active is not None:
        active.text += unescape(body[position:])
    return base_href, anchors


def classify_links(
    links: tuple[DiscoveredLink, ...],
    depth: int,
    config: CrawlConfig,
) -> tuple[DiscoveredLink, ...]:
    scope = Scope(config)
    allowed_count = 0
    result = []
    for link in links:
        reason = scope.reason(link.url) if link.url else "unsupported-or-invalid-url"
        decision = LinkDecision.EXCLUDED if reason else LinkDecision.ALLOWED
        if reason is None:
            if (
                config.traversal.max_depth is not None
                and depth >= config.traversal.max_depth
            ):
                decision, reason = LinkDecision.DEFERRED, "depth-limit"
            elif (
                config.traversal.max_links_per_page is not None
                and allowed_count >= config.traversal.max_links_per_page
            ):
                decision, reason = LinkDecision.DEFERRED, "width-limit"
            else:
                allowed_count += 1
        result.append(link.model_copy(update={"decision": decision, "reason": reason}))
    return tuple(result)


def discover_links(
    body: str, base_url: str, doc_id: UUID, depth: int, config: CrawlConfig
) -> DocumentLinks:
    base_href, anchors = extract_anchors(body)
    try:
        base = urljoin(base_url, base_href) if base_href is not None else base_url
    except ValueError:
        base = base_url
    unique: dict[str, DiscoveredLink] = {}
    for anchor in anchors:
        try:
            url = normalize_url(urljoin(base, anchor.href))
        except ValueError:
            url = None
        key = url or anchor.href
        if key not in unique:
            unique[key] = DiscoveredLink(
                href=anchor.href,
                text=" ".join(anchor.text.split()),
                url=url,
                doc_id=document_id(url) if url else None,
                decision=LinkDecision.ALLOWED,
            )
    links = tuple(unique[key] for key in sorted(unique))
    return DocumentLinks(doc_id=doc_id, links=classify_links(links, depth, config))
