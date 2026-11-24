from __future__ import annotations

import re
from enum import StrEnum

from pydantic import Field, field_validator, model_validator
from vinf_core.models import Compression, Model, RobotsStrategy


class FailureAction(StrEnum):
    DEFER = "defer"
    PAUSE = "pause"


class RateLimitScope(StrEnum):
    SITE = "site"
    PROXY = "proxy"


class ScopeConfig(Model):
    # Empty allowed_hosts derives the boundary from seeds, never from site code.
    allowed_hosts: tuple[str, ...] = ()
    include_subdomains: bool = False
    allowed_schemes: tuple[str, ...] = ("http", "https")
    allow_url_patterns: tuple[str, ...] = ()
    deny_url_patterns: tuple[str, ...] = ()

    @field_validator("allowed_schemes")
    @classmethod
    def validate_schemes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or not set(value) <= {"http", "https"}:
            raise ValueError("Only http and https schemes are supported.")
        return value

    @field_validator("allowed_hosts")
    @classmethod
    def validate_hosts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        result = []
        for host in value:
            host = host.strip().lower()
            if not host or any(char in host for char in "/@?#") or "://" in host:
                raise ValueError(
                    "allowed_hosts contains hostnames, without URLs or paths."
                )
            result.append(host.encode("idna").decode("ascii"))
        return tuple(sorted(set(result)))

    @field_validator("allow_url_patterns", "deny_url_patterns")
    @classmethod
    def validate_patterns(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for pattern in value:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise ValueError(f"Invalid URL pattern {pattern!r}: {exc}") from exc
        return value


class TraversalConfig(Model):
    max_depth: int | None = Field(default=None, ge=0)
    max_pages: int | None = Field(default=None, gt=0)
    max_total_bytes: int | None = Field(default=None, gt=0)
    max_links_per_page: int | None = Field(default=None, gt=0)
    prefer_url_patterns: tuple[str, ...] = ()

    @field_validator("prefer_url_patterns")
    @classmethod
    def validate_preferences(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return ScopeConfig.validate_patterns(value)


class HttpConfig(Model):
    user_agent: str = (
        "FIITVinfCrawler/0.10 (educational project; +https://vi2026.ui.sav.sk/doku.php)"
    )
    delay_seconds: float = Field(default=10, ge=0)
    rate_limit_scope: RateLimitScope = RateLimitScope.SITE
    connect_timeout: float = Field(default=10, gt=0)
    read_timeout: float = Field(default=30, gt=0)
    max_response_bytes: int = Field(default=10 * 1024 * 1024, ge=1024)
    max_redirects: int = Field(default=5, ge=0, le=20)

    @field_validator("user_agent")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        if not value.strip() or not value.isascii() or "\r" in value or "\n" in value:
            raise ValueError("Provide a nonempty ASCII User-Agent without newlines.")
        return value


class RobotsConfig(Model):
    strategy: RobotsStrategy = RobotsStrategy.RESPECT


class FailurePolicy(Model):
    rate_limit: FailureAction = FailureAction.DEFER
    temporary: FailureAction = FailureAction.DEFER
    other: FailureAction = FailureAction.DEFER
    max_attempts: int = Field(default=3, ge=1)
    retry_delay_seconds: float = Field(default=60, ge=0)
    max_retry_delay_seconds: float = Field(default=3600, ge=0)


class StorageConfig(Model):
    compression: Compression = Compression.NONE
    membership_cache_mib: int = Field(default=32, ge=0, le=1024)
    catalog_interval_pages: int = Field(default=1000, ge=0)


class ProxyConfig(Model):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,47}$")
    url_env: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    delay_seconds: float | None = Field(default=None, ge=0)
    robots_strategy: RobotsStrategy | None = None
    failures: FailurePolicy | None = None

    @field_validator("id")
    @classmethod
    def reserve_direct(cls, value: str) -> str:
        if value == "direct" or value.startswith("pool-"):
            raise ValueError("Transport IDs 'direct' and 'pool-*' are reserved.")
        return value


class ParallelConfig(Model):
    workers: int = Field(default=1, ge=1, le=32)


class ProxyPoolConfig(Model):
    file: str
    size: int = Field(default=16, ge=1, le=32)
    fallback_direct: bool = True
    connect_timeout: float = Field(default=3, gt=0)
    read_timeout: float = Field(default=15, gt=0)


class CrawlConfig(Model):
    schema_version: int = Field(default=2, ge=2, le=2)
    seeds: tuple[str, ...] = Field(min_length=1)
    scope: ScopeConfig = Field(default_factory=ScopeConfig)
    traversal: TraversalConfig = Field(default_factory=TraversalConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    robots: RobotsConfig = Field(default_factory=RobotsConfig)
    failures: FailurePolicy = Field(default_factory=FailurePolicy)
    storage: StorageConfig = Field(default_factory=StorageConfig)

    proxies: tuple[ProxyConfig, ...] = ()
    proxy_pool: ProxyPoolConfig | None = None
    parallel: ParallelConfig = Field(default_factory=ParallelConfig)

    @model_validator(mode="after")
    def unique_proxy_ids(self):
        ids = [proxy.id for proxy in self.proxies]
        if len(ids) != len(set(ids)):
            raise ValueError("Proxy IDs must be unique.")
        return self
