from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from pydantic import Field
from vinf_core.models import Model, PauseState, RobotsStrategy

from vinf_crawler.config import CrawlConfig, RateLimitScope


def utc_now() -> datetime:
    return datetime.now(UTC)


class TaskStatus(StrEnum):
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    DEFERRED = "deferred"
    BLOCKED = "blocked"
    SKIPPED = "skipped"


class FailureReason(StrEnum):
    RATE_LIMIT = "rate-limit"
    TIMEOUT = "timeout"
    NETWORK_ERROR = "network-error"
    PROXY_ERROR = "proxy-error"
    SERVER_ERROR = "server-error"
    HTTP_ERROR = "http-error"
    ROBOTS_UNAVAILABLE = "robots-unavailable"
    CONTENT_TYPE = "content-type"
    TOO_LARGE = "too-large"
    REDIRECT_ERROR = "redirect-error"
    BYTE_BUDGET = "byte-budget"


class CrawlTask(Model):
    doc_id: UUID
    url: str
    depth: int = Field(ge=0)
    sequence: int = Field(ge=0)
    parent_id: UUID | None = None


class TaskRecord(Model):
    task: CrawlTask
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    cycle_attempts: int = 0
    reason: str | None = None
    last_error: str | None = None
    http_status: int | None = None
    retry_at: float | None = None
    automatic_retry: bool = False
    proxy_id: str | None = None


class Request(Model):
    seq: int = Field(ge=1)


class PageCommitted(Request):
    doc_id: UUID


class FetchFailed(Request):
    proxy_id: str = "direct"
    doc_id: UUID
    reason: FailureReason
    error: str
    http_status: int | None = None
    retry_at: float | None = None
    automatic_retry: bool = False


class TaskBlocked(Request):
    doc_id: UUID
    reason: str
    error: str


class TaskSkipped(Request):
    doc_id: UUID
    reason: str


class TaskWaiting(Request):
    doc_id: UUID
    retry_at: float | None
    error: str


class TaskAssigned(Request):
    doc_id: UUID
    proxy_id: str


class TasksRetried(Request):
    proxy_id: str | None = None
    doc_ids: tuple[UUID, ...]


class Paused(Request):
    pause: PauseState


class Resumed(Request):
    pass


class ConfigChanged(Request):
    config: CrawlConfig


type Operation = (
    PageCommitted
    | FetchFailed
    | TaskBlocked
    | TaskSkipped
    | TasksRetried
    | TaskAssigned
    | TaskWaiting
    | Paused
    | Resumed
    | ConfigChanged
)


class TransportStatus(Model):
    proxy_id: str
    url_env: str | None
    robots_strategy: RobotsStrategy
    minimum_delay_seconds: float
    pause: PauseState | None
    cooldown_until: float | None
    disabled: str | None = None


class CrawlStats(Model):
    dataset: str
    discovered: int
    downloaded: int
    pending: int
    deferred: int
    blocked: int
    skipped: int
    limited_links: int
    html_bytes: int
    stored_html_bytes: int | None = None
    complete: bool
    next_retry_at: float | None
    robots_strategy: RobotsStrategy
    minimum_delay_seconds: float
    rate_limit_scope: RateLimitScope = RateLimitScope.SITE
    pause: PauseState | None
    operation_sequence: int
    pending_write: bool
    workers: int
    proxy_ids: tuple[str, ...]
    transports: tuple[TransportStatus, ...]


class RunResult(Model):
    statistics: CrawlStats
    saved_this_run: int = 0
    recovered_this_run: int = 0
    failed_attempts_this_run: int = 0
    stop_reason: str


class DoctorReport(Model):
    ok: bool
    deep: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    statistics: CrawlStats
