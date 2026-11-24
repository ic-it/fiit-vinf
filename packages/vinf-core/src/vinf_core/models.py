from __future__ import annotations

from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class VinfError(Exception):
    """An actionable error suitable for CLI output."""


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class RobotsStrategy(StrEnum):
    RESPECT = "respect"
    STRICT = "strict"
    CONFIG_FIRST = "config-first"
    IGNORE = "ignore"

    @property
    def respects_timing(self) -> bool:
        return self in (RobotsStrategy.RESPECT, RobotsStrategy.STRICT)


class LinkDecision(StrEnum):
    ALLOWED = "allowed"
    EXCLUDED = "excluded"
    DEFERRED = "deferred"


class Compression(StrEnum):
    NONE = "none"
    GZIP = "gzip"
    XZ = "xz"


class DiscoveredLink(Model):
    href: str
    text: str = ""
    url: str | None = None
    doc_id: UUID | None = None
    decision: LinkDecision
    reason: str | None = None


class DocumentLinks(Model):
    schema_version: Literal[2] = 2
    doc_id: UUID
    links: tuple[DiscoveredLink, ...] = ()


class DocumentMetadata(Model):
    schema_version: Literal[2] = 2
    doc_id: UUID
    requested_url: str
    final_url: str
    raw_path: str
    fetched_at: AwareDatetime
    http_status: int = Field(ge=100, le=599)
    content_type: str
    encoding: str
    size_bytes: int = Field(ge=0)
    compression: Compression = Compression.NONE
    stored_size_bytes: int | None = Field(default=None, ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    depth: int = Field(ge=0)
    discovered_from: UUID | None = None
    user_agent: str
    robots_strategy: RobotsStrategy
    elapsed_seconds: float = Field(ge=0)
    response_headers: dict[str, str] = Field(default_factory=dict)
    proxy_id: str = "direct"


class PauseState(Model):
    reason: str
    until: float | None = None


class CatalogManifest(Model):
    schema_version: Literal[2] = 2
    producer: str = "vinf-crawl/0.2"
    record_count: int
    catalog_path: str = "catalog/documents.jsonl"
    catalog_sha256: str
    input_fingerprint: str
    journal_sequence: int


class ArtifactCheck(Model):
    ok: bool
    checked: tuple[str, ...]
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
