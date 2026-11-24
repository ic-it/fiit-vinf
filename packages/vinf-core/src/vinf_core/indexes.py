"""Contracts for positional postings and the on-disk index."""

from typing import Literal, NamedTuple
from uuid import UUID

from pydantic import Field

from vinf_core.models import Model

INDEX_VERSION = "vinf-index/7"


class IndexConfig(Model):
    tokenizer_version: Literal["unicode-words-v2"] = "unicode-words-v2"
    stop_words: tuple[str, ...] = ()
    k1: float = Field(default=1.2, gt=0)
    b: float = Field(default=0.75, ge=0, le=1)


class Occurrence(NamedTuple):
    """Word position, document-relative UTF-8 byte start/end, section number."""

    position: int
    start: int
    end: int
    segment: int


class PostingRef(NamedTuple):
    doc_id: int
    tf: int
    offset: int
    size: int
    crc32: int


class TermInfo(Model):
    term: str
    term_id: int = Field(ge=0)
    df: int = Field(gt=0)
    occurrences: int = Field(gt=0)
    blocks: int = Field(gt=0)
    offset: int = Field(ge=8)
    size_bytes: int = Field(gt=0)


class DocumentStats(Model):
    local_id: int = Field(ge=0)
    doc_id: UUID
    length: int = Field(ge=0)
    tfidf_norm: float = Field(ge=0)
    metadata_offset: int = Field(ge=8)
    metadata_size: int = Field(gt=0)
    text_offset: int = Field(ge=8)
    text_size: int = Field(ge=0)
    metadata_crc32: int
    text_crc32: int


class DocumentInfo(Model):
    source_name: str
    source_id: str
    scientific_name: str
    common_name: str | None
    url: str


class IndexDocument(DocumentStats, DocumentInfo):
    pass


class IndexFile(Model):
    path: str
    size_bytes: int = Field(ge=8)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class IndexManifest(Model):
    producer: Literal["vinf-index/7"] = INDEX_VERSION
    generation: str = Field(pattern=r"^[0-9a-f]{16}$")
    build_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    input_fingerprint: str
    config: IndexConfig
    documents: int = Field(gt=0)
    terms: int = Field(ge=0)
    tokens: int = Field(ge=0)
    duplicate_records: int = Field(ge=0)
    average_length: float = Field(ge=0)
    files: tuple[IndexFile, ...]


class IndexResult(Model):
    output: str
    generation: str
    documents: int
    terms: int
    tokens: int
    duplicate_records: int
    reused: bool
    sort_runs: int = 0
    merge_passes: int = 0
