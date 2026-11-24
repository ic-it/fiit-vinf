"""Typed contracts for the inverted index and retrieval results."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import Field
from vinf_core.indexes import IndexConfig
from vinf_core.models import Model


class RankMethod(StrEnum):
    TFIDF = "tfidf"
    TFIDF_SUM = "tfidf-sum"
    BM25 = "bm25"


class TermScore(Model):
    term: str
    tf: int
    df: int
    idf: float
    contribution: float


class MatchSpan(Model):
    """Verified match; offsets address UTF-8 bytes in the indexed text."""

    kind: Literal["term", "phrase"]
    value: str
    start: int = Field(ge=0)
    end: int = Field(gt=0)
    position: int = Field(ge=0)
    segment: int = Field(ge=0)


class RankingContext(Model):
    documents: int
    average_length: float
    query_norm: float
    config: IndexConfig


class SearchHit(Model):
    source_name: str
    source_id: str
    rank: int
    doc_id: UUID
    scientific_name: str
    common_name: str | None
    url: str
    score: float
    snippet: str
    explanation: tuple[TermScore, ...] = ()
    document_tokens: int
    document_norm: float
    fulltext: str | None = None
    matches: tuple[MatchSpan, ...] = ()


class SearchResult(Model):
    generation: str
    input_fingerprint: str
    query: str
    method: RankMethod
    query_language: Literal["vinf-query/2"] = "vinf-query/2"
    query_tokens: tuple[str, ...]
    unknown_tokens: tuple[str, ...]
    total_matches: int
    hits: tuple[SearchHit, ...]
    ranking: RankingContext


class QueryCase(Model):
    question: str = Field(min_length=1)
    description: str = Field(min_length=1)
    query: str


class QuerySet(Model):
    queries: tuple[QueryCase, ...] = Field(min_length=3)


class QueryComparison(Model):
    query: QueryCase
    results: tuple[SearchResult, ...]


class ComparisonReport(Model):
    generation: str
    top_k: int
    comparisons: tuple[QueryComparison, ...]
    note: str = (
        "Ranking comparison without human relevance judgments; not a quality metric."
    )
