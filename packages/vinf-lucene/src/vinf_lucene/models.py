"""Saved index provenance, field definitions and retrieval results."""

from enum import StrEnum

from pydantic import AwareDatetime, Field
from vinf_core.models import Model


class Scope(StrEnum):
    ALL = "all"
    FISHBASE = "fishbase"
    WIKIPEDIA = "wikipedia"


class FieldDefinition(Model):
    name: str
    kind: str
    stored: bool
    reason: str


class InputArtifact(Model):
    path: str
    sha256: str


class Manifest(Model):
    producer: str = "vinf-lucene/1"
    created_at: AwareDatetime
    pylucene_version: str
    lucene_version: str
    java_version: str
    documents: int
    linked_documents: int
    input_fingerprint: str
    inputs: tuple[InputArtifact, ...]
    fields: tuple[FieldDefinition, ...]
    analyzer: str = "StandardAnalyzer; empty stop-word set; no stemming"
    similarity: str = "BM25(k1=1.2,b=0.75)"
    index_bytes: int
    elapsed_seconds: float


class BuildResult(Model):
    output: str
    documents: int
    linked_documents: int
    index_bytes: int
    reused: bool
    elapsed_seconds: float


class IndexAudit(Model):
    created_at: AwareDatetime
    documents: int
    identical_fishbase_records: int
    identical_wikipedia_records: int
    unique_wikipedia_pages: int
    old_same_identities: bool
    lucene_check_index_clean: bool
    input_fingerprint: str
    lucene_manifest_sha256: str
    old_manifest_sha256: str
    old_index_payload_bytes: int
    lucene_index_payload_bytes: int
    errors: tuple[str, ...] = ()


class Hit(Model):
    identity: str
    source_id: str
    rank: int
    scientific_name: str
    common_name: str | None
    url: str
    wiki_url: str | None
    score: float
    snippet: str
    explanation: str | None = None


class SearchResult(Model):
    query: str
    scope: Scope
    parsed_query: str
    total_matches: int
    documents: int
    elapsed_ms: float
    hits: tuple[Hit, ...]


class MeasuredRun(Model):
    engine: str
    total_matches: int
    median_ms: float
    top_source_ids: tuple[str, ...]
    top_names: tuple[str, ...]


class ComparisonCase(Model):
    question: str
    description: str
    query: str
    runs: tuple[MeasuredRun, ...]
    old_vs_lucene_fishbase_overlap: int
    old_vs_lucene_enriched_overlap: int


class Comparison(Model):
    created_at: AwareDatetime
    old_index: InputArtifact
    lucene_index: InputArtifact
    documents: int
    top_k: int
    repetitions: int = Field(ge=1)
    cases: tuple[ComparisonCase, ...]
    note: str = (
        "Observed rankings, top-k overlap and local median latency; no human relevance "
        "judgments, so these are not relevance-quality scores. Both indexes contain the "
        "same FishBase source IDs. Lucene has different token boundaries and per-field "
        "scoring; the enriched run additionally searches Wikipedia. One unmeasured warm-up "
        "per engine/query precedes repetitions. JVM startup is excluded; index opening "
        "and result construction are included. Scores are not comparable across engines."
    )
