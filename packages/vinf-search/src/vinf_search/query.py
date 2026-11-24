"""Streaming document-at-a-time ranking with a bounded top-k heap."""

import heapq
import math
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

from vinf_core.index_io import IndexReader
from vinf_core.indexes import IndexDocument, PostingRef
from vinf_core.models import VinfError
from vinf_core.postings import TermCursor
from vinf_core.text import bm25_idf, classic_idf, tfidf_idf

from vinf_search.language import parse_query
from vinf_search.matching import Matcher, positive_terms, prepare
from vinf_search.models import (
    ComparisonReport,
    MatchSpan,
    QueryComparison,
    QuerySet,
    RankingContext,
    RankMethod,
    SearchHit,
    SearchResult,
    TermScore,
)


def snippet(text: str, spans: tuple[MatchSpan, ...], length: int = 240) -> str:
    offset = len(text.encode()[: spans[0].start].decode()) if spans else 0
    start = max(0, offset - 65)
    return (
        ("…" if start else "")
        + " ".join(text[start : start + length].split())
        + ("…" if start + length < len(text) else "")
    )


@dataclass
class Candidate:
    score: float
    document: IndexDocument
    explanation: tuple[TermScore, ...]
    postings: tuple[tuple[TermCursor, PostingRef], ...] = ()
    phrases: tuple[MatchSpan, ...] = ()

    @property
    def key(self):
        return (
            -self.score,
            self.document.scientific_name.casefold(),
            str(self.document.doc_id),
        )

    def __lt__(self, other):
        # The heap root is the worst retained result.
        return self.key > other.key


def search(
    index: Path,
    query: str,
    *,
    method: RankMethod = RankMethod.BM25,
    limit: int = 10,
    explain: bool = False,
    include_text: bool = False,
    capture_matches: bool = False,
) -> SearchResult:
    with IndexReader(index) as reader:
        return _search(
            reader,
            query,
            method=method,
            limit=limit,
            explain=explain,
            include_text=include_text,
            capture_matches=capture_matches,
        )


def _search(
    reader: IndexReader,
    query: str,
    *,
    method: RankMethod = RankMethod.BM25,
    limit: int = 10,
    explain: bool = False,
    include_text: bool = False,
    capture_matches: bool = False,
) -> SearchResult:
    if limit < 1:
        raise VinfError("Search limit must be positive.")
    manifest = reader.manifest
    expression = prepare(parse_query(query), manifest.config)
    if expression is None:
        raise VinfError(
            "The query contains only stop words. Add a search word or quoted phrase."
        )
    query_counts = positive_terms(expression, manifest.config)
    highlight_terms = (
        positive_terms(expression, manifest.config, include_phrases=False)
        if capture_matches
        else ()
    )
    tokens = tuple(query_counts.elements())
    best: list[Candidate] = []
    matches = 0
    with ExitStack() as stack:
        matcher = Matcher(reader, stack, capture=capture_matches)
        stream = matcher.evaluate(expression)
        known = [
            info
            for term in sorted(query_counts)
            if (info := matcher.info(term)) is not None
        ]
        cursors = [(info, stack.enter_context(reader.cursor(info))) for info in known]
        query_norm = math.sqrt(
            sum(
                (
                    (1 + math.log(query_counts[info.term]))
                    * tfidf_idf(manifest.documents, info.df)
                )
                ** 2
                for info in known
            )
        )
        while stream.current is not None:
            identifier = stream.current
            document = reader.stats(identifier)
            matches += 1
            score = 0.0
            explanation = []
            postings = []
            for info, cursor in cursors:
                posting = cursor.advance_to(identifier)
                if posting is None or posting.doc_id != identifier:
                    continue
                if method == RankMethod.TFIDF_SUM:
                    idf = classic_idf(manifest.documents, info.df)
                    contribution = posting.tf * idf
                elif method == RankMethod.TFIDF:
                    idf = tfidf_idf(manifest.documents, info.df)
                    denominator = document.tfidf_norm * query_norm
                    contribution = (
                        (
                            (1 + math.log(posting.tf))
                            * idf
                            * (1 + math.log(query_counts[info.term]))
                            * idf
                            / denominator
                        )
                        if denominator
                        else 0.0
                    )
                else:
                    idf = bm25_idf(manifest.documents, info.df)
                    k1, b = manifest.config.k1, manifest.config.b
                    normalization = (
                        document.length / manifest.average_length
                        if manifest.average_length
                        else 0.0
                    )
                    contribution = (
                        idf
                        * posting.tf
                        * (k1 + 1)
                        / (posting.tf + k1 * (1 - b + b * normalization))
                    )
                score += contribution
                if info.term in highlight_terms:
                    postings.append((cursor, posting))
                if explain:
                    explanation.append(
                        TermScore(
                            term=info.term,
                            tf=posting.tf,
                            df=info.df,
                            idf=idf,
                            contribution=contribution,
                        )
                    )
            if len(best) < limit or score >= best[0].score:
                candidate = Candidate(
                    score,
                    reader.document(identifier, document),
                    tuple(explanation),
                    tuple(postings),
                    matcher.phrase_matches(identifier) if capture_matches else (),
                )
                if len(best) < limit:
                    heapq.heappush(best, candidate)
                elif candidate.key < best[0].key:
                    heapq.heapreplace(best, candidate)
            stream.next()
        unknown = tuple(
            sorted(term for term, info in matcher.infos.items() if info is None)
        )
        # Cursors remain open: read retained posting references directly, without
        # looking up documents or evaluating the query again.
        hits = tuple(
            materialize_hit(
                reader,
                value,
                rank,
                include_text=include_text or capture_matches,
            )
            for rank, value in enumerate(sorted(best, key=lambda item: item.key), 1)
        )
    return SearchResult(
        generation=manifest.generation,
        input_fingerprint=manifest.input_fingerprint,
        query=query,
        method=method,
        query_tokens=tokens,
        unknown_tokens=unknown,
        total_matches=matches,
        hits=hits,
        ranking=RankingContext(
            documents=manifest.documents,
            average_length=manifest.average_length,
            query_norm=query_norm,
            config=manifest.config,
        ),
    )


def materialize_hit(
    reader: IndexReader, value: Candidate, rank: int, *, include_text: bool
) -> SearchHit:
    spans = list(value.phrases)
    for cursor, posting in value.postings:
        with cursor.positions(posting) as positions:
            for occurrence in positions:
                spans.append(
                    MatchSpan(
                        kind="term",
                        value=cursor.info.term,
                        start=occurrence.start,
                        end=occurrence.end,
                        position=occurrence.position,
                        segment=occurrence.segment,
                    )
                )
    matches = tuple(
        sorted(
            set(spans), key=lambda span: (span.start, span.end, span.kind, span.value)
        )
    )
    document = value.document
    text = reader.text(document) if include_text else None
    if any(span.end > document.text_size or span.end <= span.start for span in matches):
        raise VinfError("Match offsets are outside the indexed text.")
    return SearchHit(
        rank=rank,
        source_name=document.source_name,
        source_id=document.source_id,
        doc_id=document.doc_id,
        scientific_name=document.scientific_name,
        common_name=document.common_name,
        url=document.url,
        score=value.score,
        snippet=snippet(text, matches) if text is not None else "",
        explanation=value.explanation,
        document_tokens=document.length,
        document_norm=document.tfidf_norm,
        fulltext=text,
        matches=matches,
    )


def compare(index: Path, queries: QuerySet, *, top_k: int = 5) -> ComparisonReport:
    with IndexReader(index) as reader:
        return _compare(reader, queries, top_k=top_k)


def _compare(reader: IndexReader, queries: QuerySet, *, top_k: int) -> ComparisonReport:
    comparisons = []
    for case in queries.queries:
        comparisons.append(
            QueryComparison(
                query=case,
                results=tuple(
                    _search(
                        reader, case.query, method=method, limit=top_k, explain=True
                    )
                    for method in RankMethod
                ),
            )
        )
    return ComparisonReport(
        generation=reader.manifest.generation,
        top_k=top_k,
        comparisons=tuple(comparisons),
    )
