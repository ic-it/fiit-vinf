"""Boolean, positional phrase, fuzzy and exact-field queries executed by Lucene."""

import re
import time
from pathlib import Path

from vinf_core.files import read_lock, read_model
from vinf_core.models import VinfError
from vinf_core.text import canonical_text

from vinf_lucene.models import Hit, Manifest, Scope, SearchResult
from vinf_lucene.runtime import analyzer, initialize
from vinf_lucene.schema import text_fields


def _snippet(document, query: str, scope: Scope):
    terms = [
        t.casefold()
        for t in re.findall(r"[^\W_]+", query)
        if t not in {"AND", "OR", "NOT"}
    ]
    values = [document.get(name) or "" for name in text_fields(scope)]
    body = canonical_text(
        max(values, key=lambda s: sum(t in s.casefold() for t in terms))
    )
    positions = [body.casefold().find(t) for t in terms if t in body.casefold()]
    start = max(0, min(positions, default=0) - 75)
    return (
        ("…" if start else "")
        + body[start : start + 320]
        + ("…" if len(body) > start + 320 else "")
    )


def search(index: Path, text: str, *, scope=Scope.ALL, limit=10, explain=False):
    if not text.strip() or len(text) > 4096:
        raise VinfError("Enter a query of 1 to 4096 characters.")
    if not 1 <= limit <= 1000:
        raise VinfError("Result limit must be between 1 and 1000.")
    lucene = initialize()
    from java.nio.file import Paths
    from org.apache.lucene.index import DirectoryReader
    from org.apache.lucene.queryparser.classic import (
        MultiFieldQueryParser,
        QueryParser,
        QueryParserBase,
    )
    from org.apache.lucene.search import IndexSearcher
    from org.apache.lucene.search.similarities import BM25Similarity
    from org.apache.lucene.store import FSDirectory

    started = time.perf_counter()
    with read_lock(index / ".lock"):
        manifest = read_model(index / "manifest.json", Manifest)
        directory = FSDirectory.open(Paths.get(str((index / "index").resolve())))
        reader, analysis = None, analyzer()
        try:
            parser = MultiFieldQueryParser(list(text_fields(scope)), analysis)
            parser.setDefaultOperator(QueryParser.Operator.AND)
            try:
                # MultiFieldQueryParser also declares static parse overloads.
                query = QueryParserBase.cast_(parser).parse(text)
            except lucene.JavaError as exc:
                raise VinfError(
                    f"Invalid Lucene query: {exc.getJavaException()}"
                ) from exc
            reader = DirectoryReader.open(directory)
            if reader.numDocs() != manifest.documents:
                raise VinfError(
                    "Lucene document count differs from its manifest. Rebuild the index."
                )
            searcher = IndexSearcher(reader)
            searcher.setSimilarity(BM25Similarity(1.2, 0.75))
            top = searcher.search(query, limit)
            count = searcher.count(query)
            stored = reader.storedFields()
            hits = []
            for rank, scored in enumerate(top.scoreDocs, 1):
                doc = stored.document(scored.doc)
                hits.append(
                    Hit(
                        identity=doc.get("identity"),
                        source_id=doc.get("source_id"),
                        rank=rank,
                        scientific_name=doc.get("scientific_name"),
                        common_name=doc.get("common_name"),
                        url=doc.get("url"),
                        wiki_url=doc.get("wiki_url"),
                        score=scored.score,
                        snippet=_snippet(doc, text, scope),
                        explanation=str(searcher.explain(query, scored.doc))
                        if explain
                        else None,
                    )
                )
            return SearchResult(
                query=text,
                scope=scope,
                parsed_query=str(query),
                total_matches=count,
                documents=reader.numDocs(),
                elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
                hits=tuple(hits),
            )
        finally:
            if reader is not None:
                reader.close()
            analysis.close()
            directory.close()
