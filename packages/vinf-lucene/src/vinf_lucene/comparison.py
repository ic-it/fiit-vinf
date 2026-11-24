"""Measure the original BM25 index, Lucene FishBase-only, and enriched Lucene."""

import statistics
import time
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

from vinf_core.files import file_hash
from vinf_core.index_io import IndexReader
from vinf_core.models import VinfError
from vinf_search import search as old_search
from vinf_search.models import QuerySet, RankMethod

from vinf_lucene.models import (
    Comparison,
    ComparisonCase,
    InputArtifact,
    MeasuredRun,
    Scope,
)
from vinf_lucene.runtime import initialize
from vinf_lucene.search import search


def compare(
    old_index: Path, new_index: Path, queries: QuerySet, *, top_k=5, repetitions=5
):
    initialize()
    # Equal counts alone would hide an accidental change of input identities.
    from java.nio.file import Paths
    from org.apache.lucene.index import DirectoryReader
    from org.apache.lucene.store import FSDirectory

    directory = FSDirectory.open(Paths.get(str((new_index / "index").resolve())))
    reader = DirectoryReader.open(directory)
    try:
        stored = reader.storedFields()
        new_ids = {
            (stored.document(i).get("source"), stored.document(i).get("source_id"))
            for i in range(reader.maxDoc())
        }
    finally:
        reader.close()
        directory.close()
    with IndexReader(old_index) as old:
        old_ids = {(doc.source_name, doc.source_id) for doc in old.documents()}
    if old_ids != new_ids:
        raise VinfError(
            "Comparison requires identical FishBase source IDs in both indexes."
        )
    cases = []
    for case in queries.queries:
        actions = (
            (
                "self-written BM25",
                partial(
                    old_search,
                    old_index,
                    case.query,
                    method=RankMethod.BM25,
                    limit=top_k,
                ),
            ),
            (
                "Lucene FishBase",
                partial(
                    search, new_index, case.query, scope=Scope.FISHBASE, limit=top_k
                ),
            ),
            (
                "Lucene enriched",
                partial(search, new_index, case.query, scope=Scope.ALL, limit=top_k),
            ),
        )
        runs = []
        for label, action in actions:
            action()  # JVM/analyzer/classes/file caches are warmed, not timed.
            timings = []
            for _ in range(repetitions):
                started = time.perf_counter()
                result = action()
                timings.append((time.perf_counter() - started) * 1000)
            runs.append(
                MeasuredRun(
                    engine=label,
                    total_matches=result.total_matches,
                    median_ms=round(statistics.median(timings), 3),
                    top_source_ids=tuple(hit.source_id for hit in result.hits),
                    top_names=tuple(hit.scientific_name for hit in result.hits),
                )
            )
        before = set(runs[0].top_source_ids)
        cases.append(
            ComparisonCase(
                question=case.question,
                description=case.description,
                query=case.query,
                runs=tuple(runs),
                old_vs_lucene_fishbase_overlap=len(
                    before & set(runs[1].top_source_ids)
                ),
                old_vs_lucene_enriched_overlap=len(
                    before & set(runs[2].top_source_ids)
                ),
            )
        )
    return Comparison(
        created_at=datetime.now(UTC),
        old_index=InputArtifact(
            path=str(old_index), sha256=file_hash(old_index / "manifest.json")
        ),
        lucene_index=InputArtifact(
            path=str(new_index), sha256=file_hash(new_index / "manifest.json")
        ),
        documents=len(old_ids),
        top_k=top_k,
        repetitions=repetitions,
        cases=tuple(cases),
    )
