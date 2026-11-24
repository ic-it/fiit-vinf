"""Read-only integrity checks using one document and one posting block at a time."""

import math
from collections import defaultdict
from itertools import zip_longest
from pathlib import Path

from vinf_core.index_io import IndexReader
from vinf_core.models import ArtifactCheck, VinfError
from vinf_core.text import occurrences, tfidf_idf


def check_index(index: Path, *, deep: bool = False) -> ArtifactCheck:
    errors = []
    try:
        with IndexReader(index) as reader:
            manifest = reader.manifest
            if deep:
                reader.verify()
            previous = None
            metadata_offset = text_offset = 8
            total_length = 0
            stop = set(manifest.config.stop_words)
            for document in reader.documents():
                if previous is not None and document.doc_id <= previous:
                    raise VinfError("Document UUIDs must be unique and sorted.")
                previous = document.doc_id
                if (
                    document.metadata_offset != metadata_offset
                    or document.text_offset != text_offset
                ):
                    raise VinfError("Document data spans must be contiguous.")
                metadata_offset += document.metadata_size
                text_offset += document.text_size
                total_length += document.length
                if not deep:
                    continue
                terms = defaultdict(list)
                for term, value in occurrences(reader.text(document)):
                    terms[term].append(value)
                if document.length != sum(len(values) for values in terms.values()):
                    raise VinfError(f"Document token count mismatch: {document.doc_id}")
                weights = []
                for term, expected in terms.items():
                    if term in stop:
                        continue
                    info = reader.find(term)
                    if info is None:
                        raise VinfError(f"Missing indexed term: {term}")
                    with reader.cursor(info) as cursor:
                        posting = cursor.advance_to(document.local_id)
                        if (
                            posting is None
                            or posting.doc_id != document.local_id
                            or posting.tf != len(expected)
                        ):
                            raise VinfError(f"Missing/invalid document posting: {term}")
                        with cursor.positions(posting) as positions:
                            if any(a != b for a, b in zip_longest(expected, positions)):
                                raise VinfError(
                                    f"Invalid positions/offsets/segments: {term}"
                                )
                    weights.append(
                        (
                            (1 + math.log(len(expected)))
                            * tfidf_idf(manifest.documents, info.df)
                        )
                        ** 2
                    )
                if not math.isclose(
                    math.sqrt(math.fsum(weights)),
                    document.tfidf_norm,
                    rel_tol=1e-12,
                    abs_tol=1e-15,
                ):
                    raise VinfError(f"TF-IDF norm mismatch: {document.doc_id}")
            if (
                metadata_offset != reader.sizes["documents.bin"]
                or text_offset != reader.sizes["texts.bin"]
            ):
                raise VinfError("Document spans do not cover their files.")
            if not math.isclose(
                manifest.average_length, total_length / manifest.documents
            ):
                raise VinfError("Index corpus statistics mismatch.")
            term_count = tokens = 0
            for info in reader.terms():
                if info.term in stop:
                    raise VinfError(f"Stop word must not have postings: {info.term}")
                term_count += 1
                tokens += info.occurrences
            if term_count != manifest.terms or tokens != manifest.tokens:
                raise VinfError("Vocabulary count mismatch.")
    except (VinfError, OSError, ValueError) as exc:
        errors.append(str(exc))
    return ArtifactCheck(
        ok=not errors, checked=("index",), errors=tuple(errors), warnings=()
    )
