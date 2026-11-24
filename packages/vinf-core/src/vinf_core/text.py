"""Shared Unicode word boundaries, offsets and ranking formulas."""

import math
import re
import unicodedata
from collections.abc import Iterator

from vinf_core.indexes import IndexConfig, Occurrence

TOKEN = re.compile(r"[^\W_]+(?:['’\-][^\W_]+)*", re.UNICODE)


def canonical_text(text: str) -> str:
    """Normalize Unicode/whitespace while preserving punctuation and display case."""
    return " ".join(unicodedata.normalize("NFKC", text).split())


def words(text: str) -> tuple[str, ...]:
    return tuple(match.casefold() for match in TOKEN.findall(canonical_text(text)))


def tokenize(text: str, config: IndexConfig) -> tuple[str, ...]:
    stop = set(config.stop_words)
    return tuple(word for word in words(text) if word not in stop)


def occurrences(text: str) -> Iterator[tuple[str, Occurrence]]:
    """Offsets address UTF-8 bytes in the stored text; each line is a segment."""
    offset = position = 0
    for segment, line in enumerate(text.split("\n")):
        cursor = 0
        byte_cursor = offset
        for match in TOKEN.finditer(line):
            byte_cursor += len(line[cursor : match.start()].encode("utf-8"))
            end = byte_cursor + len(match[0].encode("utf-8"))
            yield match[0].casefold(), Occurrence(position, byte_cursor, end, segment)
            position += 1
            cursor, byte_cursor = match.end(), end
        offset += len(line.encode("utf-8")) + 1


def tfidf_idf(documents: int, df: int) -> float:
    return math.log((documents + 1) / (df + 1)) + 1


def classic_idf(documents: int, df: int) -> float:
    return math.log(documents / df)


def bm25_idf(documents: int, df: int) -> float:
    return math.log(1 + (documents - df + 0.5) / (df + 0.5))
