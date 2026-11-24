"""Typed temporary records for external sorting; never part of parsed output."""

from io import BytesIO
from typing import NamedTuple
from uuid import UUID

from vinf_core.binary import (
    float64,
    read_float64,
    read_string,
    read_uvarint,
    string,
    uvarint,
)
from vinf_core.indexes import Occurrence
from vinf_core.models import Model


class SourceRef(Model):
    source_name: str
    source_id: str
    doc_id: UUID
    timestamp: float
    path: str
    source_fingerprint: str
    record_fingerprint: str

    @property
    def identity(self) -> tuple[str, str]:
        return self.source_name, self.source_id


class TokenRow(NamedTuple):
    term: str
    doc_id: int
    occurrence: Occurrence


def encode_token(row: TokenRow) -> bytes:
    value = row.occurrence
    return string(row.term) + b"".join(
        uvarint(number)
        for number in (
            row.doc_id,
            value.position,
            value.start,
            value.end - value.start,
            value.segment,
        )
    )


def decode_token(body: bytes) -> TokenRow:
    stream = BytesIO(body)
    term, identifier = read_string(stream), read_uvarint(stream)
    position, start, length, segment = (read_uvarint(stream) for _ in range(4))
    return TokenRow(
        term, identifier, Occurrence(position, start, start + length, segment)
    )


class NormRow(NamedTuple):
    doc_id: int
    term_id: int
    value: float


def encode_norm(row: NormRow) -> bytes:
    return uvarint(row.doc_id) + uvarint(row.term_id) + float64(row.value)


def decode_norm(body: bytes) -> NormRow:
    stream = BytesIO(body)
    return NormRow(read_uvarint(stream), read_uvarint(stream), read_float64(stream))
