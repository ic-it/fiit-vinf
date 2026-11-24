"""Bounded cursors over term regions in postings.bin; positions are lazy streams."""

import os
from collections.abc import Iterator
from io import BytesIO
from pathlib import Path
from typing import NamedTuple
from zlib import crc32

from vinf_core.binary import MAX_UINT, header, read_checked, read_exact, read_uvarint
from vinf_core.index_format import (
    BLOCK_DOCUMENTS,
    BLOCK_ROW,
    BLOCK_ROW_SIZE,
    POSTINGS_MAGIC,
    TERM_HEADER,
    TERM_HEADER_SIZE,
    TERM_MAGIC,
)
from vinf_core.indexes import Occurrence, PostingRef, TermInfo
from vinf_core.models import VinfError


class Block(NamedTuple):
    first: int
    last: int
    offset: int
    size: int
    count: int
    crc32: int


class PositionReader:
    """Decode occurrences individually; close also verifies any unread bytes."""

    def __init__(self, path: Path, posting: PostingRef):
        self.stream = path.open("rb")
        self.stream.seek(posting.offset)
        self.remaining, self.checksum = posting.size, 0
        self.posting = posting
        self.position = self.start = self.segment = self.number = 0

    def read(self, size: int) -> bytes:
        body = self.stream.read(min(size, self.remaining))
        self.remaining -= len(body)
        self.checksum = crc32(body, self.checksum)
        return body

    def __iter__(self):
        return self

    def __next__(self) -> Occurrence:
        if self.number == self.posting.tf:
            if self.remaining:
                raise VinfError("Trailing bytes in positional posting.")
            self._verify()
            raise StopIteration
        step, gap = read_uvarint(self), read_uvarint(self)
        length, segment_gap = read_uvarint(self), read_uvarint(self)
        if not length or (self.number and (not step or not gap)):
            raise VinfError("Invalid occurrence order/length.")
        self.position += step
        self.start += gap
        self.segment += segment_gap
        self.number += 1
        if max(self.position, self.start + length, self.segment) > MAX_UINT:
            raise VinfError("Occurrence delta overflow.")
        return Occurrence(self.position, self.start, self.start + length, self.segment)

    def _verify(self) -> None:
        if self.remaining or self.checksum != self.posting.crc32:
            raise VinfError("Position checksum/length mismatch. Rebuild the index.")

    def close(self) -> None:
        try:
            while self.remaining:
                if not self.read(min(self.remaining, 65536)):
                    raise VinfError("Truncated position stream.")
            self._verify()
        finally:
            self.stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class TermCursor:
    """Keep at most 128 posting references, with on-disk block-directory lookup."""

    def __init__(self, root: Path, info: TermInfo, documents: int):
        self.path, self.info, self.documents = root / "postings.bin", info, documents
        self.stream = self.path.open("rb")
        try:
            header(self.stream, POSTINGS_MAGIC)
            if (
                info.offset < 8
                or info.size_bytes < TERM_HEADER_SIZE
                or info.offset + info.size_bytes
                > os.fstat(self.stream.fileno()).st_size
            ):
                raise VinfError(f"Invalid postings span: {info.term}")
            self.stream.seek(info.offset)
            body = read_checked(self.stream, TERM_HEADER.size)
            magic, df, count, blocks, self.table_offset = TERM_HEADER.unpack(body)
            if (
                magic != TERM_MAGIC
                or not 1 <= df <= documents
                or count < df
                or df != info.df
                or count != info.occurrences
                or blocks != info.blocks
                or blocks != (df + BLOCK_DOCUMENTS - 1) // BLOCK_DOCUMENTS
                or self.table_offset < TERM_HEADER_SIZE
                or self.table_offset + blocks * BLOCK_ROW_SIZE != info.size_bytes
            ):
                raise VinfError(f"Invalid term header: {info.term}")
            self.block_number = self.index = -1
            self.entries: tuple[PostingRef, ...] = ()
            self.current: PostingRef | None = None
            self.next()
        except BaseException:
            self.stream.close()
            raise

    def block(self, number: int) -> Block:
        if not 0 <= number < self.info.blocks:
            raise VinfError("Block number is outside the term's postings span.")
        self.stream.seek(self.info.offset + self.table_offset + number * BLOCK_ROW_SIZE)
        block = Block(*BLOCK_ROW.unpack(read_checked(self.stream, BLOCK_ROW.size)))
        if (
            not 0 <= block.first <= block.last < self.documents
            or not 1 <= block.count <= BLOCK_DOCUMENTS
            or block.offset < TERM_HEADER_SIZE
            or not block.count * 8 <= block.size <= block.count * 44
            or block.count
            != min(BLOCK_DOCUMENTS, self.info.df - number * BLOCK_DOCUMENTS)
            or block.offset + block.size > self.table_offset
        ):
            raise VinfError("Invalid term block directory.")
        return block

    def _load(self, number: int) -> None:
        block = self.block(number)
        self.stream.seek(self.info.offset + block.offset)
        body = read_exact(self.stream, block.size)
        if crc32(body) != block.crc32:
            raise VinfError(f"Posting block checksum mismatch: {self.info.term}")
        stream, identifier = BytesIO(body), 0
        entries = []
        for index in range(block.count):
            gap, tf = read_uvarint(stream), read_uvarint(stream)
            offset, size = read_uvarint(stream), read_uvarint(stream)
            checksum = int.from_bytes(read_exact(stream, 4), "little")
            identifier += gap
            if (
                not tf
                or not size
                or (index and not gap)
                or not block.first <= identifier <= block.last
                or offset < TERM_HEADER_SIZE
                or offset + size > block.offset
            ):
                raise VinfError("Invalid posting reference.")
            entries.append(
                PostingRef(identifier, tf, self.info.offset + offset, size, checksum)
            )
        if (
            stream.read(1)
            or entries[0].doc_id != block.first
            or entries[-1].doc_id != block.last
        ):
            raise VinfError("Posting block bounds/length mismatch.")
        self.entries, self.block_number, self.index = tuple(entries), number, 0
        self.current = self.entries[0]

    def next(self) -> PostingRef | None:
        self.index += 1
        if self.index < len(self.entries):
            self.current = self.entries[self.index]
        elif self.block_number + 1 < self.info.blocks:
            self._load(self.block_number + 1)
        else:
            self.current = None
        return self.current

    def advance_to(self, identifier: int) -> PostingRef | None:
        if self.current is None or self.current.doc_id >= identifier:
            return self.current
        if self.entries[-1].doc_id < identifier:
            low, high = self.block_number + 1, self.info.blocks
            while low < high:
                middle = (low + high) // 2
                if self.block(middle).last < identifier:
                    low = middle + 1
                else:
                    high = middle
            if low == self.info.blocks:
                self.current = None
                return None
            self._load(low)
        while self.current is not None and self.current.doc_id < identifier:
            self.next()
        return self.current

    def positions(self, posting: PostingRef | None = None) -> PositionReader:
        value = posting or self.current
        if value is None:
            raise VinfError("Cursor has no current document.")
        if (
            not value.tf
            or not value.size
            or value.offset < self.info.offset + TERM_HEADER_SIZE
            or value.offset + value.size > self.info.offset + self.table_offset
        ):
            raise VinfError("Positions are outside the term's postings span.")
        return PositionReader(self.path, value)

    def __iter__(self) -> Iterator[PostingRef]:
        while self.current is not None:
            yield self.current
            self.next()

    def close(self) -> None:
        self.stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
