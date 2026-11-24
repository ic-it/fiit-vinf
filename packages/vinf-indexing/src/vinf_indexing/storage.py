"""Sequential index writers. Temporary tables and positional payloads stay on disk."""

import math
import os
import shutil
import tempfile
from collections.abc import Iterable
from contextlib import ExitStack
from pathlib import Path
from typing import BinaryIO
from uuid import UUID
from zlib import crc32

from vinf_core.binary import checked, read_checked, string, uvarint
from vinf_core.index_format import (
    BLOCK_DOCUMENTS,
    BLOCK_ROW,
    DOC_INDEX_MAGIC,
    DOC_ROW,
    DOC_ROW_SIZE,
    DOCS_MAGIC,
    LEX_INDEX_MAGIC,
    LEX_ROW,
    LEXICON_MAGIC,
    POSTINGS_MAGIC,
    TERM_HEADER,
    TERM_HEADER_SIZE,
    TERM_MAGIC,
    TEXT_CHUNK_SIZE,
    TEXT_CRC_MAGIC,
    TEXTS_MAGIC,
    U32,
    U64,
)
from vinf_core.indexes import DocumentInfo, Occurrence, PostingRef, TermInfo
from vinf_core.models import VinfError

from vinf_indexing.records import NormRow


def _sync(stream) -> None:
    stream.flush()
    os.fsync(stream.fileno())


class DocumentWriter:
    def __init__(self, root: Path, count: int):
        self.root, self.count, self.number, self.previous = root, count, 0, b""
        self.stack = ExitStack()
        self.index = self.stack.enter_context((root / "documents.idx").open("wb"))
        self.metadata = self.stack.enter_context((root / "documents.bin").open("wb"))
        self.texts = self.stack.enter_context((root / "texts.bin").open("wb"))
        self.index.write(DOC_INDEX_MAGIC + U64.pack(count))
        self.metadata.write(DOCS_MAGIC)
        self.texts.write(TEXTS_MAGIC)

    def add(self, identifier: UUID, info: DocumentInfo, text: str, length: int) -> None:
        if identifier.bytes <= self.previous:
            raise VinfError("Document UUIDs must be unique and sorted.")
        self.previous = identifier.bytes
        common = info.common_name
        metadata = b"".join(
            string(v) for v in (info.source_name, info.source_id, info.scientific_name)
        )
        metadata += (
            uvarint(0)
            if common is None
            else uvarint(len(common.encode()) + 1) + common.encode()
        )
        metadata += string(info.url)
        body = text.encode("utf-8")
        row = DOC_ROW.pack(
            identifier.bytes,
            length,
            0.0,
            self.metadata.tell(),
            len(metadata),
            self.texts.tell(),
            len(body),
            crc32(metadata),
            crc32(body),
        )
        self.index.write(checked(row))
        self.metadata.write(metadata)
        self.texts.write(body)
        self.number += 1

    def close(self) -> None:
        try:
            if self.number != self.count:
                raise VinfError("Document count changed during index writing.")
            for stream in (self.index, self.metadata, self.texts):
                _sync(stream)
        finally:
            self.stack.close()
        with (
            (self.root / "texts.bin").open("rb") as source,
            (self.root / "texts.crc").open("wb") as output,
        ):
            source.seek(8)
            output.write(TEXT_CRC_MAGIC)
            while block := source.read(TEXT_CHUNK_SIZE):
                output.write(U32.pack(crc32(block)))
            _sync(output)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type:
            self.stack.close()
        else:
            self.close()


def write_norms(path: Path, rows: Iterable[tuple[int, Iterable[NormRow]]]) -> None:
    with path.open("r+b") as stream:
        for identifier, values in rows:
            stream.seek(16 + identifier * DOC_ROW_SIZE)
            row = list(DOC_ROW.unpack(read_checked(stream, DOC_ROW.size)))
            row[2] = math.sqrt(math.fsum(value.value for value in values))
            stream.seek(16 + identifier * DOC_ROW_SIZE)
            stream.write(checked(DOC_ROW.pack(*row)))
        _sync(stream)


class VocabularyWriter:
    def __init__(self, root: Path):
        self.stack = ExitStack()
        self.lexicon = self.stack.enter_context((root / "lexicon.bin").open("wb"))
        self.offsets = self.stack.enter_context((root / "lexicon.offsets").open("wb"))
        self.lexicon.write(LEXICON_MAGIC)
        self.offsets.write(LEX_INDEX_MAGIC + U64.pack(0))
        self.count, self.previous = 0, ""

    def add(self, info: TermInfo) -> None:
        if info.term_id != self.count or info.term <= self.previous:
            raise VinfError("Vocabulary must have dense IDs and sorted terms.")
        body = string(info.term) + b"".join(
            uvarint(v)
            for v in (
                info.df,
                info.occurrences,
                info.blocks,
                info.offset,
                info.size_bytes,
            )
        )
        self.offsets.write(
            checked(LEX_ROW.pack(self.lexicon.tell(), len(body), crc32(body)))
        )
        self.lexicon.write(body)
        self.count += 1
        self.previous = info.term

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        try:
            if not exc_type:
                self.offsets.seek(8)
                self.offsets.write(U64.pack(self.count))
                _sync(self.lexicon)
                _sync(self.offsets)
        finally:
            self.stack.close()


class PostingsWriter:
    """Append term regions to one container and reuse one block-directory scratch file."""

    def __init__(self, root: Path, workspace: Path):
        self.stack = ExitStack()
        self.stream = self.stack.enter_context((root / "postings.bin").open("wb"))
        self.stream.write(POSTINGS_MAGIC)
        self.table = self.stack.enter_context(tempfile.TemporaryFile(dir=workspace))  # noqa: SIM115 - closed by ExitStack

    def term(self, term_id: int) -> "TermWriter":
        self.table.seek(0)
        self.table.truncate()
        return TermWriter(self.stream, self.table, term_id)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        try:
            if exc_type is None:
                _sync(self.stream)
        finally:
            self.stack.close()


class TermWriter:
    def __init__(self, stream: BinaryIO, table: BinaryIO, term_id: int):
        self.stream, self.table, self.term_id = stream, table, term_id
        self.start = stream.tell()
        self.stream.write(b"\0" * TERM_HEADER_SIZE)
        self.pending: list[PostingRef] = []
        self.df = self.total = self.blocks = 0
        self.previous = -1

    def _tell(self) -> int:
        return self.stream.tell() - self.start

    def add(self, identifier: int, occurrences: Iterable[Occurrence]) -> None:
        if identifier <= self.previous:
            raise VinfError("Posting IDs must strictly increase.")
        self.previous = identifier
        offset, checksum, count = self._tell(), 0, 0
        position = start = segment = 0
        for value in occurrences:
            if value.end <= value.start or (
                count and (value.position <= position or value.start <= start)
            ):
                raise VinfError("Occurrences must strictly increase.")
            body = b"".join(
                uvarint(v)
                for v in (
                    value.position - position,
                    value.start - start,
                    value.end - value.start,
                    value.segment - segment,
                )
            )
            self.stream.write(body)
            checksum = crc32(body, checksum)
            position, start, segment = value.position, value.start, value.segment
            count += 1
        if not count:
            raise VinfError("Empty positional posting.")
        self.pending.append(
            PostingRef(identifier, count, offset, self._tell() - offset, checksum)
        )
        self.df += 1
        self.total += count
        if len(self.pending) == BLOCK_DOCUMENTS:
            self._flush()

    def _flush(self) -> None:
        if not self.pending:
            return
        body, previous = bytearray(), 0
        for posting in self.pending:
            for value in (
                posting.doc_id - previous,
                posting.tf,
                posting.offset,
                posting.size,
            ):
                body.extend(uvarint(value))
            body.extend(U32.pack(posting.crc32))
            previous = posting.doc_id
        self.table.write(
            checked(
                BLOCK_ROW.pack(
                    self.pending[0].doc_id,
                    self.pending[-1].doc_id,
                    self._tell(),
                    len(body),
                    len(self.pending),
                    crc32(body),
                )
            )
        )
        self.stream.write(body)
        self.blocks += 1
        self.pending.clear()

    def finish(self, term: str) -> TermInfo:
        self._flush()
        table_offset = self._tell()
        self.table.seek(0)
        shutil.copyfileobj(self.table, self.stream, length=65536)
        size = self._tell()
        self.stream.seek(self.start)
        self.stream.write(
            checked(
                TERM_HEADER.pack(
                    TERM_MAGIC, self.df, self.total, self.blocks, table_offset
                )
            )
        )
        self.stream.seek(self.start + size)
        # Make this region visible to the norm reader without fsync per term.
        self.stream.flush()
        return TermInfo(
            term=term,
            term_id=self.term_id,
            df=self.df,
            occurrences=self.total,
            blocks=self.blocks,
            offset=self.start,
            size_bytes=size,
        )
