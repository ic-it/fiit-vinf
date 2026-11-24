"""Random access to the current index: no full vocabulary or document table loads."""

import math
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import ExitStack
from io import BytesIO
from pathlib import Path
from uuid import UUID
from zlib import crc32

from vinf_core.artifacts import fingerprint
from vinf_core.binary import (
    end_of_file,
    header,
    read_checked,
    read_exact,
    read_string,
    read_uvarint,
)
from vinf_core.files import file_hash, read_lock, read_model
from vinf_core.index_format import (
    DOC_ROW,
    DOC_ROW_SIZE,
    FILES,
    LEX_ROW,
    LEX_ROW_SIZE,
    TERM_HEADER_SIZE,
    TEXT_CHUNK_SIZE,
    U32,
    U64,
)
from vinf_core.indexes import (
    INDEX_VERSION,
    DocumentInfo,
    DocumentStats,
    IndexDocument,
    IndexManifest,
    TermInfo,
)
from vinf_core.models import VinfError
from vinf_core.postings import TermCursor


def read_index(index: Path) -> tuple[Path, IndexManifest]:
    manifest = read_model(index / "manifest.json", IndexManifest)
    expected = fingerprint(
        INDEX_VERSION, manifest.input_fingerprint, manifest.config.model_dump_json()
    )
    if manifest.build_fingerprint != expected or manifest.generation != expected[:16]:
        raise VinfError("Index fingerprint mismatch. Rebuild the index.")
    if {f.path for f in manifest.files} != {name for name, _ in FILES} or len(
        manifest.files
    ) != len(FILES):
        raise VinfError("Index manifest has an invalid file list.")
    return index, manifest


class IndexReader:
    """Bounded file handles, fixed-row lookups and an eight-chunk text cache."""

    def __init__(self, root: Path, *, acquire_lock: bool = True):
        self.root, self.acquire_lock = root, acquire_lock
        self.stack = ExitStack()
        self.text_cache: OrderedDict[int, bytes] = OrderedDict()

    def __enter__(self):
        try:
            if self.acquire_lock:
                self.stack.enter_context(read_lock(self.root / ".lock"))
            _, self.manifest = read_index(self.root)
            sizes = {f.path: f.size_bytes for f in self.manifest.files}
            self.streams = {}
            for name, magic in FILES:
                path = self.root / name
                if path.stat().st_size != sizes[name]:
                    raise VinfError(f"Index file size mismatch: {name}")
                stream = self.stack.enter_context(path.open("rb"))
                header(stream, magic)
                self.streams[name] = stream
            if (
                sizes["documents.idx"] != 16 + self.manifest.documents * DOC_ROW_SIZE
                or sizes["lexicon.offsets"] != 16 + self.manifest.terms * LEX_ROW_SIZE
                or U64.unpack(read_exact(self.streams["documents.idx"], 8))[0]
                != self.manifest.documents
                or U64.unpack(read_exact(self.streams["lexicon.offsets"], 8))[0]
                != self.manifest.terms
                or sizes["texts.crc"]
                != 8 + math.ceil((sizes["texts.bin"] - 8) / TEXT_CHUNK_SIZE) * 4
            ):
                raise VinfError("Index table dimensions do not match the manifest.")
            self.sizes = sizes
            return self
        except OSError as exc:
            self.stack.close()
            raise VinfError(f"Cannot open index: {exc}") from exc
        except BaseException:
            self.stack.close()
            raise

    def __exit__(self, *_):
        self.stack.close()

    def stats(self, identifier: int) -> DocumentStats:
        if not 0 <= identifier < self.manifest.documents:
            raise VinfError(f"Unknown local document ID: {identifier}")
        stream = self.streams["documents.idx"]
        stream.seek(16 + identifier * DOC_ROW_SIZE)
        values = DOC_ROW.unpack(read_checked(stream, DOC_ROW.size))
        (
            uuid,
            length,
            norm,
            meta_offset,
            meta_size,
            text_offset,
            text_size,
            meta_crc,
            text_crc,
        ) = values
        if (
            meta_offset < 8
            or meta_size <= 0
            or meta_offset + meta_size > self.sizes["documents.bin"]
            or text_offset < 8
            or text_offset + text_size > self.sizes["texts.bin"]
        ):
            raise VinfError("Document spans escape their data files.")
        return DocumentStats(
            local_id=identifier,
            doc_id=UUID(bytes=uuid),
            length=length,
            tfidf_norm=norm,
            metadata_offset=meta_offset,
            metadata_size=meta_size,
            text_offset=text_offset,
            text_size=text_size,
            metadata_crc32=meta_crc,
            text_crc32=text_crc,
        )

    def document(
        self, identifier: int, stats: DocumentStats | None = None
    ) -> IndexDocument:
        stats = stats or self.stats(identifier)
        stream = self.streams["documents.bin"]
        stream.seek(stats.metadata_offset)
        body = read_exact(stream, stats.metadata_size)
        if crc32(body) != stats.metadata_crc32:
            raise VinfError("Document metadata checksum mismatch.")
        payload = BytesIO(body)
        source_name, source_id, scientific_name = (
            read_string(payload) for _ in range(3)
        )
        common_size = read_uvarint(payload)
        try:
            common_name = (
                read_exact(payload, common_size - 1).decode("utf-8")
                if common_size
                else None
            )
        except UnicodeError as exc:
            raise VinfError("Invalid common name encoding.") from exc
        info = DocumentInfo(
            source_name=source_name,
            source_id=source_id,
            scientific_name=scientific_name,
            common_name=common_name,
            url=read_string(payload),
        )
        end_of_file(payload)
        return IndexDocument.model_validate(stats.model_dump() | info.model_dump())

    def documents(self) -> Iterator[IndexDocument]:
        for identifier in range(self.manifest.documents):
            yield self.document(identifier)

    def term(self, identifier: int) -> TermInfo:
        if not 0 <= identifier < self.manifest.terms:
            raise VinfError(f"Unknown term ID: {identifier}")
        offsets = self.streams["lexicon.offsets"]
        offsets.seek(16 + identifier * LEX_ROW_SIZE)
        offset, size, checksum = LEX_ROW.unpack(read_checked(offsets, LEX_ROW.size))
        if offset < 8 or size <= 0 or offset + size > self.sizes["lexicon.bin"]:
            raise VinfError("Invalid vocabulary span.")
        stream = self.streams["lexicon.bin"]
        stream.seek(offset)
        body = read_exact(stream, size)
        if crc32(body) != checksum:
            raise VinfError("Vocabulary entry checksum mismatch.")
        payload = BytesIO(body)
        info = TermInfo(
            term_id=identifier,
            term=read_string(payload),
            df=read_uvarint(payload),
            occurrences=read_uvarint(payload),
            blocks=read_uvarint(payload),
            offset=read_uvarint(payload),
            size_bytes=read_uvarint(payload),
        )
        end_of_file(payload)
        if (
            info.size_bytes < TERM_HEADER_SIZE
            or info.offset + info.size_bytes > self.sizes["postings.bin"]
        ):
            raise VinfError("Invalid term span in postings.bin.")
        return info

    def find(self, term: str) -> TermInfo | None:
        low, high = 0, self.manifest.terms
        while low < high:
            middle = (low + high) // 2
            value = self.term(middle)
            if value.term < term:
                low = middle + 1
            elif value.term > term:
                high = middle
            else:
                return value
        return None

    def terms(self) -> Iterator[TermInfo]:
        previous = ""
        offset = 8
        for number in range(self.manifest.terms):
            info = self.term(number)
            if info.term <= previous:
                raise VinfError("Vocabulary must be strictly sorted.")
            if info.offset != offset:
                raise VinfError("Term spans must be contiguous in postings.bin.")
            offset += info.size_bytes
            previous = info.term
            yield info
        if offset != self.sizes["postings.bin"]:
            raise VinfError("Term spans do not cover postings.bin.")

    def cursor(self, term: TermInfo) -> TermCursor:
        try:
            return TermCursor(self.root, term, self.manifest.documents)
        except OSError as exc:
            raise VinfError(f"Cannot read postings for {term.term}: {exc}") from exc

    def _text_chunk(self, number: int) -> bytes:
        if number in self.text_cache:
            self.text_cache.move_to_end(number)
            return self.text_cache[number]
        start = 8 + number * TEXT_CHUNK_SIZE
        stream = self.streams["texts.bin"]
        stream.seek(start)
        body = read_exact(stream, min(TEXT_CHUNK_SIZE, self.sizes["texts.bin"] - start))
        checksums = self.streams["texts.crc"]
        checksums.seek(8 + number * 4)
        if U32.unpack(read_exact(checksums, 4))[0] != crc32(body):
            raise VinfError("Text chunk checksum mismatch.")
        self.text_cache[number] = body
        if len(self.text_cache) > 8:
            self.text_cache.popitem(last=False)
        return body

    def text_span(self, document: DocumentStats, start: int, end: int) -> bytes:
        if not 0 <= start <= end <= document.text_size:
            raise VinfError("Requested text span is outside the document.")
        result = bytearray()
        offset, stop = document.text_offset + start - 8, document.text_offset + end - 8
        while offset < stop:
            number, within = divmod(offset, TEXT_CHUNK_SIZE)
            body = self._text_chunk(number)
            amount = min(stop - offset, len(body) - within)
            result.extend(body[within : within + amount])
            offset += amount
        return bytes(result)

    def text(self, document: DocumentStats) -> str:
        body = self.text_span(document, 0, document.text_size)
        if crc32(body) != document.text_crc32:
            raise VinfError("Document text checksum mismatch.")
        try:
            return body.decode("utf-8")
        except UnicodeError as exc:
            raise VinfError("Invalid UTF-8 document text.") from exc

    def verify(self) -> None:
        """Full byte/structural verification, still bounded by one block/chunk."""
        for item in self.manifest.files:
            if file_hash(self.root / item.path) != item.sha256:
                raise VinfError(f"Index file checksum mismatch: {item.path}")
        occurrences = 0
        for info in self.terms():
            count = total = 0
            previous = -1
            with self.cursor(info) as cursor:
                for posting in cursor:
                    if posting.doc_id <= previous:
                        raise VinfError("Posting document IDs must strictly increase.")
                    previous = posting.doc_id
                    count += 1
                    total += posting.tf
                    with cursor.positions(posting) as positions:
                        for _ in positions:
                            pass
            if count != info.df or total != info.occurrences:
                raise VinfError("Term statistics do not match postings.")
            occurrences += total
        if occurrences != self.manifest.tokens:
            raise VinfError("Posting token count mismatch.")
