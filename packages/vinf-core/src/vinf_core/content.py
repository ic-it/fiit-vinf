"""Lossless raw storage. SHA-256 and size_bytes always describe decoded bytes."""

from __future__ import annotations

import gzip
import hashlib
import io
import lzma
import zlib
from collections.abc import Iterator
from pathlib import Path

from vinf_core.files import contained_path
from vinf_core.models import Compression, DocumentMetadata, VinfError


def content_name(compression: Compression) -> str:
    match compression:
        case Compression.GZIP:
            return "content.html.gz"
        case Compression.XZ:
            return "content.html.xz"
        case Compression.NONE:
            return "content.html"


def packet_json_path(directory: Path, name: str) -> Path:
    plain, compressed = directory / name, directory / f"{name}.gz"
    if plain.exists() and compressed.exists():
        raise VinfError(
            f"Ambiguous packet metadata: both {plain.name} and {compressed.name} exist."
        )
    return compressed if compressed.exists() else plain


def encode_content(body: bytes, compression: Compression) -> bytes:
    match compression:
        case Compression.GZIP:
            buffer = io.BytesIO()
            # No filename or wall-clock timestamp in the gzip header.
            with gzip.GzipFile(
                filename="", mode="wb", fileobj=buffer, mtime=0, compresslevel=9
            ) as stream:
                stream.write(body)
            return buffer.getvalue()
        case Compression.XZ:
            return lzma.compress(body, preset=6)
        case Compression.NONE:
            return body


def iter_content(
    root: Path, document: DocumentMetadata, *, chunk_size: int = 64 * 1024
) -> Iterator[bytes]:
    """Yield decoded chunks; validate original size/hash and codec CRC at EOF."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive.")
    path = contained_path(root, document.raw_path)
    if (
        document.stored_size_bytes is not None
        and path.stat().st_size != document.stored_size_bytes
    ):
        raise VinfError(f"Stored content size mismatch: {document.doc_id}")
    try:
        match document.compression:
            case Compression.GZIP:
                opener = gzip.open
            case Compression.XZ:
                opener = lzma.open
            case Compression.NONE:
                opener = Path.open
        digest = hashlib.sha256()
        size = 0
        with opener(path, "rb") as stream:
            while chunk := stream.read(min(chunk_size, document.size_bytes - size + 1)):
                size += len(chunk)
                if size > document.size_bytes:
                    raise VinfError(
                        f"Decoded content exceeds recorded size: {document.doc_id}"
                    )
                digest.update(chunk)
                yield chunk
    except (OSError, EOFError, zlib.error, lzma.LZMAError) as exc:
        raise VinfError(f"Cannot decode raw content {document.doc_id}: {exc}") from exc
    if size != document.size_bytes or digest.hexdigest() != document.sha256:
        raise VinfError(f"Raw content checksum/size mismatch: {document.doc_id}")


def read_content(root: Path, document: DocumentMetadata) -> bytes:
    """REGEX needs one complete page; only this page is assembled in memory."""
    return b"".join(iter_content(root, document))
