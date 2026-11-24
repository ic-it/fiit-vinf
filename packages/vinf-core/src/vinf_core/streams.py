"""Read compressed artifacts without materializing decompressed files."""

from __future__ import annotations

import gzip
import io
import lzma
import zlib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from vinf_core.models import VinfError


@contextmanager
def read_stream(path: Path) -> Iterator[BinaryIO]:
    opener = (
        gzip.open
        if path.suffix == ".gz"
        else lzma.open
        if path.suffix == ".xz"
        else Path.open
    )
    try:
        with opener(path, "rb") as stream:
            yield stream
    except (OSError, EOFError, zlib.error, lzma.LZMAError) as exc:
        raise VinfError(f"Cannot read artifact stream {path}: {exc}") from exc


@contextmanager
def write_stream(stream: BinaryIO, *, gzip_output: bool) -> Iterator[BinaryIO]:
    if gzip_output:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=stream, mtime=0, compresslevel=9
        ) as compressed:
            yield compressed
    else:
        yield stream


def model_bytes(data: bytes, path: Path) -> bytes:
    if path.suffix != ".gz":
        return data
    buffer = io.BytesIO()
    with write_stream(buffer, gzip_output=True) as stream:
        stream.write(data)
    return buffer.getvalue()
