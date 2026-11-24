from __future__ import annotations

import fcntl
import hashlib
import lzma
import os
import tempfile
import zlib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from pydantic import BaseModel, ValidationError

from vinf_core.models import VinfError
from vinf_core.streams import model_bytes, read_stream


def read_model[DocumentModel: BaseModel](
    path: Path, model: type[DocumentModel]
) -> DocumentModel:
    try:
        with read_stream(path) as stream:
            return model.model_validate_json(stream.read())
    except (OSError, EOFError, lzma.LZMAError, zlib.error, ValidationError) as exc:
        raise VinfError(f"Cannot read {path}: {exc}") from exc


def sync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_bytes(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_dir(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def atomic_model(path: Path, value: BaseModel) -> None:
    atomic_bytes(
        path,
        model_bytes((value.model_dump_json() + "\n").encode("utf-8"), path),
    )


def file_hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def contained_path(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise VinfError(f"Artifact path escapes dataset: {relative}")
    return path


@contextmanager
def dataset_lock(root: Path) -> Iterator[None]:
    (root / "crawl").mkdir(parents=True, exist_ok=True)
    with (root / "crawl" / "lock").open("a+") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise VinfError("Another writer is using this dataset.") from exc
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


@contextmanager
def artifact_lock(root: Path) -> Iterator[None]:
    """A stage-local lock; parsed/indexed directories need no crawler state."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".lock").open("a+") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise VinfError(f"Another process is using this artifact: {root}") from exc
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def separate_roots(source: Path, output: Path) -> None:
    source, output = source.resolve(), output.resolve()
    if source.is_relative_to(output) or output.is_relative_to(source):
        raise VinfError(
            "Input and output must be separate, non-overlapping directories."
        )


@contextmanager
def read_lock(path: Path) -> Iterator[None]:
    """Share an existing control lock without modifying the input."""
    stream = path.open("rb")
    with stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise VinfError(f"A writer is using this input: {path.parent}") from exc
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)
