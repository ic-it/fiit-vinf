from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path

from pydantic import BaseModel, ValidationError

from vinf_core.content import packet_json_path, read_content
from vinf_core.files import contained_path, file_hash, read_model, sync_dir
from vinf_core.models import CatalogManifest, DocumentMetadata, VinfError
from vinf_core.records import (
    ParsedEntry,
    ParsedManifest,
    ParsedPage,
    ParseStatus,
    SourceDocument,
)
from vinf_core.streams import read_stream, write_stream


def fingerprint(*values: str) -> str:
    return hashlib.sha256(json.dumps(values, ensure_ascii=False).encode()).hexdigest()


def iter_models[T: BaseModel](path: Path, model: type[T]) -> Iterator[T]:
    with read_stream(path) as stream:
        for number, line in enumerate(stream, 1):
            try:
                yield model.model_validate_json(line)
            except ValidationError as exc:
                raise VinfError(f"Invalid {path}, line {number}: {exc}") from exc


def write_models(path: Path, records: Iterable[BaseModel]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as raw:
            with write_stream(raw, gzip_output=path.suffix == ".gz") as stream:
                for record in records:
                    stream.write((record.model_dump_json() + "\n").encode("utf-8"))
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temporary, path)
        sync_dir(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def source_document(document: DocumentMetadata) -> SourceDocument:
    return SourceDocument(
        doc_id=document.doc_id,
        requested_url=document.requested_url,
        url=document.final_url,
        raw_path=document.raw_path,
        encoding=document.encoding,
        sha256=document.sha256,
        fetched_at=document.fetched_at,
    )


def crawl_documents(dataset: Path) -> tuple[str, tuple[DocumentMetadata, ...]]:
    manifest = read_model(dataset / "catalog/manifest.json", CatalogManifest)
    path = contained_path(dataset, manifest.catalog_path)
    documents = tuple(iter_models(path, DocumentMetadata))
    if (
        file_hash(path) != manifest.catalog_sha256
        or len(documents) != manifest.record_count
    ):
        raise VinfError(
            "Crawler catalog changed or is corrupt. Stop crawling and run doctor."
        )
    if len({doc.doc_id for doc in documents}) != len(documents):
        raise VinfError("Crawler catalog has duplicate UUIDs.")
    return manifest.catalog_sha256, tuple(
        sorted(documents, key=lambda doc: str(doc.doc_id))
    )


def read_body(dataset: Path, source: SourceDocument) -> str:
    path = contained_path(dataset, source.raw_path)
    metadata = read_model(
        packet_json_path(path.parent, "metadata.json"), DocumentMetadata
    )
    if source_document(metadata) != source:
        raise VinfError(f"Raw metadata/source mismatch: {source.doc_id}")
    try:
        return read_content(dataset, metadata).decode(source.encoding, errors="replace")
    except LookupError as exc:
        raise VinfError(
            f"Unknown encoding for {source.doc_id}: {source.encoding}"
        ) from exc


def parsed_catalog(root: Path) -> tuple[Path, ParsedManifest]:
    manifest = read_model(root / "manifest.json", ParsedManifest)
    path = contained_path(root, manifest.records_path)
    if file_hash(path) != manifest.records_sha256:
        raise VinfError(
            f"Artifact checksum mismatch: {path}. Rerun the producing stage."
        )
    return path, manifest


def parsed_items(root: Path) -> Iterator[tuple[ParsedEntry, ParsedPage]]:
    """Read sorted, checksummed references and payloads without an in-memory ID set."""
    path, manifest = parsed_catalog(root)
    previous = None
    count = 0
    counts = {status: 0 for status in ParseStatus}
    for entry in iter_models(path, ParsedEntry):
        if previous is not None and entry.doc_id <= previous:
            raise VinfError("Parsed UUIDs must be unique and sorted.")
        previous = entry.doc_id
        count += 1
        item_path = contained_path(root, entry.path)
        if file_hash(item_path) != entry.sha256:
            raise VinfError(f"Parsed payload checksum mismatch: {item_path}")
        page = read_model(item_path, ParsedPage)
        if (
            page.source.doc_id != entry.doc_id
            or page.status != entry.status
            or page.fingerprint
            != fingerprint(manifest.producer, page.source.model_dump_json())
            or (page.record and page.record.profile != manifest.producer)
        ):
            raise VinfError(f"Parsed payload/catalog mismatch: {entry.doc_id}")
        counts[page.status] += 1
        yield entry, page
    if (
        count != manifest.record_count
        or counts[ParseStatus.ACCEPTED] != manifest.accepted
        or counts[ParseStatus.REJECTED] != manifest.rejected
        or counts[ParseStatus.ERROR] != manifest.errors
    ):
        raise VinfError("Parsed counts do not match the manifest.")


def parsed_pages(root: Path) -> Iterator[ParsedPage]:
    for _, page in parsed_items(root):
        yield page
