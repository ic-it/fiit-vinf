"""Offline corpus measurements; keep BPE statistics separate from the word index."""

from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

import tiktoken
from pydantic import AwareDatetime
from vinf_core.artifacts import (
    crawl_documents,
    fingerprint,
    parsed_catalog,
    parsed_pages,
)
from vinf_core.content import packet_json_path, read_content
from vinf_core.files import atomic_model, read_lock
from vinf_core.index_io import IndexReader
from vinf_core.models import Model, RobotsStrategy, VinfError
from vinf_core.streams import read_stream
from vinf_crawler.models import CrawlStats
from vinf_crawler.store import CrawlStore


class StorageSize(Model):
    files: int
    decoded_bytes: int
    stored_bytes: int
    allocated_bytes: int


class StageSize(Model):
    name: str
    size: StorageSize


class PolicyCount(Model):
    strategy: RobotsStrategy
    documents: int


class CorpusStatistics(Model):
    measured_at: AwareDatetime
    crawl_catalog_sha256: str
    parsed_catalog_sha256: str
    index_generation: str
    crawl: CrawlStats
    first_fetched: AwareDatetime
    last_fetched: AwareDatetime
    original_html_bytes: int
    stored_html_bytes: int
    accepted: int
    rejected: int
    errors: int
    distinct_name_keys: int
    missing_source_ids: int
    index_documents: int
    index_terms: int
    indexed_word_tokens: int
    duplicate_records: int
    policies: tuple[PolicyCount, ...]
    sizes: tuple[StageSize, ...]
    bpe_encoding: str
    tiktoken_version: str
    html_bpe_tokens: int
    indexed_text_bpe_tokens: int


@dataclass
class SizeCounter:
    files: int = 0
    decoded_bytes: int = 0
    stored_bytes: int = 0
    allocated_bytes: int = 0

    def add(self, path: Path, *, decoded: int | None = None) -> None:
        stat = path.stat()
        self.files += 1
        self.stored_bytes += stat.st_size
        self.allocated_bytes += stat.st_blocks * 512
        if decoded is None and path.suffix in {".gz", ".xz"}:
            with read_stream(path) as stream:
                decoded = sum(
                    len(chunk) for chunk in iter(lambda: stream.read(65536), b"")
                )
        self.decoded_bytes += stat.st_size if decoded is None else decoded

    def result(self) -> StorageSize:
        return StorageSize(
            files=self.files,
            decoded_bytes=self.decoded_bytes,
            stored_bytes=self.stored_bytes,
            allocated_bytes=self.allocated_bytes,
        )


def directory_size(paths: Iterable[Path]) -> StorageSize:
    counter = SizeCounter()
    for root in paths:
        for path in root.rglob("*"):
            if path.is_file() and path.name not in {"lock", ".lock"}:
                counter.add(path)
    return counter.result()


def measure_corpus(
    crawled: Path,
    parsed: Path,
    indexed: Path,
    output: Path,
    *,
    encoding_name: str = "cl100k_base",
    progress: Callable[[str], None] = lambda _: None,
) -> CorpusStatistics:
    """Count complete documents one at a time: BPE chunk counts are not additive."""
    if any(
        output.resolve().is_relative_to(root.resolve())
        for root in (crawled, parsed, indexed)
    ):
        raise VinfError("Statistics must be written outside stage input directories.")
    with (
        read_lock(crawled / "crawl/lock"),
        read_lock(parsed / ".lock"),
        IndexReader(indexed) as reader,
    ):
        crawl_hash, documents = crawl_documents(crawled)
        _, extraction = parsed_catalog(parsed)
        manifest = reader.manifest
        if extraction.input_fingerprint != fingerprint(extraction.producer, crawl_hash):
            raise VinfError("Parse the current crawl snapshot before measuring it.")
        if manifest.input_fingerprint != fingerprint(
            "parsed-collections/2", extraction.records_sha256
        ):
            raise VinfError(
                "Rebuild the index for the parsed snapshot before measuring it."
            )
        if not documents:
            raise VinfError("Cannot measure an empty crawl.")
        encoder = tiktoken.get_encoding(encoding_name)
        html_size, metadata_size = SizeCounter(), SizeCounter()
        policies = Counter()
        html_tokens = 0
        for number, document in enumerate(documents, 1):
            path = crawled / document.raw_path
            html_size.add(path, decoded=document.size_bytes)
            for name in ("metadata.json", "links.json"):
                metadata_size.add(packet_json_path(path.parent, name))
            body = read_content(crawled, document).decode(
                document.encoding, errors="replace"
            )
            html_tokens += len(encoder.encode_ordinary(body))
            policies[document.robots_strategy] += 1
            if number % 1000 == 0:
                progress(
                    f"Counted BPE tokens for {number}/{len(documents)} HTML documents"
                )
        names = set()
        missing_ids = 0
        for page in parsed_pages(parsed):
            if page.record:
                names.add(page.record.species_key)
                missing_ids += page.record.identifier(page.record.source_name) is None
        indexed_tokens = sum(
            len(encoder.encode_ordinary(reader.text(document)))
            for document in reader.documents()
        )
        result = CorpusStatistics(
            measured_at=datetime.now(UTC),
            crawl_catalog_sha256=crawl_hash,
            parsed_catalog_sha256=extraction.records_sha256,
            index_generation=manifest.generation,
            crawl=CrawlStore(crawled).statistics(),
            first_fetched=min(document.fetched_at for document in documents),
            last_fetched=max(document.fetched_at for document in documents),
            original_html_bytes=html_size.decoded_bytes,
            stored_html_bytes=html_size.stored_bytes,
            accepted=extraction.accepted,
            rejected=extraction.rejected,
            errors=extraction.errors,
            distinct_name_keys=len(names),
            missing_source_ids=missing_ids,
            index_documents=manifest.documents,
            index_terms=manifest.terms,
            indexed_word_tokens=manifest.tokens,
            duplicate_records=manifest.duplicate_records,
            policies=tuple(
                PolicyCount(strategy=policy, documents=count)
                for policy, count in sorted(policies.items())
            ),
            sizes=(
                StageSize(name="HTML", size=html_size.result()),
                StageSize(name="Raw metadata and links", size=metadata_size.result()),
                StageSize(
                    name="Frontier and HTTP state",
                    size=directory_size((crawled / "crawl",)),
                ),
                StageSize(
                    name="Crawl catalog", size=directory_size((crawled / "catalog",))
                ),
                StageSize(name="Parsed", size=directory_size((parsed,))),
                StageSize(name="Index", size=directory_size((indexed,))),
            ),
            bpe_encoding=encoding_name,
            tiktoken_version=version("tiktoken"),
            html_bpe_tokens=html_tokens,
            indexed_text_bpe_tokens=indexed_tokens,
        )
        atomic_model(output, result)
        return result
