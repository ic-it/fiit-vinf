"""Validate parsed records, optionally by re-extracting their raw input."""

from contextlib import nullcontext
from pathlib import Path

from vinf_core.artifacts import (
    crawl_documents,
    fingerprint,
    parsed_catalog,
    parsed_pages,
    read_body,
)
from vinf_core.files import read_lock
from vinf_core.models import ArtifactCheck, VinfError
from vinf_core.records import ParseStatus

from vinf_parsing.profile import ExtractionProfile


def check_parsed(
    root: Path, *, crawled: Path | None = None, profile: ExtractionProfile | None = None
) -> ArtifactCheck:
    errors: list[str] = []
    with (
        read_lock(root / ".lock"),
        read_lock(crawled / "crawl/lock") if crawled else nullcontext(),
    ):
        try:
            _, manifest = parsed_catalog(root)
            seen = set()
            for page in parsed_pages(root):
                seen.add(page.source.doc_id)
                if page.error:
                    errors.append(
                        f"Extraction error: {page.source.doc_id}: {page.error}"
                    )
                if crawled is not None and profile is not None:
                    body = read_body(crawled, page.source)
                    decision = profile.select(body)
                    if decision.selected != (page.status == ParseStatus.ACCEPTED):
                        errors.append(
                            f"Selection differs from raw input: {page.source.doc_id}"
                        )
                    if page.record and page.record != profile.extract(
                        body, page.source
                    ):
                        errors.append(
                            f"Extraction differs from raw input: {page.source.doc_id}"
                        )
            if crawled is not None:
                catalog_hash, documents = crawl_documents(crawled)
                if manifest.input_fingerprint != fingerprint(
                    manifest.producer, catalog_hash
                ):
                    errors.append("Parsed data is stale relative to the crawled input.")
                if seen != {item.doc_id for item in documents}:
                    errors.append("Parsed pages do not cover the raw catalog exactly.")
            if profile is not None and profile.version != manifest.producer:
                errors.append("The profile differs from the parsed snapshot.")
        except (VinfError, OSError, ValueError) as exc:
            errors.append(str(exc))
    return ArtifactCheck(
        ok=not errors, checked=("parsed",), errors=tuple(errors), warnings=()
    )
