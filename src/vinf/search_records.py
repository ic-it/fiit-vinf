"""Attach saved parsed pages to search hits without running extraction."""

from contextlib import ExitStack
from pathlib import Path

from vinf_core.artifacts import fingerprint, iter_models, parsed_catalog
from vinf_core.files import contained_path, file_hash, read_lock, read_model
from vinf_core.models import VinfError
from vinf_core.records import ParsedEntry, ParsedPage, ParseStatus
from vinf_search.models import SearchHit, SearchResult


class DetailedHit(SearchHit):
    parsed: ParsedPage


class DetailedResult(SearchResult):
    hits: tuple[DetailedHit, ...]


def detailed_result(result: SearchResult, roots: tuple[Path, ...]) -> DetailedResult:
    """Scan catalog references, reading payloads only for returned UUIDs."""
    roots = tuple(sorted(path.resolve() for path in roots))
    if not roots or len(set(roots)) != len(roots):
        raise VinfError("Provide distinct parsed inputs with --parsed or a project.")
    wanted = {hit.doc_id for hit in result.hits}
    pages = {}
    with ExitStack() as locks:
        for root in roots:
            locks.enter_context(read_lock(root / ".lock"))
        catalogs = [(root, *parsed_catalog(root)) for root in roots]
        if result.input_fingerprint != fingerprint(
            "parsed-collections/2", *sorted(item[2].records_sha256 for item in catalogs)
        ):
            raise VinfError(
                "Parsed inputs do not match this index snapshot. Provide the inputs "
                "used to build it with --parsed, or rebuild after parsing."
            )
        for root, catalog, manifest in catalogs:
            for entry in iter_models(catalog, ParsedEntry):
                if entry.doc_id not in wanted:
                    continue
                path = contained_path(root, entry.path)
                if file_hash(path) != entry.sha256:
                    raise VinfError(f"Parsed payload checksum mismatch: {path}")
                page = read_model(path, ParsedPage)
                if (
                    page.source.doc_id != entry.doc_id
                    or page.status != ParseStatus.ACCEPTED
                    or page.status != entry.status
                    or page.record.profile != manifest.producer
                    or page.fingerprint
                    != fingerprint(manifest.producer, page.source.model_dump_json())
                ):
                    raise VinfError(f"Parsed payload/catalog mismatch: {path}")
                key = page.record.source_name, page.source.doc_id
                if key in pages and pages[key] != page:
                    raise VinfError(f"Ambiguous parsed versions for {entry.doc_id}.")
                pages[key] = page
        hits = []
        for hit in result.hits:
            page = pages.get((hit.source_name, hit.doc_id))
            if page is None:
                raise VinfError(f"Parsed record is missing for {hit.doc_id}.")
            record = page.record
            if (
                (record.identifier(record.source_name) or str(page.source.doc_id))
                != hit.source_id
                or record.scientific_name != hit.scientific_name
                or record.common_name != hit.common_name
                or page.source.url != hit.url
            ):
                raise VinfError(f"Parsed record differs from index: {hit.doc_id}.")
            hits.append(DetailedHit(**hit.model_dump(), parsed=page))
    return DetailedResult(**result.model_dump(exclude={"hits"}), hits=tuple(hits))
