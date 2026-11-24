"""File contracts for independent Spark stages and source provenance."""

from typing import Literal

from pydantic import AwareDatetime, Field
from vinf_core.models import Model
from vinf_core.records import SourceDocument, SpeciesRecord


class Download(Model):
    url: str
    path: str
    fetched_at: AwareDatetime
    bytes: int = Field(ge=0)
    sha256: str
    sha1: str
    expected_sha1: str | None = None


class WikiPage(Model):
    page_id: int
    title: str
    revision_id: int | None = None
    timestamp: str | None = None
    url: str
    dump_file: str
    wikitext_sha256: str
    redirect: str | None = None
    scientific_name: str | None = None
    species_key: str | None = None
    genus: str | None = None
    family: str | None = None
    order: str | None = None
    authority: str | None = None
    status: str | None = None
    status_system: str | None = None
    description: str = ""
    categories: tuple[str, ...] = ()
    disambiguation: bool = False


class FishBaseSpecies(Model):
    source_id: str
    source: SourceDocument
    record: SpeciesRecord


class LinkExample(Model):
    source_id: str
    scientific_name: str
    url: str
    wiki_page_id: int
    wiki_title: str
    wiki_url: str
    family_check: str
    wiki_status: str | None = None
    fishbase_status: str | None = None
    wiki_description: str = ""


class StageReport(Model):
    stage: str
    producer: str
    created_at: AwareDatetime
    spark_version: str
    master: str
    partitions: int
    inputs: tuple[str, ...]
    input_sha256: tuple[str, ...]
    output: str
    rows: int
    counts: dict[str, int]
    elapsed_seconds: float
    examples: tuple[LinkExample, ...] = ()


class SparkSettings(Model):
    master: str = "local[2]"
    partitions: int = Field(default=4, ge=1, le=1024)
    driver_memory: str = "2g"
    compression: Literal["gzip", "none"] = "gzip"
