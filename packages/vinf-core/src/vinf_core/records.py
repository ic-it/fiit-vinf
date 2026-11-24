"""Extraction contracts. Search text and tokenization belong to the indexer."""

from __future__ import annotations

import unicodedata
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from vinf_core.models import Model


def species_key(name: str) -> str:
    """Candidate join key, not a taxonomic synonym resolver."""
    return " ".join(unicodedata.normalize("NFKC", name).casefold().split())


class SourceDocument(Model):
    doc_id: UUID
    requested_url: str
    url: str
    raw_path: str
    encoding: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    fetched_at: AwareDatetime


class Section(Model):
    name: str
    text: str
    raw_start: int = Field(ge=0)
    raw_end: int = Field(ge=0)
    rule: str = "h1-section"


class Taxonomy(Model):
    class_name: str | None = None
    order: str | None = None
    family: str | None = None


class Environment(StrEnum):
    MARINE = "marine"
    FRESHWATER = "freshwater"
    BRACKISH = "brackish"


class Length(Model):
    value: float = Field(gt=0)
    unit: Literal["mm", "cm", "m"]
    measurement: str | None = None
    sex: str | None = None
    centimetres: float = Field(gt=0)


class ExternalIdentifier(Model):
    namespace: str = Field(pattern=r"^[a-z][a-z0-9_.-]*$")
    value: str = Field(min_length=1)


class SpeciesRecord(Model):
    schema_version: Literal[3] = 3
    profile: str
    source_name: str = Field(pattern=r"^[a-z][a-z0-9_.-]*$")
    identifiers: tuple[ExternalIdentifier, ...] = ()
    scientific_name: str
    species_key: str
    common_name: str | None = None
    taxonomy: Taxonomy
    environments: tuple[Environment, ...] = ()
    climate: str | None = None
    max_length: Length | None = None
    iucn_code: str | None = None
    sections: tuple[Section, ...] = ()
    warnings: tuple[str, ...] = ()

    def identifier(self, namespace: str) -> str | None:
        return next(
            (item.value for item in self.identifiers if item.namespace == namespace),
            None,
        )

    def section(self, prefix: str) -> str | None:
        """Read a section without storing another copy of its text."""
        return next(
            (
                item.text
                for item in self.sections
                if item.name.casefold().startswith(prefix.casefold())
            ),
            None,
        )

    @property
    def habitat(self) -> str | None:
        return self.section("Environment")

    @property
    def distribution(self) -> str | None:
        return self.section("Distribution")

    @property
    def biology(self) -> str | None:
        return self.section("Biology")

    @property
    def iucn_text(self) -> str | None:
        return self.section("IUCN")

    @model_validator(mode="after")
    def validate_identity(self):
        if self.species_key != species_key(self.scientific_name):
            raise ValueError("species_key must match the normalized scientific name.")
        namespaces = [item.namespace for item in self.identifiers]
        if len(namespaces) != len(set(namespaces)):
            raise ValueError("An identifier namespace may occur only once.")
        return self


class ParseStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    ERROR = "error"


class ParsedPage(Model):
    schema_version: Literal[1] = 1
    source: SourceDocument
    fingerprint: str
    status: ParseStatus
    reason: str
    record: SpeciesRecord | None = None
    error: str | None = None

    @model_validator(mode="after")
    def validate_outcome(self):
        if (self.status == ParseStatus.ACCEPTED) != (self.record is not None):
            raise ValueError("Only accepted pages must contain an extracted record.")
        if (self.status == ParseStatus.ERROR) != (self.error is not None):
            raise ValueError("Only failed pages must contain an error.")
        return self


class ParsedEntry(Model):
    doc_id: UUID
    path: str
    sha256: str
    status: ParseStatus


class ParsedManifest(Model):
    schema_version: Literal[2] = 2
    producer: str
    input_fingerprint: str
    record_count: int = Field(ge=0)
    accepted: int = Field(ge=0)
    rejected: int = Field(ge=0)
    errors: int = Field(ge=0)
    records_path: str
    records_sha256: str


class ParseResult(Model):
    output: str
    documents: int
    accepted: int
    rejected: int
    errors: int
    written: int
    reused: int
