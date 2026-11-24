"""The only website-specific boundary used by the generic parsing stage."""

from typing import Protocol

from vinf_core.models import Model
from vinf_core.records import SourceDocument, SpeciesRecord


class SelectionDecision(Model):
    selected: bool
    reason: str


class ExtractionProfile(Protocol):
    name: str
    version: str

    def select(self, body: str) -> SelectionDecision: ...

    def extract(self, body: str, source: SourceDocument) -> SpeciesRecord: ...
