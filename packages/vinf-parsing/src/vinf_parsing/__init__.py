"""Source profiles and a resumable, offline parsing stage."""

from vinf_parsing.engine import parse_documents
from vinf_parsing.profile import ExtractionProfile, SelectionDecision
from vinf_parsing.profiles import get_profile

__all__ = ["ExtractionProfile", "SelectionDecision", "get_profile", "parse_documents"]
