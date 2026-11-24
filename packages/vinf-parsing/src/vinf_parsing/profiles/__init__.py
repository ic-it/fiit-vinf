"""Explicit profile registry. Add an adapter here after implementing its rules."""

from vinf_core.models import VinfError

from vinf_parsing.profile import ExtractionProfile
from vinf_parsing.profiles.fishbase import FishBaseProfile

PROFILES: tuple[ExtractionProfile, ...] = (FishBaseProfile(),)


def get_profile(name: str) -> ExtractionProfile:
    for profile in PROFILES:
        if profile.name == name:
            return profile
    raise VinfError(
        f"Unknown parsing profile {name!r}; available: {', '.join(p.name for p in PROFILES)}"
    )
