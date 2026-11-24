"""FishBase summary profile. All entity extraction uses Python regular expressions.

This profile is downstream of the generic crawler; it never makes HTTP requests.
Offsets refer to Unicode character positions in the decoded original HTML.
"""

from __future__ import annotations

import html
import re
from urllib.parse import parse_qs, urlsplit

from vinf_core.records import (
    Environment,
    ExternalIdentifier,
    Length,
    Section,
    SourceDocument,
    SpeciesRecord,
    Taxonomy,
    species_key,
)

from vinf_parsing.profile import SelectionDecision

PROFILE = "fishbase-summary-regex/5"
FLAGS = re.IGNORECASE | re.DOTALL
SCINAME = re.compile(
    r"<span\b[^>]*class\s*=\s*['\"][^'\"]*\bsciname\b[^'\"]*['\"][^>]*>(.*?)</span\s*>",
    FLAGS,
)
NAME_BLOCK = re.compile(
    r"<div\b[^>]*id\s*=\s*['\"]ss-sciname['\"][^>]*>(.*?)</div\s*>", FLAGS
)
HEADINGS = re.compile(r"<h1\b[^>]*>(.*?)</h1\s*>", FLAGS)


def plain_text(value: str) -> str:
    value = re.sub(r"<!--.*?-->", " ", value, flags=re.DOTALL)
    value = re.sub(r"<(script|style)\b[^>]*>.*?</\1\s*>", " ", value, flags=FLAGS)
    value = re.sub(
        r"<(?:br\b[^>]*|/p\s*|/div\s*|/li\s*)>", "\n", value, flags=re.IGNORECASE
    )
    value = re.sub(r"<[^>]*>", " ", value)
    return " ".join(html.unescape(value).split())


def scientific_name(body: str) -> str | None:
    block = NAME_BLOCK.search(body)
    if not block:
        return None
    words = [plain_text(match[1]) for match in SCINAME.finditer(block[1])]
    name = " ".join(words)
    return (
        name
        if re.fullmatch(
            r"[A-Z][a-zA-Z-]+\s+[a-z][a-zA-Z-]+(?:\s+[a-z][a-zA-Z-]+)?", name
        )
        else None
    )


def sections(body: str) -> tuple[Section, ...]:
    start = re.search(r"<div\b[^>]*id\s*=\s*['\"]ss-main['\"]", body, FLAGS)
    if not start:
        return ()
    headings = list(HEADINGS.finditer(body, start.end()))
    result = []
    for index, match in enumerate(headings):
        # Navigation labels and reference numbers inside the heading are not content.
        heading = re.split(r"<span\b|<br\b", match[1], maxsplit=1, flags=re.IGNORECASE)[
            0
        ]
        name = plain_text(heading)
        if not name:
            continue
        end = headings[index + 1].start() if index + 1 < len(headings) else len(body)
        if name.lower().startswith(
            ("tools", "special reports", "download xml", "internet sources")
        ):
            break
        text = plain_text(body[match.end() : end])
        if text:
            result.append(
                Section(name=name, text=text, raw_start=match.end(), raw_end=end)
            )
    return tuple(result)


def section_text(values: tuple[Section, ...], prefix: str) -> str | None:
    return next(
        (
            item.text
            for item in values
            if item.name.casefold().startswith(prefix.casefold())
        ),
        None,
    )


def select_reason(body: str) -> tuple[bool, str, str | None]:
    name = scientific_name(body)
    if name is None:
        return False, "no-summary-scientific-name", None
    found = sections(body)
    recognizable = sum(
        any(
            item.name.casefold().startswith(prefix)
            for prefix in (
                "classification",
                "environment",
                "distribution",
                "size",
                "biology",
            )
        )
        for item in found
    )
    if recognizable < 2:
        return False, "missing-summary-sections", name
    return True, "fishbase-summary-content", name


def summary_id(url: str) -> str | None:
    parts = urlsplit(html.unescape(url))
    numeric = re.fullmatch(r"/summary/(\d+)/?", parts.path, re.IGNORECASE)
    if numeric:
        return str(int(numeric[1]))
    if parts.path.casefold().endswith("/speciessummary.php"):
        for key, values in parse_qs(parts.query).items():
            if key.casefold() == "id" and values[0].isdigit():
                return str(int(values[0]))
    return None


def fishbase_id(body: str, source: SourceDocument) -> str | None:
    """Prefer the resolved URL; slug pages expose their ID in language links."""
    for url in (source.url, source.requested_url):
        if identifier := summary_id(url):
            return identifier
    main = re.search(r"<div\b[^>]*id\s*=\s*['\"]ss-main['\"]", body, FLAGS)
    navigation = body[: main.start()] if main else body
    identifiers = {
        identifier
        for link in re.finditer(r"\bhref\s*=\s*(['\"])(.*?)\1", navigation, FLAGS)
        if (identifier := summary_id(link[2])) is not None
    }
    # Related species may have their own links; ambiguity never invents identity.
    return next(iter(identifiers)) if len(identifiers) == 1 else None


def reported_number(value: str) -> float:
    """English summaries use thousands commas; accept decimal commas as well."""
    if "." in value or re.fullmatch(r"\d{1,3}(?:,\d{3})+", value):
        return float(value.replace(",", ""))
    return float(value.replace(",", "."))


def extract_fish(body: str, source: SourceDocument) -> SpeciesRecord:
    accepted, reason, name = select_reason(body)
    if not accepted or name is None:
        raise ValueError(f"Cannot extract species summary: {reason}")
    found = sections(body)
    block = NAME_BLOCK.search(body)
    common_match = (
        re.search(
            r"<span\b[^>]*class\s*=\s*['\"]sheader2['\"][^>]*>(.*?)</span>",
            block[1],
            FLAGS,
        )
        if block
        else None
    )
    common_name = plain_text(common_match[1]) if common_match else None
    classification = section_text(found, "Classification")
    lineage = (
        (classification or "").split("Etymology:", 1)[0].split("Eponymy:", 1)[0].strip()
    )
    levels = [part.strip() for part in lineage.split(">")]

    def taxon(index: int) -> str | None:
        if len(levels) <= index:
            return None
        match = re.match(r"([A-Za-z][A-Za-z/-]*)", levels[index])
        return match[1] if match else None

    habitat = section_text(found, "Environment")
    environments = tuple(
        item
        for item in Environment
        if re.search(rf"\b{item.value}\b", habitat or "", re.IGNORECASE)
    )
    climate_match = re.search(
        r"\b(tropical|subtropical|temperate|boreal|polar|deep-water)\b",
        habitat or "",
        re.IGNORECASE,
    )
    size = (
        section_text(found, "Size")
        or section_text(found, "Length at first maturity")
        or ""
    )
    length_block = re.search(
        r"Max\s+length\s*:\s*(.*?)(?=common\s+length|Max\s+(?:published|reported)|$)",
        size,
        re.IGNORECASE,
    )
    reported_lengths = []
    for length_match in re.finditer(
        r"(?<![\d.,])(\d+(?:[.,]\d+)*)\s*(mm|cm|m)\b"
        r"(?:\s+(TL|SL|FL|WD|DW|NG))?(?:\s+\(?(male/unsexed|female)\)?)?",
        length_block[1] if length_block else "",
        re.IGNORECASE,
    ):
        value = reported_number(length_match[1])
        if value > 0:
            unit = length_match[2].lower()
            reported_lengths.append(
                Length(
                    value=value,
                    unit=unit,
                    measurement=length_match[3].upper() if length_match[3] else None,
                    sex=length_match[4],
                    centimetres=value * {"mm": 0.1, "cm": 1, "m": 100}[unit],
                )
            )
    maximum = max(reported_lengths, key=lambda value: value.centimetres, default=None)
    iucn_text = section_text(found, "IUCN")
    status_match = re.search(
        r"\b(Extinct in the Wild|Extinct|Critically Endangered|Endangered|Vulnerable|Near Threatened|Least Concern|Data Deficient|Not Evaluated)\b(?:\s*\((EW|EX|CR|EN|VU|NT|LC|DD|NE)\))?",
        iucn_text or "",
        re.IGNORECASE,
    )
    codes = {
        "extinct in the wild": "EW",
        "extinct": "EX",
        "critically endangered": "CR",
        "endangered": "EN",
        "vulnerable": "VU",
        "near threatened": "NT",
        "least concern": "LC",
        "data deficient": "DD",
        "not evaluated": "NE",
    }
    status = codes[status_match[1].lower()] if status_match else None
    raw_id = fishbase_id(body, source)
    warnings = []
    if raw_id is None:
        warnings.append("fishbase-id-unavailable")
    if not maximum:
        warnings.append("maximum-length-unavailable")
    if not status:
        warnings.append("iucn-status-unavailable")
    distribution = section_text(found, "Distribution")
    if not habitat:
        warnings.append("habitat-unavailable")
    if not distribution:
        warnings.append("distribution-unavailable")
    return SpeciesRecord(
        profile=PROFILE,
        source_name="fishbase",
        identifiers=(ExternalIdentifier(namespace="fishbase", value=raw_id),)
        if raw_id is not None
        else (),
        species_key=species_key(name),
        scientific_name=name,
        common_name=common_name or None,
        taxonomy=Taxonomy(class_name=taxon(0), order=taxon(1), family=taxon(2)),
        environments=environments,
        climate=climate_match[1].lower() if climate_match else None,
        max_length=maximum,
        iucn_code=status,
        sections=found,
        warnings=tuple(warnings),
    )


def select_page(body: str) -> SelectionDecision:
    selected, reason, _ = select_reason(body)
    return SelectionDecision(selected=selected, reason=reason)


class FishBaseProfile:
    name = "fishbase"
    version = PROFILE
    select = staticmethod(select_page)
    extract = staticmethod(extract_fish)
