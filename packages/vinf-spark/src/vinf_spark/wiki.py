"""REGEX extraction from MediaWiki XML records and unrendered wikitext.

Extraction uses REGEX without an XML/HTML parser, XPath or MediaWiki API. This is a
deliberately limited extractor for the common taxobox/speciesbox conventions.
"""

import bz2
import gzip
import hashlib
import html
import re
from pathlib import Path
from urllib.parse import quote

from vinf_core.records import species_key

from vinf_spark.models import WikiPage

VERSION = "mediawiki-taxobox-regex/1"
PAGE = re.compile(r"<page>\s*(.*?)$", re.DOTALL)
TEXT = re.compile(r"<text\b[^>]*>(.*?)</text>|<text\b[^>]*/>", re.DOTALL)
TAXOBOX = re.compile(
    r"\{\{\s*(?:speciesbox|subspeciesbox|taxobox|automatic[ _]taxobox)\s*\n(.*?)(?:\n\}\}|\Z)",
    re.IGNORECASE | re.DOTALL,
)
PARAMETER = re.compile(
    r"\|[ \t]*([a-z_0-9 ]+)[ \t]*=[ \t]*(.*?)(?=\|[ \t]*[a-z_0-9 ]+[ \t]*=|\n|$)",
    re.MULTILINE | re.IGNORECASE,
)
NESTED_TEMPLATE = re.compile(
    r"\{\{(?!\s*(?:speciesbox|subspeciesbox|taxobox|automatic[ _]taxobox)\b)[^{}]*\}\}",
    re.IGNORECASE,
)
LATIN = re.compile(r"[A-Z][a-zA-Z-]+ [a-z][a-z-]+(?: [a-z][a-z-]+)?")
PAGE_END = re.compile(r"</page\s*>")


def page_fragments(path: Path):
    """Stream concatenated bzip2 members without Hadoop split-boundary overlap.

    The buffer is bounded by one XML page plus a 256 KiB input chunk. Dump
    files are Spark work items; neither a whole file nor its XML tree is loaded.
    """
    opener = (
        bz2.open
        if path.suffix == ".bz2"
        else gzip.open
        if path.suffix == ".gz"
        else open
    )
    with opener(path, "rt", encoding="utf-8") as stream:
        buffer = ""
        for chunk in iter(lambda: stream.read(256 * 1024), ""):
            buffer += chunk
            while match := PAGE_END.search(buffer):
                fragment, buffer = buffer[: match.start()], buffer[match.end() :]
                if "<page>" in fragment:
                    yield fragment
        if "<page>" in buffer:
            raise ValueError(f"Incomplete XML page in {path}")


def clean(value: str) -> str:
    value = re.sub(r"<!--.*?-->", " ", value, flags=re.DOTALL)
    value = re.sub(
        r"<ref\b[^>]*>.*?</ref\s*>|<ref\b[^>]*/>",
        " ",
        value,
        flags=re.IGNORECASE | re.DOTALL,
    )
    value = re.sub(r"\[\[(?:[^\]\n|]+\|)?([^\]\n]+)\]\]", r"\1", value)
    value = re.sub(r"\{\{[^{}]*\}\}", " ", value)
    value = re.sub(r"<[^>]*>", " ", value)
    return " ".join(html.unescape(value).replace("'''", "").replace("''", "").split())


def lead_text(wikitext: str) -> str:
    # A lead paragraph usually starts with the bold article name after infoboxes.
    match = re.search(
        r"^(?![|{}!=#*])(?=[^\n]*''')[^\n]+(?:\n(?!\s*\n|==)[^\n]+)*",
        wikitext,
        re.MULTILINE,
    )
    return clean(match[0])[:4000] if match else ""


def extract_page(fragment: str, dump_file: str) -> WikiPage | None:
    page = PAGE.search(fragment)
    if not page:
        return None  # trailing </mediawiki>, not an article
    xml = page[1]
    namespace = re.search(r"<ns>(\d+)</ns>", xml)
    if not namespace or namespace[1] != "0":
        return None
    title = re.search(r"<title>(.*?)</title>", xml, re.DOTALL)
    page_id = re.search(r"<id>(\d+)</id>", xml)
    text = TEXT.search(xml)
    if not title or not page_id or text is None:
        raise ValueError("Main-namespace page is missing title, ID or text.")
    title = html.unescape(title[1])
    wikitext = html.unescape(text[1] or "")
    redirect = re.search(r'<redirect\b[^>]*title="([^"]*)"', xml)
    revision = re.search(r"<revision>\s*<id>(\d+)</id>", xml)
    timestamp = re.search(r"<timestamp>(.*?)</timestamp>", xml)
    box_view = re.sub(
        r"<ref\b[^>]*>.*?</ref\s*>|<ref\b[^>]*/>",
        " ",
        wikitext,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for _ in range(16):
        box_view, removed = NESTED_TEMPLATE.subn(" ", box_view)
        if not removed:
            break
    box = TAXOBOX.search(box_view)
    fields = (
        {
            key.strip().lower().replace(" ", "_"): clean(value) or None
            for key, value in PARAMETER.findall(box[1])
        }
        if box
        else {}
    )
    name = fields.get("taxon") or fields.get("binomial")
    genus, species = fields.get("genus"), fields.get("species")
    if genus:
        genus = re.sub(r"^([A-Z][a-zA-Z-]+)\s*\([^)]*\)$", r"\1", genus)
    if not name and genus and species:
        name = f"{genus} {species}"
        if fields.get("subspecies"):
            name += f" {fields['subspecies']}"
    name = name if name and LATIN.fullmatch(name) else None
    return WikiPage(
        page_id=int(page_id[1]),
        title=title,
        revision_id=int(revision[1]) if revision else None,
        timestamp=timestamp[1] if timestamp else None,
        url="https://en.wikipedia.org/wiki/"
        + quote(title.replace(" ", "_"), safe="()_"),
        dump_file=dump_file,
        wikitext_sha256=hashlib.sha256(wikitext.encode()).hexdigest(),
        redirect=html.unescape(redirect[1]) if redirect else None,
        scientific_name=name,
        species_key=species_key(name) if name else None,
        genus=genus or (name.split()[0] if name else None),
        family=fields.get("familia") or fields.get("family"),
        order=fields.get("ordo") or fields.get("order"),
        authority=fields.get("binomial_authority") or fields.get("authority"),
        status=fields.get("status"),
        status_system=fields.get("status_system"),
        description=lead_text(wikitext),
        categories=tuple(
            sorted(set(re.findall(r"\[\[Category:([^\]|]+)", wikitext, re.IGNORECASE)))
        ),
        disambiguation=bool(
            re.search(
                r"\{\{\s*(?:disambiguation|disambig|dab)\b", wikitext, re.IGNORECASE
            )
        ),
    )
