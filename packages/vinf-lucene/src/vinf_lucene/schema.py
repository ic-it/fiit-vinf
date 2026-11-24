"""A small explicit schema: text, exact filters and stored provenance."""

from vinf_lucene.models import FieldDefinition, Scope

FISHBASE_TEXT = ("scientific_name", "common_name", "fishbase_text")
WIKIPEDIA_TEXT = ("wiki_text", "wiki_categories", "wiki_authority")
KEYWORDS = (
    "identity",
    "source",
    "source_id",
    "scientific_exact",
    "family",
    "order",
    "environment",
    "iucn",
    "wiki_status",
    "linked",
    "wiki_page_id",
)


def text_fields(scope: Scope) -> tuple[str, ...]:
    return (
        FISHBASE_TEXT
        if scope == Scope.FISHBASE
        else WIKIPEDIA_TEXT
        if scope == Scope.WIKIPEDIA
        else FISHBASE_TEXT + WIKIPEDIA_TEXT
    )


FIELDS = tuple(
    FieldDefinition(name=name, kind=kind, stored=stored, reason=reason)
    for name, kind, stored, reason in (
        (
            "identity",
            "StringField",
            True,
            "Unique source-qualified identity; exact lookup.",
        ),
        (
            "source",
            "StringField",
            True,
            "Exact source filter and multi-source identity.",
        ),
        (
            "source_id",
            "StringField",
            True,
            "FishBase identifier; comparisons use the same IDs.",
        ),
        (
            "doc_id",
            "StoredField",
            True,
            "Original crawler UUID; provenance, not relevance.",
        ),
        (
            "url",
            "StoredField",
            True,
            "Open the original FishBase page without indexing URL tokens.",
        ),
        (
            "scientific_name",
            "TextField",
            True,
            "Search scientific-name words and display the original name.",
        ),
        (
            "scientific_exact",
            "StringField",
            False,
            "Exact normalized scientific-name lookup.",
        ),
        ("common_name", "TextField", True, "Search and display the common name."),
        (
            "fishbase_text",
            "TextField",
            True,
            "Extracted sections, stored once; positional phrase search.",
        ),
        (
            "family",
            "StringField",
            True,
            "Exact explicit FishBase family; no inherited Wiki taxonomy guesses.",
        ),
        ("order", "StringField", True, "Exact explicit FishBase order."),
        (
            "environment",
            "StringField, repeated",
            True,
            "Exact marine/freshwater/brackish filters.",
        ),
        (
            "iucn",
            "StringField",
            True,
            "Exact FishBase conservation code, kept separate from Wikipedia.",
        ),
        (
            "linked",
            "StringField",
            True,
            "Filter records with an accepted Wikipedia link.",
        ),
        (
            "wiki_text",
            "TextField",
            True,
            "Wikipedia lead; descriptive enrichment stored once.",
        ),
        (
            "wiki_categories",
            "TextField",
            True,
            "Search topic/category words from Wikipedia.",
        ),
        (
            "wiki_authority",
            "TextField",
            True,
            "Search explicitly extracted taxonomic authority.",
        ),
        (
            "wiki_status",
            "StringField",
            True,
            "Wikipedia conservation code without overwriting FishBase.",
        ),
        (
            "wiki_status_system",
            "StoredField",
            True,
            "Interpret the Wikipedia conservation code.",
        ),
        (
            "wiki_page_id",
            "StringField",
            True,
            "Exact identity of the linked Wikipedia page.",
        ),
        (
            "wiki_revision_id",
            "StoredField",
            True,
            "Reproduce the Wikipedia source revision.",
        ),
        ("wiki_url", "StoredField", True, "Open the linked Wikipedia article."),
        (
            "wiki_wikitext_sha256",
            "StoredField",
            True,
            "Trace extracted Wikipedia text to saved source content.",
        ),
    )
)
