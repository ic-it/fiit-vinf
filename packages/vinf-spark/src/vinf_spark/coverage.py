"""Explain unmatched records without loading Wikipedia text into driver memory."""

from collections import Counter
from pathlib import Path

from vinf_core.files import file_hash
from vinf_core.models import Model

from vinf_spark.selection import Selection, json_rows, title_key


class ArticleIdentity(Model):
    title: str
    page_id: int
    scientific_name: str | None
    species_key: str | None
    redirect: str | None
    disambiguation: bool


class UnmatchedExample(Model):
    source_id: str
    scientific_name: str
    reason: str
    article_title: str | None = None
    article_taxon: str | None = None


class Coverage(Model):
    fishbase_species: int
    linked_species: int
    coverage_percent: float
    scientific_titles_in_index: int
    unmatched_reasons: dict[str, int]
    examples: tuple[UnmatchedExample, ...]
    join_manifest_sha256: str


def analyze_coverage(
    fishbase: Path, wikipedia: Path, linked: Path, selection: Selection
):
    wanted = {title_key(hit.title) for hit in selection.hits}
    articles = {}
    for _ in range(8):
        added = set()
        for row in json_rows(wikipedia / "pages"):
            key = title_key(row["title"])
            if key not in wanted:
                continue
            identity = ArticleIdentity(
                **{name: row.get(name) for name in ArticleIdentity.model_fields}
            )
            previous = articles.get(key)
            if previous is None or (
                bool(identity.redirect),
                identity.disambiguation,
                identity.page_id,
            ) < (bool(previous.redirect), previous.disambiguation, previous.page_id):
                articles[key] = identity
            if row.get("redirect"):
                added.add(title_key(row["redirect"].split("#")[0]))
        added -= wanted
        if not added:
            break
        wanted |= added
    accepted = {row["fishbase"]["source_id"] for row in json_rows(linked / "records")}
    rejected = {
        row["fishbase"]["source_id"]: row["decision"]
        for row in json_rows(linked / "review")
    }
    reasons = Counter()
    examples = []
    count = 0
    for row in json_rows(fishbase / "species"):
        count += 1
        identifier, fish = row["source_id"], row["record"]
        if identifier in accepted:
            continue
        article = articles.get(fish["species_key"])
        if identifier in rejected:
            reason = rejected[identifier]
        elif article:
            visited = set()
            while article.redirect and article.page_id not in visited:
                visited.add(article.page_id)
                target = articles.get(title_key(article.redirect.split("#")[0]))
                if target is None:
                    break
                article = target
            if article.redirect:
                reason = "redirect_target_not_retrieved"
            elif article.disambiguation:
                reason = "disambiguation_target"
            elif article.species_key and article.species_key != fish["species_key"]:
                reason = "different_taxonomic_name"
            elif not article.species_key:
                reason = "target_without_supported_taxon"
            else:
                reason = "candidate_requires_review"
        elif fish.get("common_name") and title_key(fish["common_name"]) in articles:
            article = articles[title_key(fish["common_name"])]
            reason = "common_title_only"
        else:
            reason = "no_exact_title_in_index"
        reasons[reason] += 1
        if len(examples) < 30:
            examples.append(
                UnmatchedExample(
                    source_id=identifier,
                    scientific_name=fish["scientific_name"],
                    reason=reason,
                    article_title=article.title if article else None,
                    article_taxon=article.scientific_name if article else None,
                )
            )
    return Coverage(
        fishbase_species=count,
        linked_species=len(accepted),
        coverage_percent=round(100 * len(accepted) / count, 3) if count else 0,
        scientific_titles_in_index=selection.scientific_titles_found,
        unmatched_reasons=dict(sorted(reasons.items())),
        examples=tuple(examples),
        join_manifest_sha256=file_hash(linked / "manifest.json"),
    )
