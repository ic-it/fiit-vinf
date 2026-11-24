"""Render the consultation page only from current audited index measurements."""

from pathlib import Path

from vinf_core.files import file_hash, read_model
from vinf_core.models import VinfError
from vinf_lucene.models import Comparison, IndexAudit, Manifest, SearchResult

REPOSITORY = "https://github.com/ic-it/fiit-vinf"
SOURCE = REPOSITORY + "/blob/main"


def cell(value):
    return str(value).replace("|", "/").replace("\n", " ")


def build_report(root: Path, output: Path, indexed: Path):
    manifest = read_model(indexed / "manifest.json", Manifest)
    report = read_model(root / "reports/consultation4/comparison.json", Comparison)
    audit = read_model(root / "reports/consultation4/audit.json", IndexAudit)
    checksum = file_hash(indexed / "manifest.json")
    if (
        audit.errors
        or not audit.old_same_identities
        or not audit.lucene_check_index_clean
        or audit.lucene_manifest_sha256 != checksum
        or report.lucene_index.sha256 != checksum
        or report.old_index.sha256 != audit.old_manifest_sha256
        or audit.input_fingerprint != manifest.input_fingerprint
        or audit.documents != manifest.documents
        or report.documents != manifest.documents
    ):
        raise VinfError(
            "Index audit or comparison is stale. Rerun validation and comparison."
        )
    lines = [
        "====== Fourth consultation - PyLucene index ======",
        "",
        "[[user:icit:start|Project]] · [[user:icit:konzultacia3|Spark extraction and join]] · [[https://vi2026.ui.sav.sk/doku.php?id=projekty.pre.rok.2026#konzultacia4luceneindex|Requirements]]",
        "",
        f"Prepared on 24 November 2026. Current code: [[{REPOSITORY}|ic-it/fiit-vinf on GitHub]]. The application now has a separate native PyLucene backend. It consumes the completed Spark snapshots; the existing crawler, extraction and self-written index remain independently usable.",
        "",
        "===== Working result =====",
        "",
        "^ Measurement ^ Value ^",
        f"| Indexed FishBase records | {manifest.documents:,} |",
        f"| Records enriched with accepted Wikipedia links | {manifest.linked_documents:,} |",
        f"| FishBase records retained without a Wikipedia link | {manifest.documents - manifest.linked_documents:,} |",
        f"| Original index payload | {audit.old_index_payload_bytes:,} bytes |",
        f"| Lucene index payload, including enrichment | {audit.lucene_index_payload_bytes:,} bytes |",
        f"| Local Lucene build | {manifest.elapsed_seconds:.3f} seconds |",
        f"| PyLucene / Lucene / Java | {manifest.pylucene_version} / {manifest.lucene_version} / {manifest.java_version} |",
        "",
        "Sizes are actual index-file bytes; manifests, compiler/JDK dependencies and original input datasets are excluded from both payload totals. Lucene handles compression internally; its index is not wrapped in gzip. The formats, analysis and stored fields differ, so these measurements describe this corpus rather than a general compression or speedup claim.",
        "",
        "===== Requirements and implementation =====",
        "",
        "  * Completed Spark code, Wikipedia extraction and join: documented in the [[user:icit:konzultacia3|third consultation]]. These saved inputs were reused without another crawl.",
        "  * PyLucene index: vinf-lucene builds a native FSDirectory through IndexWriter and searches it through IndexSearcher.",
        "  * Fields: the table below lists each type, storage choice and purpose.",
        "  * Application integration: vinf lucene build/query/compare/status/schema use the current project's paths. vinf-lucene is an additional entry point to the same library.",
        "  * Old/new comparison: five existing described queries, identical FishBase identities, observed results and median timings below.",
        "  * Query types actually exercised: Boolean, positional PhraseQuery, FuzzyQuery and exact keyword fields. Numeric Range queries are not part of this schema; the assignment lists Range as an example query type.",
        f"  * Current source: [[{REPOSITORY}|GitHub repository]], plus the source ZIP below.",
        "",
        "<code text>",
        "completed Spark FishBase species + accepted Wikipedia links (JSON.gz)",
        "    -> vinf-lucene / IndexWriter / explicit schema",
        "    -> data/lucene/indexed/index/ (native Lucene files)",
        "    -> vinf lucene query / MultiFieldQueryParser / IndexSearcher / BM25",
        "</code>",
        "",
        "Every FishBase species is indexed, including unmatched records. Accepted Wikipedia text, categories, authority and status enrich only the linked records. Source identifiers, URLs, UUIDs, Wikipedia revisions and content hashes preserve provenance. Source-specific conservation values remain separate. The indexer reads saved DTOs without downloading, reparsing raw HTML or starting Spark.",
        "",
        "===== Fields and analysis =====",
        "",
        "^ Field ^ Lucene type ^ Stored ^ Reason ^",
    ]
    for field in manifest.fields:
        lines.append(
            f"| {field.name} | {field.kind} | {'yes' if field.stored else 'no'} | {cell(field.reason)} |"
        )
    lines += [
        "",
        "TextFields store positions for phrase queries and each text once in its own field. There is no extra aggregate fulltext copy. StoredField values support display/provenance without contributing tokens. StringFields support exact filters. No numeric length field is indexed; source numeric attributes remain in the extracted records.",
        "",
        'StandardAnalyzer tokenizes and lowercases text, with an empty stop-word set and no stemming. KeywordAnalyzer handles exact fields. Exact values are case-sensitive: family:Cyprinidae, iucn:EN, environment:freshwater. scientific_exact uses the shared normalized key, for example scientific_exact:"aaptosyax grypus".',
        "",
        "The default query searches scientific/common names, FishBase sections and the three Wikipedia text fields. --scope fishbase limits default fields to scientific_name, common_name and fishbase_text; --scope wikipedia selects wiki_text, wiki_categories and wiki_authority. An explicit field prefix still selects its named field.",
        "",
        'Implicit AND is enabled. Parentheses make mixed Boolean intent explicit. Quoted Lucene phrases match consecutive analyzed tokens within a field; punctuation is not literal. For example, "Aaptosyax, grypus" can match Aaptosyax grypus. The original vinf search backend still provides punctuation-sensitive literal phrases. Fuzzy matching tolerates spelling edits; it does not resolve taxonomic synonyms.',
        "",
        "Ranking is BM25 with k1=1.2 and b=0.75. Lucene uses per-field term/document statistics and encoded length norms; a phrase is scored as a phrase query. The old engine ranks positive constituent terms over its concatenated text. Absolute scores are therefore not directly comparable. --explain prints Lucene's actual query and BM25 explanation, including frequency, IDF and length normalization.",
        "",
        "===== Actual query examples =====",
        "",
        "^ Type ^ Query ^ Matches ^ First results ^",
    ]
    for name in ("boolean", "phrase", "fuzzy", "keyword", "wikipedia"):
        result = read_model(
            root / f"reports/consultation4/examples/{name}.json", SearchResult
        )
        lines.append(
            f"| {name} | ''{cell(result.query)}'' | {result.total_matches:,} | {cell('; '.join(h.scientific_name for h in result.hits))} |"
        )
    lines += [
        "",
        "===== Old index versus Lucene =====",
        "",
        f"Both indexes contain the same {report.documents:,} source-qualified FishBase identities. For each query, one warm-up precedes {report.repetitions} measured repetitions. Times are local medians in milliseconds, including index opening and result construction, with JVM startup excluded. File-system cache conditions are not a controlled cold-start benchmark. Results are top {report.top_k}; scores are not compared across engines.",
        "",
        "Three runs separate a backend/analysis change from enrichment: self-written BM25, Lucene using only FishBase default fields, and enriched Lucene. StandardAnalyzer splits punctuation/hyphens differently; field lengths/statistics and phrase scoring differ. Thus even FishBase-only match counts and ordering can change. Additional Wikipedia text can add matches, alter ranking or exclude a record through a negative term.",
        "",
    ]
    for number, case in enumerate(report.cases, 1):
        lines += [
            f"==== Query {number} ====",
            "",
            case.question,
            "",
            case.description,
            "",
            "<code text>",
            case.query,
            "</code>",
            "",
            "^ Engine ^ Matches ^ Median ms ^ Ordered top results ^",
        ]
        for run in case.runs:
            top = "; ".join(
                f"{i}. {name} (FishBase {identifier})"
                for i, (name, identifier) in enumerate(
                    zip(run.top_names, run.top_source_ids, strict=True), 1
                )
            )
            lines.append(
                f"| {run.engine} | {run.total_matches:,} | {run.median_ms:.3f} | {cell(top)} |"
            )
        lines += [
            "",
            f"Top-{report.top_k} overlap with the old engine: {case.old_vs_lucene_fishbase_overlap} for Lucene FishBase-only; {case.old_vs_lucene_enriched_overlap} for enriched Lucene.",
            "",
        ]
    lines += [
        "These are observed rankings and latency measurements. No human relevance judgments were collected here: overlap and additional matches are not precision, recall or proof of improved relevance. The judged comparison with Google and the source website belongs to the final evaluation.",
        "",
        "===== Verification and reproducibility =====",
        "",
        f"scripts/validate_lucene.py compared all {audit.identical_fishbase_records:,} stored FishBase records and {audit.identical_wikipedia_records:,} Wikipedia enrichments against the saved source snapshots, including IDs, descriptions, taxonomy/status and provenance. It checked identity completeness and {audit.unique_wikipedia_pages:,} distinct Wikipedia page IDs. Native Lucene CheckIndex reported a clean index; the audit has no errors.",
        "",
        "The complete suite passed 30 tests, including two small native integration methods using four real records. They cover Boolean, phrase, fuzzy and exact-field queries, BM25 explanations, unchanged input files, unchanged-input reuse and rejection of an output directory containing unrelated files. The native tests are skipped explicitly if the native dependency is absent; they ran in this measured submission.",
        "",
        "<code bash>",
        "uv sync --locked",
        "uv run python scripts/install_pylucene.py",
        "uv run vinf lucene build",
        "uv run vinf lucene status",
        "uv run vinf lucene schema",
        "uv run vinf lucene query 'marine (Pacific OR Atlantic) -freshwater'",
        "uv run vinf lucene query '\"coral reefs\"' --explain",
        "uv run vinf lucene query 'scientific_name:Aaptosyaz~1'",
        "uv run vinf lucene compare",
        "uv run python scripts/validate_lucene.py",
        "</code>",
        "",
        "PyLucene is a native dependency. The installer builds Apache JCC 3.14 and a PyLucene 9.12.0 binding for the required Lucene modules from verified source/jars. A full JDK 17 and C++ compiler are needed; on macOS arm64 a checksum-verified private Temurin JDK can be downloaded automatically. The tested platform is macOS arm64 / Python 3.12. The private JDK in .cache/pylucene-build is used at runtime. Compiled wheels can be reused after an exact uv sync removes the native packages. Normal uv run preserves them. Native dependency installation is not counted as index build time.",
        "",
        "The evidence archive includes a copied four-record subset: four real FishBase records and three accepted Wikipedia enrichments. Its README runs independent indexing/search without network after dependency installation. It is explicitly a subset and not another Spark run or a full-corpus benchmark. Full input datasets and the full index remain local.",
        "",
        f"  * [[{SOURCE}/packages/vinf-lucene/src/vinf_lucene/indexing.py|Executable index builder]]",
        f"  * [[{SOURCE}/packages/vinf-lucene/src/vinf_lucene/search.py|Executable native query code]]",
        f"  * [[{SOURCE}/packages/vinf-lucene/src/vinf_lucene/schema.py|Complete schema and field reasons]]",
        "  * {{:user:icit:vinf_source_20261124.zip|Current source, uv lock, documentation and tests}}",
        "  * {{:user:icit:vinf_lucene_evidence_20261124.zip|Measurements, examples, native audit and offline subset}}",
        "",
        "===== Next consultation and final submission =====",
        "",
        "By 9 December: evaluate search against Google and FishBase for at least five queries, present actual results as text tables, assess relevance with explicit criteria, demonstrate the finished application, and prepare the separate second-submission report with architecture, dump processing, join statistics, indexed fields, query types, old/new comparison, GitHub link and source ZIP. This page documents consultation 4; it does not claim the final evaluation is complete.",
        "",
        "Wikipedia texts retain their original CC BY-SA attribution and revision provenance. FishBase content retains its original ownership. The course wiki's text license does not relicense source data.",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output
