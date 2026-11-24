"""Render a short submission page and a separate appendix from matching snapshots."""

import inspect
from pathlib import Path

from vinf_core.artifacts import fingerprint, parsed_catalog, parsed_pages
from vinf_core.files import atomic_bytes, read_lock, read_model
from vinf_core.index_io import read_index
from vinf_core.models import CatalogManifest, VinfError
from vinf_crawler import discovery
from vinf_crawler.store import CrawlStore
from vinf_parsing.profiles import fishbase
from vinf_search.models import ComparisonReport, RankMethod, SearchResult

from vinf.statistics import CorpusStatistics


def wiki_text(value: object) -> str:
    return str(value).replace("|", "&#124;").replace("^", "&#94;").replace("\n", " ")


def result_names(result: SearchResult, *, scores: bool = True, limit: int = 5) -> str:
    return (
        "; ".join(
            f"{hit.scientific_name} ({hit.score:.6f})"
            if scores
            else hit.scientific_name
            for hit in result.hits[:limit]
        )
        or "No matches"
    )


def make_report(
    crawled: Path,
    parsed: Path,
    index: Path,
    comparison: Path,
    output: Path,
    statistics: Path,
) -> str:
    for target in (output, output.with_name("implementation.dokuwiki.txt")):
        if any(
            target.resolve().is_relative_to(root.resolve())
            for root in (crawled, parsed, index)
        ):
            raise VinfError("Report output cannot be inside a stage input directory.")
    with read_lock(crawled / "crawl/lock"), read_lock(parsed / ".lock"):
        return _make_report(crawled, parsed, index, comparison, output, statistics)


def _make_report(
    crawled: Path,
    parsed: Path,
    index: Path,
    comparison: Path,
    output: Path,
    statistics: Path,
) -> str:
    store = CrawlStore(crawled)
    crawl_manifest = read_model(crawled / "catalog/manifest.json", CatalogManifest)
    _, extraction = parsed_catalog(parsed)
    _, manifest = read_index(index)
    measured = read_model(statistics, CorpusStatistics)
    report = read_model(comparison, ComparisonReport)
    if (
        extraction.input_fingerprint
        != fingerprint(extraction.producer, crawl_manifest.catalog_sha256)
        or manifest.input_fingerprint
        != fingerprint("parsed-collections/2", extraction.records_sha256)
        or measured.crawl_catalog_sha256 != crawl_manifest.catalog_sha256
        or measured.parsed_catalog_sha256 != extraction.records_sha256
        or measured.index_generation != manifest.generation
        or report.generation != manifest.generation
    ):
        raise VinfError(
            "Report inputs describe different snapshots; rerun their producing stages."
        )
    if len(report.comparisons) < 5 or any(
        {result.method for result in case.results} != set(RankMethod)
        for case in report.comparisons
    ):
        raise VinfError("Compare all three rankers on at least five described queries.")
    wanted = (
        "Gadus macrocephalus",
        "Rhincodon typus",
        "Plectrochilus wieneri",
        "Ophthalmolebias ilheusensis",
        "Myliobatis freminvillei",
    )
    examples = {}
    for page in parsed_pages(parsed):
        if page.record and page.record.scientific_name in wanted:
            examples.setdefault(page.record.scientific_name, page)
    if len(examples) != 5:
        raise VinfError("Five report example species are missing from this snapshot.")
    first = next(store.iter_documents())
    ratio = measured.original_html_bytes / measured.stored_html_bytes
    lines = [
        "====== First submission - FishBase species search ======",
        "",
        "[[user:icit:start|Project]] · [[user:icit:konzultacia2|Crawler]] · [[user:icit:implementation|Code and full examples]]",
        "",
        f"Snapshot: {measured.first_fetched.date()}–{measured.last_fetched.date()} (UTC). I collected {measured.crawl.downloaded:,} HTML pages and indexed {manifest.documents:,} source records. Collection was stopped deliberately; {measured.crawl.pending:,} pending URLs and {measured.crawl.deferred} deferred tasks remain saved. This is a partial corpus, not a complete FishBase mirror.",
        "",
        "===== Goal, source and five extraction examples =====",
        "",
        "Search English fish descriptions from [[https://www.fishbase.se/|FishBase]] using words, phrases and Boolean expressions. A generic crawler collects linked HTML; a separate REGEX extractor selects species summaries and extracts names, taxonomy, environment, distribution, reported maximum length, biology and available IUCN status. Missing facts stay null.",
        "",
        "^ Saved source ^ FishBase ID ^ Environment ^ Reported maximum ^ IUCN ^",
    ]
    for name in wanted:
        page = examples[name]
        record = page.record
        length = record.max_length
        value = (
            f"{length.value:g} {length.unit} {length.measurement or ''} {length.sex or ''}"
            if length
            else "Not reported"
        )
        lines.append(
            f"| [[{page.source.url}|{name}]] | {record.identifier('fishbase')} | {', '.join(record.environments)} | {value.strip()} | {record.iucn_code or 'Not available'} |"
        )
    lines += [
        "",
        "Units are normalized to centimetres while TL/SL/width types are preserved. These are reported species maxima, not individual measurements. [[user:icit:implementation|Full JSON examples]] retain source URLs, UUIDs, retrieval times and original SHA-256.",
        "",
        "===== Search scenarios and observed answers =====",
        "",
        "Each scenario states a search intention and its literal text conditions. Words can occur in different sections of a description; quoted phrases must match inside one section. The returned text provides the context for interpreting habitats, diet and conservation status. Counts refer to this snapshot, and the displayed species are examples of matching descriptions.",
    ]
    for number, case in enumerate(report.comparisons, 1):
        result = next(item for item in case.results if item.method == RankMethod.TFIDF)
        lines.extend(
            (
                "",
                f"==== Query {number} ====",
                "",
                f"**{wiki_text(case.query.question)}**",
                "",
                "<code>",
                case.query.query,
                "</code>",
                "",
                case.query.description,
                "",
                f"Observed answer: {result.total_matches:,} matching descriptions. First three with TF-IDF cosine: {wiki_text(result_names(result, scores=False, limit=3))}.",
            )
        )
    lines += [
        "",
        "===== Libraries and architecture =====",
        "",
        "Python 3.12 and uv manage four independent stage libraries and vinf-core. Core owns Pydantic file contracts; crawl, parse, index and search have separate CLI commands and separate data directories. Requests handles HTTP, Protego evaluates robots, Typer/Rich provide the CLI, and standard gzip/lzma handle optional compression. tiktoken is used only for measurements. Extraction, index construction and ranking are self-written; no database or search framework is used.",
        "",
        "{{:user:icit:architecture.png?950|Crawler and independent processing stages}}",
        "",
        "===== HTTP policy and actual metadata =====",
        "",
        f"Final settings: robots={store.config.robots.strategy}; minimum interval {store.config.http.delay_seconds:g}s per origin; site pacing; {store.config.parallel.workers} worker; no proxies. Longer robots timing and server cooldowns take precedence. Connect/read timeouts are {store.config.http.connect_timeout:g}/{store.config.http.read_timeout:g}s, responses are limited to 10 MiB and redirects to five. These bound waits and response size. Redirects are checked again; TLS verification stays enabled.",
        f"User-Agent: ''{store.config.http.user_agent}''. Accept requests HTML/XHTML. Failures remain available for retry.",
        "The collection used several policies. The table below counts the policy recorded on each page, including ignore. The saved bulk-run configuration used config-first with a one-second interval: path restrictions applied, but robots timing was overridden. The four ignore pages were collected without enforcing robots. Metadata records the policy name, not every historical delay. Before submission I changed the dataset and project to respect/10s and completed a separate two-page check. Historical metadata is unchanged.",
        "",
        "^ Recorded robots policy ^ Saved pages ^",
    ]
    lines += [f"| {item.strategy} | {item.documents:,} |" for item in measured.policies]
    lines += ["", "Example saved metadata:", "", "^ Field ^ Actual value ^"]
    for name in (
        "doc_id",
        "requested_url",
        "final_url",
        "fetched_at",
        "http_status",
        "content_type",
        "encoding",
        "compression",
        "size_bytes",
        "stored_size_bytes",
        "depth",
        "discovered_from",
        "user_agent",
        "robots_strategy",
        "elapsed_seconds",
        "response_headers",
        "sha256",
    ):
        lines.append(f"| {name} | {wiki_text(getattr(first, name))} |")
    lines += [
        "",
        "===== Implemented REGEX code =====",
        "",
        "The [[user:icit:implementation|implementation appendix]] includes the actual URL extraction source (tag/attribute regexes, relative-link resolution and scope checks) and entity extraction source (summary selection, fields and numeric normalization). Both are current executable source, not pseudocode; complete files are also in the source ZIP. No DOM parser or XPath is used.",
        "",
        "===== TF index and three ranking methods =====",
        "",
        "The indexer selects the newest record per (source, FishBase ID), assigns dense docIDs and externally sorts term occurrences with bounded buffers. Postings store TF, word positions, UTF-8 offsets and section identity. DF is computed during construction. An on-disk sorted lexicon addresses blocks in postings.bin. Search text is stored once; queries merge posting streams and keep a top-k heap.",
        f"One shared index contains {manifest.documents:,} documents, {manifest.terms:,} terms and {manifest.tokens:,} word occurrences, after removing {manifest.duplicate_records:,} duplicate source records. Stop words are disabled in this snapshot. A changed parsed catalog triggers a rebuild; unchanged input is verified and reused. Search never builds an index.",
        "",
        "^ Method ^ IDF and scoring ^",
        "| TF-IDF sum | %%idf=ln(N/DF); sum(TF×idf)%% over distinct positive query terms; no length normalization |",
        "| TF-IDF cosine | %%idf=ln((N+1)/(DF+1))+1; weight=(1+ln(TF))×idf%%; normalized query-document dot product |",
        "| BM25 (additional experiment) | %%idf=ln(1+(N−DF+0.5)/(DF+0.5))%%; TF saturation and length normalization; k1=1.2, b=0.75 |",
        "",
        "The required comparison uses the two TF-IDF variants with non-probabilistic IDF. BM25 is an additional experiment.",
        "Quoted phrases require consecutive words in one section and matching punctuation in the stored span. Case, NFKC and whitespace are normalized. reef-associated is a different token from reef. [[user:icit:implementation|Query semantics and optional stop words]].",
        "",
        f"===== Ranking comparison ({len(report.comparisons)} queries) =====",
        "",
        "All methods use the same index and query expression. Scores have different scales; compare ordering. This comparison has no human relevance judgments and does not establish which method is better. Full top-five results and per-term explanations are in the evidence archive.",
        "",
        "Query numbers refer to the scenarios above.",
        "",
        "^ Scenario ^ Method ^ Matches ^ Top three: species (score) ^",
    ]
    for number, case in enumerate(report.comparisons, 1):
        for result in case.results:
            lines.append(
                f"| Query {number} | {result.method} | {result.total_matches:,} | {wiki_text(result_names(result, limit=3))} |"
            )
    lines.append("")
    for number, case in enumerate(report.comparisons, 1):
        leaders = {
            result.method: result.hits[0] for result in case.results if result.hits
        }
        if len(leaders) != len(RankMethod):
            continue
        summed = leaders[RankMethod.TFIDF_SUM]
        strongest = max(
            summed.explanation, key=lambda term: term.contribution, default=None
        )
        detail = (
            f"TF({strongest.term})={strongest.tf}"
            if strongest is not None
            else "no positive scoring terms"
        )
        lines.append(
            f"  * Query {number}: TF-IDF sum starts with {summed.scientific_name} ({summed.document_tokens} words; {detail}). Cosine starts with {leaders[RankMethod.TFIDF].scientific_name}; BM25 starts with {leaders[RankMethod.BM25].scientific_name}."
        )
    lines += [
        "",
        "Linear TF rewards repeated query words without a document-length penalty. Cosine normalizes the complete weighted vector; BM25 saturates TF and adjusts for document length. The changed leaders show that these choices affect ordering even though Boolean candidate sets are identical.",
    ]
    lines += ["", "===== Corpus statistics and bonuses =====", "", "^ Metric ^ Value ^"]
    for label, value in (
        ("Saved HTML pages", measured.crawl.downloaded),
        ("HTML bytes before compression", measured.original_html_bytes),
        ("HTML bytes in gzip", measured.stored_html_bytes),
        (
            "Accepted / rejected / errors",
            f"{measured.accepted} / {measured.rejected} / {measured.errors}",
        ),
        ("Distinct scientific-name keys", measured.distinct_name_keys),
        ("Accepted pages without a source ID", measured.missing_source_ids),
        (
            "Indexed documents / word occurrences",
            f"{manifest.documents} / {manifest.tokens}",
        ),
        (
            "BPE tokens in raw HTML / indexed text",
            f"{measured.html_bpe_tokens} / {measured.indexed_text_bpe_tokens}",
        ),
        (
            "Pending / deferred URLs",
            f"{measured.crawl.pending} / {measured.crawl.deferred}",
        ),
    ):
        formatted = f"{value:,}" if isinstance(value, int) else value
        lines.append(f"| {label} | {formatted} |")
    lines += [
        "",
        "^ Data category ^ Decoded file bytes ^ Stored file bytes ^ Allocated filesystem bytes ^",
    ]
    for stage in measured.sizes:
        value = stage.size
        lines.append(
            f"| {stage.name} | {value.decoded_bytes:,} | {value.stored_bytes:,} | {value.allocated_bytes:,} |"
        )
    lines += [
        "",
        f"HTML is {ratio:.2f}× smaller in gzip. Its original size exceeds 500 MB; queue overhead is reported separately. Requests decodes HTTP content encodings before storage, so these are HTML-body sizes, not wire-traffic measurements. They include page markup and URL variants; deduplication counts are shown separately. Accepted pages are selected automatically by REGEX, not manually judged for relevance. Other pages remain in raw storage and are excluded from the species index. The full website is not claimed as downloaded.",
        f"Token bonus: tiktoken {measured.tiktoken_version}, {measured.bpe_encoding}. Counts cover complete raw HTML including markup and, separately, deduplicated indexed texts. Each document is counted in memory without decompressed files. BPE counts differ from the index's word-token count.",
        "REGEX-test bonus: tests/test_extraction_gold.py checks 20 independently labeled real species pages and two real negative pages. Expected values were transcribed from source HTML. Fixtures preserve original bytes and SHA-256; cases include slug IDs, several environments and IUCN states, absent lengths, thousands separators, parenthesized sex labels and TL/SL/WD/NG measurements.",
        "The extractor depends on FishBase's HTML template. Section text can retain reference and navigation labels; the gold cases check the listed fields rather than corpus-wide accuracy.",
        "",
        "===== Reproduction and attachments =====",
        "",
        "<code bash>\nuv sync --locked\nuv run vinf parse run\nuv run vinf index build\nuv run vinf search query 'marine \"Western Pacific\" (tropical OR subtropical) -freshwater' --method tfidf --explain\nuv run vinf search compare\nuv run vinf report stats\nuv run vinf report build\nuv run python -m unittest discover -s tests -v\n</code>",
        "",
        "The source ZIP includes code, lockfile, schemas, configuration examples, tests and real fixtures. The evidence ZIP includes measurements, checks, comparison and a small reproducible demo. [[user:icit:start|Download links and demo instructions]]. The complete collected raw corpus and frontier remain preserved locally.",
        "",
        "===== Further coursework =====",
        "",
        "Next: Wikipedia REGEX extraction in Spark, a justified join with unique-page counts and match evidence, PyLucene fields and old/new index comparison, then relevance evaluation against Google/FishBase. Precision/recall will require an explicitly judged set. These later stages are not claimed as implemented.",
        "[[https://vi2026.ui.sav.sk/doku.php?id=projekty.pre.rok.2026|Requirements and deadlines]]",
        "",
    ]
    appendix = [
        "====== Implementation and extraction evidence ======",
        "",
        "[[user:icit:odovzdanie1|First submission]]",
        "",
        "===== URL discovery =====",
        "",
        "Current source: packages/vinf-crawler/src/vinf_crawler/discovery.py.",
        "<code python>",
        inspect.getsource(discovery).rstrip(),
        "</code>",
        "",
        "===== Entity extraction =====",
        "",
        "Current source: packages/vinf-parsing/src/vinf_parsing/profiles/fishbase.py.",
        "<code python>",
        inspect.getsource(fishbase).rstrip(),
        "</code>",
        "",
        "===== Five extracted records =====",
        "",
    ]
    for name in wanted:
        page = examples[name]
        appendix += [
            f"==== {name} ====",
            f"Source: [[{page.source.url}|saved page]], UUID {page.source.doc_id}.",
            "<code json>",
            page.model_dump_json(indent=2),
            "</code>",
            "",
        ]
    appendix += [
        "===== Query semantics =====",
        "",
        "Words use implicit AND. Uppercase AND/OR/NOT, minus, parentheses and double-quoted phrases are supported. Phrase candidates come from positional postings; the stored span verifies punctuation. Matches stay inside one segment. Punctuation outside the requested span is irrelevant. NFKC, casefold and whitespace normalization apply. Stop words are optional: omitted postings retain positional gaps and searchable text, so phrases with stop words can be verified in candidate text; an all-stop-word phrase scans texts sequentially. No stop words are configured in the submitted index. Numeric attribute filters, stemming, wildcard and fuzzy operators are not implemented.",
        "",
    ]
    atomic_bytes(output, "\n".join(lines).encode())
    atomic_bytes(
        output.with_name("implementation.dokuwiki.txt"), "\n".join(appendix).encode()
    )
    return str(output)
