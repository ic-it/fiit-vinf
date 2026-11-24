"""Build the consultation report from completed, independently checked runs."""

import inspect
import json
from pathlib import Path

from vinf_core.files import file_hash, read_model
from vinf_core.models import VinfError
from vinf_spark import wiki
from vinf_spark.engine import _fishbase_partition
from vinf_spark.models import StageReport
from vinf_spark.selection import Selection


def _cell(value):
    return (
        str(value if value is not None else "not available")
        .replace("|", "/")
        .replace("\n", " ")
    )


def build_report(root: Path, output: Path):
    reports = [
        read_model(root / path / "manifest.json", StageReport)
        for path in ("data/spark/fishbase", "data/wikipedia/parsed", "data/linked")
    ]
    fish, wikipedia, joined = reports
    if [report.stage for report in reports] != ["fishbase", "wikipedia", "join"]:
        raise VinfError("Expected the three completed Spark stages.")
    audit = json.loads((root / "reports/consultation3/audit.json").read_text())
    if audit["errors"] or audit["linked_records"] != joined.rows:
        raise VinfError("The independent audit does not confirm this join.")
    if (
        audit.get("fishbase_manifest_sha256")
        != file_hash(root / "data/spark/fishbase/manifest.json")
        or audit.get("wikipedia_manifest_sha256")
        != file_hash(root / "data/wikipedia/parsed/manifest.json")
        or audit.get("join_manifest_sha256")
        != file_hash(root / "data/linked/manifest.json")
    ):
        raise VinfError("The audit is stale. Rerun scripts/validate_spark.py.")
    selection = read_model(root / "data/wikipedia/selection.json", Selection)
    diagnostics = json.loads((root / "reports/consultation3/coverage.json").read_text())
    if diagnostics.get("join_manifest_sha256") != file_hash(
        root / "data/linked/manifest.json"
    ):
        raise VinfError("Coverage diagnostics are stale. Run 'vinf spark coverage'.")
    index_metadata = json.loads(
        Path(selection.index_path + ".metadata.json").read_text()
    )
    coverage = 100 * joined.rows / joined.counts["fishbase_species"]
    lines = [
        "====== Third consultation - Spark extraction and Wikipedia join ======",
        "",
        "[[user:icit:start|Project]] · [[user:icit:odovzdanie1|First submission]] · [[https://vi2026.ui.sav.sk/doku.php?id=projekty.pre.rok.2026#konzultacia3spracovanievelkychdat|Requirements]]",
        "",
        "Updated on 10 November 2026 for the third consultation. This stage moves extraction and linking to Apache Spark. The existing crawler, sequential parser and search index remain independent. No new FishBase HTTP collection was needed.",
        "",
        "===== What changed =====",
        "",
        f"The new vinf-spark workspace library reuses FishBaseProfile and the shared Pydantic contracts. Spark mapPartitions executes the same REGEX extraction on all {fish.rows:,} saved HTML pages. A DataFrame window chooses one record per FishBase ID using newest retrieval time and then UUID, matching the first indexer's policy. Separate commands process Wikipedia XML and join the results.",
        "",
        f"Measured with PySpark {fish.spark_version}, Java 17 and master {fish.master}. This is a real Spark run on one laptop with two concurrent task slots and {fish.partitions} output/shuffle partitions, not a multi-machine performance test. For a remote master, install the same workspace dependencies on executors and mount the raw packet paths identically. The input catalog is partitioned; raw pages are read by workers rather than collected on the driver.",
        "",
        "<code text>",
        "saved FishBase catalog + raw HTML.gz -> Spark workers / existing REGEX profile",
        "                                    -> outcomes + deduplicated species JSON.gz",
        "",
        "official Wikipedia XML.bz2          -> Spark file partitions / streaming REGEX workers",
        "                                    -> main-namespace pages JSON.gz",
        "",
        "FishBase species + Wikipedia pages  -> Spark scientific-name join",
        "                                    -> records / review / unmatched JSON.gz",
        "</code>",
        "",
        "===== Wikipedia input and extraction =====",
        "",
        f"The complete title/offset index of the official 20261101 English Wikipedia multistream dump was scanned for all {selection.scientific_names:,} FishBase scientific names and available common names. It found {selection.scientific_titles_found:,} scientific-title matches and {selection.common_titles_found:,} common-title matches. These select candidate input, not automatic entity links. Redirects found for corpus titles guide retrieval of their target articles in a subsequent pass.",
        "",
        f"The resulting selection contains {len(selection.blocks):,} non-overlapping byte ranges made of complete bzip2 members. Exact HTTP Content-Range and length are verified; a server returning the whole file is rejected. Downloaded bytes are original dump bytes, with URL, offsets, retrieval time and SHA-256 retained. Neighbouring pages within selected members are processed too. Existing verified ranges are reused on repeated fetches. This is an automatically selected corpus from across the whole dump, not two arbitrary dump parts or manually chosen API pages.",
        "",
        "^ Input ^ Stored bytes ^ Provenance ^",
        f"| Complete title/offset index | {index_metadata['bytes']:,} | [[{index_metadata['url']}|Official index]]; SHA-256 ''{selection.index_sha256}'' |",
        f"| Selected original XML ranges | {selection.selected_bytes:,} | [[{selection.dump_url}|Dated parent dump]]; per-range SHA-256 and offsets in metadata |",
    ]
    lines += [
        "",
        f"The selected ranges contain {audit['xml_pages']:,} XML page records and {audit['xml_main_namespace_pages']:,} main-namespace pages. Original XML: {audit['xml_original_bytes']:,} bytes; bzip2: {audit['xml_bzip2_bytes']:,} bytes. The index is reported separately and is not counted as XML content. These are file sizes, not network traffic. The full 27 GB parent XML file was not downloaded.",
        "",
        "Spark partitions the input file paths. Each worker reads concatenated bzip2 members directly, with a 256 KiB input chunk and a buffer bounded by one XML page plus that chunk. REGEX frames pages and extracts page ID, namespace, title, revision, timestamp and text; html.unescape decodes XML entities. Further expressions select a Speciesbox/Taxobox, explicit taxon or genus/species fields, optional family/order/authority/status, categories and a lead paragraph. No XML/HTML DOM parser, XPath or MediaWiki API is used. Raw XML remains compressed and unchanged. One file is handled by one task, so input parallelism is bounded by the number of dump files; it does not split a single bzip2 file between workers.",
        "",
        "===== Join rule and useful attributes =====",
        "",
        "The join key is the scientific name normalized with Unicode NFKC, casefold and collapsed whitespace, shared with FishBase. Common names help retrieve candidates but do not establish a link: several species can share one, and Wikipedia titles may use common names. For example, Glauert's anglerfish contains the scientific name Allenichthys glauerti.",
        "",
        "Only main-namespace, non-redirect, non-disambiguation pages with an extracted scientific name participate. An exact key with more than one Wikipedia candidate is marked ambiguous. Conflicting explicit families are kept for review. An absent family is marked not_available, not reported as confirmed agreement. Accepted links preserve both sources; one source's conservation status does not overwrite the other.",
        "",
        "Useful Wikipedia additions are the lead description, categories, explicit taxonomy/authority and conservation status with its status system. Page ID, revision ID, URL, timestamp, input file and wikitext SHA-256 provide provenance. These fields are available for the next search-index stage; the first submission's index has not been silently replaced.",
        "",
        "===== Observed results =====",
        "",
        "^ Metric ^ Count ^",
        f"| FishBase raw pages processed by Spark | {fish.rows:,} |",
    ]
    for name, count in fish.counts.items():
        lines.append(f"| FishBase: {name.replace('_', ' ')} | {count:,} |")
    for name, count in wikipedia.counts.items():
        lines.append(f"| Wikipedia: {name.replace('_', ' ')} | {count:,} |")
    for name, count in joined.counts.items():
        lines.append(f"| Join: {name.replace('_', ' ')} | {count:,} |")
    lines += [
        "",
        "Unmatched records are also diagnosed against the complete index and retrieved canonical pages:",
        "",
        "^ Reason ^ Species ^",
    ]
    for reason, count in diagnostics["unmatched_reasons"].items():
        lines.append(f"| {reason.replace('_', ' ')} | {count:,} |")
    lines += [
        "",
        f"There are {joined.counts['unique_wikipedia_pages']:,} distinct linked Wikipedia pages, covering {coverage:.2f}% of the {joined.counts['fishbase_species']:,} FishBase records. The initial two-part sample linked only 29 species (0.30%); it was replaced by complete-index-guided selection. Unmatched means no accepted candidate under the current extraction and exact-taxon rule, not proof that Wikipedia has no article. Redirects can guide retrieval; taxonomic synonym links and inherited automatic-taxonomy rendering are not inferred.",
        "",
        "^ Scientific name ^ FishBase ID ^ Wikipedia article / page ID ^ Family check ^ FishBase / Wiki status ^",
    ]
    for example in joined.examples[:5]:
        lines.append(
            f"| {_cell(example.scientific_name)} | [[{example.url}|{example.source_id}]] | [[{example.wiki_url}|{_cell(example.wiki_title)}]] / {example.wiki_page_id} | {example.family_check} | {_cell(example.fishbase_status)} / {_cell(example.wiki_status)} |"
        )
    if joined.examples:
        lines += [
            "",
            "An actual saved example:",
            "",
            "<code json>",
            joined.examples[0].model_dump_json(indent=2),
            "</code>",
        ]
    lines += [
        "",
        "===== Verification =====",
        "",
        f"The independent scripts/validate_spark.py audit compared all {audit['identical_fishbase_records']:,} Spark species records and source metadata with the original sequential parser's deduplicated results. They are equal. It also sequentially framed the original bzip2 XML without Hadoop and checked the complete set of {audit['spark_wikipedia_pages']:,} main-namespace page IDs against Spark output. Accepted links have matching scientific keys and no accepted redirects, disambiguations or family conflicts. Linked and unmatched identities form a complete, disjoint partition of the FishBase species set. The audit completed with no errors.",
        "",
        "Two small extraction test methods use seven unchanged official XML pages. They check scientific names, conservation fields, lead descriptions, revision provenance, inline parameters, nested references, disambiguated genus names and preservation of subspecies rank. They also cover empty parameters, redirects, excluded namespaces and streaming across concatenated bzip2 members. One HTTP-range test checks exact ranges, verified reuse and rejection of full-file responses. The existing FishBase extraction tests remain in place.",
        "",
        "^ Stage ^ Elapsed seconds ^",
        *[f"| {report.stage} | {report.elapsed_seconds:.3f} |" for report in reports],
        "",
        "Timings are individual local runs with different I/O and Spark startup costs. They are not a controlled speedup or cluster-scaling benchmark. The saved Spark physical plan records the actual join/exchanges used.",
        "",
        "===== Independent commands and files =====",
        "",
        "<code bash>",
        "uv sync --locked --extra spark",
        "uv run --extra spark vinf spark fishbase",
        "uv run --extra spark vinf spark plan",
        "uv run --extra spark vinf spark fetch",
        "uv run --extra spark vinf spark wiki",
        "uv run --extra spark vinf spark join",
        "uv run --extra spark vinf spark status",
        "uv run --extra spark vinf spark examples --limit 3",
        "uv run --extra spark vinf spark coverage",
        "uv run --extra spark python scripts/validate_spark.py",
        "</code>",
        "",
        "Download the dated complete multistream index into data/wikipedia/index first, with its publisher dumpstatus.json. plan reads the local index and corpus names; fetch retrieves its ranges. Each command is independent. Repeating plan after Wikipedia extraction incorporates discovered redirect targets; fetch reuses verified ranges, and wiki/join regenerate with --overwrite. Status and examples read manifests without starting Java or scanning all data. A separate vinf-spark CLI uses the same library and supports --project.",
        "",
        "<code text>",
        "data/fishbase/crawled/             existing immutable FishBase input",
        "data/spark/fishbase/species/       deduplicated Spark extraction, JSON.gz parts",
        "data/spark/fishbase/outcomes/      UUID, selection outcome and errors",
        "data/wikipedia/index/             complete title index and publisher metadata",
        "data/wikipedia/selection.json     title hits, offsets and selected byte ranges",
        "data/wikipedia/selected/          original XML.bz2 ranges and download metadata",
        "data/wikipedia/parsed/pages/      extracted main-namespace pages, JSON.gz parts",
        "data/linked/records/              accepted FishBase + Wikipedia records",
        "data/linked/review/               rejected ambiguous/conflicting candidates",
        "data/linked/unmatched/            source IDs and reasons, without copied full text",
        "data/linked/spark-plan.txt         actual Spark execution plan",
        "reports/consultation3/            manifests, logs, independent audit and tests",
        "</code>",
        "",
        "Every Spark stage has a manifest; generated JSON is partitioned and gzip-compressed in this run. Pass --compression none for plain readable JSON. Spark's _SUCCESS and checksum files are framework output markers. Temporary persisted RDDs use DISK_ONLY and are released after processing; full datasets are not collected into driver Python memory. Source raw packets, the old parsed snapshot and the old search index are retained independently.",
        "",
        "===== Actual REGEX code =====",
        "",
        "The following is the executable Wikipedia extractor, not pseudocode:",
        "",
        "<code python>",
        Path(wiki.__file__).read_text().rstrip(),
        "</code>",
        "",
        "FishBase worker extraction reuses the original profile:",
        "",
        "<code python>",
        inspect.getsource(_fishbase_partition).rstrip(),
        "</code>",
        "",
        "===== Downloads =====",
        "",
        "  * {{:user:icit:vinf_spark_source_20261110.zip|Current code, uv lockfile, documentation and tests}}",
        "  * {{:user:icit:vinf_spark_evidence_20261110.zip|Measured results, independent audit, physical plan and offline mini-demo}}",
        "",
        "The full inputs and generated full-corpus outputs remain local; the small demo is explicitly a subset. Wikipedia texts retain the original CC BY-SA attribution and revision provenance. The course wiki's Public Domain license does not relicense Wikipedia or FishBase source data.",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output
