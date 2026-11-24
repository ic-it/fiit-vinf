"""Spark stages. Each command reads saved inputs and publishes its own output."""

import io
import time
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path

from vinf_core.artifacts import fingerprint, read_body, source_document
from vinf_core.files import (
    atomic_model,
    contained_path,
    file_hash,
    read_lock,
    read_model,
    separate_roots,
)
from vinf_core.models import CatalogManifest, DocumentMetadata, VinfError
from vinf_core.records import ParsedPage, ParseStatus
from vinf_parsing.profiles.fishbase import FishBaseProfile

from vinf_spark.models import SparkSettings, StageReport
from vinf_spark.runtime import session, stage_output, write_json
from vinf_spark.wiki import extract_page, page_fragments

PRODUCER = "vinf-spark/1"


def _fishbase_partition(lines, crawled: str):
    profile = FishBaseProfile()
    for line in lines:
        metadata = DocumentMetadata.model_validate_json(line)
        source = source_document(metadata)
        body = read_body(Path(crawled), source)
        selection = profile.select(body)
        record = error = None
        status = ParseStatus.REJECTED
        if selection.selected:
            try:
                record = profile.extract(body, source)
                status = ParseStatus.ACCEPTED
            except ValueError as exc:
                error, status = str(exc), ParseStatus.ERROR
        yield ParsedPage(
            source=source,
            fingerprint=fingerprint(profile.version, source.model_dump_json()),
            status=status,
            reason=selection.reason,
            record=record,
            error=error,
        ).model_dump_json()


def _wiki_partition(paths):
    for path in map(Path, paths):
        for fragment in page_fragments(path):
            page = extract_page(fragment, path.as_uri())
            if page is not None:
                yield page.model_dump_json()


def _report(
    spark, settings, started, stage, inputs, hashes, output, rows, counts, examples=()
):
    return StageReport(
        stage=stage,
        producer=PRODUCER,
        created_at=datetime.now(UTC),
        spark_version=spark.version,
        master=spark.sparkContext.master,
        partitions=settings.partitions,
        inputs=tuple(str(path) for path in inputs),
        input_sha256=tuple(hashes),
        output=str(output),
        rows=rows,
        counts=counts,
        elapsed_seconds=round(time.monotonic() - started, 3),
        examples=examples,
    )


def parse_fishbase(
    crawled: Path, output: Path, settings: SparkSettings, *, overwrite=False
):
    from pyspark import StorageLevel
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    crawled, output = crawled.resolve(), output.resolve()
    separate_roots(crawled, output)
    started = time.monotonic()
    with read_lock(crawled / "crawl/lock"), stage_output(output, overwrite) as work:
        manifest = read_model(crawled / "catalog/manifest.json", CatalogManifest)
        catalog = contained_path(crawled, manifest.catalog_path)
        checksum = file_hash(catalog)
        if checksum != manifest.catalog_sha256:
            raise VinfError("Crawler catalog checksum mismatch.")
        with session(settings) as spark:
            lines = spark.sparkContext.textFile(str(catalog), settings.partitions)
            pages_rdd = lines.mapPartitions(
                lambda batch: _fishbase_partition(batch, str(crawled))
            ).persist(StorageLevel.DISK_ONLY)
            pages = spark.read.json(pages_rdd)
            counts = {
                row.status: row["count"]
                for row in pages.groupBy("status").count().collect()
            }
            total = sum(counts.values())
            if total != manifest.record_count:
                raise VinfError(
                    "Spark extraction row count differs from the crawler catalog."
                )
            if counts.get("error", 0):
                raise VinfError(f"Spark extraction failed for {counts['error']} pages.")
            outcomes = pages.select("source.doc_id", "status", "reason", "error")
            write_json(
                outcomes,
                work / "outcomes",
                partitions=settings.partitions,
                key="doc_id",
                compression=settings.compression,
            )
            identifier = F.try_element_at(
                F.filter(
                    "record.identifiers", lambda item: item.namespace == "fishbase"
                ),
                F.lit(1),
            ).value
            accepted = pages.where(F.col("status") == "accepted").withColumn(
                "source_id", F.coalesce(identifier, F.col("source.doc_id"))
            )
            order = Window.partitionBy("record.source_name", "source_id").orderBy(
                F.col("source.fetched_at").desc(), F.col("source.doc_id").asc()
            )
            species = (
                accepted.withColumn("row_number", F.row_number().over(order))
                .where("row_number = 1")
                .select("source_id", "source", "record")
            )
            write_json(
                species,
                work / "species",
                partitions=settings.partitions,
                key="source_id",
                compression=settings.compression,
            )
            unique = species.count()
            counts.update(
                unique_species=unique, duplicates=counts.get("accepted", 0) - unique
            )
            value = _report(
                spark,
                settings,
                started,
                "fishbase",
                (catalog,),
                (checksum,),
                output,
                total,
                counts,
            )
            atomic_model(work / "manifest.json", value)
            pages_rdd.unpersist()
    return value


def parse_wikipedia(
    inputs: tuple[Path, ...], output: Path, settings: SparkSettings, *, overwrite=False
):
    from pyspark.sql import functions as F

    inputs, output = tuple(path.resolve() for path in inputs), output.resolve()
    if len(set(inputs)) != len(inputs):
        raise VinfError("Each Wikipedia dump file must occur only once.")
    for path in inputs:
        separate_roots(path.parent, output)
    started = time.monotonic()
    hashes = tuple(file_hash(path) for path in inputs)
    with stage_output(output, overwrite) as work, session(settings) as spark:
        files = spark.sparkContext.parallelize(
            [str(path) for path in inputs], min(len(inputs), settings.partitions)
        )
        from pyspark import StorageLevel

        extracted = files.mapPartitions(_wiki_partition).persist(StorageLevel.DISK_ONLY)
        pages = spark.read.json(extracted)
        write_json(
            pages,
            work / "pages",
            partitions=settings.partitions,
            key="page_id",
            compression=settings.compression,
        )
        totals = (
            pages.agg(
                F.count("*").alias("main_namespace_pages"),
                F.count("redirect").alias("redirect_pages"),
                F.count("species_key").alias("species_named_pages"),
                F.sum(F.col("disambiguation").cast("int")).alias(
                    "disambiguation_pages"
                ),
            )
            .first()
            .asDict()
        )
        value = _report(
            spark,
            settings,
            started,
            "wikipedia",
            inputs,
            hashes,
            output,
            totals["main_namespace_pages"],
            totals,
        )
        atomic_model(work / "manifest.json", value)
        extracted.unpersist()
    return value


def link_species(
    fishbase: Path,
    wikipedia: Path,
    output: Path,
    settings: SparkSettings,
    *,
    overwrite=False,
):
    from pyspark import StorageLevel
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    fishbase, wikipedia, output = (
        fishbase.resolve(),
        wikipedia.resolve(),
        output.resolve(),
    )
    for source in (fishbase, wikipedia):
        separate_roots(source, output)
    started = time.monotonic()
    with (
        read_lock(fishbase / ".lock"),
        read_lock(wikipedia / ".lock"),
        stage_output(output, overwrite) as work,
        session(settings) as spark,
    ):
        fish = spark.read.json(str(fishbase / "species")).alias("f")
        wiki = (
            spark.read.json(str(wikipedia / "pages"))
            .where(
                F.col("species_key").isNotNull()
                & F.col("redirect").isNull()
                & ~F.col("disambiguation")
            )
            .withColumn(
                "candidate_count", F.count("*").over(Window.partitionBy("species_key"))
            )
            .alias("w")
        )
        family_known = (
            F.col("f.record.taxonomy.family").isNotNull()
            & F.col("w.family").isNotNull()
        )
        family_conflict = family_known & (
            F.lower(F.col("f.record.taxonomy.family")) != F.lower(F.col("w.family"))
        )
        candidates = (
            fish.join(
                wiki, F.col("f.record.species_key") == F.col("w.species_key"), "inner"
            )
            .select(
                F.struct("f.*").alias("fishbase"),
                F.struct("w.*").alias("wikipedia"),
                F.lit("exact_scientific_name").alias("method"),
                F.when(F.col("w.candidate_count") > 1, "ambiguous")
                .when(family_conflict, "taxonomy_conflict")
                .otherwise("linked")
                .alias("decision"),
                F.when(family_conflict, "conflict")
                .when(family_known, "agrees")
                .otherwise("not_available")
                .alias("family_check"),
            )
            .persist(StorageLevel.DISK_ONLY)
        )
        decisions = {
            row.decision: row["count"]
            for row in candidates.groupBy("decision").count().collect()
        }
        linked = candidates.where("decision = 'linked'")
        linked_ids = linked.select(F.col("fishbase.source_id").alias("source_id"))
        all_candidates = (
            candidates.select(F.col("fishbase.source_id").alias("source_id"))
            .distinct()
            .withColumn("has_candidate", F.lit(True))
        )
        unmatched = (
            fish.join(linked_ids, "source_id", "left_anti")
            .join(all_candidates, "source_id", "left")
            .select(
                "source_id",
                "record.scientific_name",
                "record.species_key",
                F.when(F.col("has_candidate"), "candidate_requires_review")
                .otherwise("not_found_in_selected_dump")
                .alias("reason"),
            )
        )
        write_json(
            candidates.where("decision != 'linked'"),
            work / "review",
            partitions=settings.partitions,
            key="fishbase.source_id",
            compression=settings.compression,
        )
        write_json(
            linked,
            work / "records",
            partitions=settings.partitions,
            key="fishbase.source_id",
            compression=settings.compression,
        )
        write_json(
            unmatched,
            work / "unmatched",
            partitions=settings.partitions,
            key="source_id",
            compression=settings.compression,
        )
        plan = io.StringIO()
        with redirect_stdout(plan):
            linked.explain(mode="formatted")
        (work / "spark-plan.txt").write_text(plan.getvalue())
        counts = {
            "fishbase_species": fish.count(),
            "wiki_species_candidates": wiki.count(),
            "linked_species": decisions.get("linked", 0),
            "unique_wikipedia_pages": linked.select("wikipedia.page_id")
            .distinct()
            .count(),
            "unmatched_species": unmatched.count(),
            "ambiguous_candidates": decisions.get("ambiguous", 0),
            "taxonomy_conflicts": decisions.get("taxonomy_conflict", 0),
        }
        examples = tuple(
            row.asDict(recursive=True)
            for row in linked.select(
                "fishbase.source_id",
                "fishbase.record.scientific_name",
                "fishbase.source.url",
                F.col("wikipedia.page_id").alias("wiki_page_id"),
                F.col("wikipedia.title").alias("wiki_title"),
                F.col("wikipedia.url").alias("wiki_url"),
                "family_check",
                F.col("wikipedia.status").alias("wiki_status"),
                F.col("fishbase.record.iucn_code").alias("fishbase_status"),
                F.col("wikipedia.description").alias("wiki_description"),
            )
            .orderBy("scientific_name")
            .limit(8)
            .collect()
        )
        value = _report(
            spark,
            settings,
            started,
            "join",
            (fishbase, wikipedia),
            (
                file_hash(fishbase / "manifest.json"),
                file_hash(wikipedia / "manifest.json"),
            ),
            output,
            counts["linked_species"],
            counts,
            examples,
        )
        atomic_model(work / "manifest.json", value)
        candidates.unpersist()
    return value
