from __future__ import annotations

import hashlib
import math
import shutil
import tempfile
import unittest
from contextlib import chdir
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from uuid import NAMESPACE_URL, uuid5
from zipfile import ZipFile

from vinf_core.artifacts import fingerprint, write_models
from vinf_core.binary import read_uvarint, uvarint
from vinf_core.content import encode_content, read_content
from vinf_core.files import atomic_model, file_hash
from vinf_core.index_io import IndexReader, read_index
from vinf_core.indexes import IndexConfig
from vinf_core.models import Compression, DocumentMetadata, RobotsStrategy, VinfError
from vinf_core.records import (
    ExternalIdentifier,
    ParsedEntry,
    ParsedManifest,
    ParsedPage,
    ParseStatus,
    Section,
    SourceDocument,
)
from vinf_crawler.config import CrawlConfig
from vinf_crawler.discovery import discover_links
from vinf_crawler.export import export_crawl, repack_crawl
from vinf_crawler.models import utc_now
from vinf_crawler.store import CrawlStore
from vinf_indexing import build_index
from vinf_parsing import get_profile, parse_documents
from vinf_parsing.checks import check_parsed
from vinf_parsing.profiles.fishbase import extract_fish, select_reason
from vinf_search.checks import check_index
from vinf_search.models import RankMethod
from vinf_search.query import search


class ProcessingTests(unittest.TestCase):
    def test_binary_integers_and_malformed_input(self):
        for number in (0, 1, 127, 128, 16384, 2**32, 2**64 - 1):
            self.assertEqual(read_uvarint(BytesIO(uvarint(number))), number)
        for data in (b"", b"\x80", b"\x80\x00", b"\xff" * 10, b"\x80" * 9 + b"\x02"):
            with self.subTest(data=data), self.assertRaises(VinfError):
                read_uvarint(BytesIO(data))
        for number in (-1, 2**64):
            with self.assertRaises(VinfError):
                uvarint(number)

    def test_disk_cursors_external_merge_and_top_k(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            ZipFile(
                Path(__file__).parent / "fixtures/fishbase-20261013.zip"
            ) as archive,
        ):
            parent = Path(temporary)
            parsed, indexed = parent / "parsed", parent / "indexed"
            parsed.mkdir()
            (parsed / ".lock").touch()
            name = next(
                name
                for name in archive.namelist()
                if name.endswith("source.json") and b"5036" in archive.read(name)
            )
            source = SourceDocument.model_validate_json(archive.read(name))
            profile = get_profile("fishbase")
            template = profile.extract(
                archive.read(name.replace("source.json", "content.html")).decode(
                    source.encoding
                ),
                source,
            )
            bodies = "common river blue, fish"
            identifiers = sorted(
                (uuid5(NAMESPACE_URL, f"https://synthetic.test/{n}"), n)
                for n in range(400)
            )

            def pages():
                for identifier, number in identifiers:
                    current = source.model_copy(
                        update={
                            "doc_id": identifier,
                            "url": f"https://synthetic.test/{number}",
                        }
                    )
                    scientific_name = f"Species specimen{number:04d}"
                    record = template.model_copy(
                        update={
                            "scientific_name": scientific_name,
                            "species_key": scientific_name.casefold(),
                            "common_name": None,
                            "identifiers": (
                                ExternalIdentifier(
                                    namespace="fishbase", value=str(number)
                                ),
                            ),
                            "sections": (
                                Section(
                                    name="Biology",
                                    text=bodies,
                                    raw_start=0,
                                    raw_end=len(bodies),
                                ),
                            ),
                        }
                    )
                    page = ParsedPage(
                        source=current,
                        fingerprint=fingerprint(
                            profile.version, current.model_dump_json()
                        ),
                        status=ParseStatus.ACCEPTED,
                        reason="synthetic",
                        record=record,
                    )
                    path = parsed / "pages" / f"{identifier}.json"
                    atomic_model(path, page)
                    yield ParsedEntry(
                        doc_id=identifier,
                        path=path.relative_to(parsed).as_posix(),
                        sha256=file_hash(path),
                        status=page.status,
                    )

            catalog = parsed / "catalog.jsonl"
            write_models(catalog, pages())
            atomic_model(
                parsed / "manifest.json",
                ParsedManifest(
                    producer=profile.version,
                    input_fingerprint="synthetic",
                    record_count=400,
                    accepted=400,
                    rejected=0,
                    errors=0,
                    records_path=catalog.name,
                    records_sha256=file_hash(catalog),
                ),
            )
            result = build_index(parsed, indexed, memory_limit=8192, merge_fan_in=2)
            self.assertGreater(result.sort_runs, 10)
            self.assertGreater(result.merge_passes, 1)
            self.assertTrue((indexed / "postings.bin").is_file())
            self.assertFalse((indexed / "terms").exists())
            with IndexReader(indexed) as reader:
                info = reader.find("common")
                self.assertEqual((info.df, info.blocks), (400, 4))
                with reader.cursor(info) as cursor:
                    with self.assertRaises(VinfError):
                        cursor.block(info.blocks)
                    self.assertEqual(cursor.current.doc_id, 0)
                    self.assertEqual(cursor.advance_to(129).doc_id, 129)
                    self.assertEqual(cursor.advance_to(399).doc_id, 399)
                    with cursor.positions() as positions:
                        self.assertEqual(len(tuple(positions)), 1)
                    self.assertIsNone(cursor.next())
                with (
                    reader.cursor(info) as first,
                    reader.cursor(reader.find("river")) as second,
                ):
                    first.advance_to(129)
                    second.advance_to(271)
                    with first.positions() as left, second.positions() as right:
                        self.assertEqual(next(left).position, 2)
                        self.assertEqual(next(right).position, 3)
                    self.assertEqual(first.current.doc_id, 129)
                    self.assertEqual(second.next().doc_id, 272)
                    with self.assertRaises(VinfError):
                        first.positions(second.current)
                invalid = info.model_copy(
                    update={"offset": reader.sizes["postings.bin"] - 1}
                )
                with self.assertRaises(VinfError):
                    reader.cursor(invalid)
            with (
                patch.object(
                    IndexReader,
                    "documents",
                    side_effect=AssertionError("eager document scan"),
                ),
                patch.object(
                    IndexReader,
                    "terms",
                    side_effect=AssertionError("eager vocabulary scan"),
                ),
            ):
                ranked = search(indexed, "common river", limit=3)
                self.assertEqual(ranked.total_matches, 400)
                self.assertEqual(len(ranked.hits), 3)
                self.assertEqual(
                    [hit.scientific_name for hit in ranked.hits],
                    [f"Species specimen{n:04d}" for n in range(3)],
                )
                self.assertEqual(search(indexed, '"blue fish"').total_matches, 0)
                self.assertEqual(search(indexed, '"blue, fish"').total_matches, 400)
                self.assertEqual(search(indexed, "common -river").total_matches, 0)
                self.assertEqual(
                    search(indexed, "NOT missing", limit=3).total_matches, 400
                )
            self.assertTrue(check_index(indexed, deep=True).ok)
            before = {
                str(path.relative_to(indexed)): file_hash(path)
                for path in indexed.rglob("*")
                if path.is_file()
            }
            with chdir(parent):
                build_index(
                    Path("parsed"),
                    Path("indexed"),
                    rebuild=True,
                    memory_limit=65536,
                    merge_fan_in=4,
                )
            self.assertEqual(
                before,
                {
                    str(path.relative_to(indexed)): file_hash(path)
                    for path in indexed.rglob("*")
                    if path.is_file()
                },
            )

    def test_source_qualified_identity_and_duplicate_snapshots(self):
        """Two providers may use the same numeric ID for the same species."""
        fishbase = get_profile("fishbase")

        class OtherProfile:
            # A test adapter over the same fixture, not another supported site.
            name = "other"
            version = "other-test/1"
            select = staticmethod(fishbase.select)

            @staticmethod
            def extract(body, source):
                record = fishbase.extract(body, source)
                return record.model_copy(
                    update={
                        "profile": OtherProfile.version,
                        "source_name": "other",
                        "identifiers": (
                            ExternalIdentifier(namespace="other", value="5036"),
                        ),
                    }
                )

        with (
            tempfile.TemporaryDirectory() as temporary,
            ZipFile(
                Path(__file__).parent / "fixtures/fishbase-20261013.zip"
            ) as archive,
        ):
            root = Path(temporary)
            name = next(
                name
                for name in archive.namelist()
                if name.endswith("source.json") and b"5036" in archive.read(name)
            )
            saved = SourceDocument.model_validate_json(archive.read(name))
            body = archive.read(name.replace("source.json", "content.html"))
            parsed_inputs = []
            for profile in (fishbase, OtherProfile()):
                crawled, parsed = (
                    root / profile.name / "crawled",
                    root / profile.name / "parsed",
                )
                store = CrawlStore.initialize(
                    crawled,
                    CrawlConfig(seeds=(f"https://{profile.name}.test/summary/5036",)),
                )
                task = store.next_task()
                metadata = DocumentMetadata(
                    doc_id=task.doc_id,
                    requested_url=task.url,
                    final_url=task.url,
                    raw_path=f"raw/{task.doc_id}/content.html",
                    fetched_at=saved.fetched_at,
                    http_status=200,
                    content_type="text/html",
                    encoding=saved.encoding,
                    size_bytes=len(body),
                    sha256=saved.sha256,
                    depth=0,
                    user_agent=store.config.http.user_agent,
                    robots_strategy=RobotsStrategy.RESPECT,
                    elapsed_seconds=0,
                )
                links = discover_links(
                    body.decode(saved.encoding), task.url, task.doc_id, 0, store.config
                )
                store.save_packet(task, metadata, body, links)
                store.commit_page(task, metadata, links)
                store.publish_catalog()
                parse_documents(crawled, parsed, profile=profile)
                parsed_inputs.append(parsed)
            shutil.copytree(parsed_inputs[0], root / "duplicate")
            parsed_inputs.append(root / "duplicate")
            indexed = root / "indexed"
            result = build_index(parsed_inputs, indexed)
            self.assertEqual(result.documents, 2)
            hits = search(indexed, "marine").hits
            self.assertEqual({hit.source_name for hit in hits}, {"fishbase", "other"})
            generation, _ = read_index(indexed)
            with IndexReader(generation) as reader:
                records = list(reader.documents())
            self.assertEqual(
                {(record.source_name, record.source_id) for record in records},
                {("fishbase", "5036"), ("other", "5036")},
            )
            self.assertEqual(len({record.scientific_name for record in records}), 1)
            self.assertEqual(len({record.doc_id for record in records}), 2)
            self.assertTrue(check_index(indexed, deep=True).ok)
            self.assertEqual(
                build_index(list(reversed(parsed_inputs)), indexed).generation,
                result.generation,
            )

    def test_compressed_exports_and_independent_stages(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            ZipFile(
                Path(__file__).parent / "fixtures/fishbase-20261013.zip"
            ) as archive,
        ):
            parent = Path(temporary)
            name = next(
                name
                for name in archive.namelist()
                if name.endswith("source.json") and b"5036" in archive.read(name)
            )
            source = SourceDocument.model_validate_json(archive.read(name))
            body = archive.read(name.replace("source.json", "content.html"))
            original = parent / "original"
            store = CrawlStore.initialize(
                original, CrawlConfig(seeds=(source.requested_url,))
            )
            task = store.next_task()
            metadata = DocumentMetadata(
                doc_id=task.doc_id,
                requested_url=task.url,
                final_url=source.url,
                raw_path=f"raw/{task.doc_id}/content.html",
                fetched_at=source.fetched_at,
                http_status=200,
                content_type="text/html",
                encoding=source.encoding,
                size_bytes=len(body),
                sha256=source.sha256,
                depth=0,
                user_agent=store.config.http.user_agent,
                robots_strategy=RobotsStrategy.RESPECT,
                elapsed_seconds=0,
            )
            links = discover_links(
                body.decode(source.encoding), source.url, task.doc_id, 0, store.config
            )
            store.save_packet(task, metadata, body, links)
            store.commit_page(task, metadata, links)
            store.publish_catalog()
            snapshot = {
                str(path.relative_to(original)): (
                    path.read_bytes(),
                    path.stat().st_mtime_ns,
                )
                for path in original.rglob("*")
                if path.is_file()
            }
            for compression in Compression:
                with self.subTest(compression=compression):
                    crawled = parent / compression / "crawled"
                    parsed = parent / compression / "parsed"
                    indexed = parent / compression / "indexed"
                    result = export_crawl(original, crawled, compression)
                    self.assertTrue(result.content_verified)
                    exported = CrawlStore(crawled)
                    document = next(iter(exported.iter_documents()))
                    self.assertEqual(document.compression, compression)
                    self.assertEqual(read_content(crawled, document), body)
                    self.assertEqual(
                        encode_content(body, compression),
                        encode_content(body, compression),
                    )
                    parsed_result = parse_documents(
                        crawled,
                        parsed,
                        profile=get_profile("fishbase"),
                        compression=Compression.GZIP,
                    )
                    self.assertEqual(parsed_result.accepted, 1)
                    self.assertTrue((parsed / "catalog.jsonl.gz").is_file())
                    self.assertFalse(list(parsed.rglob("*.jsonl")))
                    self.assertFalse(
                        (parsed / "pages" / f"{task.doc_id}.json").exists()
                    )
                    self.assertEqual(
                        parse_documents(
                            crawled, parsed, profile=get_profile("fishbase")
                        ).reused,
                        1,
                    )
                    self.assertFalse((crawled / "extraction").exists())
                    self.assertFalse((parsed / "raw").exists())
                    plain = parse_documents(
                        crawled,
                        parsed,
                        profile=get_profile("fishbase"),
                        compression=Compression.NONE,
                    )
                    self.assertEqual(plain.reused, 1)
                    self.assertTrue((parsed / "catalog.jsonl").is_file())
                    self.assertFalse(list(parsed.rglob("*.gz")))
                    self.assertTrue(
                        check_parsed(
                            parsed, crawled=crawled, profile=get_profile("fishbase")
                        ).ok
                    )
                    repacked = repack_crawl(crawled, Compression.NONE)
                    self.assertTrue(Path(repacked.backup).is_dir())
                    self.assertFalse(list((crawled / "raw").rglob("*.gz")))
                    document = next(iter(CrawlStore(crawled).iter_documents()))
                    self.assertEqual(read_content(crawled, document), body)
                    parse_documents(crawled, parsed, profile=get_profile("fishbase"))
                    crawled.rename(crawled.with_name("raw-unavailable"))
                    first = build_index(parsed, indexed)
                    self.assertFalse(first.reused)
                    self.assertTrue(build_index(parsed, indexed).reused)
                    parsed.rename(parsed.with_name("parsed-unavailable"))
                    hits = search(indexed, "marine", explain=True)
                    self.assertEqual(hits.total_matches, 1)
                    self.assertEqual(
                        hits.hits[0].scientific_name, "Trachinus pellegrini"
                    )
                    self.assertFalse((indexed / "crawl").exists())
                    self.assertFalse(list(indexed.rglob("*.gz")))
                    with self.assertRaises(VinfError):
                        parse_documents(
                            original,
                            original / "nested",
                            profile=get_profile("fishbase"),
                        )
                    if compression != Compression.NONE:
                        compressed_document = next(
                            iter(CrawlStore(Path(repacked.backup)).iter_documents())
                        )
                        raw = Path(repacked.backup) / compressed_document.raw_path
                        raw.write_bytes(raw.read_bytes()[:-3])
                        with self.assertRaises(VinfError):
                            read_content(Path(repacked.backup), compressed_document)
            self.assertEqual(
                snapshot,
                {
                    str(path.relative_to(original)): (
                        path.read_bytes(),
                        path.stat().st_mtime_ns,
                    )
                    for path in original.rglob("*")
                    if path.is_file()
                },
            )

    def test_real_saved_html_extraction_and_negative_page(self):
        # Expected values are read from the five original summary pages, not
        # generated from extractor output. The sixth fixture is a donation page.
        expected = {
            5036: (
                "Trachinus pellegrini",
                "Cape Verde weever",
                ("marine",),
                20.0,
                "TL",
                "LC",
                "Trachinidae",
            ),
            52448: (
                "Torquigener squamicauda",
                "Brush-tail toadfish",
                ("marine", "brackish"),
                15.0,
                "TL",
                "LC",
                "Tetraodontidae",
            ),
            63126: (
                "Trichomycterus aguarague",
                None,
                ("freshwater",),
                None,
                None,
                "LC",
                "Trichomycteridae",
            ),
            71139: (
                "Triplophysa wudangensis",
                None,
                ("freshwater",),
                5.9,
                "SL",
                "NE",
                "Nemacheilidae",
            ),
            71140: (
                "Triplophysa qingzhenensis",
                None,
                ("freshwater",),
                10.5,
                "SL",
                "NE",
                "Nemacheilidae",
            ),
        }
        seen = set()
        with ZipFile(
            Path(__file__).parent / "fixtures/fishbase-20261013.zip"
        ) as archive:
            for name in archive.namelist():
                if not name.endswith("source.json"):
                    continue
                source = SourceDocument.model_validate_json(archive.read(name))
                raw = archive.read(name.replace("source.json", "content.html"))
                self.assertEqual(hashlib.sha256(raw).hexdigest(), source.sha256)
                body = raw.decode(source.encoding)
                selected, _, _ = select_reason(body)
                if "/donate/" in source.url:
                    self.assertFalse(selected)
                    continue
                self.assertTrue(selected)
                fish = extract_fish(body, source)
                seen.add(int(fish.identifier("fishbase")))
                length = fish.max_length
                actual = (
                    fish.scientific_name,
                    fish.common_name,
                    tuple(fish.environments),
                    length.centimetres if length else None,
                    length.measurement if length else None,
                    fish.iucn_code,
                    fish.taxonomy.family,
                )
                self.assertEqual(actual, expected[int(fish.identifier("fishbase"))])
                self.assertTrue(fish.distribution)
                if int(fish.identifier("fishbase")) == 5036:
                    alternate_heading = body.replace(
                        "Size / Weight / Age",
                        "Length at first maturity / Size / Weight / Age",
                        1,
                    )
                    self.assertEqual(
                        extract_fish(alternate_heading, source).max_length.centimetres,
                        20,
                    )
                    variant = body.replace(
                        "20.0 cm TL male/unsexed",
                        "20.0 cm TL male/unsexed; 0.3 m TL female",
                        1,
                    )
                    female_maximum = extract_fish(variant, source).max_length
                    self.assertEqual(female_maximum.centimetres, 30)
                    self.assertEqual(female_maximum.sex, "female")
                self.assertTrue(
                    all(
                        0 <= section.raw_start < section.raw_end <= len(body)
                        for section in fish.sections
                    )
                )
        self.assertEqual(seen, set(expected))

    def test_stages_ranking_resume_invalidation_and_corrupt_posting(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            crawled, parsed, indexed = (
                root / name for name in ("crawled", "parsed", "indexed")
            )
            config = CrawlConfig(
                seeds=tuple(
                    f"https://example.test/summary/{identifier}"
                    for identifier in range(1, 5)
                )
            )
            store = CrawlStore.initialize(crawled, config)

            def save(identifier: int, name: str, habitat: str, biology: str):
                task = next(
                    value.task
                    for value in store.iter_tasks()
                    if value.task.url.endswith(f"/{identifier}")
                )
                body = (
                    f'<title>{name}</title><div id="ss-sciname"><h1><span class="sciname">{name}</span></h1></div>'
                    f'<div id="ss-main"><h1>Environment</h1>{habitat}'
                    "<h1>Distribution</h1>China<h1>Size / Weight / Age</h1>Max length : 10 cm SL"
                    f"<h1>Biology</h1>{biology}<h1>IUCN Red List Status</h1>Least Concern (LC)<h1>Tools</h1></div>"
                ).encode()
                metadata = DocumentMetadata(
                    doc_id=task.doc_id,
                    requested_url=task.url,
                    final_url=task.url,
                    raw_path=f"raw/{task.doc_id}/content.html",
                    fetched_at=utc_now(),
                    http_status=200,
                    content_type="text/html",
                    encoding="utf-8",
                    size_bytes=len(body),
                    sha256=hashlib.sha256(body).hexdigest(),
                    depth=0,
                    user_agent=config.http.user_agent,
                    robots_strategy=RobotsStrategy.RESPECT,
                    elapsed_seconds=0,
                )
                links = discover_links(body.decode(), task.url, task.doc_id, 0, config)
                store.save_packet(task, metadata, body, links)
                store.commit_page(task, metadata, links)
                store.publish_catalog()

            save(
                1,
                "Alpha primus",
                "Freshwater",
                "needle needle needle in the reef; least, concern; very very; very, rare; (status); fish - reef; Straße; «café — reef»",
            )
            save(2, "Beta secundus", "Marine", "needle in reef least concern very rare")
            save(3, "Gamma tertius", "Freshwater", "rocky within reef-fish")
            parsed_result = parse_documents(
                crawled, parsed, profile=get_profile("fishbase")
            )
            self.assertEqual(parsed_result.accepted, 3)
            first = build_index(parsed, indexed)
            self.assertEqual(first.documents, 3)
            self.assertFalse(first.reused)
            repeated = parse_documents(crawled, parsed, profile=get_profile("fishbase"))
            self.assertEqual(repeated.reused, 3)
            self.assertTrue(build_index(parsed, indexed).reused)
            self.assertTrue(check_index(indexed, deep=True).ok)
            ranked = search(indexed, "needle", explain=True)
            self.assertEqual(ranked.hits[0].scientific_name, "Alpha primus")
            index_root, manifest = read_index(indexed)
            with IndexReader(indexed) as reader:
                alpha = next(
                    document
                    for document in reader.documents()
                    if document.scientific_name == "Alpha primus"
                )
            expected = (
                math.log(1 + 1.5 / 2.5)
                * (3 * 2.2)
                / (3 + 1.2 * (0.25 + 0.75 * alpha.length / manifest.average_length))
            )
            self.assertAlmostEqual(ranked.hits[0].score, expected)
            cosine = search(indexed, "needle", method=RankMethod.TFIDF, explain=True)
            self.assertTrue(all(0 < hit.score <= 1 for hit in cosine.hits))
            self.assertAlmostEqual(
                cosine.hits[0].score,
                sum(term.contribution for term in cosine.hits[0].explanation),
            )
            summed = search(
                indexed, "needle needle", method=RankMethod.TFIDF_SUM, explain=True
            )
            self.assertAlmostEqual(summed.hits[0].score, 3 * math.log(3 / 2))
            self.assertAlmostEqual(summed.hits[0].explanation[0].idf, math.log(3 / 2))
            # A term present in every document has classic IDF zero, but still matches.
            common = search(indexed, "least", method=RankMethod.TFIDF_SUM)
            self.assertEqual(common.total_matches, 3)
            self.assertTrue(all(hit.score == 0 for hit in common.hits))
            self.assertEqual(
                search(indexed, "needle nonexistent").total_matches,
                0,
            )

            def names(query):
                return {hit.scientific_name for hit in search(indexed, query).hits}

            self.assertEqual(names('"in the reef"'), {"Alpha primus"})
            self.assertEqual(names('"in reef"'), {"Beta secundus"})
            phrase_cases = (
                ('"least concern"', {"Alpha primus", "Beta secundus", "Gamma tertius"}),
                ('"least, concern"', {"Alpha primus"}),
                ('"very, rare"', {"Alpha primus"}),
                ('"very rare"', {"Beta secundus"}),
                ('"very very"', {"Alpha primus"}),
                ('"(status)"', {"Alpha primus"}),
                ('"fish - reef"', {"Alpha primus"}),
                ('"STRASSE"', {"Alpha primus"}),
                ('"«CAFÉ — reef»"', {"Alpha primus"}),
                ('"café reef"', set()),
                ('"China Max"', set()),  # Adjacent words across two sections.
                ('"very concern"', set()),
                ('"needle in the reef"', {"Alpha primus"}),
                ('"needle in reef"', {"Beta secundus"}),
                ('"needle in the"', {"Alpha primus"}),
                ('"in the"', {"Alpha primus"}),
                ('"in"', {"Alpha primus", "Beta secundus"}),
                ('"reef"', {"Alpha primus", "Beta secundus"}),
                ('"with"', set()),
                ('"needle the needle"', set()),
            )
            for query, expected_names in phrase_cases:
                with self.subTest(query=query):
                    self.assertEqual(names(query), expected_names)
            configured = root / "configured"
            settings = IndexConfig(
                stop_words=(
                    "in",
                    "the",
                    "reef",
                    "least",
                    "concern",
                    "very",
                    "status",
                    "strasse",
                    "café",
                    "china",
                    "with",
                )
            )
            reduced = build_index(parsed, configured, settings)
            self.assertLess(reduced.terms, first.terms)
            self.assertLess(reduced.tokens, first.tokens)
            self.assertTrue(check_index(configured, deep=True).ok)
            self.assertTrue(build_index(parsed, configured, settings).reused)
            for query, expected_names in phrase_cases:
                with self.subTest(stop_words=True, query=query):
                    result = search(configured, query)
                    self.assertEqual(
                        {hit.scientific_name for hit in result.hits}, expected_names
                    )
                    self.assertTrue(
                        set(result.unknown_tokens).isdisjoint(settings.stop_words)
                    )
            self.assertEqual(search(configured, "the needle").total_matches, 2)
            self.assertEqual(search(configured, 'needle -"in the"').total_matches, 1)
            self.assertEqual(search(configured, '"in the" OR rocky').total_matches, 2)
            self.assertEqual(search(configured, '"in the" -marine').total_matches, 1)
            self.assertTrue(
                all(hit.score == 0 for hit in search(configured, '"in the"').hits)
            )
            with self.assertRaises(VinfError):
                search(configured, "in the")

            def positions(reader, identifier):
                with reader.cursor(reader.find("rare")) as cursor:
                    posting = cursor.advance_to(identifier)
                    if posting is None or posting.doc_id != identifier:
                        return ()
                    with cursor.positions() as values:
                        return tuple(values)

            with IndexReader(indexed) as original, IndexReader(configured) as reader:
                for term in settings.stop_words:
                    self.assertIsNone(reader.find(term))
                self.assertEqual(
                    reader.manifest.average_length, manifest.average_length
                )
                for document in reader.documents():
                    previous = original.document(document.local_id)
                    self.assertEqual(document.length, previous.length)
                    self.assertEqual(reader.text(document), original.text(previous))
                    self.assertEqual(
                        positions(original, document.local_id),
                        positions(reader, document.local_id),
                    )
                all_terms = tuple(info.term for info in original.terms())
            for method in RankMethod:
                ranked = search(configured, "needle", method=method, explain=True)
                self.assertTrue(all(hit.score > 0 for hit in ranked.hits))
                if method == RankMethod.TFIDF:
                    self.assertTrue(all(hit.score <= 1 for hit in ranked.hits))
            empty = build_index(parsed, configured, IndexConfig(stop_words=all_terms))
            self.assertEqual((empty.terms, empty.tokens), (0, 0))
            self.assertEqual((configured / "postings.bin").stat().st_size, 8)
            self.assertTrue(check_index(configured, deep=True).ok)
            self.assertEqual(search(configured, '"least concern"').total_matches, 3)
            self.assertEqual(search(configured, '"STRASSE"').total_matches, 1)
            with IndexReader(indexed) as reader:
                location = reader.find("needle")
                with reader.cursor(location) as cursor:
                    self.assertEqual(sorted(posting.tf for posting in cursor), [1, 3])
            self.assertEqual(names('"Alpha primus" -marine'), {"Alpha primus"})
            self.assertEqual(
                names("needle (marine OR freshwater) -Alpha"), {"Beta secundus"}
            )
            self.assertEqual(
                names("needle OR nonexistent"), {"Alpha primus", "Beta secundus"}
            )
            self.assertEqual(names("needle AND nonexistent"), set())
            self.assertEqual(names("the needle"), {"Alpha primus"})
            self.assertEqual(names("needle OR the"), {"Alpha primus", "Beta secundus"})
            self.assertEqual(names("NOT NOT needle"), {"Alpha primus", "Beta secundus"})
            self.assertEqual(names("needle -reef"), set())
            with IndexReader(indexed) as reader:
                location = reader.find("needle")
                with reader.cursor(location) as cursor:
                    posting = cursor.current
            # Positions are independently checked and need not be read for TF scoring.
            posting_path = index_root / "postings.bin"
            corrupt = bytearray(posting_path.read_bytes())
            corrupt[posting.offset + posting.size - 1] ^= 1
            posting_path.write_bytes(corrupt)
            self.assertEqual(search(indexed, "needle").total_matches, 2)
            with self.assertRaises(VinfError):
                search(indexed, '"needle"')
            self.assertFalse(check_index(indexed, deep=True).ok)
            with self.assertRaises(VinfError):
                build_index(parsed, indexed)  # Cached reuse verifies every file.
            posting_path.unlink()
            self.assertFalse(check_index(indexed, deep=True).ok)
            with self.assertRaises(VinfError):
                search(indexed, "needle")
            self.assertFalse(build_index(parsed, indexed, rebuild=True).reused)
            self.assertTrue(check_index(indexed, deep=True).ok)
            save(4, "Delta quartus", "Brackish", "needle")
            self.assertEqual(search(indexed, "needle").total_matches, 2)
            self.assertFalse(check_parsed(parsed, crawled=crawled).ok)
            updated = parse_documents(crawled, parsed, profile=get_profile("fishbase"))
            self.assertEqual(updated.written, 1)
            second = build_index(parsed, indexed)
            self.assertNotEqual(first.generation, second.generation)
            self.assertTrue(check_index(indexed, deep=True).ok)
            self.assertFalse((indexed / "builds").exists())
            alternative = build_index(parsed, indexed, IndexConfig(k1=1.5))
            _, active = read_index(indexed)
            self.assertEqual(active.generation, alternative.generation)
            junk = indexed / "obsolete-output"
            junk.mkdir()
            (junk / "old.bin").write_bytes(b"obsolete")
            build_index(parsed, indexed, rebuild=True)
            self.assertFalse(junk.exists())


if __name__ == "__main__":
    unittest.main()
