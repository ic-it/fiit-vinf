from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch
from uuid import NAMESPACE_URL, uuid5
from zipfile import ZipFile

from rich.console import Console
from typer.testing import CliRunner
from vinf_core.artifacts import fingerprint, write_models
from vinf_core.files import atomic_model, file_hash
from vinf_core.index_io import IndexReader
from vinf_core.indexes import IndexConfig
from vinf_core.models import VinfError
from vinf_core.records import (
    ExternalIdentifier,
    ParsedEntry,
    ParsedManifest,
    ParsedPage,
    ParseStatus,
    Section,
    SourceDocument,
)
from vinf_indexing import build_index
from vinf_parsing import get_profile
from vinf_search import search
from vinf_search.models import SearchResult

from vinf.cli.app import app
from vinf.search_output import display_hits, highlighted_text
from vinf.search_records import DetailedResult, detailed_result


def sample(root: Path, *, stop_words: tuple[str, ...] = ()) -> tuple[Path, Path]:
    """Three small records over the existing real-page extraction fixture."""
    profile = get_profile("fishbase")
    with ZipFile(Path(__file__).parent / "fixtures/fishbase-20261013.zip") as archive:
        name = next(
            name
            for name in archive.namelist()
            if name.endswith("source.json") and b"5036" in archive.read(name)
        )
        original = SourceDocument.model_validate_json(archive.read(name))
        template = profile.extract(
            archive.read(name.replace("source.json", "content.html")).decode(
                original.encoding
            ),
            original,
        )
    parsed, indexed = root / "parsed", root / "indexed"
    parsed.mkdir()
    (parsed / ".lock").touch()
    entries = []
    for number, (name, body) in enumerate(
        (
            (
                "Alpha primus",
                "Straße; least, concern; least concern; reef reef; «café — reef»; fish - reef",
            ),
            ("Beta secundus", "least concern; plain reef"),
            ("Gamma tertius", "rocky whale"),
        ),
        1,
    ):
        url = f"https://example.test/{number}"
        source = original.model_copy(
            update={
                "doc_id": uuid5(NAMESPACE_URL, url),
                "url": url,
                "requested_url": url,
            }
        )
        record = template.model_copy(
            update={
                "scientific_name": name,
                "species_key": name.casefold(),
                "common_name": "Fish [red]",
                "identifiers": (
                    ExternalIdentifier(namespace="fishbase", value=str(number)),
                ),
                "sections": (
                    Section(
                        name="Biology", text=body, raw_start=10, raw_end=10 + len(body)
                    ),
                ),
            }
        )
        page = ParsedPage(
            source=source,
            fingerprint=fingerprint(profile.version, source.model_dump_json()),
            status=ParseStatus.ACCEPTED,
            reason="fixture",
            record=record,
        )
        path = parsed / "pages" / f"{source.doc_id}.json.gz"
        atomic_model(path, page)
        entries.append(
            ParsedEntry(
                doc_id=source.doc_id,
                path=path.relative_to(parsed).as_posix(),
                sha256=file_hash(path),
                status=page.status,
            )
        )
    catalog = parsed / "catalog.jsonl.gz"
    write_models(catalog, sorted(entries, key=lambda entry: entry.doc_id))
    atomic_model(
        parsed / "manifest.json",
        ParsedManifest(
            producer=profile.version,
            input_fingerprint="fixture",
            record_count=3,
            accepted=3,
            rejected=0,
            errors=0,
            records_path=catalog.name,
            records_sha256=file_hash(catalog),
        ),
    )
    build_index(parsed, indexed, IndexConfig(stop_words=stop_words))
    return parsed, indexed


class SearchViewTests(unittest.TestCase):
    def test_evidence_punctuation_stop_words_and_top_k_reads(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, indexed = sample(Path(temporary), stop_words=("least", "concern"))
            original_text = IndexReader.text
            with patch.object(
                IndexReader, "text", autospec=True, side_effect=original_text
            ) as read:
                normal = search(indexed, "reef", limit=1, explain=True)
                self.assertEqual(read.call_count, 0)
                traced = search(
                    indexed, "reef", limit=1, explain=True, capture_matches=True
                )
                self.assertEqual(read.call_count, 1)
            self.assertEqual(normal.total_matches, 2)
            self.assertEqual(normal.hits[0].doc_id, traced.hits[0].doc_id)
            self.assertEqual(normal.hits[0].score, traced.hits[0].score)
            self.assertEqual(normal.hits[0].explanation, traced.hits[0].explanation)
            self.assertIsNone(normal.hits[0].fulltext)
            for query, expected in (
                ('"least, concern"', ["least, concern"]),
                ('"least concern"', ["least concern"]),
                ('"STRASSE"', ["Straße"]),
                ('"«CAFÉ — reef»"', ["«café — reef»"]),
            ):
                with self.subTest(query=query):
                    result = search(indexed, query, capture_matches=True)
                    self.assertTrue(result.hits)
                    for hit in result.hits:
                        fragments = [
                            hit.fulltext.encode()[span.start : span.end].decode()
                            for span in hit.matches
                        ]
                        self.assertEqual(fragments, expected)
                        self.assertTrue(
                            all(span.kind == "phrase" for span in hit.matches)
                        )
            excluded = search(indexed, 'reef -"least, concern"', capture_matches=True)
            self.assertEqual(
                [hit.scientific_name for hit in excluded.hits], ["Beta secundus"]
            )
            self.assertTrue(
                all(
                    span.kind == "term" and span.value == "reef"
                    for span in excluded.hits[0].matches
                )
            )
            result = search(indexed, '"STRASSE"', capture_matches=True, explain=True)
            replay = SearchResult.model_validate_json(result.model_dump_json())
            indexed.rename(Path(temporary) / "index-unavailable")
            hit = replay.hits[0]
            rendered = highlighted_text(hit.fulltext, hit.matches)
            self.assertEqual(
                [rendered.plain[span.start : span.end] for span in rendered.spans],
                ["Straße"],
            )
            output = StringIO()
            display_hits(
                replay,
                Console(file=output, width=120),
                feed=True,
                highlight=True,
                explain=True,
            )
            text = output.getvalue()
            self.assertIn("Fish [red]", text)
            self.assertGreater(text.index("Explanation"), text.index("Full text"))

    def test_detailed_cli_compression_and_snapshot_validation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            parsed, indexed = sample(root)
            runner = CliRunner()
            arguments = [
                "search",
                "query",
                "reef",
                "--index",
                str(indexed),
                "--limit",
                "1",
            ]
            for flags in (
                [],
                ["--text"],
                ["--highlight", "--explain"],
                ["--text", "--highlight"],
                ["--detailed", "--parsed", str(parsed), "--explain"],
            ):
                with self.subTest(flags=flags):
                    response = runner.invoke(app, arguments + flags)
                    self.assertEqual(
                        response.exit_code,
                        0,
                        response.output + repr(response.exception),
                    )
                    self.assertIn("Matched: 2", response.output)
                    self.assertEqual(
                        "Full text" in response.output,
                        "--text" in flags or "--highlight" in flags,
                    )
                    if "--detailed" in flags:
                        self.assertIn("HTML chars", response.output)
                        self.assertIn("Taxonomy", response.output)
                        self.assertGreater(
                            response.output.index("Explanation"),
                            response.output.index("Biology"),
                        )
            response = runner.invoke(app, arguments + ["--highlight", "--json"])
            payload = json.loads(response.output)
            self.assertTrue(payload["hits"][0]["fulltext"])
            self.assertTrue(payload["hits"][0]["matches"])
            result = search(indexed, "reef", limit=1, explain=True)
            detailed = detailed_result(result, (parsed,))
            replay = DetailedResult.model_validate_json(detailed.model_dump_json())
            output = StringIO()
            display_hits(
                replay,
                Console(file=output, width=120),
                feed=True,
                detailed=True,
                explain=True,
            )
            self.assertIn("Taxonomy", output.getvalue())
            self.assertNotEqual(
                runner.invoke(app, arguments + ["--text", "--detailed"]).exit_code, 0
            )
            for flags in (
                ["--highlight", "--detailed"],
                ["--detailed", "--highlight", "--json"],
            ):
                with patch("vinf.cli.search.search") as run_search:
                    response = runner.invoke(app, arguments + flags)
                    self.assertNotEqual(response.exit_code, 0)
                    self.assertIn(
                        "--highlight cannot be combined with --detailed",
                        response.output,
                    )
                    run_search.assert_not_called()
            catalog = parsed / "catalog.jsonl.gz"
            catalog.write_bytes(
                gzip.compress(
                    gzip.decompress(catalog.read_bytes()).replace(
                        b'"path":', b'"path": '
                    )
                )
            )
            path = parsed / "manifest.json"
            manifest = ParsedManifest.model_validate_json(path.read_bytes())
            atomic_model(
                path, manifest.model_copy(update={"records_sha256": file_hash(catalog)})
            )
            with self.assertRaisesRegex(VinfError, "do not match this index"):
                detailed_result(result, (parsed,))


if __name__ == "__main__":
    unittest.main()
