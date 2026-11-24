"""One focused check for exact ranges, cached reuse and retryable rate limits."""

import bz2
import hashlib
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from vinf_spark.selection import Block, Selection, fetch_selection


class WikiSelectionTests(unittest.TestCase):
    def test_exact_ranges_resume_and_reject_full_responses(self):
        payload = bz2.compress(
            b"<page><title>Example</title><ns>0</ns><id>1</id><text/></page>"
        )
        block = Block(start=100, end=100 + len(payload) - 1)
        selection = Selection(
            dump_url="https://example.org/dump.xml.bz2",
            dump_bytes=10000,
            index_path="index.txt.bz2",
            index_sha256="0" * 64,
            fishbase_manifest_sha256="1" * 64,
            created_at=datetime.now(UTC),
            scientific_names=1,
            common_names=0,
            scientific_titles_found=1,
            common_titles_found=0,
            extra_titles=(),
            missing_scientific_names=(),
            hits=(),
            blocks=(block,),
            selected_bytes=len(payload),
        )
        response = MagicMock()
        response.status_code = 206
        response.headers = {"Content-Range": f"bytes {block.start}-{block.end}/10000"}
        response.iter_content.return_value = [payload]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("vinf_spark.selection.requests.get") as get:
                get.return_value.__enter__.return_value = response
                first = fetch_selection(selection, root, workers=1, interval=0)
                second = fetch_selection(selection, root, workers=1, interval=0)
                self.assertEqual(first.downloaded, 1)
                self.assertEqual(second.reused, 1)
                self.assertEqual(get.call_count, 1)
                self.assertEqual(
                    get.call_args.kwargs["headers"]["Range"],
                    f"bytes={block.start}-{block.end}",
                )
                self.assertEqual(
                    hashlib.sha256((root / block.filename).read_bytes()).digest(),
                    hashlib.sha256(payload).digest(),
                )
            response.status_code = 200
            with patch("vinf_spark.selection.requests.get") as get:
                get.return_value.__enter__.return_value = response
                failed = fetch_selection(selection, root / "bad", workers=1, interval=0)
                self.assertEqual(len(failed.failures), 1)
                self.assertFalse((root / "bad" / block.filename).exists())
            limited = MagicMock()
            limited.status_code = 429
            limited.headers = {"Retry-After": "0"}
            response.status_code = 206
            with patch("vinf_spark.selection.requests.get") as get:
                get.return_value.__enter__.side_effect = [limited, response]
                retried = fetch_selection(
                    selection, root / "retry", workers=1, interval=0
                )
                self.assertEqual(retried.downloaded, 1)
                self.assertEqual(retried.failures, ())
                self.assertEqual(get.call_count, 2)
