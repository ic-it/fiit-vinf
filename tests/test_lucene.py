"""Two native integration checks on four real, provenance-preserving records."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

from vinf_core.files import file_hash
from vinf_core.models import VinfError
from vinf_lucene.indexing import build
from vinf_lucene.models import Scope
from vinf_lucene.search import search

from scripts.package_lucene_demo import write_sample


@unittest.skipUnless(
    importlib.util.find_spec("lucene"),
    "Install native PyLucene to run integration checks",
)
class LuceneIntegrationChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.fishbase, cls.linked = write_sample(cls.root)
        cls.output = cls.root / "data/lucene/indexed"
        cls.result = build(cls.fishbase, cls.linked, cls.output)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_boolean_phrase_fuzzy_and_exact_fields(self):
        for query in (
            "Aaptosyax AND grypus",
            '"Aaptosyax grypus"',
            "Aaptosyaz~1",
            '"Aaptosyax, grypus"',
        ):
            with self.subTest(query=query):
                result = search(self.output, query, explain=True)
                self.assertEqual(result.total_matches, 1)
                self.assertEqual(result.hits[0].scientific_name, "Aaptosyax grypus")
                self.assertIn("BM25", result.hits[0].explanation)
        self.assertEqual(
            search(self.output, "(Aaptosyax OR Abalistes) -grypus")
            .hits[0]
            .scientific_name,
            "Abalistes filamentosus",
        )
        self.assertEqual(search(self.output, "linked:false").total_matches, 1)
        self.assertEqual(
            search(self.output, "Aaptosyax", scope=Scope.FISHBASE).total_matches, 1
        )
        self.assertEqual(
            search(self.output, "source_id:does-not-exist").total_matches, 0
        )
        with self.assertRaises(VinfError):
            search(self.output, '"unclosed phrase')

    def test_complete_ro_inputs_reuse_and_owned_output(self):
        files = tuple(
            p
            for root in (self.fishbase, self.linked)
            for p in root.rglob("*")
            if p.is_file()
        )
        before = {p: file_hash(p) for p in files}
        self.assertEqual(self.result.documents, 4)
        self.assertEqual(self.result.linked_documents, 3)
        self.assertTrue(build(self.fishbase, self.linked, self.output).reused)
        self.assertEqual(before, {p: file_hash(p) for p in files})
        foreign = self.root / "foreign"
        foreign.mkdir()
        (foreign / "important.txt").write_text("keep me")
        with self.assertRaises(VinfError):
            build(self.fishbase, self.linked, foreign, rebuild=True)
        self.assertEqual((foreign / "important.txt").read_text(), "keep me")
