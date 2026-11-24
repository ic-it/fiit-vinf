"""Small extraction checks using unchanged pages from the official XML dump."""

import bz2
import tempfile
import unittest
from pathlib import Path

from vinf_spark.wiki import extract_page, page_fragments


class WikipediaExtractionTests(unittest.TestCase):
    def test_real_speciesbox_pages(self):
        fixtures = Path(__file__).parent / "fixtures"
        for page_id, title, name, revision in (
            (3209, "Mexican tetra", "Astyanax mexicanus", 1383400378),
            (5009, "Zebrafish", "Danio rerio", 1380373470),
        ):
            with self.subTest(title=title):
                page = extract_page(
                    (fixtures / f"wikipedia-{page_id}.xml").read_text(), "fixture.xml"
                )
                self.assertEqual(page.page_id, page_id)
                self.assertEqual(page.title, title)
                self.assertEqual(page.scientific_name, name)
                self.assertEqual(page.revision_id, revision)
                self.assertEqual(page.status, "LC")
                self.assertEqual(page.status_system, "IUCN3.1")
                self.assertIn(name, page.description)
                self.assertNotIn("<ref", page.description)
        body = (
            b"<mediawiki>"
            + b"".join(
                (fixtures / f"wikipedia-{identifier}.xml").read_bytes()
                for identifier in (3209, 5009)
            )
            + b"</mediawiki>"
        )
        cut = body.index(b"</page>") + 3
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "multistream.xml.bz2"
            path.write_bytes(bz2.compress(body[:cut]) + bz2.compress(body[cut:]))
            pages = [
                extract_page(fragment, "fixture.xml").page_id
                for fragment in page_fragments(path)
            ]
            self.assertEqual(pages, [3209, 5009])

    def test_empty_values_redirect_and_namespace(self):
        head = "<page><title>Test</title><ns>0</ns><id>1</id>"
        page = extract_page(
            head + '<redirect title="Zebrafish"/><revision><text/></revision></page>',
            "fixture.xml",
        )
        self.assertEqual(page.redirect, "Zebrafish")
        self.assertIsNone(page.scientific_name)
        self.assertIsNone(
            extract_page(
                head.replace("<ns>0</ns>", "<ns>1</ns>") + "<text/></page>",
                "fixture.xml",
            )
        )
        page = extract_page(
            head
            + "<text>{{Speciesbox\n|taxon = Danio rerio\n|status = \n|status_system = IUCN3.1\n}}</text></page>",
            "fixture.xml",
        )
        self.assertIsNone(page.status)
        self.assertEqual(page.status_system, "IUCN3.1")
        for page_id, name, status in (
            (49593, "Vandellia cirrhosa", "LC"),
            (637713, "Oncorhynchus masou", "LC"),
            (5812376, "Anguilla bengalensis labiata", None),
            (6181681, "Argentina elongata", None),
            (12619697, "Ophthalmotilapia nasuta", "LC"),
        ):
            with self.subTest(page_id=page_id):
                path = Path(__file__).parent / "fixtures" / f"wikipedia-{page_id}.xml"
                page = extract_page(path.read_text(), "fixture.xml")
                self.assertEqual(page.scientific_name, name)
                self.assertEqual(page.status, status)
