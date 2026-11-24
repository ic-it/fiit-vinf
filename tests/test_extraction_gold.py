"""Twenty independently labeled real summaries and two non-species pages."""

import hashlib
import unittest
from pathlib import Path
from zipfile import ZipFile

from pydantic import TypeAdapter
from vinf_core.models import Model
from vinf_core.records import SourceDocument
from vinf_parsing import get_profile


class ExpectedFields(Model):
    scientific_name: str
    fishbase_id: str
    common_name: str | None
    class_name: str
    order: str
    family: str
    environments: tuple[str, ...]
    climate: str
    max_length_cm: float | None
    measurement: str | None
    sex: str | None
    iucn_code: str


class GoldCase(Model):
    archive: str
    source: str
    expected: ExpectedFields | None


class ExtractionGoldTests(unittest.TestCase):
    def test_twenty_real_summaries_and_two_negative_pages(self):
        fixtures = Path(__file__).parent / "fixtures"
        cases = TypeAdapter(tuple[GoldCase, ...]).validate_json(
            (fixtures / "fishbase-gold.json").read_bytes()
        )
        self.assertEqual(sum(case.expected is not None for case in cases), 20)
        self.assertEqual(len({case.source for case in cases}), 22)
        profile = get_profile("fishbase")
        for case in cases:
            with (
                self.subTest(source=case.source),
                ZipFile(fixtures / case.archive) as archive,
            ):
                source = SourceDocument.model_validate_json(archive.read(case.source))
                raw = archive.read(case.source.replace("source.json", "content.html"))
                self.assertEqual(hashlib.sha256(raw).hexdigest(), source.sha256)
                body = raw.decode(source.encoding, errors="replace")
                self.assertEqual(
                    profile.select(body).selected, case.expected is not None
                )
                if case.expected is None:
                    continue
                fish = profile.extract(body, source)
                length = fish.max_length
                actual = ExpectedFields(
                    scientific_name=fish.scientific_name,
                    fishbase_id=fish.identifier("fishbase"),
                    common_name=fish.common_name,
                    class_name=fish.taxonomy.class_name,
                    order=fish.taxonomy.order,
                    family=fish.taxonomy.family,
                    environments=fish.environments,
                    climate=fish.climate,
                    max_length_cm=length.centimetres if length else None,
                    measurement=length.measurement if length else None,
                    sex=length.sex if length else None,
                    iucn_code=fish.iucn_code,
                )
                self.assertEqual(actual, case.expected)
