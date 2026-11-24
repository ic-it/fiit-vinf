# Real FishBase extraction cases

`fishbase-gold.json` contains 20 positive species-summary cases and two negative
pages. Expected fields were transcribed from the visible source HTML, separately
from the extractor. This is a field-accuracy regression set, not a random sample
or an estimate of accuracy over the entire website.

`fishbase-20261013.zip` retains the original five species pages and donation page.
`fishbase-additional.zip` adds 15 species pages and a family page from the saved
25–27 October 2026 crawl. Each ZIP contains original HTML bytes and a SourceDocument
JSON file. The gold manifest references their ZIP members; source metadata retains
the requested/final URL, retrieval time, UUID and original SHA-256. The test verifies
that hash before extraction. Decoding uses the recorded encoding with replacement
for invalid bytes, as in production. HTML is not cleaned or rewritten in fixtures.

The cases check selection, FishBase identity, scientific/common names, class, order,
family, environments, climate, reported maximum length/measurement/sex and IUCN
status. They include numeric and slug URLs, absent fields, thousands/decimal
separators, parenthesized sex labels and TL, SL, WD and NG measurements. Negative
pages must be rejected rather than emitted as species records.

Run from the source checkout:

    uv sync --locked
    uv run python -m unittest discover -s tests -p test_extraction_gold.py -v

The corpus and content belong to their original FishBase contributors. Fixtures
are saved source material for reproducible educational checks; the wiki's text
license does not relicense the downloaded source pages.

## Wikipedia XML cases

wikipedia-3209.xml (Mexican tetra, revision 1383400378) and wikipedia-5009.xml
(Zebrafish, revision 1380373470) are unchanged page records from the official
enwiki-20261101-pages-articles-multistream1.xml-p1p41242.bz2 dump. The original
file is available at https://dumps.wikimedia.org/enwiki/20261101/ and its SHA-256
is a6dd82edf5c32e10fe6e294becde7116c97a0b275b1ffd08a3ddd71d3919caca.

Expected names and conservation fields were read from the source Speciesbox
parameters. Tests also check lead extraction, revision IDs, empty parameters and
streaming across concatenated bzip2 members. Wikipedia contributors retain the
original CC BY-SA license; authorship and revision provenance remain in the XML.

Five further unchanged records from the same dated multistream dump cover real
format variations: Vandellia cirrhosa (49593), Oncorhynchus masou (637713), Anguilla
labiata (5812376), Argentina elongata (6181681), and Ophthalmotilapia nasuta
(12619697). They exercise inline parameters after references, disambiguated genus
names, nested citation templates, and Subspeciesbox. The Anguilla article explicitly
names Anguilla bengalensis labiata; the extractor must retain that three-part taxon
rather than invent an exact match to the two-part FishBase name. Original XML page
and revision IDs remain in each fixture. Parent dump URL and exact compressed
range provenance are recorded by the selection plan and download metadata.

    uv run python -m unittest discover -s tests -p test_wikipedia.py -v

## Native Lucene sample

lucene-sample.json preserves four actual extracted FishBase records and three
accepted Wikipedia enrichments from the completed Spark snapshots. Their original
source URLs, crawler UUIDs, content hashes, Wikipedia page/revision IDs and wikitext
hashes are retained. The fixture also records the full source manifest checksums.
It is a labeled subset, not a new Spark execution or an accuracy sample.

Two native integration methods cover Boolean, token phrases, fuzzy spelling and
keyword filters, actual BM25 explanations, reuse, read-only inputs and output
ownership. scripts/package_lucene_demo.py uses the same records for the offline
demonstration. Source ownership and Wikipedia attribution remain unchanged.
