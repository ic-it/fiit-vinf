# Fish species search

A file-based FIIT VINF project: generic crawling, REGEX extraction, a self-written
positional index, Boolean/exact search and TF-IDF/BM25 ranking. Spark extracts FishBase
and Wikipedia records and links accepted scientific names. An independent PyLucene
backend indexes the enriched corpus and supports Boolean, phrase and fuzzy queries.
Both retrieval backends store indexes in files without a database service.

## Independent stages

Independent stage libraries share DTOs and file-format utilities through a small core.
Each stage has its own CLI; no command starts another stage.

| Package | Owns | Reads | Writes |
| --- | --- | --- | --- |
| vinf-crawler | HTTP, FIFO queue, robots, scope, retries | Seeds and saved tasks | Original page packets and crawl state |
| vinf-parsing | Source-specific REGEX extraction | Crawled pages | One canonical result per page |
| vinf-indexing | Search text, tokenization, deduplication, positional postings | Parsed results | Standalone index |
| vinf-search | Query lexer/parser, matching, ranking | Index only | Results on stdout or explicit reports |
| vinf-spark | Parallel REGEX extraction, deduplication and joining | Saved FishBase packets and Wikipedia dump | Species, pages and accepted/review/unmatched links |
| vinf-lucene | Native Lucene indexing, querying and comparison | Completed Spark species and accepted links | Standalone Lucene index and measured results |
| vinf-core | Pydantic contracts, streams, locks, checksums and shared text rules | Local files | Shared file utilities |

The application in src/vinf/cli composes public APIs through Typer and Rich.
Libraries reuse core contracts and the existing extraction profiles. The Lucene
backend consumes saved Spark DTOs without starting Spark. Stage libraries do not
depend on the CLI. Index construction cannot download or invoke an extraction
adapter. Search does not import the index builder. Python 3.12+ and uv are required.

## Commands and projects

    uv sync --locked
    uv run vinf project init --name fish-search --seed 'https://www.fishbase.se/summary/268'
    uv run vinf crawl init
    uv run vinf crawl run --limit 50
    uv run vinf parse run
    uv run vinf index build
    uv run vinf search query 'marine (China OR Japan) -reef' --explain

A local vinf.project.json stores source names, crawl/parse paths, index path,
initial crawl settings and optional query file. It is discovered from the working
directory upwards. There is no global registry or background pipeline. In an already
configured project, skip project/crawl initialization.

Use vinf --project PATH to select another project, or explicit --input, --output,
--index and --dataset options to override stage paths. The separate vinf-crawl,
vinf-parse, vinf-index and vinf-search entry points use the same configuration.
Input/output directories must not overlap. Consumers leave their inputs unchanged.

Project crawl settings initialize a new dataset. Existing datasets retain their
own saved settings; inspect or change them with crawl policy. New configurations
default to no page, depth, per-page link-count or aggregate byte cap. Optional
traversal limits remain available. Each response is still bounded separately.

    uv run vinf crawl run

The default run has no page-count cap (--limit 0). It continues while ready tasks
are available, until interrupted or another configured policy stops it. Pass
--limit 50 for a bounded sample. The run limit is additional to any dataset limits;
restarting resumes the saved queues. Cooling-down or manual-retry tasks remain
retained when a run has no ready work left.

Register another source with project add-source NAME --profile PROFILE --seed URL.
Crawler/parser commands require --source when there are several sources. Indexing
combines all registered parsed inputs by default. Only the FishBase extraction
adapter is currently implemented; registration alone does not add an adapter.

## Storage

    data/fishbase/
      crawled/
        raw/<uuid>/
          content.html
          metadata.json
          links.json
        crawl/
          state.json            # Configuration, counts, queue cursor and worker claims
          queue.jsonl           # UUIDs appended at the tail, one JSON string per line
          priority.jsonl        # Preferred UUIDs, consumed before the ordinary FIFO
          tasks/00.json         # Tasks grouped by the first two UUID hex digits
          tasks/01.json
          ...                   # Exactly 256 JSON buckets, through ff.json
                                # One canonical task: URL, status, attempts and error
          limited/<prefix>/<uuid>
                                # Empty markers for links retained by depth/width limits
          pending.json          # Exists only while a recoverable write is unfinished
          network.json          # Atomic site-wide and per-proxy clocks/cooldowns
          transports/<id>/robots/*.json
          lock                  # One writer per dataset
        catalog/                # Streamed committed-document catalog for parsing
      parsed/
        pages/<uuid>.json       # One result: accepted, rejected or error
        catalog.jsonl           # UUID, path, checksum, status; no payload copies
        manifest.json
      indexed/
        manifest.json
        documents.idx          # Fixed rows: UUID, length/norm, byte spans, CRC32
        documents.bin          # Names, source identity and URL
        lexicon.offsets        # Fixed vocabulary address table
        lexicon.bin            # Sorted words and term statistics
        texts.bin              # Search texts, each stored once as UTF-8
        texts.crc              # One CRC32 per 64 KiB text chunk
        postings.bin           # All term regions, addressed by offset and length

Raw packets retain original bytes, URL provenance, response metadata and discovered
links. Requests decodes HTTP content encodings before storage; the HTML bytes are
otherwise unchanged. Reported HTML sizes describe these bodies, not wire traffic.
Stable UUIDs derive from normalized URLs. A new URL is accepted only if its
UUID is absent from its JSON task bucket, then its UUID is appended to queue.jsonl.
The first two UUID hex digits select one of 256 buckets. Each bucket maps UUIDs to
task records, omitting default/null fields. Status and retry details share the same
record. An LRU cache holds at most eight full buckets. A separate exact-membership
LRU retains only UUID integer values, avoiding task deserialization for repeated
discovery checks. storage.membership_cache_mib budgets its estimated Python object
size (32 MiB by default; 0 disables it); this is not a limit on total process RSS.
The full task collection is never loaded into RAM. Changes in a write batch are
grouped by bucket, each replaced atomically using compact JSON.

The scheduler reads each queue from its saved byte cursor and claims only a worker
batch. Completed tasks retain their bucket records for deduplication. Automatic retries
and temporarily unavailable tasks return to the queue tail; explicit retry makes
retained failed/skipped tasks runnable again. Depth can limit how far links are
followed. Buckets avoid a separate small file for every discovered URL. Updating a
task rewrites its bucket; this trades some write volume for readable storage.

traversal.prefer_url_patterns contains optional regular expressions. Matching URLs
enter priority.jsonl; other URLs enter queue.jsonl. Both lanes are FIFO, with the
preferred lane consumed first. Preference rules are project configuration, with no
site paths in the scheduler. Updating them repartitions the remaining entries while
retaining claims and every URL. The two temporary *.reorder.jsonl files exist only
during this recoverable operation; pending.json also covers interrupted replacement.
Unchanged preference rules do not scan or repartition the queues on restart.

For FishBase, the local project can prioritize English/default-language species
summaries, including numeric paths, SpeciesSummary.php and Genus_species.html
aliases. This is a URL-based hint: parsing later validates the actual HTML. Other
allowed pages remain in the ordinary queue and continue to expand the corpus.

    uv run vinf crawl policy --prefer-url '/summary/SpeciesSummary[.]php' --prefer-url '/summary/[0-9]+(?:[/?]|$)'

crawl status reads only state.json and the saved network policy. It does not walk
queue entries or task files. next_retry_at summarizes saved future network cooldowns;
the scheduler separately checks each task's retry deadline.

One writer updates at most 1024 task records per transaction. Discovery checks UUIDs
in bounded batches grouped by bucket, preserving link order when appending tasks.
pending.json holds the intended task replacements, appended UUIDs and final small
state.json. Replacing affected buckets and writing the same queue suffix can safely
be repeated after a crash.
Unchanged FIFO files and unaffected directories are not rewritten or synchronized.
Worker claims remain in state.json until their result is saved, so restarting recovers
unfinished work.

New discoveries are saved before their parent is marked downloaded. If interrupted,
the saved raw packet is recovered and its links are revisited using the same UUID
membership check. Scope/depth changes rescan saved packets; an interrupted rescan
can restart safely because discoveries are deduplicated. Document checks and catalog
generation visit task buckets sequentially, once per pass, and stream committed raw
metadata. The catalog is published every storage.catalog_interval_pages new pages
(default 1000) and on exit. A value of 0 disables periodic publication. If no new
pages were committed, only its small queue checkpoint is updated when needed.
Packet metadata is immutable after commit; publish_catalog(force=True) explicitly
rebuilds the catalog when repairing it.

    uv run vinf crawl policy --membership-cache-mib 32 --catalog-interval 1000

A parsed page contains source provenance once, extraction status/reason and an
optional SpeciesRecord. Extracted section text is stored once alongside structured
facts and original HTML character offsets. habitat/distribution/biology/iucn_text
are read-only views of those sections, not duplicated JSON fields. The parser does
not create search text or tokens. A small checksummed catalog references page files.
Unchanged per-page fingerprints allow resume; extraction errors are retried on rerun.

The indexer combines scientific/common names and extracted sections into search
segments. A dense integer docID identifies each document inside the index; its stable
16-byte UUID is stored once in documents.idx. Names, source identity and URL live in
documents.bin. The searchable text occurs once in texts.bin. Search needs only the
indexed directory.

Words receive dense termIDs in lexical order. Each vocabulary entry stores the
term's byte offset and length in postings.bin. Term regions are contiguous and
independently readable. A fixed address table allows binary search in the on-disk
lexicon without loading all words into a Python dict. Document rows are also
addressed directly by docID, without loading all metadata.

### Building with a bounded sort buffer

    uv run vinf index build --memory-mib 64 --merge-fan-in 16

The indexer externally sorts source references, selects the newest source-qualified
records and assigns docIDs in UUID order. It reads one selected parsed page at a time
and emits individual token occurrences. A buffer stores encoded records and sort
keys; when its estimated memory budget is reached, it writes a sorted temporary run.
Runs are merged with bounded input buffers and at most merge-fan-in open inputs.
Additional merge passes handle larger collections. Directory scanning is lazy too.

The final merge streams one term and document at a time into postings.bin, reusing
one temporary block-directory file across terms. The container is flushed for norm
reads and fsynced once at completion. A second external sort aggregates TF-IDF
squared weights by docID, then updates the numeric document table. It never creates
an all-document positions dictionary or a complete posting list in Python.
Temporary files are removed after the build.

memory-mib limits the estimated sort-buffer footprint, not total process RSS. The
current parsed page/search text, Python runtime, merge buffers and file handles use
additional memory. A single oversized temporary record can exceed the buffer budget.
The output is deterministic across different buffer sizes and merge fan-in values.
Build results include sort_runs and merge_passes for checking that disk spills occur.

### Reading and searching

A term region keeps document/TF references in blocks of at most 128 documents. Positional
payloads are separate byte ranges. The on-disk block directory supports next() and
advance_to(docID); cursors retain only the current block. Position readers yield one
occurrence at a time. Each cursor has an independent file handle, validates its
term's bounds and reads only the requested blocks and positional ranges. A very
frequent term does not require a full in-memory list.

Boolean AND/OR/exclusion compose sorted docID streams. Exclusion walks the numeric
docID range as needed and does not allocate a universe set. Ranking evaluates one
matching document at a time and retains only the requested top-k results in a heap.
Term metadata and cursors are proportional to query size. Normal queries never scan
or materialize the whole vocabulary or document table.

Quoted phrases without stop words align positional streams within one segment, then
read the relevant UTF-8 text span to check literal punctuation. Phrases containing
stop words intersect the remaining terms' postings, then verify the full phrase in
each candidate's text. A phrase made entirely of stop words scans document texts
sequentially. Text reads verify 64 KiB chunks with an eight-chunk cache (at most
512 KiB of payload). Full text is loaded one document at a time for these phrase checks.
The default result table reads metadata and scores; text views load complete texts
only for the retained top-k hits. There is no permanent cache of complete texts or postings.

    uv run vinf index inspect marine --limit 3 --occurrences 5
    uv run vinf index inspect marine --json

Inspection resolves only the requested term and displayed documents. It decodes a
bounded number of occurrences and verifies the consumed positional payloads. TF/DF
remain full counts even when display limits are smaller.

### Current binary format

Every file has an eight-byte magic identifying its layout. Fixed integers and
float64 values are little endian; variable integers are canonical unsigned LEB128,
limited to 64 bits. Strings have UTF-8 byte-length prefixes. Common-name prefix 0
means null; otherwise it is byte length + 1. End offsets are exclusive.

| File | Layout after magic |
| --- | --- |
| documents.idx | uint64 count, then 76-byte rows: UUID, token count, norm, metadata/text spans and CRC32 checks |
| documents.bin | Source name, source ID, scientific name, optional common name and URL strings |
| lexicon.offsets | uint64 count, then 20-byte checked address records |
| lexicon.bin | Sorted term records: word, DF, occurrence count, block count, container offset and region size |
| texts.bin | Concatenated searchable UTF-8 texts |
| texts.crc | uint32 checksums of successive 64 KiB data chunks, excluding the text magic |
| postings.bin | Contiguous term regions, each with a checked 44-byte header, positional payloads, document blocks and checked 40-byte block-directory rows |

A term header contains DF, total occurrences, block count and directory offset.
Block-directory and positional addresses on disk are relative to the term region;
the cursor adds its container offset when seeking. Regions are checked for bounds,
gaps and overlaps when the vocabulary is fully traversed.
A block-directory row contains first/last docID, document-block span, count and CRC32.
Document-block entries store docID delta, TF, positional offset/size and positional
CRC32. The first docID in each block is absolute. Positional entries store word-position
delta, byte-start delta, byte length and segment delta, resetting per document.
Those byte offsets are relative to the document's text, not the entire texts.bin file.

CRC32 detects accidental damage in accessed rows, blocks and payloads. The manifest
stores full SHA-256 checksums for the seven binary files. Normal queries validate
headers, dimensions and accessed data; deep doctor/cached-build verification reads
all files and positional payloads. Deep doctor also re-tokenizes one document at a
time to verify positions, TF/DF, lengths and norms independently of the builder.

There is one disposable current index directly in indexed/. A changed input/config
or --rebuild replaces its contents under an exclusive lock, publishing the manifest
last. Identical inputs/settings reuse a verified current index.
The short generation string in results is informational; the full fingerprint controls
reuse. If the format changes, delete the affected output and rerun its producing stage.

## Compression and retention

New raw and parsed datasets default to plain readable files. Raw bodies support
none, gzip or xz; parsed JSON supports none or gzip. Compressed raw packets also gzip
their metadata/links. Control files and index files remain uncompressed.

    uv run vinf crawl init --compression gzip
    uv run vinf parse run --compression gzip

The parser remembers an existing parsed format unless --compression overrides it.
Gzip streams are read in memory, without unpacked intermediate files. HTML is assembled
one page at a time for regex extraction; catalogs are streamed.

Existing data can be converted locally, independently of HTTP:

    uv run vinf crawl repack --compression gzip
    uv run vinf parse run --compression gzip
    uv run vinf index build

Use none to return to plain files. Repacking verifies original hashes and keeps a
backup by default; --no-keep-backup explicitly discards the previous representation
after a successful conversion. crawl export --output PATH creates a separate verified
copy. Raw conversion changes physical paths, so parsing must run before rebuilding.

Index rebuilds replace only indexed output. Raw and parsed datasets are independent
inputs and remain unchanged.

## Query language

The self-written lexer and recursive-descent parser accept a small Google-style
subset. Quote the complete argument for the shell:

    uv run vinf search query 'marine (China OR Japan) -reef'
    uv run vinf search query '"Least Concern"'
    uv run vinf search query 'marine "Least Concern"' --method tfidf --explain
    uv run vinf search syntax 'marine -reef'

| Query | Meaning |
| --- | --- |
| marine China | Both words, anywhere in the document |
| China OR Japan | Either word |
| marine -reef | Include marine, exclude reef |
| marine NOT reef | The same exclusion |
| marine (China OR Japan) | Grouped alternatives |
| "Least Concern" | Adjacent words with matching punctuation in one segment |

Uppercase AND/OR/NOT are operators; whitespace implies AND. Precedence is negation,
AND, then OR. Internal hyphens remain part of words such as deep-water. Quotes allow
escaped quotes/backslashes; operators inside quotes are literal text.
For a query starting with a minus, use search query -- '-reef'.

The optional index.stop_words list in vinf.project.json removes configured words from
the vocabulary and postings, unquoted expressions and ranking. The default list is
empty; entries use normalized lowercase token forms, for example ["in", "the", "of"].
Retained words keep their original positions, including gaps left by omitted words.
The complete search text remains unchanged. After changing the list, rebuild the index:

    uv run vinf index build --rebuild

Crawling and parsing do not need to run again. Quoted phrases still require every
word, including stop words. A phrase containing only stop words is supported through
a slower sequential text scan. An unquoted query containing only stop words returns
a validation error.

Exact matching requires consecutive words within one segment and a matching text
span, using positions or candidate text as described above. Unicode NFKC, case and
whitespace are normalized; punctuation is preserved. "least concern" does not match
"least, concern". Field/section boundaries cannot create phrases. A query must contain
a word or number. There is no stemming,
translation, fuzzy/wildcard search, field filter or numeric query syntax.

Boolean conditions select candidates. BM25 (default), TF-IDF cosine or TF-IDF sum ranks them.
Negative terms add no score; exclusion-only queries are valid with zero scores and
deterministic name/UUID ordering.

### Result views

The default output is a table. Text views print a separate section for each result:

    uv run vinf search query 'marine "Western Pacific"' --text --limit 3
    uv run vinf search query 'marine "Western Pacific"' --highlight --explain --limit 3
    uv run vinf search query 'marine' --detailed --explain --limit 1

--text shows source information and the complete text stored in the index.
--detailed shows the saved parsed page, including extracted fields, sections,
source metadata and extraction rules. --text and --detailed are mutually exclusive.
--highlight marks verified matches in yellow and implies --text. It cannot be
combined with --detailed, including in JSON output. --explain prints the saved
ranking contributions below each result.

Terminal views use cyan headings, green scores, yellow matches and muted technical
details. Each hit has a compact name/score header, a source link and readable text
sections. Detailed fields are aligned without JSON nesting; empty fields are omitted.
UUIDs, checksums, source paths and extraction ranges appear at the bottom of the
detailed record. Text views show indexed names once in the header and the remaining
text as paragraphs. Color is automatic in terminals; redirected output stays plain.

Highlighting uses UTF-8 byte ranges retained by search: term positions from the
scoring postings and exact phrase ranges verified during matching. Formatting does
not rerun the query, tokenize the text or search again with regexes. Negative clauses
are not highlighted. A phrase is highlighted only where its complete text and
punctuation matched. Match coordinates address the indexed text only. Detailed
views display saved parsed fields and sections directly, without mapping them to
indexed text.

Detailed views use all parsed inputs configured in the project. With an explicit
index outside a project, provide --parsed PATH; repeat it for multiple inputs.
Their catalog fingerprints must match the index snapshot. Payload checksums and
identities are verified, and only the returned pages are loaded. No parsing runs.
After changing parsed data, rebuild the index before requesting a detailed view.

--json works with these modes. --highlight --json includes full indexed texts and
match ranges; --detailed --json also includes parsed pages. Add --explain to retain
per-term score contributions. The result DTO contains the query, index generation,
ranking context and scores, so a saved result can be rendered without reopening the
index. Without a text/highlight mode, fulltext is null and snippet is empty.

## Ranking and multiple sources

Deduplication uses source name and source-specific identifier, falling back to raw
UUID. The newest snapshot wins, with a stable UUID tie-break. Records from different
providers remain distinct. Conflicting records with the same raw identity are rejected.
The scientific-name species_key is a candidate future join key, not a synonym resolver.

    uv run vinf index build --input data/fishbase/parsed --input data/other/parsed --output data/combined/indexed

The other source is illustrative. A real adapter must implement ExtractionProfile,
return the shared extraction DTOs and be registered in vinf_parsing/profiles.

The index contains occurrences of all terms except configured stop words. All three
rankers use the same postings:

- TF-IDF sum: idf = ln(N/DF), score = sum(TF*idf) for distinct positive query terms.
  It has no length normalization; a term present in every document contributes zero.
- TF-IDF cosine: idf = ln((N+1)/(DF+1))+1, weight = (1+ln(TF))*idf; cosine similarity.
- BM25: idf = ln(1+(N-DF+0.5)/(DF+0.5)), with k1=1.2 and b=0.75.

Stop words have no postings or vector weights. BM25 length normalization still uses
the full document token count, including stop words. The manifest's tokens field
counts stored posting occurrences; average_length describes full document lengths.

    uv run vinf search compare --queries queries.example.json

Each configured query has a question, a description of its intention and literal
conditions, and the executable query expression. Question/description are displayed
by compare and retained in its JSON; matching and ranking use only query.
queries.example.json contains five scenarios covering region/climate, conservation,
reef habitats/diet, caves/vision and freshwater migration.

Set queries in the project to omit that path. Comparison saves reports/comparison.json.
Scores have different scales; compare ordering. Agreement between top-k lists is not
a quality metric without relevance judgments. Two non-probabilistic TF-IDF variants
provide the first-submission comparison; BM25 is an additional experiment. Index construction uses external sorting, and search
uses bounded cursors and a top-k heap. Full course-scale latency still needs measurement.

## Crawling policy

Supply arbitrary seeds. The crawler has no FishBase path rules or species filter.
It extracts hrefs through regular expressions, resolves relative URLs and base href,
normalizes identities and appends new tasks within configured hosts, depth, width and
page/byte limits. Images, CSS, JavaScript execution, form submissions and API discovery
are outside the current crawler.

robots.txt is respected by default. The default minimum request interval is 10 seconds;
longer robots delays take precedence. An honest User-Agent, bounded responses, timeouts
and scope/robots checks on redirects limit load. Failed robots retrieval retains the
task for retry without fetching the page. Robots strategy can be changed explicitly
through crawl policy.

Robots strategies are explicit:

- respect (default): enforce URL rules and robots timing directives.
- strict: enforce the same rules and require an HTTP 200 robots response.
- config-first: enforce Allow/Disallow, but use configured intervals instead of
  Crawl-delay, Request-rate and Visit-time directives. Robots retrieval/cache and
  retrieval failure handling still apply.
- ignore: do not retrieve or enforce robots.txt.

Server Retry-After, HTTP 429 cooldowns and manual pauses remain enforced in
config-first mode. Existing clock histories are retained when changing strategy.

    uv run vinf crawl policy --robots-strategy config-first --delay 1
    uv run vinf crawl run

Stop an active writer before updating its policy; restart it to use the new setting.
Startup reports the configured interval. Wait messages identify whether configured
pacing or robots timing causes the delay.

Interrupted packets can be recovered without HTTP. Failures, manual skips, rate-limit
cooldowns and attempts remain saved. Reaching the retry limit does not forget the URL.
New tasks go to the tail of their configured priority lane. Retry timing and website
changes can affect the collected content.

    uv run vinf crawl tasks --status deferred
    uv run vinf crawl retry --reason rate-limit
    uv run vinf crawl skip --id UUID --reason 'Review later'
    uv run vinf crawl retry --status skipped

### Proxies and parallel requests

The default is one worker and a direct connection. Set parallel.workers (1–32) or
use crawl run --workers 4 to save a different worker count. Workers perform bounded
HTTP requests; one writer saves packets, discovers links and updates the FIFO.
Workers do not share Requests sessions. Results are saved in dispatch order within
a worker batch. On interruption, completed responses are saved and unfinished worker
claims remain available on restart.

[proxies.example.json](proxies.example.json) shows two named proxies. Each proxy URL
is read from its url_env environment variable at run time. Use HTTP or HTTPS proxy
URLs, with optional URL-encoded credentials; HTTPS destinations use CONNECT. SOCKS
proxies are not supported. The configuration, task files, metadata and progress messages
store proxy IDs, never endpoint URLs or passwords. Ambient HTTP_PROXY, HTTPS_PROXY
and NO_PROXY do not affect routing. REQUESTS_CA_BUNDLE and CURL_CA_BUNDLE can supply
a trusted CA bundle; TLS verification stays enabled.

For a new standalone dataset, set the environment variables to your proxy endpoints,
edit the example's seeds and then run:

    export VINF_PROXY_FIRST='http://proxy-one.example:3128'
    export VINF_PROXY_SECOND='http://proxy-two.example:3128'
    uv run vinf crawl init --dataset data/proxy-sample --config proxies.example.json
    uv run vinf crawl run --dataset data/proxy-sample --limit 20
    uv run vinf crawl transports --dataset data/proxy-sample

For an existing dataset, inspect crawl policy, add the proxies/parallel sections to
its complete configuration, and apply it with crawl policy --config FILE. Seeds must
stay the same. Project settings initialize new datasets; they do not replace an
existing dataset's saved policy.

Each stable proxy ID has its own robots cache, minimum interval, failure policy,
pause and cooldown. Optional delay_seconds, robots_strategy and failures overrides
inherit the global configuration when absent. The failures object is a complete
policy with its own defaults. A newly dispatched URL is assigned deterministically
across sorted proxy IDs; its assignment survives retries and restarts. Keep IDs
stable when restarting. Missing proxy credentials stop the run before HTTP; an
unconfigured assigned proxy leaves its task blocked until restored or explicitly
reassigned. Named proxies retain this binding; they do not switch automatically.

An optional public proxy pool is separate from named, credentialed proxies. Its text
file accepts public numeric HTTP(S) endpoints without credentials, one per line.
Blank/comment lines and duplicate endpoints are ignored; invalid/private endpoints
are skipped. The list is never treated as executable content.

    uv run vinf crawl proxy-check --input candidates.txt --output data/proxy-sample/proxies/working.txt
    uv run vinf crawl policy --dataset data/proxy-sample --proxy-file data/proxy-sample/proxies/working.txt --pool-size 16 --workers 8
    uv run vinf crawl run --dataset data/proxy-sample --limit 20

proxy-check uses bounded workers, short timeouts and an HTTPS request with certificate
verification. It saves working endpoints in latency order and a sibling JSON report.
Passing this check does not guarantee that a proxy can reach the crawl target.
proxy_pool.file is absolute or relative to the crawl dataset; size caps active pool
routes. Pool connect/read timeouts default to 3/15 seconds, and fallback_direct defaults
to true. Pool route IDs are stable hashes. Connection failures disable that route in
network.json; its task remains pending and can use another route. Site HTTP errors,
robots decisions and shared rate limits do not disable a route or trigger this failover.
Explicit --resume-proxy can re-enable an inspected route. Adding the pool preserves
the selected pacing scope and the shared cooldowns described below.

http.rate_limit_scope chooses where normal pacing applies:

- site (default): all proxies share the configured minimum interval for each origin
  (scheme, host and port), and the most restrictive observed timing rules from
  transports using respect/strict. A config-first transport can still be constrained
  by another transport's respected timing rules under shared site pacing.
- proxy: each transport/origin has its own interval and robots timing windows.
  Distinct ready routes can start concurrently, up to the worker count. Eight routes
  with a 10-second interval can start eight requests within that interval. Multiple
  workers using one route still share its clock. Direct fallback is a separate route.

Proxy scope increases aggregate traffic; applying a robots delay separately to each
route is different from applying it to the crawler's combined traffic. The default
remains site scope. Robots path restrictions and retrieval failure policies continue
to apply in both modes. Clock histories survive restarts and scope changes.

    uv run vinf crawl policy --rate-limit-scope proxy
    uv run vinf crawl run --rate-limit-scope proxy --workers 8 --limit 20
    uv run vinf crawl policy --rate-limit-scope site

The CLI saves the selected scope and reports it in crawl status and run startup.
HTTP 429 or Retry-After creates a shared origin cooldown in either mode,
including when it occurs while fetching robots.txt or following a redirect. Requests
already in flight can finish. In site scope, adding workers/proxies does not multiply
the allowed request frequency to one origin. In proxy scope it can increase aggregate
frequency while preserving each route's interval. Parallelism also overlaps slow
responses and independent origins.

Inspect and adjust a proxy after configuring it:

    uv run vinf crawl transports --json
    uv run vinf crawl policy --proxy first --delay 15 --robots-strategy respect
    uv run vinf crawl policy --proxy first --rate-limit-action pause
    uv run vinf crawl run --resume-proxy first
    uv run vinf crawl retry --id UUID --proxy second

--resume-proxy clears only the named proxy's pause; --resume-paused clears all
configured transport pauses and a dataset pause. Neither clears cooldown deadlines
or resets exhausted task retry budgets. Use crawl retry for a retained task that
needs a fresh retry cycle. Explicit reassignment also preserves its cooldown.

Both site and proxy clocks are saved together in crawl/network.json before each
request. Robots snapshots live under crawl/transports/<id>/robots. Packet metadata
and task listings record proxy_id. These files contain no proxy endpoint credentials.
At most workers responses are held in memory, each bounded by max_response_bytes.
Near max_total_bytes, concurrency is reduced conservatively; committed HTML cannot
exceed the byte budget.

Do not crawl the same host from multiple datasets concurrently: throttling is per dataset.

## Checks and coursework

    uv run vinf doctor crawl --deep
    uv run vinf doctor parsed --deep
    uv run vinf doctor index --deep
    uv run python -m unittest discover -s tests -v
    uv run ruff check packages src tests scripts
    uv run ruff format --check packages src tests scripts
    uv run python scripts/export_schemas.py
    uv run python scripts/package_release.py

Focused tests exercise real saved HTML, localhost HTTP policies/recovery, parallel
proxy forwarding, authenticated HTTPS CONNECT, durable per-proxy policies, compression,
independent stages, source identity, query syntax, positional/punctuation matching,
ranking, saved match highlighting and detailed compressed-record views, external
merge passes, block skipping, bounded top-k queries, binary round
trips, FIFO ordering, file deduplication and interrupted-write recovery. The extraction
bonus has 20 independently labeled real
species pages and two real negative pages; see tests/fixtures/README.md. One test
function iterates the gold cases and checks source checksums and expected fields.
Proxy integration tests use the openssl executable to create a temporary local TLS
certificate; crawling itself does not require it.

The current index producer is vinf-index/7. Only the current artifact formats are
supported. Regenerate parsed/indexed outputs after a format change.

The first-submission corpus is a deliberately stopped partial crawl, not a complete
FishBase mirror. The third-consultation extension adds Spark extraction and a
Wikipedia join; PyLucene and judged relevance evaluation remain outstanding.
See [requirements](https://vi2026.ui.sav.sk/doku.php?id=projekty.pre.rok.2026)
and [technology rules](https://vi2026.ui.sav.sk/doku.php?id=nepovolene.odporucane.a.neodporucane.ramce.a.technologie).
The first-stage extractor and URL discovery use Python re; search uses our positional
index and ranking code.

JSON schemas are provided for user-authored crawl, project, index and query settings.
Regenerate them with export_schemas.py after changing those contracts. Output DTOs
are defined in the libraries rather than duplicated as generated schemas.

The report command generates an English DokuWiki draft from verified artifacts:

    uv run vinf search compare
    uv run vinf report stats
    uv run vinf report build

report stats verifies matching crawl/parsed/index fingerprints, measures original,
stored and filesystem-allocated bytes separately, and counts cl100k_base BPE tokens
using tiktoken locally. Raw HTML (including markup) and deduplicated indexed text
are counted separately. Each whole document is encoded in memory: independent
chunk token counts would not be additive. The first use downloads the public
tiktoken encoding table, not corpus content. Set TIKTOKEN_CACHE_DIR for a persistent
cache. This local measurement is optional for search.

Generated evidence belongs in reports/ and is excluded from the source archive.
wiki/ holds the submitted page text, implementation appendix and architecture diagram. Regenerate
the report before publication so its code excerpts, metrics and results match the
current artifacts. Packaging includes sources, configuration schemas, examples and
tests. Data and credentials are excluded; publishing is a separate action.

## Spark extraction and Wikipedia linking

The optional vinf-spark library adds three independently invoked stages. Install
the locked Spark 3.5 and bundled Java 17 environment with uv; it does not require
a system-wide Java installation:

    uv sync --locked --extra spark
    uv run --extra spark vinf spark fishbase
    uv run --extra spark vinf spark plan
    uv run --extra spark vinf spark fetch
    uv run --extra spark vinf spark wiki
    uv run --extra spark vinf spark join
    uv run --extra spark vinf spark status
    uv run --extra spark vinf spark examples --limit 3
    uv run --extra spark vinf spark coverage

The default is local[2], with four output/shuffle partitions. This runs real Spark
tasks on one computer; it does not imply a multi-machine deployment or speedup.
Use --master and --partitions to change execution settings. Remote executors must
have the workspace packages installed and raw file paths mounted identically.
The separate vinf-spark CLI reuses the library and accepts --project.

fishbase reads the selected project's saved crawler catalog. Workers decompress
raw HTML in memory and call the existing FishBaseProfile REGEX extractor. The
species output deduplicates by FishBase ID, choosing the latest retrieval and then
smallest UUID, as in the original indexer. Outcomes retain UUID, selection reason
and status without another copy of every extracted record. It does not modify the
original sequential parser output or the search index.

Input selection scans the complete dated multistream title/offset index for every
FishBase scientific name and available common name. Download that index and its
publisher manifest first:

    uv run --extra spark vinf spark download 'https://dumps.wikimedia.org/enwiki/20261101/dumpstatus.json' --output data/wikipedia/index/dumpstatus.json
    uv run --extra spark vinf spark download 'https://dumps.wikimedia.org/enwiki/20261101/enwiki-20261101-pages-articles-multistream-index.txt.bz2' --output data/wikipedia/index/enwiki-20261101-pages-articles-multistream-index.txt.bz2
    uv run --extra spark vinf spark plan
    uv run --extra spark vinf spark fetch

plan writes exact title hits and non-overlapping byte ranges. Adjacent complete
bzip2 members are combined into requests up to 4 MiB without fetching gap bytes.
fetch verifies HTTP 206, Content-Range, payload length and bzip2 boundaries, then
saves original compressed bytes and source offsets/hashes. It refuses a server
response containing the entire file. Verified existing ranges are reused; failures
remain in the plan and can be retried by running fetch again. The default is two
download workers with a shared 0.25-second request-start interval, without proxies.

Without --input, wiki reads only the files listed in selection.json from
data/wikipedia/selected. Explicit --input files are also supported, for example
the offline mini-demo. The complete 27 GB parent XML is not required. Repeating
plan after extraction adds targets of redirects originating from corpus names;
fetch retrieves newly required ranges and wiki/join regenerate with --overwrite.
Common names guide retrieval, while the join still requires an explicit scientific
name. The original two arbitrary dump parts remain a historical sample and are
excluded from the active selection.

Spark distributes file paths; each worker streams plain XML, gzip or concatenated
bzip2 members in 256 KiB input chunks, retaining at most one page plus that chunk.
REGEX frames XML pages and extracts namespace, title, identifiers, revision and
wikitext. Further expressions read common Speciesbox/Taxobox fields and clean
the lead paragraph. No DOM/XML parser, XPath or MediaWiki API is used. File-level
partitioning avoids split-boundary overlap for multistream bzip2. Parallel input
tasks are bounded by the number of files; a single file is not split between
workers. Unknown templates and inherited automatic taxonomy are not inferred.

join uses the shared NFKC/casefold/whitespace scientific-name key. Only articles
with an explicit extracted species name participate. Redirects and disambiguation
pages are retained in Wikipedia output but excluded from automatic matching.
Multiple candidate articles for a key or conflicting explicit families go to
review. A missing family is marked not_available. Scientific synonyms and fuzzy
taxonomic matching are not inferred. Redirects guide retrieval of canonical pages;
they do not by themselves establish taxonomic identity. Both sources'
conservation statuses remain separate; differences do not silently overwrite data.

Outputs use separate roots:

| Directory | Produced data |
|---|---|
| data/spark/fishbase/species | Deduplicated FishBase records with source provenance |
| data/spark/fishbase/outcomes | Raw UUIDs, selection outcomes and errors |
| data/wikipedia/index | Complete title/offset index and publisher manifest |
| data/wikipedia/selection.json | Matched titles and exact compressed byte ranges |
| data/wikipedia/selected | Original selected compressed ranges and metadata |
| data/wikipedia/parsed/pages | Extracted main-namespace Wikipedia records |
| data/linked/records | Accepted records with both sources and matching evidence |
| data/linked/review | Ambiguous or conflicting candidates |
| data/linked/unmatched | Unmatched source IDs/names and reasons |

Generated records use partitioned JSON.gz, with standard Spark output markers.
Pass --compression none to any extraction/join command for readable plain JSON.
Every stage publishes a manifest; join also saves its physical execution plan.
Existing generated output requires --overwrite. Commands never start the next
stage automatically. Large intermediate RDDs use DISK_ONLY; only aggregates and
eight example links are collected on the driver. status and examples read saved
manifests without launching Spark or scanning the datasets.

The independent full-corpus audit compares Spark species to the existing sequential
parser and checks XML page IDs using a separate line-based reader. It also verifies
that accepted/unmatched FishBase IDs partition the corpus and that accepted links
have exact keys and no redirects, disambiguations or family conflicts:

    uv run --extra spark python scripts/validate_spark.py
    uv run --extra spark vinf spark report

The report requires a completed audit and writes wiki/konzultacia3.dokuwiki.txt.
Unmatched records are not evidence that Wikipedia lacks an article: the input is
partial and the matching rule is intentionally conservative. Wikipedia text retains
its original CC BY-SA attribution and revision provenance.

coverage distinguishes missing exact index titles, unresolved redirect targets,
different taxonomic names, unsupported taxoboxes and common-title-only candidates.
It reads only lightweight identities for relevant titles into memory and records
the join manifest hash, so stale diagnostics cannot be published as current results.

## PyLucene indexing and retrieval

The fourth-consultation backend is a separate vinf-lucene workspace library and
Typer/Rich CLI. It reads the completed data/spark/fishbase and data/linked stages;
it neither starts Spark nor crawls. All 9,724 FishBase source records are retained.
Accepted links enrich 5,157 records with Wikipedia; unmatched fish remain searchable.
The original index and its three rankers remain available through vinf search.

### Native dependency

    uv sync --locked
    uv run python scripts/install_pylucene.py

PyLucene is a native JCC/JNI binding, not a pure Python dependency. The installer
builds JCC 3.14 and PyLucene 9.12.0 using checksum-verified Apache sources and Maven
modules: core, analysis-common, queryparser, queries, sandbox and facet. It uses a
full JDK 17 and a C++ compiler, with two compilation workers. On macOS arm64 it can
download a checksum-verified private Temurin JDK; on other systems set JAVA_HOME to
a full JDK 17 first. Xcode command-line tools are required on macOS. The measured
and tested platform is macOS arm64 / Python 3.12; other builds are not claimed tested.
No system Java installation is changed.

Native dependencies and generated build files live under .cache/pylucene-build,
which is excluded from source packages and Git. Its private JDK is used at runtime
and must remain available. Wheels are local to the Python ABI, machine and JDK
location. A fresh checkout builds its own bindings. Normal uv run preserves the
native packages. An exact uv sync can remove them; rerun the installer to reinstall
its cached wheels. This installation cost is separate from index/query timings.

### Independent commands and project paths

    uv run vinf lucene build
    uv run vinf lucene status
    uv run vinf lucene schema
    uv run vinf lucene query 'marine (Pacific OR Atlantic) -freshwater'
    uv run vinf lucene query '"coral reefs"' --explain
    uv run vinf lucene query 'scientific_name:Aaptosyaz~1'
    uv run vinf lucene query 'family:Cyprinidae AND environment:freshwater'
    uv run vinf lucene query 'wiki_authority:Hamilton'
    uv run vinf lucene compare

The same commands are available as vinf-lucene. The current project supplies paths;
there is no need to repeat input/output arguments. An optional lucene object in
vinf.project.json overrides fishbase, linked and indexed; defaults are
data/spark/fishbase, data/linked and data/lucene/indexed. --project, --fishbase,
--linked, --output and --index remain explicit overrides where relevant.

build reuses unchanged source fingerprints. Use --rebuild after replacing Spark
snapshots; it regenerates only the Lucene output. Search reads only the Lucene index.
status and schema do not start Java. --json provides typed machine-readable results;
--explain shows the actual Lucene parsed query and per-hit BM25 explanation.

### Fields, analysis and query semantics

lucene schema lists every field and its purpose. Scientific/common names, FishBase
sections, Wikipedia lead, categories and authority have separate TextFields with
positions. Each text is stored once in its own field; there is no duplicated
aggregate fulltext field. Exact StringFields cover source identity, normalized
scientific name, family/order, environments, conservation codes and link presence.
Stored-only fields retain URLs, UUID, Wikipedia revision, status system and hash.
The two sources' conservation values remain distinct. No numeric length filter is
implemented; numeric attributes remain available in the extracted source records.

StandardAnalyzer lowercases/tokenizes text, keeps stop words and performs no stemming.
KeywordAnalyzer handles exact fields: values are case-sensitive, e.g.
family:Cyprinidae, iucn:EN and environment:freshwater. scientific_exact uses the shared
normalized key, e.g. scientific_exact:"aaptosyax grypus".

Lucene's classic MultiFieldQueryParser searches all text fields by default, with
implicit AND. Use parentheses to make mixed AND/OR intent explicit. Quoted phrases
use token positions within a field: punctuation is analyzed, not matched literally.
For example, "Aaptosyax, grypus" can match Aaptosyax grypus. The original vinf search
backend retains its punctuation-sensitive literal phrase behavior. Fuzzy terms use
Lucene edit distance, e.g. scientific_name:Aaptosyaz~1; this is spelling tolerance,
not taxonomic synonym resolution. Exact field filters use the StringFields above.

--scope fishbase selects scientific_name, common_name and fishbase_text as default
fields; --scope wikipedia selects wiki_text, wiki_categories and wiki_authority.
Explicit field-qualified expressions still select their named field. Ranking uses
BM25 with k1=1.2 and b=0.75. Norms and statistics are per field, so absolute scores
are not comparable to the original concatenated-text index.

### Storage and verification

    data/lucene/indexed/
      .lock
      manifest.json       # Source-part hashes, native versions, fields, counts, timing
      index/              # Native Lucene FSDirectory; stored fields, postings, norms, segments
    reports/consultation4/
      comparison.json     # Same queries and source IDs; three observed rankings
      examples/           # Actual Boolean/phrase/fuzzy/field queries with explanations
      audit.json          # Every stored record compared with source + native CheckIndex

Inputs remain read-only. The builder keeps compact identity fingerprints and the
current record in Python; Lucene's IndexWriter has a 64 MiB buffer. The JVM is capped
at 1 GiB heap. These are component limits, not a claim about total process RSS.
Indexes use Lucene's own storage/compression and are not wrapped in gzip.

    uv run python scripts/validate_lucene.py
    uv run python -m unittest discover -s tests -p test_lucene.py -v

Two native integration tests use four real, provenance-preserving records. They
exercise Boolean, positional phrase, fuzzy and keyword search, explanations, reuse,
read-only inputs and rejection of foreign output directories. Without the native
binding these two tests are explicitly skipped; the measured submission installed
it and ran them. The small demo is generated by scripts/package_lucene_demo.py.

The comparison uses the existing five described queries, identical FishBase IDs,
one unmeasured warm-up and five measured repetitions per engine/query. It reports
old BM25, Lucene FishBase-only and enriched Lucene separately, with match counts,
top results, overlap and median local latency. StandardAnalyzer boundaries and
per-field scoring differ from the original engine; Wikipedia also changes searched
text. These observations do not establish relevance quality without human judgments.

Current code: https://github.com/ic-it/fiit-vinf
