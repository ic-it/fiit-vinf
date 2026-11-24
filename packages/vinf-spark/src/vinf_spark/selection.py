"""Locate corpus names in the complete multistream index; fetch original blocks."""

import bz2
import gzip
import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

import requests
from pydantic import AwareDatetime
from vinf_core.files import atomic_model, file_hash, read_model
from vinf_core.models import Model, VinfError
from vinf_core.records import species_key

from vinf_spark.downloads import USER_AGENT
from vinf_spark.models import FishBaseSpecies


class TitleHit(Model):
    page_id: int
    title: str
    offset: int
    kind: str


class Block(Model):
    start: int
    end: int

    @property
    def size(self):
        return self.end - self.start + 1

    @property
    def filename(self):
        return f"block-{self.start:012d}-{self.end:012d}.xml.bz2"


class Selection(Model):
    dump_url: str
    dump_bytes: int
    index_path: str
    index_sha256: str
    fishbase_manifest_sha256: str
    created_at: AwareDatetime
    scientific_names: int
    common_names: int
    scientific_titles_found: int
    common_titles_found: int
    extra_titles: tuple[str, ...]
    missing_scientific_names: tuple[str, ...]
    hits: tuple[TitleHit, ...]
    blocks: tuple[Block, ...]
    selected_bytes: int


class RangeDownload(Model):
    url: str
    start: int
    end: int
    dump_bytes: int
    bytes: int
    sha256: str
    fetched_at: AwareDatetime


class FetchResult(Model):
    blocks: int
    downloaded: int
    reused: int
    stored_bytes: int
    failures: tuple[str, ...]


def json_rows(directory: Path):
    for path in sorted(directory.glob("part-*.json*")):
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as stream:
            for line in stream:
                yield json.loads(line)


def title_key(title: str) -> str:
    return species_key(title.replace("_", " "))


def coalesce(blocks):
    """Combine adjacent complete members without downloading gap bytes."""
    merged = []
    for block in blocks:
        if (
            merged
            and merged[-1].end + 1 == block.start
            and block.end - merged[-1].start < 4 * 1024 * 1024
        ):
            merged[-1] = Block(start=merged[-1].start, end=block.end)
        else:
            merged.append(block)
    return tuple(merged)


def make_selection(
    index: Path,
    fishbase: Path,
    dump_url: str,
    dump_bytes: int,
    output: Path,
    *,
    parsed: Path | None = None,
):
    scientific, common = set(), set()
    for row in json_rows(fishbase / "species"):
        fish = FishBaseSpecies.model_validate(row).record
        scientific.add(fish.species_key)
        if fish.common_name:
            common.add(title_key(fish.common_name))
    wanted = scientific | common
    extra = set()
    if parsed and (parsed / "pages").exists():
        # Follow only redirects originating from corpus names, never all redirects.
        for _ in range(8):
            added = set()
            for row in json_rows(parsed / "pages"):
                if row.get("redirect") and title_key(row["title"]) in wanted:
                    added.add(title_key(row["redirect"].split("#")[0]))
            added -= wanted
            if not added:
                break
            extra |= added
            wanted |= added
    blocks, hits, current = [], [], []
    previous = None
    found_scientific, found_common = set(), set()
    with bz2.open(index, "rt", encoding="utf-8") as stream:
        for line in stream:
            offset_text, page_id, title = line.rstrip("\n").split(":", 2)
            offset = int(offset_text)
            if previous is not None and offset != previous:
                if offset < previous or offset >= dump_bytes:
                    raise VinfError(
                        "The global index has invalid or unordered offsets."
                    )
                if current:
                    blocks.append(Block(start=previous, end=offset - 1))
                    hits.extend(current)
                current = []
            previous = offset
            key = title_key(title)
            if key not in wanted:
                continue
            kind = (
                "scientific"
                if key in scientific
                else "common"
                if key in common
                else "redirect_target"
            )
            current.append(
                TitleHit(page_id=int(page_id), title=title, offset=offset, kind=kind)
            )
            if key in scientific:
                found_scientific.add(key)
            if key in common:
                found_common.add(key)
        if current:
            blocks.append(Block(start=previous, end=dump_bytes - 1))
            hits.extend(current)
    result = Selection(
        dump_url=dump_url,
        dump_bytes=dump_bytes,
        index_path=str(index.resolve()),
        index_sha256=file_hash(index),
        fishbase_manifest_sha256=file_hash(fishbase / "manifest.json"),
        created_at=datetime.now(UTC),
        scientific_names=len(scientific),
        common_names=len(common),
        scientific_titles_found=len(found_scientific),
        common_titles_found=len(found_common),
        extra_titles=tuple(sorted(extra)),
        missing_scientific_names=tuple(sorted(scientific - found_scientific)),
        hits=tuple(hits),
        blocks=coalesce(blocks),
        selected_bytes=sum(block.size for block in blocks),
    )
    atomic_model(output, result)
    return result


def fetch_selection(
    plan: Selection,
    output: Path,
    *,
    workers: int = 2,
    interval: float = 0.25,
    progress=None,
):
    output.mkdir(parents=True, exist_ok=True)
    gate, next_request = threading.Lock(), [0.0]

    def fetch(block):
        path = output / block.filename
        metadata = path.with_name(path.name + ".metadata.json")
        if path.exists() and metadata.exists():
            saved = read_model(metadata, RangeDownload)
            if (
                (saved.url, saved.start, saved.end, saved.dump_bytes)
                == (
                    plan.dump_url,
                    block.start,
                    block.end,
                    plan.dump_bytes,
                )
                and saved.bytes == block.size
                and path.stat().st_size == block.size
                and file_hash(path) == saved.sha256
            ):
                return True
        temporary = path.with_name(path.name + ".part")
        for attempt in range(3):
            try:
                while True:
                    with gate:
                        now = time.monotonic()
                        delay = next_request[0] - now
                        if delay <= 0:
                            next_request[0] = now + interval
                            break
                    # A different response can extend the shared cooldown while
                    # waiting. Never hold the gate during a Retry-After sleep.
                    time.sleep(min(delay, 1))
                with requests.get(
                    plan.dump_url,
                    headers={
                        "User-Agent": USER_AGENT,
                        "Accept-Encoding": "identity",
                        "Range": f"bytes={block.start}-{block.end}",
                    },
                    stream=True,
                    timeout=(10, 60),
                ) as response:
                    if response.status_code == 429:
                        retry = response.headers.get("Retry-After", "60")
                        wait = float(retry) if retry.isdigit() else 60
                        if wait > 120:
                            raise VinfError(
                                f"Server requested a {wait}s cooldown; rerun fetch later."
                            )
                        with gate:
                            next_request[0] = max(
                                next_request[0], time.monotonic() + wait
                            )
                        continue
                    response.raise_for_status()
                    expected = f"bytes {block.start}-{block.end}/{plan.dump_bytes}"
                    if (
                        response.status_code != 206
                        or response.headers.get("Content-Range") != expected
                    ):
                        raise VinfError(
                            "Server did not honor the exact byte range; full download aborted."
                        )
                    digest, size = hashlib.sha256(), 0
                    with temporary.open("wb") as stream:
                        for chunk in response.iter_content(256 * 1024):
                            size += len(chunk)
                            if size > block.size:
                                raise VinfError("HTTP range returned too many bytes.")
                            stream.write(chunk)
                            digest.update(chunk)
                        stream.flush()
                        os.fsync(stream.fileno())
                    if size != block.size:
                        raise requests.ConnectionError("Truncated byte-range response.")
                    with temporary.open("rb") as stream:
                        if stream.read(3) != b"BZh":
                            raise VinfError(
                                "Selected offset is not a bzip2 member boundary."
                            )
                    os.replace(temporary, path)
                    atomic_model(
                        metadata,
                        RangeDownload(
                            url=plan.dump_url,
                            start=block.start,
                            end=block.end,
                            dump_bytes=plan.dump_bytes,
                            bytes=size,
                            sha256=digest.hexdigest(),
                            fetched_at=datetime.now(UTC),
                        ),
                    )
                    return False
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(2**attempt)
            finally:
                temporary.unlink(missing_ok=True)
        raise VinfError(
            "Server rate limit prevented this block download; rerun fetch later."
        )

    downloaded = reused = 0
    failures = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch, block): block for block in plan.blocks}
        for number, future in enumerate(as_completed(futures), 1):
            try:
                cached = future.result()
                reused += int(cached)
                downloaded += int(not cached)
            except (requests.RequestException, VinfError, OSError) as exc:
                failures.append(f"{futures[future].filename}: {exc}")
            if progress and (number % 25 == 0 or number == len(futures)):
                progress(number, len(futures), len(failures))
    return FetchResult(
        blocks=len(plan.blocks),
        downloaded=downloaded,
        reused=reused,
        stored_bytes=sum(
            block.size for block in plan.blocks if (output / block.filename).exists()
        ),
        failures=tuple(failures),
    )
