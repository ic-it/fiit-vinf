"""Fetch an explicitly selected public dump with streaming checksums."""

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import requests
from vinf_core.files import atomic_model, read_model
from vinf_core.models import VinfError

from vinf_spark.models import Download

USER_AGENT = (
    "FIITVinfCrawler/0.10 (educational project; +https://vi2026.ui.sav.sk/doku.php)"
)


def download_dump(url: str, output: Path, expected_sha1: str | None = None) -> Download:
    if urlsplit(url).scheme != "https":
        raise VinfError("Use an HTTPS dump URL.")
    output = output.resolve()
    metadata = output.with_name(output.name + ".metadata.json")
    if output.exists():
        if not metadata.exists():
            raise VinfError(f"Existing input has no download metadata: {output}")
        previous = read_model(metadata, Download)
        with output.open("rb") as stream:
            checksum = hashlib.file_digest(stream, "sha256").hexdigest()
        if previous.url != url or previous.sha256 != checksum:
            raise VinfError("Existing dump does not match the requested download.")
        if expected_sha1 and previous.sha1 != expected_sha1:
            raise VinfError("Existing dump does not match the publisher SHA-1.")
        return previous
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".part")
    sha256, sha1 = hashlib.sha256(), hashlib.sha1()
    size = 0
    try:
        with requests.get(
            url, headers={"User-Agent": USER_AGENT}, timeout=(10, 60), stream=True
        ) as response:
            response.raise_for_status()
            with temporary.open("wb") as stream:
                for chunk in response.iter_content(1024 * 1024):
                    stream.write(chunk)
                    sha256.update(chunk)
                    sha1.update(chunk)
                    size += len(chunk)
                stream.flush()
                os.fsync(stream.fileno())
        if expected_sha1 and sha1.hexdigest() != expected_sha1:
            raise VinfError("Downloaded dump does not match the publisher SHA-1.")
        value = Download(
            url=url,
            path=str(output),
            fetched_at=datetime.now(UTC),
            bytes=size,
            sha256=sha256.hexdigest(),
            sha1=sha1.hexdigest(),
            expected_sha1=expected_sha1,
        )
        os.replace(temporary, output)
        atomic_model(metadata, value)
        return value
    except requests.RequestException as exc:
        raise VinfError(f"Cannot download the selected dump: {exc}") from exc
    finally:
        temporary.unlink(missing_ok=True)
