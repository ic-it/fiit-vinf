"""Bounded external merge sort for typed, privately encoded temporary records."""

import heapq
import os
import sys
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from vinf_core.binary import read_exact, read_uvarint, uvarint


def _weight(value: Any) -> int:
    size = sys.getsizeof(value)
    return size + sum(_weight(v) for v in value) if isinstance(value, tuple) else size


class ExternalSorter[T]:
    """Only a bounded buffer and one current item per merge input stay in Python."""

    def __init__(
        self,
        directory: Path,
        *,
        key: Callable[[T], tuple],
        encode: Callable[[T], bytes],
        decode: Callable[[bytes], T],
        memory_bytes: int,
        fan_in: int = 16,
    ):
        directory.mkdir(parents=True, exist_ok=True)
        self.directory, self.key = directory, key
        self.encode, self.decode = encode, decode
        self.memory_bytes, self.fan_in = memory_bytes, fan_in
        self.buffer: list[tuple[tuple, bytes]] = []
        self.buffer_bytes = self.max_buffer_bytes = 0
        self.runs = self.passes = self.serial = 0

    def add(self, value: T) -> None:
        item = (self.key(value), self.encode(value))
        weight = _weight(item) + sys.getsizeof(item[1]) + 8
        if self.buffer and self.buffer_bytes + weight > self.memory_bytes:
            self._flush()
        self.buffer.append(item)
        self.buffer_bytes += weight
        self.max_buffer_bytes = max(self.max_buffer_bytes, self.buffer_bytes)
        if self.buffer_bytes >= self.memory_bytes:
            self._flush()

    def _path(self) -> Path:
        result = self.directory / f"{self.serial:08x}.run"
        self.serial += 1
        return result

    def _flush(self) -> None:
        if not self.buffer:
            return
        self.buffer.sort(key=lambda item: item[0])
        with self._path().open("wb") as stream:
            for _, body in self.buffer:
                stream.write(uvarint(len(body)))
                stream.write(body)
        self.runs += 1
        self.buffer.clear()
        self.buffer_bytes = 0

    def _read(self, stream) -> Iterator[T]:
        while stream.peek(1):
            yield self.decode(read_exact(stream, read_uvarint(stream)))

    def _merge(self, paths: list[Path], output: Path) -> None:
        with ExitStack() as stack, output.open("wb") as stream:
            readers = [self._read(stack.enter_context(p.open("rb"))) for p in paths]
            for value in heapq.merge(*readers, key=self.key):
                body = self.encode(value)
                stream.write(uvarint(len(body)))
                stream.write(body)
        for path in paths:
            path.unlink()

    def values(self) -> Iterator[T]:
        self._flush()
        # Discover runs in bounded groups instead of retaining all paths in RAM.
        while True:
            count = 0
            group = []
            # Newly written runs go into the next directory, outside this scan.
            following = self.directory.with_name(self.directory.name + "-next")
            following.mkdir()
            with os.scandir(self.directory) as entries:
                for entry in entries:
                    group.append(Path(entry.path))
                    count += 1
                    if len(group) == self.fan_in:
                        self._merge(group, following / f"{count:08x}.run")
                        group = []
            if count <= 1:
                following.rmdir()
                if group:
                    with group[0].open("rb") as stream:
                        yield from self._read(stream)
                    group[0].unlink()
                self.directory.rmdir()
                return
            if group:
                self._merge(group, following / f"{count:08x}.run")
            self.directory.rmdir()
            following.rename(self.directory)
            self.passes += 1
