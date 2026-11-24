"""Small, bounded primitives for the versioned index format (little endian)."""

import struct
from typing import BinaryIO

from vinf_core.models import VinfError

MAX_UINT = (1 << 64) - 1


def uvarint(value: int) -> bytes:
    if not 0 <= value <= MAX_UINT:
        raise VinfError("Binary integer is outside the unsigned 64-bit range.")
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def read_exact(stream: BinaryIO, size: int) -> bytes:
    if size < 0:
        raise VinfError("Negative binary read length.")
    # Chunk allocation even when an untrusted length prefix is very large.
    result = bytearray()
    while len(result) < size:
        block = stream.read(min(size - len(result), 65536))
        if not block:
            raise VinfError("Truncated binary index. Rebuild the index.")
        result.extend(block)
    return bytes(result)


def read_uvarint(stream: BinaryIO) -> int:
    value = 0
    for shift in range(0, 70, 7):
        byte = read_exact(stream, 1)[0]
        if shift == 63 and byte > 1:
            raise VinfError("Binary integer overflow.")
        value |= (byte & 127) << shift
        if byte < 128:
            if shift and byte == 0:
                raise VinfError("Non-canonical binary integer.")
            return value
    raise VinfError("Invalid binary integer.")


def string(value: str) -> bytes:
    body = value.encode("utf-8")
    return uvarint(len(body)) + body


def read_string(stream: BinaryIO) -> str:
    try:
        return read_exact(stream, read_uvarint(stream)).decode("utf-8")
    except UnicodeError as exc:
        raise VinfError("Invalid UTF-8 in binary index.") from exc


def float64(value: float) -> bytes:
    return struct.pack("<d", value)


def read_float64(stream: BinaryIO) -> float:
    return struct.unpack("<d", read_exact(stream, 8))[0]


def header(stream: BinaryIO, magic: bytes) -> None:
    if read_exact(stream, len(magic)) != magic:
        raise VinfError("Unsupported binary index header/version. Rebuild the index.")


def end_of_file(stream: BinaryIO) -> None:
    if stream.read(1):
        raise VinfError("Unexpected trailing bytes in binary index.")


def checked(body: bytes) -> bytes:
    """CRC32 detects accidental corruption; manifest SHA-256 covers whole files."""
    from zlib import crc32

    return body + struct.pack("<I", crc32(body))


def read_checked(stream: BinaryIO, size: int) -> bytes:
    from zlib import crc32

    body = read_exact(stream, size)
    if struct.unpack("<I", read_exact(stream, 4))[0] != crc32(body):
        raise VinfError("Binary block checksum mismatch. Rebuild the index.")
    return body
