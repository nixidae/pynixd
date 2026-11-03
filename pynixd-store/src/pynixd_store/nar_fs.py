"""NAR to and from the file system, streamed, as Nix's `archive.cc` does.

`dump_path` is `SourceAccessor::dumpPath` (`archive.cc:44`): entries sorted
by their bytes, `executable` when the owner may execute a regular file, and
nothing else of the metadata. `restore_path` is `parseDump` into a
`RestoreSink` (`archive.cc:177`, `fs-sink.cc:146-328`): it checks the grammar
as Nix does, creates every file with `O_EXCL`, and gives an executable file
all three execute bits. Neither holds a whole NAR or a whole file in memory.

Both are synchronous: the work is file system calls. An async caller runs
them on a worker thread. `nar.py` holds the in-memory tree, for test NARs.
"""

from __future__ import annotations

import os
import stat
import struct
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path

NAR_MAGIC = b"nix-archive-1"
MAX_DEPTH = 64
"""`narMaxDepth`, `archive.cc:40`: a deeper tree is an error on both sides."""
_MAX_TAG = 32
_MAX_NAME = 255
_MAX_TARGET = 4095
_CHUNK = 64 * 1024
"""Bytes per read of a file and per piece of the stream."""


class BadArchive(ValueError):
    """`badArchive`, `archive.cc:144`: the stream is not a NAR Nix would accept."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"bad archive: {reason}")


def _string(data: bytes) -> bytes:
    """One NAR string: its length, the bytes, and zero padding to 8."""
    return struct.pack("<Q", len(data)) + data + b"\0" * (-len(data) % 8)


def dump_path(path: Path) -> Generator[bytes]:
    """The NAR of *path*, in pieces of about 64 KiB."""
    out = bytearray(_string(NAR_MAGIC))

    def flush() -> Generator[bytes]:
        if out:
            yield bytes(out)
            out.clear()

    def node(at: bytes, depth: int) -> Generator[bytes]:
        if depth >= MAX_DEPTH:
            raise ValueError(f"path '{os.fsdecode(at)}' exceeds maximum NAR directory depth of {MAX_DEPTH}")
        st = os.lstat(at)
        out.extend(_string(b"(") + _string(b"type"))
        if stat.S_ISREG(st.st_mode):
            out.extend(_string(b"regular"))
            if st.st_mode & stat.S_IXUSR:
                out.extend(_string(b"executable") + _string(b""))
            yield from contents(at)
        elif stat.S_ISDIR(st.st_mode):
            out.extend(_string(b"directory"))
            for name in sorted(os.listdir(at)):
                out.extend(_string(b"entry") + _string(b"(") + _string(b"name") + _string(name) + _string(b"node"))
                yield from node(os.path.join(at, name), depth + 1)
                out.extend(_string(b")"))
        elif stat.S_ISLNK(st.st_mode):
            out.extend(_string(b"symlink") + _string(b"target") + _string(os.readlink(at)))
        else:
            raise ValueError(f"file '{os.fsdecode(at)}' has an unsupported type")
        out.extend(_string(b")"))
        if len(out) >= _CHUNK:
            yield from flush()

    def contents(at: bytes) -> Generator[bytes]:
        with open(at, "rb") as file:
            size = os.fstat(file.fileno()).st_size
            out.extend(_string(b"contents") + struct.pack("<Q", size))
            yield from flush()
            left = size
            while left:
                chunk = file.read(min(_CHUNK, left))
                if not chunk:
                    raise ValueError(f"file '{os.fsdecode(at)}' shrank while it was read")
                left -= len(chunk)
                yield chunk
        out.extend(b"\0" * (-size % 8))

    yield from node(os.fsencode(path), 0)
    yield from flush()


class _Source:
    """Reads a NAR through *read*, which answers at most n bytes and `b""` at the end."""

    def __init__(self, read: Callable[[int], bytes]) -> None:
        self._read = read

    def exactly(self, n: int) -> bytes:
        parts = []
        while n:
            chunk = self._read(n)
            if not chunk:
                raise BadArchive("unexpected end of the archive")
            parts.append(chunk)
            n -= len(chunk)
        return b"".join(parts)

    def number(self) -> int:
        value: int = struct.unpack("<Q", self.exactly(8))[0]
        return value

    def padding(self, size: int) -> None:
        pad = -size % 8
        if pad and self.exactly(pad) != b"\0" * pad:
            raise BadArchive("non-zero padding")

    def string(self, limit: int) -> bytes:
        size = self.number()
        if size > limit:
            raise BadArchive(f"string of {size} bytes is longer than {limit}")
        data = self.exactly(size)
        self.padding(size)
        return data

    def tag(self, expected: bytes) -> None:
        got = self.string(_MAX_TAG)
        if got != expected:
            raise BadArchive(f"expected tag '{expected.decode()}', got '{got.decode(errors='replace')}'")


def restore_path(dest: Path, read: Callable[[int], bytes]) -> None:
    """Unpack the NAR that *read* gives into *dest*, which must not exist."""
    source = _Source(read)
    try:
        magic = source.string(len(NAR_MAGIC))
    except BadArchive:
        magic = b""
    if magic != NAR_MAGIC:
        raise BadArchive("input doesn't look like a Nix archive")
    _restore(source, os.fsencode(dest), 0)


def _restore(source: _Source, at: bytes, depth: int) -> None:
    if depth >= MAX_DEPTH:
        raise BadArchive(f"NAR directory nesting exceeds maximum depth of {MAX_DEPTH}")
    source.tag(b"(")
    source.tag(b"type")
    kind = source.string(_MAX_TAG)
    if kind == b"regular":
        _restore_file(source, at)
    elif kind == b"directory":
        os.mkdir(at, 0o777)
        previous = b""
        while True:
            tag = source.string(_MAX_TAG)
            if tag == b")":
                return
            if tag != b"entry":
                raise BadArchive(f"expected tag 'entry' or ')', got '{tag.decode(errors='replace')}'")
            source.tag(b"(")
            source.tag(b"name")
            name = source.string(_MAX_NAME)
            if name in (b"", b".", b"..") or b"/" in name or b"\0" in name:
                raise BadArchive(f"NAR contains invalid file name '{name.decode(errors='replace')}'")
            if name <= previous:
                raise BadArchive("NAR directory is not sorted")
            previous = name
            source.tag(b"node")
            _restore(source, os.path.join(at, name), depth + 1)
            source.tag(b")")
    elif kind == b"symlink":
        source.tag(b"target")
        target = source.string(_MAX_TARGET)
        if not target or b"\0" in target:
            raise BadArchive("NAR contains invalid symlink target")
        os.symlink(target, at)
        source.tag(b")")
    else:
        raise BadArchive(f"unknown file type '{kind.decode(errors='replace')}'")


def _restore_file(source: _Source, at: bytes) -> None:
    # O_EXCL with O_CREAT follows no symlink in the last component (`fs-sink.cc:268`).
    fd = os.open(at, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_CLOEXEC | os.O_NOFOLLOW, 0o666)
    with open(fd, "wb", closefd=True) as file:
        tag = source.string(_MAX_TAG)
        if tag == b"executable":
            if source.string(0) != b"":
                raise BadArchive("executable marker has non-empty value")
            mode = os.fstat(file.fileno()).st_mode
            os.fchmod(file.fileno(), mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            tag = source.string(_MAX_TAG)
        if tag != b"contents":
            raise BadArchive(f"expected tag 'contents', got '{tag.decode(errors='replace')}'")
        size = source.number()
        left = size
        while left:
            chunk = source.exactly(min(_CHUNK, left))
            file.write(chunk)
            left -= len(chunk)
        source.padding(size)
    source.tag(b")")
