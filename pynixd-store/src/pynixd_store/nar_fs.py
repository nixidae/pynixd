"""NAR to and from the file system, streamed, as Nix's `archive.cc` does.

`dump_path` is `SourceAccessor::dumpPath` (`archive.cc:44`): entries sorted
by their bytes, `executable` when the owner may execute a regular file, and
nothing else of the metadata. `restore_path` is `parseDump` into a
`RestoreSink` (`archive.cc:177`, `fs-sink.cc:146-328`): it checks the grammar
as Nix does, creates every file with `O_EXCL`, and gives an executable file
all three execute bits. Neither holds a whole NAR or a whole file in memory.

Both are synchronous: the work is file system calls. An async caller runs
them on a worker thread. `nar.py` holds the in-memory tree, for test NARs.

`use-case-hack` (`archive.cc:23`) is one setting of the process, as in Nix,
because the Nix in this process applies it through its own global: the two
must agree on each name. On a case-insensitive file system, a restore gives
a name that equals an earlier one under `strcasecmp` the suffix
`~nix~case~hack~N`, and a dump removes it again.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import os
import stat
import struct
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable
    from pathlib import Path

NAR_MAGIC = b"nix-archive-1"
MAX_DEPTH = 64
"""`narMaxDepth`, `archive.cc:40`: a deeper tree is an error on both sides."""
_MAX_TAG = 32
_MAX_NAME = 255
_MAX_TARGET = 4095
_CHUNK = 64 * 1024
"""Bytes per read of a file and per piece of the stream."""


CASE_HACK_SUFFIX = b"~nix~case~hack~"
"""`caseHackSuffix`, `archive.hh:83`."""


@dataclass(slots=True)
class ArchiveSettings:
    """`ArchiveSettings` (`archive.cc:17`). The engine sets it from the store's Nix settings."""

    use_case_hack: bool = sys.platform == "darwin"


archive_settings = ArchiveSettings()


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
            for name, on_disk in sorted(_unhacked(at).items()):
                out.extend(_string(b"entry") + _string(b"(") + _string(b"name") + _string(name) + _string(b"node"))
                yield from node(os.path.join(at, on_disk), depth + 1)
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


def _unhacked(at: bytes) -> dict[bytes, bytes]:
    """Each entry of the directory *at* by its NAR name, to its name on disk (`archive.cc:86-101`)."""
    names = os.listdir(at)
    if not archive_settings.use_case_hack:
        return {name: name for name in names}
    unhacked: dict[bytes, bytes] = {}
    for on_disk in names:
        name = on_disk.split(CASE_HACK_SUFFIX, 1)[0]
        if name in unhacked:
            raise ValueError(
                f"file name collision between '{os.fsdecode(os.path.join(at, unhacked[name]))}' "
                f"and '{os.fsdecode(os.path.join(at, on_disk))}'"
            )
        unhacked[name] = on_disk
    return unhacked


def nar_sha256(path: Path) -> hashlib._Hash:
    """The SHA-256 of *path*'s NAR, as `hashPath(..., NixArchive, SHA256)` of Nix computes it.

    `optimiseStore` reads it in Nix32 (`.links` entry names) and `verifyStore`
    in Base16 (the `narHash` column), so the caller takes the digest in the
    form it needs.
    """
    digest = hashlib.sha256()
    for piece in dump_path(path):
        digest.update(piece)
    return digest


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
        # `CaseInsensitiveCompare` (`archive.cc:170`): `strcasecmp`, which folds ASCII alone, as `bytes.lower`.
        # The key is the folded name, the value the first spelling and its collision count.
        seen: dict[bytes, tuple[bytes, int]] = {}
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
            if archive_settings.use_case_hack:
                name = _case_hacked(name, seen)
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


FICLONE: Final = 0x40049409
"""`_IOW(0x94, 9, int)` from `linux/fs.h`: the target shares every extent of the source."""

_COPY_REFUSED: Final = frozenset(
    {
        errno.ENOSYS,
        errno.ENOTTY,
        errno.ENOTSUP,
        errno.EOPNOTSUPP,
        errno.EINVAL,
        errno.EBADF,
        errno.EXDEV,
        errno.ETXTBSY,
        errno.EPERM,
        errno.EACCES,
        errno.ENOENT,
    }
)
"""What `copy_file_range` answers when it cannot copy these two files at all, before the first byte.

coreutils 9.12 `is_CLONENOTSUP` (`copy-file-data.c:92`), plus `ENOENT`, seen over CIFS (`:171`).
`EPERM` is a seccomp filter that refuses the call; a read and a write still work."""

_COPY_CHUNK: Final = 1 << 30
"""coreutils' `copy_max`, a multiple of every block size."""


def copy_path(src: Path, dest: Path) -> None:
    """What `restore_path(dest, dump_path(src))` makes, with each file's bytes copied by the kernel.

    The same file types, modes and symlinks as the restore, so the copy's NAR is the source's.
    A file's bytes go the way coreutils' `cp --reflink=auto` takes them: a clone that shares
    the extents, then `copy_file_range`, then a read and a write. Measured by
    `internal.tests.copy-offload` (310 MB, cold cache, against the NAR stream): btrfs 21% and XFS 37%
    faster with 1% of the space, ext4 12% faster. Blocking: call it from a worker thread.

    With the case hack, the entries go through a NAR, which renames them as a restore does.
    """
    if archive_settings.use_case_hack:
        restore_path(dest, _PieceReader(dump_path(src)))
        return
    _copy(os.fsencode(src), os.fsencode(dest), 0)


class _PieceReader:
    """The `read` that `restore_path` takes, over `dump_path`'s pieces."""

    def __init__(self, pieces: Iterable[bytes]) -> None:
        self._pieces = iter(pieces)
        self._piece = memoryview(b"")

    def __call__(self, n: int) -> bytes:
        while not self._piece:
            piece = next(self._pieces, None)
            if piece is None:
                return b""
            self._piece = memoryview(piece)
        data = bytes(self._piece[:n])
        self._piece = self._piece[n:]
        return data


def _copy(src: bytes, dest: bytes, depth: int) -> None:
    if depth >= MAX_DEPTH:
        raise ValueError(f"path '{os.fsdecode(src)}' exceeds maximum NAR directory depth of {MAX_DEPTH}")
    mode = os.lstat(src).st_mode
    if stat.S_ISREG(mode):
        _copy_file(src, dest, executable=bool(mode & stat.S_IXUSR))
    elif stat.S_ISDIR(mode):
        os.mkdir(dest, 0o777)
        for name in os.listdir(src):
            _copy(os.path.join(src, name), os.path.join(dest, name), depth + 1)
    elif stat.S_ISLNK(mode):
        target = os.readlink(src)
        os.symlink(target, dest)
    else:
        raise ValueError(f"file '{os.fsdecode(src)}' has an unsupported type")


def _copy_file(src: bytes, dest: bytes, *, executable: bool) -> None:
    with open(src, "rb") as source:
        # As `_restore_file`: O_EXCL follows no symlink, and the mode is 0666 plus the execute bits.
        fd = os.open(dest, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_CLOEXEC | os.O_NOFOLLOW, 0o666)
        try:
            if executable:
                mode = os.fstat(fd).st_mode
                os.fchmod(fd, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            copy_file_data(source.fileno(), fd)
        finally:
            os.close(fd)


def copy_file_data(src_fd: int, dest_fd: int) -> None:
    """Every byte of *src_fd* into the empty *dest_fd*: a clone, else `copy_file_range`, else read and write."""
    try:
        fcntl.ioctl(dest_fd, FICLONE, src_fd)
    except OSError as exc:
        # A clone that fails with EIO may leave the target half made (Linux, bugs.gnu.org/60489); every
        # other refusal leaves it empty, and a copy may still work: XFS clones within an allocation group.
        if exc.errno == errno.EIO:
            raise
    else:
        return
    copied = 0
    while True:
        try:
            n = os.copy_file_range(src_fd, dest_fd, _COPY_CHUNK)
        except OSError as exc:
            # EFBIG: a source at its end beside a huge target offset (Linux 6.17, `copy-file-data.c:152`);
            # the read below finds what is left, which is nothing.
            if exc.errno == errno.EFBIG or (copied == 0 and exc.errno in _COPY_REFUSED):
                break
            raise
        if n == 0:
            # 0 at the first call is procfs reporting no data, not an empty file: read it instead.
            if copied == 0:
                break
            return
        copied += n
    while chunk := os.read(src_fd, _CHUNK):
        view = memoryview(chunk)
        while view:
            view = view[os.write(dest_fd, view) :]


def _case_hacked(name: bytes, seen: dict[bytes, tuple[bytes, int]]) -> bytes:
    """*name* as `parse` writes it with `use-case-hack` (`archive.cc:256-270`)."""
    key = name.lower()
    if key not in seen:
        seen[key] = (name, 0)
        return name
    first, count = seen[key]
    seen[key] = (first, count + 1)
    hacked = name + CASE_HACK_SUFFIX + str(count + 1).encode()
    clash = seen.get(hacked.lower())
    if clash is not None:
        raise BadArchive(
            f"NAR contains file name '{name.decode(errors='replace')}' that collides with "
            f"case-hacked file name '{clash[0].decode(errors='replace')}'"
        )
    return hacked


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
