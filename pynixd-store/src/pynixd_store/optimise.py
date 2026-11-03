"""`LocalStore::optimiseStore` (`optimise-store.cc`): deduplicate store files by hard link.

Each regular file (and, on Linux, each symlink) is hashed over its NAR
serialisation. The first file of a hash becomes a link under `.links`; a later
file of the same hash is replaced by a hard link to it, so identical content
sits on disk once. `optimise_path` is the per-path recursion, so a caller that
adds one path can optimise it alone (Nix's `optimisePath`, the
`auto-optimise-store` path).

The algorithm is byte-for-byte Nix's across 2.34 and 2.35; the later version
adds the garbage-collection race guards (a link that a concurrent collector
removes between the hash and the link), which are kept here.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import os
import random
import stat
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from .add import MTIME_STORE
from .nar_fs import dump_path
from .utils import nix32_encode

if TYPE_CHECKING:
    from pathlib import Path

log = structlog.get_logger(__name__)


@dataclass
class OptimiseStats:
    """What `optimiseStore` reports: how many files it linked, and the bytes freed."""

    files_linked: int = 0
    bytes_freed: int = 0


def load_inode_hash(links_dir: Path) -> set[int]:
    """The inodes already linked under `.links` (`loadInodeHash`).

    A file whose inode is here is already deduplicated, so the walk skips it.
    """
    seen: set[int] = set()
    with os.scandir(links_dir) as entries:
        for entry in entries:
            seen.add(entry.inode())
    return seen


def _read_directory_ignoring_inodes(path: Path, inode_hash: set[int]) -> list[str]:
    """`readDirectoryIgnoringInodes`: the names under *path*, minus the already-linked ones."""
    names: list[str] = []
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.inode() in inode_hash:
                continue
            names.append(entry.name)
    return names


def _nar_hash_32(path: Path) -> str:
    """The NAR hash of one file, in Nix32 without a prefix: the name of its `.links` entry."""
    digest = hashlib.sha256()
    for piece in dump_path(path):
        digest.update(piece)
    return nix32_encode(digest.digest())


def _make_writable(path: Path) -> None:
    os.chmod(path, os.lstat(path).st_mode | stat.S_IWUSR)


def _make_read_only(path: Path) -> None:
    """`canonicaliseTimestampAndPermissions` for the one directory the link toggled writable."""
    st = os.lstat(path)
    if not stat.S_ISLNK(st.st_mode):
        mode = stat.S_IMODE(st.st_mode)
        is_dir = stat.S_ISDIR(st.st_mode)
        if (mode != 0o444 or is_dir) and mode != 0o555:
            os.chmod(path, 0o444 | (0o111 if (st.st_mode & stat.S_IXUSR or is_dir) else 0))
    if int(st.st_mtime) != MTIME_STORE:
        os.utime(path, ns=(st.st_atime_ns, MTIME_STORE * 1_000_000_000), follow_symlinks=False)


def _temp_link(real_store_dir: Path) -> Path:
    """`makeTempPath(realStoreDir, ".tmp-link")`: a unique name on the store's file system."""
    return real_store_dir / f".tmp-link-{os.getpid()}-{random.getrandbits(32):08x}"


def optimise_path(
    path: Path,
    inode_hash: set[int],
    *,
    real_store_dir: Path,
    links_dir: Path,
    stats: OptimiseStats,
) -> None:
    """`LocalStore::optimisePath_`: deduplicate *path* and, when it is a directory, every file under it."""
    st = os.lstat(path)

    if stat.S_ISDIR(st.st_mode):
        for name in _read_directory_ignoring_inodes(path, inode_hash):
            optimise_path(path / name, inode_hash, real_store_dir=real_store_dir, links_dir=links_dir, stats=stats)
        return

    # Regular files and symlinks link; anything else is left alone.
    if not stat.S_ISREG(st.st_mode) and not stat.S_ISLNK(st.st_mode):
        return

    # A writable regular file may be mutated in place (`$fontconfig/var/cache`), so skip it.
    if stat.S_ISREG(st.st_mode) and st.st_mode & stat.S_IWUSR:
        log.warning("optimise_skip_writable", path=str(path))
        return

    if st.st_nlink > 1 and st.st_ino in inode_hash:
        return

    link_path = links_dir / _nar_hash_32(path)

    st_link = _maybe_lstat(link_path)
    # A link of the wrong size is corrupt; drop it and re-create. Only the size,
    # because `optimiseStore` repairs nothing (`NoRepair`).
    if st_link is not None and st.st_size != st_link.st_size:
        log.warning("optimise_removing_corrupted_link", link=str(link_path))
        _unlink_if_exists(link_path)
        st_link = None

    if st_link is None:
        try:
            os.link(path, link_path, follow_symlinks=False)
            inode_hash.add(st.st_ino)
        except FileExistsError:
            # Another process created the link first; use it.
            pass
        except OSError as error:
            # A full directory index on ext4 only disables dedup of this file.
            if error.errno == errno.ENOSPC:
                log.info("optimise_no_space_for_link", link=str(link_path), path=str(path))
                return
            raise

    # A concurrent collector may have removed the link since the size check.
    st_link = _maybe_lstat(link_path)
    if st_link is None:
        return
    if st.st_ino == st_link.st_ino:
        return

    # Toggle the containing directory writable, except the store root itself.
    parent = path.parent
    must_toggle = parent != real_store_dir
    if must_toggle:
        _make_writable(parent)
    try:
        temp = _temp_link(real_store_dir)
        try:
            os.link(link_path, temp, follow_symlinks=False)
            inode_hash.add(st.st_ino)
        except OSError as error:
            if error.errno == errno.EMLINK:
                # The link holds the maximum number of links (empty files hit this).
                if st.st_size:
                    log.info("optimise_link_at_maximum", link=str(link_path))
                return
            if error.errno == errno.ENOENT:
                # A concurrent collector removed the link; a later pass dedups this file.
                return
            raise
        try:
            os.rename(temp, path)
        except OSError as error:
            with contextlib.suppress(OSError):
                os.unlink(temp)
            if error.errno == errno.EMLINK:
                # Some file systems raise the link count on the rename, not the link.
                return
            raise
        stats.files_linked += 1
        stats.bytes_freed += st.st_size
    finally:
        if must_toggle:
            with contextlib.suppress(OSError):
                _make_read_only(parent)


def _maybe_lstat(path: Path) -> os.stat_result | None:
    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None


def _unlink_if_exists(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)
