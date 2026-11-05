"""`LocalStore::verifyStore` (`local-store.cc:1380`): cross-check the database and the store directory.

The sync building blocks live here; the engine orchestrates them, because the
walk and the fix-ups touch the database. `verify_all_valid_paths` lists the
store directory and reports every valid path the directory still holds; a path
the directory lost is removed from the database when its referrers can go too.
With `check_contents`, each `.links` entry and each valid path is re-hashed
against the hash Nix recorded.

The algorithm is Nix's across 2.34 and 2.35. A repair calls back into the
caller for `Store::repairPath`, which substitutes or builds: the engine has
neither.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
from typing import TYPE_CHECKING

from .errors import PynixdError
from .nar_fs import dump_path, nar_sha256
from .utils import nix32_encode

if TYPE_CHECKING:
    from pathlib import Path

GC_LOCK_FILE = "gc.lock"


class RepairError(PynixdError):
    """`Store::repairPath` (`build/entry-points.cc:141`) found no way to repair the path; the text is Nix's."""


#: `Hash nullHash(SHA256)` as the `narHash` column holds it: Base16 of 32 zero bytes.
NULL_SHA256_HEX = "0" * 64


def scan_store_dir(real_store_dir: Path) -> set[str]:
    """The base names the store directory holds (`verifyAllValidPaths`, `local-store.cc:1490`).

    Nix reads every directory entry into a `StorePath`, skipping the names that
    are not store paths (`.links`, a `.tmp-link-*`). Those names never equal a
    valid path's base name, so the raw listing gives the same membership for
    every database path and never calls one absent that is present.
    """
    return set(os.listdir(real_store_dir))


def list_links(links_dir: Path) -> list[str]:
    """The `.links` entry names, for `verifyStore`'s link-hash pass (`local-store.cc:1396`)."""
    if not links_dir.exists():
        return []
    with os.scandir(links_dir) as entries:
        return [entry.name for entry in entries]


def link_hash(link: Path) -> str:
    """The Nix32 NAR hash of a `.links` entry. The entry's name is this hash, so a mismatch means modified content."""
    return nix32_encode(nar_sha256(link).digest())


def unlink_if_exists(path: Path) -> None:
    """`unlinkIfExists` (`file-system.cc`)."""
    path.unlink(missing_ok=True)


def nar_hash_and_size(path: Path) -> tuple[str, int]:
    """The NAR hash (Base16, no prefix) and NAR byte length of *path* (`HashSink`, `local-store.cc:1427`).

    The store-hash pass compares the hash to the `narHash` column and fills a
    zero `narSize`. Nix's `numBytesDigested` is the count of bytes fed to the
    sink, which is the NAR's own length.
    """
    digest = hashlib.sha256()
    size = 0
    for piece in dump_path(path):
        digest.update(piece)
        size += len(piece)
    return digest.hexdigest(), size


def open_gc_lock(state_dir: Path) -> int:
    """Open the store's `gc.lock`, as `LocalStore::openGCLock` (`gc.cc`) does, and return its descriptor."""
    return os.open(state_dir / GC_LOCK_FILE, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)


def try_gc_lock_shared(fd: int) -> bool:
    """Try the big garbage-collector lock for reading without waiting (`FdLock`, `pathlocks.cc:146`).

    `verifyStore` holds it for a consistent snapshot: the collector takes the
    same lock for writing, so it waits for this read, and a path cannot
    disappear mid-walk and read as lost. A shared lock is per open descriptor,
    so the engine opens its own, never the temporary-roots descriptor that
    releases its lock per write. False means the caller reports the wait,
    then calls `wait_gc_lock_shared`.
    """
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def wait_gc_lock_shared(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_SH)


def release_gc_lock(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)
