"""The sync building blocks of `verifyStore` (`pynixd_store.verify`).

`scan_store_dir` lists the store directory, `link_hash` re-hashes a `.links`
entry, and `nar_hash_and_size` re-hashes a path. The database walk
and the fix-ups are the engine's; these touch only the file system and the GC
lock.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
from typing import TYPE_CHECKING

from pynixd_store import verify
from pynixd_store.nar_fs import dump_path
from pynixd_store.utils import nix32_encode

if TYPE_CHECKING:
    from pathlib import Path

HELLO = b"hello\n"


def _nar_digest(content: bytes, tmp_path: Path) -> hashlib._Hash:
    probe = tmp_path / "probe"
    probe.write_bytes(content)
    probe.chmod(0o444)
    digest = hashlib.sha256()
    for piece in dump_path(probe):
        digest.update(piece)
    probe.unlink()
    return digest


def test_scan_store_dir_lists_every_name(tmp_path: Path) -> None:
    store = tmp_path / "store"
    store.mkdir()
    (store / "aaa-one").write_bytes(HELLO)
    (store / ".links").mkdir()
    assert verify.scan_store_dir(store) == {"aaa-one", ".links"}


def test_link_hash_is_the_name_of_a_sound_entry(tmp_path: Path) -> None:
    links = tmp_path / ".links"
    links.mkdir()
    name = nix32_encode(_nar_digest(HELLO, tmp_path).digest())
    (links / name).write_bytes(HELLO)

    assert verify.list_links(links) == [name]
    assert verify.link_hash(links / name) == name


def test_link_hash_differs_for_a_modified_entry(tmp_path: Path) -> None:
    links = tmp_path / ".links"
    links.mkdir()
    name = nix32_encode(_nar_digest(HELLO, tmp_path).digest())
    # The right name, the wrong content: the re-hash does not match the name.
    (links / name).write_bytes(b"tampered")

    assert verify.link_hash(links / name) != name


def test_list_links_without_a_links_dir_is_empty(tmp_path: Path) -> None:
    assert verify.list_links(tmp_path / ".links") == []


def test_nar_hash_and_size_matches_an_independent_hash(tmp_path: Path) -> None:
    path = tmp_path / "aaa-one"
    path.write_bytes(HELLO)
    path.chmod(0o444)

    got_hash, got_size = verify.nar_hash_and_size(path)

    digest = hashlib.sha256()
    expected_size = 0
    for piece in dump_path(path):
        digest.update(piece)
        expected_size += len(piece)
    assert got_hash == digest.hexdigest()
    assert got_size == expected_size


def test_the_gc_lock_is_shared(tmp_path: Path) -> None:
    """Two readers hold the lock at once; the collector's write lock would wait."""
    first = verify.open_gc_lock(tmp_path)
    try:
        assert verify.try_gc_lock_shared(first)
        second = os.open(tmp_path / verify.GC_LOCK_FILE, os.O_RDWR)
        try:
            # A second shared lock succeeds; an exclusive one would not.
            fcntl.flock(second, fcntl.LOCK_SH | fcntl.LOCK_NB)
            fcntl.flock(second, fcntl.LOCK_UN)
        finally:
            os.close(second)
    finally:
        verify.release_gc_lock(first)


def test_null_sha256_hex_is_thirty_two_zero_bytes() -> None:
    """`Hash nullHash(SHA256)` is 32 zero bytes; Base16 of that is 64 zeros."""
    assert bytes(32).hex() == verify.NULL_SHA256_HEX
