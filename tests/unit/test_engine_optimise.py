"""The engine deduplicates store files as Nix does (`LocalStore::optimiseStore`).

`optimisePath_` (`optimise-store.cc`) hashes each file over its NAR, links the
first file of a hash under `.links`, and replaces a later file of the same hash
with a hard link to it. The `.links` entry's name is the NAR hash in Nix32, so
a link pynixd makes and a link the daemon makes for the same content share a
name and so share an inode.
"""

from __future__ import annotations

import hashlib
import os
from typing import TYPE_CHECKING, Any

import pytest

from pynixd_store import optimise
from pynixd_store.engine import StoreEngine
from pynixd_store.nar_fs import dump_path
from pynixd_store.store_layout import StoreLayout
from pynixd_store.utils import nix32_encode

if TYPE_CHECKING:
    from pathlib import Path

HELLO = b"hello\n"


def _links_name(content: bytes, tmp_path: Path) -> str:
    """The `.links` name Nix gives a regular file of *content*: Nix32 of its NAR hash."""
    probe = tmp_path / "probe"
    probe.write_bytes(content)
    probe.chmod(0o444)
    digest = hashlib.sha256()
    for piece in dump_path(probe):
        digest.update(piece)
    probe.unlink()
    return nix32_encode(digest.digest())


def _store(tmp_path: Path) -> tuple[Path, Path]:
    """A store directory and its `.links`, both empty."""
    store_dir = tmp_path / "store"
    links_dir = store_dir / ".links"
    links_dir.mkdir(parents=True)
    return store_dir, links_dir


def _write(store_dir: Path, name: str, content: bytes) -> Path:
    path = store_dir / name
    path.write_bytes(content)
    path.chmod(0o444)
    return path


def _optimise(store_dir: Path, links_dir: Path) -> optimise.OptimiseStats:
    stats = optimise.OptimiseStats()
    optimise.optimise_path(
        store_dir, optimise.load_inode_hash(links_dir), real_store_dir=store_dir, links_dir=links_dir, stats=stats
    )
    return stats


def test_two_files_of_one_content_share_an_inode(tmp_path: Path) -> None:
    store_dir, links_dir = _store(tmp_path)
    first = _write(store_dir, "aaa-one", HELLO)
    second = _write(store_dir, "bbb-two", HELLO)

    stats = _optimise(store_dir, links_dir)

    assert first.stat().st_ino == second.stat().st_ino
    # The two files plus the `.links` entry: three names for one inode.
    assert first.stat().st_nlink == 3
    assert stats.files_linked == 1
    assert stats.bytes_freed == len(HELLO)


def test_the_links_name_is_nix_s_nar_hash(tmp_path: Path) -> None:
    """The discriminating check: a link pynixd makes must match the daemon's name."""
    store_dir, links_dir = _store(tmp_path)
    _write(store_dir, "aaa-one", HELLO)

    _optimise(store_dir, links_dir)

    assert [entry.name for entry in os.scandir(links_dir)] == [_links_name(HELLO, tmp_path)]


def test_different_content_is_not_linked(tmp_path: Path) -> None:
    store_dir, links_dir = _store(tmp_path)
    one = _write(store_dir, "aaa-one", b"one\n")
    two = _write(store_dir, "bbb-two", b"two\n")

    _optimise(store_dir, links_dir)

    assert one.stat().st_ino != two.stat().st_ino
    assert len(list(os.scandir(links_dir))) == 2


def test_a_writable_regular_file_is_skipped(tmp_path: Path) -> None:
    """Nix skips a writable file: it may be mutated in place (`$fontconfig/var/cache`)."""
    store_dir, links_dir = _store(tmp_path)
    writable = store_dir / "aaa-one"
    writable.write_bytes(HELLO)
    writable.chmod(0o644)
    _write(store_dir, "bbb-two", HELLO)

    _optimise(store_dir, links_dir)

    # The writable file is never hashed, so it leaves no `.links` entry; the
    # read-only file still links itself.
    assert writable.stat().st_nlink == 1


def test_a_symlink_is_linked_on_linux(tmp_path: Path) -> None:
    """`CAN_LINK_SYMLINK`: two symlinks with the same target dedup, hashing the target."""
    store_dir, links_dir = _store(tmp_path)
    first = store_dir / "aaa-one"
    second = store_dir / "bbb-two"
    first.symlink_to("/nix/store/some-target")
    second.symlink_to("/nix/store/some-target")

    _optimise(store_dir, links_dir)

    assert os.lstat(first).st_ino == os.lstat(second).st_ino


def test_a_corrupted_link_is_replaced(tmp_path: Path) -> None:
    """A `.links` entry of the wrong size is dropped and re-created from the file."""
    store_dir, links_dir = _store(tmp_path)
    _write(store_dir, "aaa-one", HELLO)
    # A stale link of the right name but the wrong content (hence size).
    (links_dir / _links_name(HELLO, tmp_path)).write_bytes(b"corrupt and longer")

    _optimise(store_dir, links_dir)

    link = links_dir / _links_name(HELLO, tmp_path)
    assert link.read_bytes() == HELLO


def test_a_second_pass_changes_nothing(tmp_path: Path) -> None:
    """An already-linked file carries no work the next time."""
    store_dir, links_dir = _store(tmp_path)
    _write(store_dir, "aaa-one", HELLO)
    _write(store_dir, "bbb-two", HELLO)
    _optimise(store_dir, links_dir)

    again = _optimise(store_dir, links_dir)

    assert again.files_linked == 0


def test_a_nested_file_links_and_its_parent_is_restored_read_only(tmp_path: Path) -> None:
    """A file inside a store-path directory: Nix makes the directory writable, links, restores it."""
    store_dir, links_dir = _store(tmp_path)
    first_dir = store_dir / "aaa-one"
    second_dir = store_dir / "bbb-two"
    for directory in (first_dir, second_dir):
        directory.mkdir()
        (directory / "foo").write_bytes(HELLO)
        (directory / "foo").chmod(0o444)
        directory.chmod(0o555)

    _optimise(store_dir, links_dir)

    assert (first_dir / "foo").stat().st_ino == (second_dir / "foo").stat().st_ino
    # The replaced file's directory is back to read-only, with the store mtime.
    restored = second_dir.stat()
    assert oct(restored.st_mode & 0o777) == oct(0o555)
    assert int(restored.st_mtime) == 1


async def _no_config() -> dict[str, Any]:
    raise AssertionError("optimiseStore reads no Nix setting")


async def test_the_engine_refuses_to_optimise_with_writes_off(tmp_path: Path) -> None:
    layout = StoreLayout.chroot(tmp_path)
    engine = StoreEngine(layout, writes=False, read_config=_no_config, on_change=lambda _: None, name="test")

    async def _hold(_path: str) -> None:
        raise AssertionError("the guard refuses before it walks a path")

    with pytest.raises(RuntimeError, match="pynixd_writes is off"):
        await engine.optimise_store(hold=_hold)
