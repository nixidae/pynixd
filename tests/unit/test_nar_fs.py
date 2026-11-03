"""NAR to and from the file system, as Nix's `archive.cc` does.

`dump_path` must give the bytes `nar.py`'s serialiser gives for the same tree,
which is an implementation of its own; `tests/functional/test_nar_from_path.py`
holds `dump_path` against `nix-store --dump`. `restore_path` must give back a
tree whose dump is the NAR it read, and refuse each archive `parseDump`
refuses (`archive.cc:177`).
"""

from __future__ import annotations

import io
import os
import stat
import struct
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_dir import reset_store_dir, set_real_store_dir
from pynixd.config import LocalSocketStoreSpec
from pynixd.nar import NarDirectory, NarDirectoryEntry, NarRegular, NarSymlink, write_nar
from pynixd.serde import StorePath
from pynixd.store.local_db import LocalDBStore
from pynixd_store.nar_fs import MAX_DEPTH, BadArchive, dump_path, restore_path

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.nar import NarNode

BIG = bytes(range(256)) * 1024
"""256 KiB, so a file crosses several 64 KiB pieces."""


def _tree(root: Path) -> NarNode:
    """A tree with every node shape, on disk under *root*, and the same tree as `nar.py` nodes."""
    root.mkdir()
    (root / "bin").mkdir()
    (root / "bin" / "hello").write_bytes(b"#!/bin/sh\n")  # noqa: ASYNC240 -- test setup
    (root / "bin" / "hello").chmod(0o744)
    (root / "big").write_bytes(BIG)  # noqa: ASYNC240 -- test setup
    (root / "empty").write_bytes(b"")  # noqa: ASYNC240 -- test setup
    (root / "group-exec-only").write_bytes(b"x")  # noqa: ASYNC240 -- test setup
    (root / "group-exec-only").chmod(0o654)
    (root / "link").symlink_to("bin/hello")
    (root / "Zed").write_bytes(b"z")  # noqa: ASYNC240 -- test setup
    return NarDirectory(
        entries=[
            NarDirectoryEntry("Zed", NarRegular(b"z")),
            NarDirectoryEntry("big", NarRegular(BIG)),
            NarDirectoryEntry(
                "bin", NarDirectory(entries=[NarDirectoryEntry("hello", NarRegular(b"#!/bin/sh\n", executable=True))])
            ),
            NarDirectoryEntry("empty", NarRegular(b"")),
            NarDirectoryEntry("group-exec-only", NarRegular(b"x")),
            NarDirectoryEntry("link", NarSymlink("bin/hello")),
        ]
    )


def _dump(path: Path) -> bytes:
    return b"".join(dump_path(path))


def _restore(dest: Path, nar: bytes) -> None:
    restore_path(dest, io.BytesIO(nar).read)


class TestDump:
    def test_a_tree_dumps_as_the_serialiser_writes_it(self, tmp_path: Path) -> None:
        """Byte order puts `Zed` first; only the owner's execute bit makes a file `executable`."""
        expected = _tree(tmp_path / "t")
        assert _dump(tmp_path / "t") == write_nar(expected)

    def test_a_single_file_and_a_single_symlink(self, tmp_path: Path) -> None:
        (tmp_path / "f").write_bytes(b"hi")  # noqa: ASYNC240 -- test setup
        (tmp_path / "s").symlink_to("/nowhere")
        assert _dump(tmp_path / "f") == write_nar(NarRegular(b"hi"))
        assert _dump(tmp_path / "s") == write_nar(NarSymlink("/nowhere"))

    def test_a_file_streams_in_pieces(self, tmp_path: Path) -> None:
        (tmp_path / "big").write_bytes(BIG)  # noqa: ASYNC240 -- test setup
        pieces = list(dump_path(tmp_path / "big"))
        assert len(pieces) > 2
        assert max(len(piece) for piece in pieces) <= 64 * 1024

    def test_a_fifo_is_refused(self, tmp_path: Path) -> None:
        os.mkfifo(tmp_path / "fifo")
        with pytest.raises(ValueError, match="unsupported type"):
            _dump(tmp_path / "fifo")

    def test_a_tree_too_deep_is_refused(self, tmp_path: Path) -> None:
        deep = tmp_path / "d"
        (deep / "/".join(["x"] * MAX_DEPTH)).mkdir(parents=True)
        with pytest.raises(ValueError, match="maximum NAR directory depth of 64"):
            _dump(deep)
        (deep / "/".join(["x"] * MAX_DEPTH)).rmdir()
        _dump(deep)


class TestRestore:
    def test_a_restored_tree_dumps_as_the_nar_it_came_from(self, tmp_path: Path) -> None:
        nar = write_nar(_tree(tmp_path / "t"))
        _restore(tmp_path / "back", nar)
        assert _dump(tmp_path / "back") == nar
        assert stat.S_IMODE((tmp_path / "back" / "bin" / "hello").stat().st_mode) & 0o111 == 0o111
        assert os.readlink(tmp_path / "back" / "link") == "bin/hello"

    def test_an_existing_destination_is_refused(self, tmp_path: Path) -> None:
        (tmp_path / "there").write_bytes(b"")  # noqa: ASYNC240 -- test setup
        with pytest.raises(FileExistsError):
            _restore(tmp_path / "there", write_nar(NarRegular(b"x")))

    def test_the_stream_is_read_in_bounded_pieces(self, tmp_path: Path) -> None:
        asked: list[int] = []
        stream = io.BytesIO(write_nar(NarRegular(BIG)))

        def read(n: int) -> bytes:
            asked.append(n)
            return stream.read(n)

        restore_path(tmp_path / "f", read)
        assert (tmp_path / "f").read_bytes() == BIG  # noqa: ASYNC240 -- test assertion
        assert max(asked) <= 64 * 1024


@pytest.mark.anyio
async def test_a_store_dumps_its_own_files_when_the_process_names_another(tmp_path: Path) -> None:
    """The functional suite runs several chroot stores in one process, and the process-wide
    `real_store_dir` names whichever was set last. The session store then dumped another
    test's directory and broke the client's NAR."""
    name = "00000000000000000000000000000001-hello"
    for root, text in ((tmp_path / "mine", b"mine"), (tmp_path / "other", b"other")):
        (root / "nix" / "store").mkdir(parents=True)
        (root / "nix" / "store" / name).write_bytes(text)  # noqa: ASYNC240 -- test setup
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=tmp_path / "mine"))
    set_real_store_dir(tmp_path / "other" / "nix" / "store")
    try:
        dumped = b"".join([piece async for piece in store.nar_pieces(StorePath(path=f"/nix/store/{name}"))])
    finally:
        reset_store_dir()
    assert dumped == write_nar(NarRegular(b"mine"))


def _entries(*names: str) -> bytes:
    return write_nar(NarDirectory(entries=[NarDirectoryEntry(name, NarRegular(b"")) for name in names]))


@pytest.mark.parametrize(
    ("nar", "message"),
    [
        (b"", "doesn't look like a Nix archive"),
        (write_nar(NarRegular(b"x")).replace(b"nix-archive-1", b"nix-archive-2"), "doesn't look like a Nix archive"),
        (_entries("b", "a"), "not sorted"),
        (_entries("a", "a"), "not sorted"),
        (_entries(".."), "invalid file name"),
        (_entries("a/b"), "invalid file name"),
        (write_nar(NarSymlink("")), "invalid symlink target"),
        (write_nar(NarRegular(b"x"))[:-8], "unexpected end"),
        (write_nar(NarRegular(b"x")).replace(b"x" + b"\0" * 7, b"x" + b"\1" + b"\0" * 6), "non-zero padding"),
        (
            write_nar(NarRegular(b"", executable=True)).replace(
                struct.pack("<Q", 10) + b"executable\0\0\0\0\0\0" + struct.pack("<Q", 0),
                struct.pack("<Q", 10) + b"executable\0\0\0\0\0\0" + struct.pack("<Q", 1) + b"x" + b"\0" * 7,
            ),
            "string of 1 bytes is longer than 0",
        ),
    ],
    ids=["empty", "magic", "unsorted", "duplicate", "dotdot", "slash", "empty-target", "truncated", "padding", "exec"],
)
def test_restore_refuses_what_nix_refuses(tmp_path: Path, nar: bytes, message: str) -> None:
    with pytest.raises(BadArchive, match=message):
        _restore(tmp_path / "out", nar)


def test_restore_refuses_a_tree_too_deep(tmp_path: Path) -> None:
    node: NarNode = NarRegular(b"")
    for _ in range(MAX_DEPTH):
        node = NarDirectory(entries=[NarDirectoryEntry("x", node)])
    with pytest.raises(BadArchive, match="maximum depth of 64"):
        _restore(tmp_path / "out", write_nar(node))
