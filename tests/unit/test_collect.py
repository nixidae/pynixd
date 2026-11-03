"""The building blocks of `collectGarbage` (`pynixd_store.collect`).

Deletion and its count of bytes, the order a closure is deleted in, the
`.links` sweep, the lock, and the socket a client gives a root to while the
collector runs. The walk over the database is the engine's.
"""

from __future__ import annotations

import os
import socket
from functools import partial
from typing import TYPE_CHECKING

import anyio
import pytest

from nix_daemon_protocol.protocol import Verbosity
from pynixd_store import collect
from pynixd_store.temp_roots import COLLECTOR_ACK

if TYPE_CHECKING:
    from pathlib import Path

STORE = "/nix/store"
HASH = "0" * 32


def test_store_base_name_takes_a_store_path_and_nothing_else() -> None:
    assert collect.store_base_name(f"{STORE}/{HASH}-hello", STORE) == f"{HASH}-hello"
    assert collect.store_base_name(f"{STORE}/{HASH}-hello/bin", STORE) is None
    assert collect.store_base_name(f"/other/{HASH}-hello", STORE) is None
    assert collect.store_base_name(f"{STORE}/{'e' * 32}-hello", STORE) is None
    assert collect.store_base_name(f"{STORE}/{HASH}_hello", STORE) is None
    assert collect.store_base_name(f"{STORE}/{HASH}-", STORE) is None
    assert collect.store_base_name(f"{STORE}/{HASH}-.-x", STORE) is None
    assert collect.store_base_name(f"{STORE}/{HASH}-.hidden", STORE) == f"{HASH}-.hidden"
    assert collect.store_base_name(f"{STORE}/tmp-123-4", STORE) is None


def test_delete_path_counts_files_with_one_or_two_links(tmp_path: Path) -> None:
    """`_deletePath` (`unix/file-system.cc:178`): three links or more free nothing."""
    tree = tmp_path / "tree"
    (tree / "sub").mkdir(parents=True)
    (tree / "one").write_bytes(b"a" * 10)
    (tree / "sub" / "two").write_bytes(b"b" * 100)
    os.link(tree / "sub" / "two", tmp_path / "two-link")
    (tree / "three").write_bytes(b"c" * 1000)
    os.link(tree / "three", tmp_path / "three-a")
    os.link(tree / "three", tmp_path / "three-b")
    (tree / "sub").chmod(0o555)
    tree.chmod(0o555)

    assert collect.delete_path(tree) == 110
    assert not tree.exists()


def test_delete_path_of_nothing_frees_nothing(tmp_path: Path) -> None:
    assert collect.delete_path(tmp_path / "gone") == 0
    assert collect.delete_path(tmp_path / "gone" / "deeper") == 0


def test_topo_sort_puts_a_referrer_before_its_references() -> None:
    a, b, c = (f"{STORE}/{n * 32}-x" for n in "abc")
    references: dict[str, set[str]] = {a: {b, a}, b: {c}, c: set()}
    assert collect.topo_sort_paths({a, b, c}, references) == [a, b, c]
    # A reference outside the set does not order anything.
    assert collect.topo_sort_paths({a, c}, references) == [c, a]


def test_topo_sort_names_a_cycle() -> None:
    a, b = (f"{STORE}/{n * 32}-x" for n in "ab")
    with pytest.raises(collect.CycleError, match="cycle detected"):
        collect.topo_sort_paths({a, b}, {a: {b}, b: {a}})


def test_the_sweep_unlinks_what_no_file_shares(tmp_path: Path) -> None:
    links = tmp_path / ".links"
    links.mkdir()
    (links / "lonely").write_bytes(b"x" * 7)
    (links / "shared").write_bytes(b"y" * 50)
    os.link(links / "shared", tmp_path / "user-a")
    os.link(links / "shared", tmp_path / "user-b")

    sweep = collect.sweep_unused_links(links)

    assert sweep.unlinked == [links / "lonely"]
    assert sorted(os.listdir(links)) == ["shared"]
    assert sweep.actual_size == 50
    assert sweep.unshared_size == 100
    assert sweep.saving == 50 - sweep.overhead


def test_the_sweep_needs_the_links_directory(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        collect.sweep_unused_links(tmp_path / ".links")


def test_a_locked_tmp_dir_is_skipped(tmp_path: Path) -> None:
    tmp = tmp_path / "tmp-1-2"
    tmp.mkdir()
    holder = collect.lock_tmp_dir(tmp)
    assert holder is not None
    try:
        assert collect.lock_tmp_dir(tmp) is None
    finally:
        os.close(holder)
    free = collect.lock_tmp_dir(tmp)
    assert free is not None
    os.close(free)


def test_the_gc_lock_is_exclusive(tmp_path: Path) -> None:
    first = os.open(tmp_path / "gc.lock", os.O_RDWR | os.O_CREAT)
    second = os.open(tmp_path / "gc.lock", os.O_RDWR)
    try:
        assert collect.try_gc_lock_exclusive(first)
        assert not collect.try_gc_lock_exclusive(second)
    finally:
        os.close(first)
        os.close(second)


class _Lines:
    def __init__(self) -> None:
        self.lines: list[tuple[Verbosity, str]] = []

    async def report(self, level: Verbosity, text: str) -> None:
        self.lines.append((level, text))


def _ask(path: Path, line: bytes) -> bytes:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(str(path))
        sock.sendall(line)
        return sock.recv(1)


async def test_the_server_takes_a_root_and_answers_one(tmp_path: Path) -> None:
    """`gc.cc:463`: the hash part is kept, and the client gets `1`."""
    lines = _Lines()
    server = collect.RootsServer(STORE, lines.report)
    sock = tmp_path / "gc-socket" / "socket"
    async with anyio.create_task_group() as tg:
        await tg.start(partial(server.serve, sock))
        assert sock.stat().st_mode & 0o777 == 0o666
        assert await anyio.to_thread.run_sync(_ask, sock, f"{STORE}/{HASH}-hello\n".encode()) == COLLECTOR_ACK
        assert await anyio.to_thread.run_sync(_ask, sock, b"garbage\n") == COLLECTOR_ACK
        tg.cancel_scope.cancel()
    assert server.temp_roots == {HASH}
    assert (Verbosity.ERROR, "received garbage instead of a root from client") in lines.lines
    assert not server.hold(HASH)


async def test_the_answer_waits_for_the_deletion_of_that_path(tmp_path: Path) -> None:
    """`gc.cc:476`: a client that roots the path being deleted waits until it is gone."""
    server = collect.RootsServer(STORE, _Lines().report)
    sock = tmp_path / "socket"
    answered: list[bytes] = []

    async def client() -> None:
        answered.append(await anyio.to_thread.run_sync(_ask, sock, f"{STORE}/{HASH}-hello\n".encode()))

    async with anyio.create_task_group() as tg:
        await tg.start(partial(server.serve, sock))
        assert server.hold(HASH)
        tg.start_soon(client)
        with anyio.move_on_after(0.3):
            while HASH not in server.temp_roots:
                await anyio.sleep(0.01)
        await anyio.sleep(0.1)
        assert answered == []
        await server.release()
        with anyio.fail_after(5):
            while not answered:
                await anyio.sleep(0.01)
        tg.cancel_scope.cancel()
    assert answered == [COLLECTOR_ACK]
