"""The temporary roots that pynixd writes, and how the collector reads them.

Each test uses the real file and the real `flock`, because the contract is
with `LocalStore::findTempRoots` of Nix and not with a Python object. The
tests read the file the way that function reads it. Issue #20.
"""

from __future__ import annotations

import fcntl
import os
import threading
import time
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread
import pytest

from nix_daemon_protocol.store_path import StorePath
from pynixd_store import temp_roots
from pynixd_store.temp_roots import RootWriter
from tests._conftest.temp_roots import Collector, read_roots, short_path as short_path

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

PATH_A = "/nix/store/00000000000000000000000000000000-a"
PATH_B = "/nix/store/11111111111111111111111111111111-b"


@pytest.fixture
async def writer(tmp_path: Path) -> AsyncIterator[RootWriter]:
    async with RootWriter(tmp_path) as running:
        yield running


@pytest.fixture
async def short_writer(short_path: Path) -> AsyncIterator[RootWriter]:
    async with RootWriter(short_path) as running:
        yield running


def can_write_lock(path: Path) -> bool:
    """True when the collector would call this file stale and delete it."""
    fd = os.open(path, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    finally:
        os.close(fd)
    return True


@pytest.mark.anyio
async def test_a_root_reaches_the_file(tmp_path: Path, writer: RootWriter) -> None:
    roots = writer.session()
    await roots.add(PATH_A)
    try:
        assert read_roots(roots.path) == [PATH_A]
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_a_store_path_writes_what_its_string_writes(tmp_path: Path, writer: RootWriter) -> None:
    """The `StorePath` short-circuit skips the re-parse, not the write."""
    roots = writer.session()
    await roots.add(StorePath(PATH_A))
    try:
        assert read_roots(roots.path) == [PATH_A]
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_the_file_holds_every_root_of_the_session(tmp_path: Path, writer: RootWriter) -> None:
    roots = writer.session()
    await roots.add(PATH_A)
    await roots.add(PATH_B)
    try:
        assert read_roots(roots.path) == [PATH_A, PATH_B]
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_the_collector_cannot_take_a_live_file(tmp_path: Path, writer: RootWriter) -> None:
    roots = writer.session()
    await roots.add(PATH_A)
    try:
        assert not can_write_lock(roots.path)
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_close_releases_every_root(tmp_path: Path, writer: RootWriter) -> None:
    roots = writer.session()
    await roots.add(PATH_A)
    await roots.close()
    assert not roots.path.exists()


@pytest.mark.anyio
async def test_the_gc_lock_is_opened_once_for_the_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writer: RootWriter
) -> None:
    """A session opens `gc.lock` once, and not once for each root.

    A build adds a root for each derivation of its closure, so this is the hot
    path of a build. Nix keeps one descriptor in `LocalStore::_fdGCLock` for
    the process (`src/libstore/gc.cc`, `addTempRoot`), and the open is the part
    that a session can share.
    """
    opens: list[str | os.PathLike[str]] = []
    real_open = os.open

    def counting_open(path: str | os.PathLike[str], *args: int, **kwargs: int) -> int:
        if str(path) == str(tmp_path / "gc.lock"):
            opens.append(path)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", counting_open)

    roots = writer.session()
    try:
        await roots.add(PATH_A)
        await roots.add(PATH_B)
        assert len(opens) == 1
        assert read_roots(roots.path) == [PATH_A, PATH_B]
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_two_sessions_write_two_files(tmp_path: Path, writer: RootWriter) -> None:
    one = writer.session()
    two = writer.session()
    await one.add(PATH_A)
    await two.add(PATH_B)
    try:
        assert one.path != two.path
        assert read_roots(one.path) == [PATH_A]
        assert read_roots(two.path) == [PATH_B]
    finally:
        await one.close()
        await two.close()


@pytest.mark.anyio
async def test_one_session_ends_and_the_other_keeps_its_root(tmp_path: Path, writer: RootWriter) -> None:
    """The reason for this class. A pooled connection cannot do this."""
    one = writer.session()
    two = writer.session()
    await one.add(PATH_A)
    await two.add(PATH_B)
    await one.close()
    try:
        assert not one.path.exists()
        assert read_roots(two.path) == [PATH_B]
        assert not can_write_lock(two.path)
    finally:
        await two.close()


@pytest.mark.anyio
async def test_a_path_of_another_store_is_refused(tmp_path: Path, writer: RootWriter) -> None:
    roots = writer.session()
    with pytest.raises(ValueError, match="not a path of the store"):
        await roots.add("/somewhere/else/00000000000000000000000000000000-a")


@pytest.mark.skipif(os.geteuid() == 0, reason="root writes a directory whatever its mode says")
@pytest.mark.anyio
async def test_a_state_directory_pynixd_cannot_write_gives_no_root(tmp_path: Path) -> None:
    """pynixd serves the system store as a plain user. It says so, and goes on."""
    unwritable = tmp_path / "state"
    unwritable.mkdir()
    unwritable.chmod(0o500)
    try:
        async with RootWriter(unwritable) as writer:
            roots = writer.session()
            await roots.add(PATH_A)
            await roots.add(PATH_B)  # The second one takes the disabled path.
            assert not roots.path.exists()
            await roots.close()
    finally:
        unwritable.chmod(0o700)


@pytest.mark.anyio
async def test_a_root_added_while_the_collector_runs_goes_over_the_socket(
    short_path: Path, short_writer: RootWriter
) -> None:
    collector = Collector(short_path)
    roots = short_writer.session()
    try:
        await roots.add(PATH_A)
        # The collector read the directory before this file existed, so the
        # socket is the only way it learns about the root.
        assert collector.roots == [PATH_A]
        # And the file still gets it, for the next run of the collector.
        assert read_roots(roots.path) == [PATH_A]
    finally:
        await roots.close()
        collector.stop()


def _hold_the_big_lock(state: Path) -> int:
    """A collector that holds `gc.lock` for writing and serves no socket, as one does while the kernel dumps its core."""
    fd = os.open(state / "gc.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


@pytest.mark.anyio
async def test_a_root_waits_for_a_collector_with_no_socket(
    short_path: Path, monkeypatch: pytest.MonkeyPatch, short_writer: RootWriter
) -> None:
    """Nix's `addTempRoot` asks again with no limit (`gc.cc:95-135`), and pynixd does too.

    Counted in attempts, not seconds: pynixd gave up after 100, and a dying
    worker held the lock for 18 s on dynhetz (nixidae/nixidae#1386).
    """
    monkeypatch.setattr(temp_roots, "RETRY_DELAY", 0)
    lock = _hold_the_big_lock(short_path)
    roots = short_writer.session()
    attempts = 0
    asked = threading.Event()
    tell = roots._tell_the_collector  # noqa: SLF001 -- each call is one attempt

    def counted(root: str) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 300:
            asked.set()
        return tell(root)

    monkeypatch.setattr(roots, "_tell_the_collector", counted)
    try:
        with anyio.fail_after(30):
            async with anyio.create_task_group() as tg:
                tg.start_soon(roots.add, PATH_A)
                await anyio.to_thread.run_sync(asked.wait, 30)
                os.close(lock)
        assert read_roots(roots.path) == [PATH_A]
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_a_waiting_root_ends_with_its_session(short_path: Path, short_writer: RootWriter) -> None:
    """The wait sleeps on the event loop, so a cancelled session does not hold the writer's thread."""
    lock = _hold_the_big_lock(short_path)
    roots = short_writer.session()
    started = time.monotonic()
    try:
        with anyio.move_on_after(0.5) as scope:
            await roots.add(PATH_A)
        assert scope.cancelled_caught
        assert time.monotonic() - started < 2
    finally:
        os.close(lock)
        await roots.close()


SUBSTITUTED = [f"/nix/store/{i:032d}-path" for i in range(3344)]
"""The roots of one `ekn kubeapply` store-path check on sg1, which held pynixd-0's event loop until its liveness probe
killed the pod: one write each, on the loop, on Ceph RBD."""


@pytest.mark.anyio
async def test_many_roots_are_one_write_off_the_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writer: RootWriter
) -> None:
    roots = writer.session()
    loop_thread = threading.get_ident()
    writes: list[int] = []
    write = os.write

    def counted(fd: int, data: bytes | memoryview) -> int:
        if fd == roots._fd:
            assert threading.get_ident() != loop_thread, "a roots file write blocks the event loop"
            writes.append(len(data))
        return write(fd, data)

    monkeypatch.setattr(os, "write", counted)
    try:
        assert await roots.add_many(SUBSTITUTED)
        assert len(writes) == 1
        assert read_roots(roots.path) == SUBSTITUTED
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_many_roots_added_while_the_collector_runs_go_over_the_socket(
    short_path: Path, short_writer: RootWriter
) -> None:
    collector = Collector(short_path)
    roots = short_writer.session()
    try:
        assert await roots.add_many([PATH_A, PATH_B])
        assert collector.roots == [PATH_A, PATH_B]
        assert read_roots(roots.path) == [PATH_A, PATH_B]
    finally:
        await roots.close()
        collector.stop()
