"""The temporary roots that pynixd writes, and how the collector reads them.

Each test uses the real file and the real `flock`, because the contract is
with `LocalStore::findTempRoots` of Nix and not with a Python object. The
tests read the file the way that function reads it. Issue #20.
"""

from __future__ import annotations

import fcntl
import os
import shutil
import socket
import tempfile
import threading
from pathlib import Path

import pytest

from nix_daemon_protocol.store_path import StorePath
from pynixd.temp_roots import TempRoots

PATH_A = "/nix/store/00000000000000000000000000000000-a"
PATH_B = "/nix/store/11111111111111111111111111111111-b"


def read_roots(path) -> list[str]:
    """The roots of a temporary roots file, as `findTempRoots` reads them."""
    return [part.decode() for part in path.read_bytes().split(b"\0") if part]


def can_write_lock(path) -> bool:
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
async def test_a_root_reaches_the_file(tmp_path) -> None:
    roots = TempRoots(tmp_path)
    await roots.add(PATH_A)
    try:
        assert read_roots(roots.path) == [PATH_A]
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_a_store_path_writes_what_its_string_writes(tmp_path) -> None:
    """The `StorePath` short-circuit skips the re-parse, not the write."""
    roots = TempRoots(tmp_path)
    await roots.add(StorePath(PATH_A))
    try:
        assert read_roots(roots.path) == [PATH_A]
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_the_file_holds_every_root_of_the_session(tmp_path) -> None:
    roots = TempRoots(tmp_path)
    await roots.add(PATH_A)
    await roots.add(PATH_B)
    try:
        assert read_roots(roots.path) == [PATH_A, PATH_B]
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_the_collector_cannot_take_a_live_file(tmp_path) -> None:
    roots = TempRoots(tmp_path)
    await roots.add(PATH_A)
    try:
        assert not can_write_lock(roots.path)
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_close_releases_every_root(tmp_path) -> None:
    roots = TempRoots(tmp_path)
    await roots.add(PATH_A)
    await roots.close()
    assert not roots.path.exists()


@pytest.mark.anyio
async def test_the_gc_lock_is_opened_once_for_the_session(tmp_path, monkeypatch) -> None:
    """A session opens `gc.lock` once, and not once for each root.

    A build adds a root for each derivation of its closure, so this is the hot
    path of a build. Nix keeps one descriptor in `LocalStore::_fdGCLock` for
    the process (`src/libstore/gc.cc`, `addTempRoot`), and the open is the part
    that a session can share.
    """
    opens = []
    real_open = os.open

    def counting_open(path, *args, **kwargs):
        if str(path) == str(tmp_path / "gc.lock"):
            opens.append(path)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", counting_open)

    roots = TempRoots(tmp_path)
    try:
        await roots.add(PATH_A)
        await roots.add(PATH_B)
        assert len(opens) == 1
        assert read_roots(roots.path) == [PATH_A, PATH_B]
    finally:
        await roots.close()


@pytest.mark.anyio
async def test_two_sessions_write_two_files(tmp_path) -> None:
    one = TempRoots(tmp_path)
    two = TempRoots(tmp_path)
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
async def test_one_session_ends_and_the_other_keeps_its_root(tmp_path) -> None:
    """The reason for this class. A pooled connection cannot do this."""
    one = TempRoots(tmp_path)
    two = TempRoots(tmp_path)
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
async def test_a_path_of_another_store_is_refused(tmp_path) -> None:
    roots = TempRoots(tmp_path)
    with pytest.raises(ValueError, match="not a path of the store"):
        await roots.add("/somewhere/else/00000000000000000000000000000000-a")


@pytest.mark.skipif(os.geteuid() == 0, reason="root writes a directory whatever its mode says")
@pytest.mark.anyio
async def test_a_state_directory_pynixd_cannot_write_gives_no_root(tmp_path) -> None:
    """pynixd serves the system store as a plain user. It says so, and goes on."""
    unwritable = tmp_path / "state"
    unwritable.mkdir()
    unwritable.chmod(0o500)
    roots = TempRoots(unwritable)
    try:
        await roots.add(PATH_A)
        await roots.add(PATH_B)  # The second one takes the disabled path.
        assert not roots.path.exists()
    finally:
        unwritable.chmod(0o700)
        await roots.close()


class Collector:
    """The socket half of `LocalStore::collectGarbage`, for one client.

    The real collector holds a write lock on `gc.lock` for as long as it
    runs, and it takes each later root over `gc-socket/socket`. This is what
    pynixd must talk to when it cannot get the shared lock.
    """

    def __init__(self, state) -> None:
        self.state = state
        self.roots: list[str] = []
        self.lock_fd = os.open(state / "gc.lock", os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(self.lock_fd, fcntl.LOCK_EX)
        socket_path = state / "gc-socket" / "socket"
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(str(socket_path))
        self.server.listen(1)
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        conn, _ = self.server.accept()
        with conn:
            data = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                data += chunk
                while b"\n" in data:
                    line, data = data.split(b"\n", 1)
                    self.roots.append(line.decode())
                    conn.sendall(b"1")

    def stop(self) -> None:
        self.server.close()
        os.close(self.lock_fd)


@pytest.fixture
def short_path():
    """A directory whose name fits in `sun_path`, which is 104 bytes here.

    The `tmp_path` of pytest holds the name of the test, and the name of this
    test plus `gc-socket/socket` passes that limit on Darwin.
    """
    path = Path(tempfile.mkdtemp(prefix="/tmp/pynixd-tr-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.mark.anyio
async def test_a_root_added_while_the_collector_runs_goes_over_the_socket(short_path) -> None:
    collector = Collector(short_path)
    roots = TempRoots(short_path)
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
