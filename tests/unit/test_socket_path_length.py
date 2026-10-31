"""A store under a long path connects on Linux, and fails at once elsewhere.

`sockaddr_un.sun_path` is 108 bytes on Linux. Nix binds a longer path by
forking a helper that `chdir`s into the directory and binds the base name
(`bindConnectProcHelper`, `src/libutil/unix/unix-domain-socket.cc`), so the
socket file exists at the long path. Python has no such helper: an absolute
`socket.connect()` answers `OSError: AF_UNIX path too long`.

pynixd therefore waited its whole 30 second start-up budget and reported
`Managed daemon socket not accepting connections ... '(the daemon wrote
nothing)'`. Every word of that was true and none of it named the fault.
The guard named the fault next, and now `open_unix_connection` removes it
on Linux: the directory is opened once and the base name is addressed
through `/proc/self/fd/<n>/`, which is short whatever the directory is.
Issue #44.
"""

from __future__ import annotations

import os
import socket
import sys
from pathlib import Path

import anyio
import pytest
from anyio.to_thread import run_sync

from pynixd.store.local_daemon import _refuse_a_socket_path_python_cannot_reach
from pynixd.unix_socket import _SUN_PATH_MAX, open_unix_connection
from pynixd.wire import UnixNixReader


def _a_path_of(length: int) -> Path:
    """An absolute path of exactly `length` bytes."""
    head = "/tmp/"
    return Path(head + "a" * (length - len(head)))


def _a_long_dir_with_a_short_name() -> Path:
    """A path over the limit whose base name fits: the reroutable shape."""
    base = "sock"
    head = "/tmp/"
    return Path(head + "d" * (_SUN_PATH_MAX + 1 - len(head) - 1 - len(base)) + "/" + base)


def _a_path_with_a_long_base_name() -> Path:
    """A path no connect can reach: the base name alone is over the limit,
    and even the helper Nix binds with cannot bind that."""
    return Path("/tmp/" + "b" * (_SUN_PATH_MAX + 1))


class TestTheGuard:
    def test_a_long_path_is_refused(self) -> None:
        with pytest.raises(RuntimeError, match="Unix socket takes"):
            _refuse_a_socket_path_python_cannot_reach(_a_path_with_a_long_base_name())

    def test_the_error_carries_both_numbers(self) -> None:
        long_one = _a_path_of(200)

        with pytest.raises(RuntimeError) as caught:
            _refuse_a_socket_path_python_cannot_reach(long_one)

        assert "200 bytes" in str(caught.value)
        assert str(_SUN_PATH_MAX) in str(caught.value)

    def test_a_long_base_name_is_refused_even_where_reroutes_work(self) -> None:
        with pytest.raises(RuntimeError, match="Unix socket takes"):
            _refuse_a_socket_path_python_cannot_reach(_a_path_with_a_long_base_name())

    @pytest.mark.skipif(sys.platform != "linux", reason="the reroute is Linux-only")
    def test_a_long_directory_with_a_short_name_passes_on_linux(self) -> None:
        _refuse_a_socket_path_python_cannot_reach(_a_long_dir_with_a_short_name())

    def test_a_long_directory_with_a_short_name_is_refused_off_linux(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "platform", "darwin")
        with pytest.raises(RuntimeError, match="Unix socket takes"):
            _refuse_a_socket_path_python_cannot_reach(_a_long_dir_with_a_short_name())

    def test_a_path_at_the_limit_passes(self) -> None:
        """The negative control. A guard that refuses everything would pass
        the case above and break every ordinary store."""
        _refuse_a_socket_path_python_cannot_reach(_a_path_of(_SUN_PATH_MAX))

    def test_an_ordinary_path_passes(self) -> None:
        _refuse_a_socket_path_python_cannot_reach(Path("/nix/var/nix/daemon-socket/socket"))


def _a_long_socket_under(tmp_path: Path) -> Path:
    """A socket path over the limit with a short base name, as a file."""
    base = "sock"
    need = _SUN_PATH_MAX + 1 - len(os.fsencode(str(tmp_path))) - 1 - len(base)
    long_dir = tmp_path if need < 1 else tmp_path / ("d" * need)
    long_dir.mkdir(parents=True, exist_ok=True)
    candidate = long_dir / base
    assert len(os.fsencode(str(candidate))) > _SUN_PATH_MAX
    return candidate


def _bind_the_way_nix_binds(sock_path: Path) -> socket.socket:
    """Bind `sock_path`, whose directory alone is over the limit.

    Nix binds such a path from a helper that `chdir`s into the directory, so
    the test does the same: the `chdir` is process-global and restored at
    once, and nothing else runs while it holds.
    """
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    old = os.getcwd()
    os.chdir(sock_path.parent)
    try:
        listener.bind(sock_path.name)
    finally:
        os.chdir(old)
    listener.listen(1)
    return listener


def _serve_one_ping(listener: socket.socket) -> bytes:
    """Block for one connection, answer one ping. Runs off the loop."""
    conn, _ = listener.accept()
    with conn:
        data = conn.recv(4)
        conn.sendall(b"pong")
        return data


class TestTheReroute:
    async def test_reaches_a_socket_bound_the_way_nix_binds(self, tmp_path: Path) -> None:
        """The capability of issue #44: the socket file exists at a path no
        absolute `connect` can name, and the reroute carries the bytes."""
        sock_path = _a_long_socket_under(tmp_path)
        with pytest.raises(OSError, match="too long"):
            socket.socket(socket.AF_UNIX, socket.SOCK_STREAM).connect(str(sock_path))

        listener = _bind_the_way_nix_binds(sock_path)
        served: list[bytes] = []

        async def serve() -> None:
            served.append(await run_sync(_serve_one_ping, listener))

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(serve)
                link = await open_unix_connection(sock_path)
                await link.stream.send(b"ping")
                assert await UnixNixReader(link).readexactly(4) == b"pong"
                await link.stream.aclose()
        finally:
            listener.close()
        assert served == [b"ping"]

    async def test_an_ordinary_path_connects_directly(self, tmp_path: Path) -> None:
        """The negative control: the reroute changes nothing that fits."""
        sock_path = tmp_path / "sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(sock_path))
        listener.listen(1)
        served: list[bytes] = []

        async def serve() -> None:
            served.append(await run_sync(_serve_one_ping, listener))

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(serve)
                link = await open_unix_connection(sock_path)
                await link.stream.send(b"ping")
                assert await UnixNixReader(link).readexactly(4) == b"pong"
                await link.stream.aclose()
        finally:
            listener.close()
        assert served == [b"ping"]


class TestTheTestStoresFit:
    def test_the_longest_test_name_still_leaves_room(self, tmp_path: Path) -> None:
        """`tests/_conftest/fixtures.py` truncates a test's name so that the
        daemon socket of its store fits. This is that budget, measured
        against the real fixture rather than against its arithmetic."""
        socket_of_a_store = tmp_path / "store/nix/var/nix/daemon-socket/pynixd-nix"

        _refuse_a_socket_path_python_cannot_reach(socket_of_a_store)


class TestTheLimitIsReal:
    """The constant is measured against the kernel, not copied from a header.

    A number nobody checks drifts. These two run in a hundredth of a second
    and they are what makes `_SUN_PATH_MAX` a fact rather than a belief.
    """

    def test_the_kernel_refuses_one_byte_more(self, tmp_path: Path) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            with pytest.raises(OSError, match="too long"):
                sock.bind(str(_a_path_of(_SUN_PATH_MAX + 1)))
        finally:
            sock.close()

    def test_the_kernel_takes_the_limit_itself(self, tmp_path: Path) -> None:
        # Inside tmp_path, so the bind leaves nothing behind. The name is
        # padded to reach the limit exactly.
        room = _SUN_PATH_MAX - len(os.fsencode(str(tmp_path))) - 1
        if room < 1:
            pytest.skip(f"tmp_path is already {len(os.fsencode(str(tmp_path)))} bytes")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.bind(str(tmp_path / ("s" * room)))
        finally:
            sock.close()
