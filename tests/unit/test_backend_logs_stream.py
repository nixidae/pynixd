"""A backend's log stream reaches the client while it is read, and is not kept.

`QueryValidPaths` with `substitute` set downloads the whole closure inside one
operation, and the daemon reports progress for every chunk it writes. pynixd
kept every one of those messages until the answer came. Measured on sg1: a
push of the vLLM closure grew pynixd to 10.4 GB, each full collection of the
garbage collector walked every kept message, the event loop stalled for 2.1 s
and then up to 9.4 s, and the liveness probe killed the process. Issue #507.
"""

from __future__ import annotations

import time
import tracemalloc
from typing import Any, cast

import anyio
import pytest

from nix_daemon_protocol.constants import STDERR_ERROR, STDERR_LAST, STDERR_RESULT
from nix_daemon_protocol.io import BytesWriter
from nix_daemon_protocol.logs import ActivityField, LogError, LogResult
from nix_daemon_protocol.protocol import FieldType, ResultType
from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest
from pynixd.connection import ClientConn, Connection
from pynixd.exceptions import BackendError
from pynixd.wire import UnixNixReader, UnixNixWriter

_MESSAGES = 100_000

_HELD_LIMIT = 16 * 1024 * 1024
"""Far above one message in flight, far below 50,000 kept ones (about 130 MiB)."""

_MAX_GAP = 0.5
"""The longest the stream may hold the event loop, in seconds.

Measured in the guest under the harness's profiler: 22 ms to 86 ms per 256
messages. Without the yield the gap is the whole stream."""


def _progress() -> bytes:
    """One `resProgress` result, as the daemon sends it for each chunk it copies."""
    w = BytesWriter()
    w.write_uint64(STDERR_RESULT)
    w.write_uint64(7)  # act_id
    w.write_uint64(ResultType.PROGRESS)
    w.write_uint64(4)
    for value in (1 << 20, 1 << 30, 1, 0):  # done, expected, running, failed
        w.write_uint64(FieldType.INT)
        w.write_uint64(value)
    return w.get_bytes()


def _error(message: str) -> bytes:
    w = BytesWriter()
    w.write_uint64(STDERR_ERROR)
    w.write_string("Error")
    w.write_uint64(0)
    w.write_string("Error")
    w.write_string(message)
    w.write_uint64(0)
    w.write_uint64(0)
    return w.get_bytes()


def _answer() -> bytes:
    w = BytesWriter()
    w.write_uint64(STDERR_LAST)
    w.write_uint64(0)  # an empty set of valid paths
    return w.get_bytes()


class _Daemon:
    """A daemon socket that makes its stream as it is read, so the test holds none of it."""

    def __init__(self, messages: int, tail: bytes) -> None:
        self.sent = 0
        self._messages = messages
        self._tail = tail
        self._one = _progress()
        self._pending = b""

    async def read(self, n: int) -> bytes:
        while len(self._pending) < n and self.sent < self._messages:
            self._pending += self._one
            self.sent += 1
        if len(self._pending) < n and self._tail:
            self._pending += self._tail
            self._tail = b""
        out, self._pending = self._pending[:n], self._pending[n:]
        return out


class _Socket:
    def __init__(self) -> None:
        self.writes = 0
        self.written = 0

    def write(self, data: bytes) -> None:
        self.writes += 1
        self.written += len(data)

    async def drain(self) -> None:
        return None


class _Client:
    """The client of the operation: counts what reaches it, and when."""

    options = None

    def __init__(self, daemon: _Daemon) -> None:
        self.daemon = daemon
        self.received = 0
        self.daemon_sent_at_first: int | None = None
        self.errors = 0
        self.held_midway: int | None = None

    async def send(self, msg: object) -> None:
        if self.daemon_sent_at_first is None:
            self.daemon_sent_at_first = self.daemon.sent
        if isinstance(msg, LogError):
            self.errors += 1
        else:
            assert isinstance(msg, LogResult)
            self.received += 1
            if self.received == _MESSAGES // 2 and tracemalloc.is_tracing():
                self.held_midway = _held()


def _held() -> int:
    """Bytes traced now, without the profiler the harness runs around every test."""
    snapshot = tracemalloc.take_snapshot().filter_traces(
        [
            tracemalloc.Filter(inclusive=False, filename_pattern="*pyinstrument*"),
            tracemalloc.Filter(inclusive=False, filename_pattern=tracemalloc.__file__),
        ],
    )
    return sum(stat.size for stat in snapshot.statistics("filename"))


def _connection(daemon: _Daemon) -> Connection:
    conn = Connection(UnixNixReader(cast("Any", daemon)), UnixNixWriter(cast("Any", _Socket())), "backend")
    conn.connected = True
    return conn


async def test_progress_streams_to_the_client_and_is_not_kept() -> None:
    daemon = _Daemon(_MESSAGES, _answer())
    client = _Client(daemon)
    conn = _connection(daemon)

    tracemalloc.start()
    try:
        resp = await conn.call(QueryValidPathsRequest(paths=set(), substitute=1), client=cast("Any", client))
    finally:
        tracemalloc.stop()

    assert client.received == _MESSAGES
    assert client.daemon_sent_at_first is not None
    assert client.daemon_sent_at_first < _MESSAGES, "the first message waited for the whole stream"
    assert resp.logs.messages == [], "a streamed message is kept as well, and the proxy sends it twice"
    assert client.held_midway is not None
    assert client.held_midway < _HELD_LIMIT, f"{client.held_midway / 2**20:.1f} MiB held halfway through the stream"


async def test_a_long_stream_lets_the_event_loop_run() -> None:
    """A socket that always has data never suspends a read, so the stream must yield itself.

    Each read here returns at once, as `StreamReader.read` does while its
    buffer holds bytes. `/healthz` and every other client wait for the loop.
    """
    daemon = _Daemon(_MESSAGES, _answer())
    conn = _connection(daemon)
    gaps: list[float] = []
    done = anyio.Event()

    async def tick() -> None:
        last = time.monotonic()
        while not done.is_set():
            await anyio.sleep(0)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    async with anyio.create_task_group() as tg:
        tg.start_soon(tick)
        await anyio.sleep(0)
        try:
            await conn.call(QueryValidPathsRequest(paths=set(), substitute=1), client=cast("Any", _Client(daemon)))
        finally:
            done.set()
    assert max(gaps) < _MAX_GAP, f"the loop waited {max(gaps):.3f} s for the stream"


async def test_an_error_ends_the_stream_once() -> None:
    """The error is not streamed: the caller reports it, so a streamed copy doubles it."""
    daemon = _Daemon(3, _error("substituter refused"))
    client = _Client(daemon)
    conn = _connection(daemon)

    with pytest.raises(BackendError, match="substituter refused"):
        await conn.call(
            QueryValidPathsRequest(paths=set(), substitute=1),
            client=cast("Any", client),
            raise_on_error=True,
        )
    assert client.received == 3
    assert client.errors == 0


def _progress_message() -> LogResult:
    return LogResult(
        act_id=7,
        result_type=ResultType.PROGRESS,
        fields=[ActivityField(type=FieldType.INT, valint=v) for v in (1 << 20, 1 << 30, 1, 0)],
    )


async def test_streamed_messages_reach_the_client_in_chunks() -> None:
    """One transport write per chunk, not one per message.

    Over SSH each write became a packet of its own, and asyncssh encrypted
    the queued packets in one callback when the window opened: the loop
    stalled for up to 10.4 s with the CPU at its limit.
    """
    socket = _Socket()
    client = ClientConn(UnixNixWriter(cast("Any", socket)))
    msg = _progress_message()
    for _ in range(_MESSAGES):
        await client.send(msg)
    await client.flush()

    assert socket.written > 0
    # Not one per chunk exactly: the linger timer also fires while the loop turns.
    assert socket.writes < _MESSAGES // 100, f"{socket.writes} writes for {_MESSAGES} messages"


async def test_a_lone_message_reaches_the_client_without_a_flush() -> None:
    """A build that prints one line a minute still shows it at once."""
    socket = _Socket()
    client = ClientConn(UnixNixWriter(cast("Any", socket)))
    await client.send(_progress_message())
    with anyio.fail_after(1):
        while socket.written == 0:
            await anyio.sleep(0.01)
