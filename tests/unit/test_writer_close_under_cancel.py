"""A cancelled close still closes the transport. Issue #91.

A client hangup and a cancelled build both cancel through an anyio scope,
and anyio cancels every await in that scope, the cleanup's included. The
writer's close awaited the drain first, so the cancel skipped the close and
left the upstream connection open, and the daemon's work ran on.
"""

from __future__ import annotations

import anyio

from pynixd.connection import Connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests._conftest.socket_pair import socket_pair


async def _closes(writer: UnixNixWriter) -> bool:
    """Whether the transport closes. It frees the descriptor on a later loop turn, and says so through nothing a test can await."""
    with anyio.move_on_after(1):
        while not writer._transport.is_closing():
            await anyio.sleep(0.01)
    return writer._transport.is_closing()


def _stuck(writer: UnixNixWriter) -> UnixNixWriter:
    """Fill *writer* past what its peer, which never reads, can take: its drain then blocks."""
    writer.write(b"\0" * (8 << 20))
    writer.flush()
    assert writer._transport_is_dirty()
    return writer


async def test_a_cancelled_close_closes_the_transport() -> None:
    async with socket_pair() as (ours, _):
        writer = _stuck(UnixNixWriter(ours, identifier="test:stuck"))
        with anyio.CancelScope() as scope:
            scope.cancel()
            await writer.close()
        assert await _closes(writer)


async def test_a_cancelled_connection_close_reports_to_its_pool() -> None:
    async with socket_pair() as (ours, _):
        writer = _stuck(UnixNixWriter(ours, identifier="test:stuck"))
        conn = Connection(UnixNixReader(ours, identifier="test:stuck"), writer, conn_id="test:stuck")
        reported: list[Connection] = []
        conn.on_close = reported.append
        with anyio.CancelScope() as scope:
            scope.cancel()
            await conn.close()
        assert reported == [conn]
        assert await _closes(writer)
