"""A retired connection closes after its daemon worker has exited.

A worker of `nix-daemon` holds the lock on its temporary roots file until it
exits (`gc.cc:62`), and it closes its socket only then. `FindRoots` walks
those files right after `retire_idle`, so the close waits for the peer's
end. The peer here closes a while after it reads the end of the stream, as
a worker that still tears down does.
"""

from __future__ import annotations

import anyio
import pytest

from pynixd.connection import Connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests._conftest.socket_pair import socket_pair

_TEARDOWN = 0.2


async def _closes(*, wait: bool) -> tuple[bool, bool]:
    """`(peer saw the end, peer had closed)` at the moment the close returned."""
    saw_end = anyio.Event()
    closed = anyio.Event()
    async with socket_pair() as (link, peer), anyio.create_task_group() as tg:

        async def worker() -> None:
            with pytest.raises(anyio.EndOfStream):
                await peer.receive()
            saw_end.set()
            await anyio.sleep(_TEARDOWN)
            await peer.aclose()
            closed.set()

        tg.start_soon(worker)
        conn = Connection(UnixNixReader(link), UnixNixWriter(link), "test")
        if wait:
            await conn.close_after_peer(timeout=5)
        else:
            await conn.close()
        seen = (saw_end.is_set(), closed.is_set())
    return seen


@pytest.mark.anyio
class TestCloseAfterPeer:
    async def test_the_close_waits_for_the_worker(self) -> None:
        assert await _closes(wait=True) == (True, True)

    async def test_a_plain_close_does_not(self) -> None:
        """The negative control: without the wait, the worker is still there."""
        assert (await _closes(wait=False))[1] is False
