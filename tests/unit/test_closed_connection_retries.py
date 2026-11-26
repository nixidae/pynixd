"""A build on a connection that closed under it is retried.

sg1's central restarted at 09:43:46Z, and pynixd discarded every connection to
it. A build that held one of them called it after the close, and
`Connection.call` raised `RuntimeError: Connection 'sg1-46' not connected`.
The executor read that as a pynixd bug and failed the build, and Siso failed
the Chromium step with it.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue
from pynixd.connection import Connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests._conftest.execution import FakeLocal, fake_executor, queued
from tests._conftest.placement import FakeStore
from tests._conftest.socket_pair import socket_pair


@pytest.mark.anyio
async def test_a_build_on_a_closed_connection_is_retried() -> None:
    store = FakeStore(StoreId("builder1"))
    queue = BuildQueue()
    build = await queued(queue)
    async with socket_pair() as (link, _peer):
        conn = Connection(UnixNixReader(link), UnixNixWriter(link), "builder1-0")
        await conn.close()
        store.conn = cast("Any", conn)
        await fake_executor(queue, {store.store_id: store}, local=FakeLocal()).run(build, store)

    assert not build.answer.done()
    assert list(build.store_failures) == [StoreId("builder1")]
