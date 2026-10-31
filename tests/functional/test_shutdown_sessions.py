from __future__ import annotations

import asyncio
from pathlib import Path
from unittest import mock

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.connection import Connection
from pynixd.proxy import DaemonProxy
from pynixd.serde import IsValidPathRequest, StorePath
from pynixd.store import LocalSocketStore
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests.conftest import make_test_spec

"""
Shutdown answers established clients (issue #64).

Closing the listeners stops new clients, but the handlers of established
ones are tasks nobody holds. Before this fix an in-flight operation
survived `Server.close` and its client waited on a dead socket. The test
below holds an operation inside dispatch, closes the server, and asserts
the client receives the shutdown error instead of hanging.
"""


async def test_shutdown_answers_in_flight_op(tmp_path: Path) -> None:
    store_path = tmp_path / "store"
    store_path.mkdir()
    socket_path = tmp_path / "pynixd.sock"

    local_store = LocalSocketStore(
        make_test_spec(store_id="local", store_path=store_path, no_probe=True),
    )

    async with Server(
        stores={StoreId("local"): local_store},
        unix_path=socket_path,
        ssh_port=None,
        http_port=None,
    ) as server:
        reader, writer = await asyncio.open_unix_connection(str(socket_path))
        conn = Connection(
            UnixNixReader(reader, identifier="test"),
            UnixNixWriter(writer, identifier="test"),
            "test-shutdown",
        )
        await conn.connect()

        entered = anyio.Event()
        release = anyio.Event()
        real_dispatch = DaemonProxy.dispatch

        async def blocking_dispatch(self: DaemonProxy, op_num: int):  # type: ignore[no-untyped-def]
            entered.set()
            await release.wait()
            return await real_dispatch(self, op_num)

        async def op() -> None:
            with pytest.raises(Exception, match="shutting down"):
                await conn.call(
                    IsValidPathRequest(path=StorePath(path="/nix/store/does-not-exist")),
                    raise_on_error=True,
                )

        with mock.patch.object(DaemonProxy, "dispatch", blocking_dispatch), anyio.fail_after(60):
            async with anyio.create_task_group() as tg:
                tg.start_soon(op)
                await entered.wait()
                await server.close()
        await conn.close()
