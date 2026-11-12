"""The client socket exists while the local store still opens; a client that connects then is served after.

nix-daemon binds its socket before it opens a store. Nix's functional suite
waits 6 s for the socket (`tests/functional/common/functions.sh:82`), and
pynixd's store work was 0.3-0.4 s of a 1.5 s start in the nixft guest.
nixidae/nixidae#1302.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio

from nix_daemon_protocol.constants import WORKER_MAGIC_1, WORKER_MAGIC_2
from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from tests.functional.mock_store import MockStore

if TYPE_CHECKING:
    from pathlib import Path


class _HeldStore(MockStore):
    """A local store whose start notes whether the socket is there, then waits for the test."""

    def __init__(self, socket_path: Path) -> None:
        super().__init__("local", feature_matrix={"x86_64-linux": set()})
        self.socket_path = socket_path
        self.holding = anyio.Event()
        self.release = anyio.Event()
        self.socket_at_start: bool | None = None

    async def start(self, sync_paths: bool = True) -> None:
        self.socket_at_start = await anyio.Path(self.socket_path).is_socket()
        self.holding.set()
        await self.release.wait()
        await super().start(sync_paths)


async def test_a_client_that_connects_during_the_store_start_is_served_after_it(tmp_path: Path) -> None:
    socket_path = tmp_path / "pynixd.sock"
    local = _HeldStore(socket_path)
    server = Server(stores={StoreId("local"): local}, ssh_port=None, http_port=None, unix_path=socket_path)
    served = anyio.Event()
    with anyio.fail_after(10):
        async with anyio.create_task_group() as tg:

            async def run() -> None:
                async with server:
                    await served.wait()

            tg.start_soon(run)
            await local.holding.wait()
            assert local.socket_at_start, "the socket was not bound when the local store started"

            async with await anyio.connect_unix(socket_path) as client:
                await client.send(WORKER_MAGIC_1.to_bytes(8, "little"))
                with anyio.move_on_after(0.2):
                    early = await client.receive(8)
                    raise AssertionError(f"a client was answered before the store started: {early!r}")
                local.release.set()
                assert await client.receive(8) == WORKER_MAGIC_2.to_bytes(8, "little")
            served.set()
