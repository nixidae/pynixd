"""A server listens while its other stores open; what reads the store set waits for them.

`Server.start` opened every store before its listeners, so a substituter that
could not be reached held every client for its whole open: 10.5 s on a DNS
timeout in the offline guest. Nix's daemon accepts at once and opens a
substituter on first use. nixidae/nixidae#1268.
"""

from __future__ import annotations

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd_store.store_path import StorePath
from tests.functional.mock_store import MockStore

_PATH = StorePath("/nix/store/00000000000000000000000000000000-example")


class _SlowStore(MockStore):
    """A substituter whose open waits for the test."""

    def __init__(self, release: anyio.Event) -> None:
        super().__init__("slow", feature_matrix={"x86_64-linux": set()})
        self.no_schedule = True
        self.release = release

    async def start(self, sync_paths: bool = True) -> None:
        await self.release.wait()
        await super().start(sync_paths)


@pytest.mark.anyio
async def test_the_server_starts_before_a_store_has_opened_and_a_query_waits_for_it() -> None:
    release = anyio.Event()
    slow = _SlowStore(release)
    local = MockStore("local", feature_matrix={"x86_64-linux": set()})
    server = Server(stores={StoreId("local"): local, StoreId("slow"): slow}, ssh_port=None, unix_path=None)
    with anyio.fail_after(5):
        async with server:
            queue = server.ctx.scheduler.substitution_queue if server.ctx.scheduler is not None else None
            assert queue is not None
            answered = anyio.Event()

            async def ask() -> None:
                await queue.can_substitute(_PATH)
                answered.set()

            async with anyio.create_task_group() as tg:
                tg.start_soon(ask)
                await anyio.sleep(0.2)
                assert not answered.is_set(), "the query ran before the substituter opened"
                release.set()
                await answered.wait()
