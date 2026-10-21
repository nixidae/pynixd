"""A garbage collection closes the idle connections before it starts.

A worker of `nix-daemon` holds a temporary root for each path that it took,
and it releases those roots when it exits. pynixd pools its connection to the
local store, so the worker under an idle connection stays alive and keeps
holding. The collector then reads a root that no client asked for, and frees
nothing.

`nix-daemon` has no such connection: the client that made the root is gone, so
`readTempRoots` finds a stale file and removes it.

The host probe of the stream mode found this. `nix store add-file` and then
`nix store gc` deleted the file against `nix-daemon` and deleted nothing
against pynixd, and the two recordings differed in `paths_deleted` and in
`bytes_freed`.

**The rule narrows the window and does not close it.** A connection in flight
keeps its roots, another client may take a connection and add a root a moment
later, and a client that reaches the store with no pynixd in the path never
asks pynixd anything. Issue #20 holds the answer that closes it: one upstream
connection for each client session, closed with the session.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import pytest
from test_pool_lifetime import FakeConnection, make_pool

from pynixd.daemon_extensions.protocol import PynixdGCAction
from pynixd.daemon_extensions.pynixd_collect_garbage import PynixdCollectGarbageRequest
from pynixd.gc import Collector
from pynixd.handlers.collect_garbage import CollectGarbageHandler
from pynixd.handlers.pynixd_collect_garbage import PynixdCollectGarbageHandler
from pynixd.serde import CollectGarbageRequest
from pynixd.serde.auth import Role
from pynixd.serde.context import WriteContext
from pynixd.serde.protocol import GCAction
from pynixd.store.base import Store
from pynixd.store.daemon import DaemonStore
from pynixd.wire import BytesReader, BytesWriter

VERSION = 0x126


@pytest.mark.anyio
async def test_retire_idle_closes_each_idle_connection():
    pool = make_pool(300.0)
    now = time.monotonic()
    conns = [FakeConnection("a"), FakeConnection("b")]
    for conn in conns:
        pool.idle_conns.append((conn, now))
        pool.all_conns.append(conn)

    closed = await pool.retire_idle()

    assert closed == 2
    assert all(conn.closed for conn in conns)
    assert pool.idle_conns == []
    assert pool.all_conns == []


@pytest.mark.anyio
async def test_retire_idle_leaves_a_connection_in_flight():
    """A build holds its paths, and issue #20 accepts that difference."""
    pool = make_pool(300.0)
    busy = FakeConnection("busy")
    pool.all_conns.append(busy)

    closed = await pool.retire_idle()

    assert closed == 0
    assert not busy.closed
    assert pool.all_conns == [busy]


@pytest.mark.anyio
async def test_a_store_that_pools_nothing_answers_zero():
    """The default of `Store`. A store with no pool holds no such root."""

    class NoPool:
        retire_idle_connections = Store.retire_idle_connections

    assert await NoPool().retire_idle_connections() == 0


class FakeStore:
    """A local store that counts what the handler asked it, and in what order."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def retire_idle_connections(self) -> int:
        self.calls.append("retire")
        return 1

    async def call(self, request: Any) -> str:
        self.calls.append(f"call:{type(request).__name__}")
        return "answered"

    async def execute(self, request: Any) -> str:
        """The road through `DaemonStore.collect_garbage`, which forgets what it deleted."""
        self.calls.append(f"execute:{type(request).__name__}")
        return "answered"


class FakeProxy:
    def __init__(self, body: bytes) -> None:
        self.r = BytesReader(body, identifier="test:gc")
        self.version = VERSION
        # What the client handshake negotiated. Empty is what Nix 2.34 names,
        # and every codec here has the shape that goes with it. Issue #14.
        self.standard_features: frozenset[str] = frozenset()
        self.local_store = FakeStore()
        self.errors: list[str] = []
        self.ctx: Any = None
        """What `Collector` is built from. The collector itself is patched."""
        self.client: Any = None
        """No downstream client: the collector buffers its lines only."""

    async def send_error(self, message: str) -> None:
        self.errors.append(message)


@dataclass
class FakeContext:
    proxy: FakeProxy
    role: Role
    version: int = VERSION
    username: str = "test"


async def _body(request: Any) -> bytes:
    """The request as the dispatcher hands it over: no operation number."""
    writer = BytesWriter("test")
    await request.to_writer(WriteContext(writer=writer, version=VERSION))
    return writer.get_bytes()[8:]


def _request() -> CollectGarbageRequest:
    return CollectGarbageRequest(
        action=GCAction.DELETE_DEAD,
        ignore_liveness=0,
        max_freed=2**63 - 1,
        obsolete1=0,
        obsolete2=0,
        obsolete3=0,
    )


async def _handle(handler: Any, request: Any, role: Role) -> FakeProxy:
    proxy = FakeProxy(await _body(request))
    await handler.handle(FakeContext(proxy=proxy, role=role))  # type: ignore[arg-type] -- a fake context
    return proxy


@pytest.mark.anyio
async def test_a_garbage_collection_retires_the_idle_connections_first():
    proxy = await _handle(CollectGarbageHandler(), _request(), Role.ADMIN)

    # `execute`, so the cached info of each deleted path is forgotten.
    # nixidae/nixidae#1007.
    assert proxy.local_store.calls == ["retire", "execute:CollectGarbageRequest"]


@pytest.mark.anyio
async def test_an_untrusted_client_collects_as_well():
    """Nix lets every allowed user collect garbage, `daemon.cc:735`. Issue #56."""
    proxy = await _handle(CollectGarbageHandler(), _request(), Role.USER)

    assert proxy.local_store.calls == ["retire", "execute:CollectGarbageRequest"]
    assert not proxy.errors


@pytest.mark.anyio
async def test_a_query_of_the_roots_retires_them_as_well():
    """`nix-store -q --roots` prints `{temp:NNN}` for a root that a worker holds.

    `gc.sh:16` of the functional tests of Nix compares that output with one
    line, so a root of pynixd there fails the test.
    """

    class FakeDaemonStore:
        find_roots = DaemonStore.find_roots

        def __init__(self) -> None:
            self.calls: list[str] = []

        async def retire_idle_connections(self) -> int:
            self.calls.append("retire")
            return 1

        async def call(self, request: Any, client: Any = None, suppress_last: bool = False) -> str:
            del request, client, suppress_last
            self.calls.append("call")
            return "answered"

    store = FakeDaemonStore()

    assert await store.find_roots(object()) == "answered"
    assert store.calls == ["retire", "call"]


@pytest.mark.anyio
async def test_the_pynixd_operation_reaches_the_collector():
    """Op 101 is pynixd's own, and the `nix daemon` under the local store
    answers `invalid operation 101` (`daemon.cc:1110`). The handler runs the
    collector, which retires the idle connections before it deletes."""
    request = PynixdCollectGarbageRequest(action=PynixdGCAction.EXECUTE)
    proxy = FakeProxy(await _body(request))
    seen: list[PynixdGCAction] = []

    async def run(self: Any, action: PynixdGCAction, **kwargs: Any) -> str:
        del self
        seen.append(action)
        assert kwargs == {"client": None, "limit": None}
        return "collected"

    with patch.object(Collector, "run", run):
        answer = await PynixdCollectGarbageHandler().handle(
            FakeContext(proxy=proxy, role=Role.ADMIN),  # type: ignore[arg-type] -- a fake context
        )

    assert answer == "collected"
    assert seen == [PynixdGCAction.EXECUTE]
    assert proxy.local_store.calls == []
