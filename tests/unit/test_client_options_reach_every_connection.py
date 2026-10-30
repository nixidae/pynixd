"""An option of a client reaches every connection that works for that client.

`nix-daemon` holds one connection for one client, so `SetOptions` reaches
every operation of that client. pynixd holds a pool, and it sent the request
over the pool, so the option landed on whichever connection was free. A client
that passed `--post-build-hook` then saw the hook run for three of the five
derivations that its request built, because pynixd built the five at the same
time on several connections.

Issue nixidae/nixidae#231 holds the measurement, and `scratchpad/probe_posthook.py` is the
program that made it.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.ids import BuildId, RequestId
from pynixd.background import Background
from pynixd.connection import ClientConn, Connection
from pynixd.goals.build_derivation import BuildDerivationGoal
from pynixd.handlers.set_options import SetOptionsHandler
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    SetOptionsRequest,
    StorePath,
    TimeSpan,
    Verbosity,
)
from pynixd.serde.auth import Role
from pynixd.serde.context import WriteContext
from pynixd.store.pool import ConnectionPool
from pynixd.trust import TrustPolicy
from pynixd.wire import PROTOCOL_VERSION, STDERR_LAST, BytesReader, BytesWriter

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine

VERSION = PROTOCOL_VERSION


def _options(hook: str) -> SetOptionsRequest:
    """Every field of the request, because none of them has a default."""
    return SetOptionsRequest(
        keep_failed=0,
        keep_going=0,
        try_fallback=0,
        verbosity=Verbosity.ERROR,
        max_build_jobs=1,
        max_silent_time=TimeSpan(0),
        obsolete_use_build_hook=0,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=1,
        use_substitutes=1,
        overrides={"post-build-hook": hook},
    )


def _last() -> bytes:
    """The whole answer of the daemon to `SetOptions`: an empty log stream."""
    writer = BytesWriter("upstream")
    writer.write_uint64(STDERR_LAST)
    return writer.get_bytes()


async def _connection(answers: int) -> Connection:
    conn = Connection(
        BytesReader(_last() * answers, identifier="upstream"),
        BytesWriter("upstream"),
        "upstream",
    )
    conn.connected = True
    conn.version = VERSION
    return conn


@pytest.mark.anyio
async def test_a_connection_takes_the_option_set_of_the_client() -> None:
    conn = await _connection(1)

    await conn.apply_options(_options("/hook.sh"))

    assert conn.op_log == ["SetOptionsRequest"]
    assert conn.applied_options == _options("/hook.sh")


@pytest.mark.anyio
async def test_a_connection_that_carries_the_set_already_sends_nothing() -> None:
    """The common case is one client and one set, so it must cost one comparison."""
    conn = await _connection(1)
    await conn.apply_options(_options("/hook.sh"))

    await conn.apply_options(_options("/hook.sh"))

    assert conn.op_log == ["SetOptionsRequest"]


@pytest.mark.anyio
async def test_a_second_client_replaces_the_set() -> None:
    conn = await _connection(2)
    await conn.apply_options(_options("/first.sh"))

    await conn.apply_options(_options("/second.sh"))

    assert conn.op_log == ["SetOptionsRequest", "SetOptionsRequest"]
    assert conn.applied_options == _options("/second.sh")


@pytest.mark.anyio
async def test_no_option_set_leaves_the_connection_as_it_is() -> None:
    conn = await _connection(1)

    await conn.apply_options(None)

    assert conn.op_log == []


# ── The handler keeps the set, and sends nothing upstream ────────────


class FakeStore:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def call(self, request: Any) -> str:
        self.calls.append(type(request).__name__)
        return "answered"


class FakeProxy:
    def __init__(self, body: bytes) -> None:
        self.r = BytesReader(body, identifier="test:set-options")
        self.version = VERSION
        # What the client handshake negotiated. Empty is what Nix 2.34 names,
        # and every codec here has the shape that goes with it. Issue #14.
        self.standard_features: frozenset[str] = frozenset()
        self.local_store = FakeStore()
        self.client = ClientConn(BytesWriter("client"))
        self.ctx = SimpleNamespace(trust=TrustPolicy())


@dataclass
class FakeContext:
    proxy: FakeProxy
    role: Role
    version: int = VERSION
    username: str = "test"


async def _body(request: SetOptionsRequest) -> bytes:
    """The request as the dispatcher hands it over: no operation number."""
    writer = BytesWriter("test")
    await request.to_writer(WriteContext(writer=writer, version=VERSION))
    return writer.get_bytes()[8:]


@pytest.mark.anyio
async def test_the_handler_keeps_the_set_on_the_session() -> None:
    ctx = FakeContext(FakeProxy(await _body(_options("/hook.sh"))), Role.ADMIN)

    await SetOptionsHandler().handle(cast("Any", ctx))

    assert ctx.proxy.client.options == _options("/hook.sh")


@pytest.mark.anyio
async def test_the_handler_sends_nothing_over_the_pool() -> None:
    """A connection of the pool is not the connection that does the work."""
    ctx = FakeContext(FakeProxy(await _body(_options("/hook.sh"))), Role.ADMIN)

    await SetOptionsHandler().handle(cast("Any", ctx))

    assert ctx.proxy.local_store.calls == []


@pytest.mark.anyio
async def test_an_untrusted_user_keeps_the_set_without_its_restricted_settings() -> None:
    """`daemon.cc:297`: `post-build-hook` is restricted, and the fixed fields stay. Issue #56."""
    ctx = FakeContext(FakeProxy(await _body(_options("/hook.sh"))), Role.USER)

    await SetOptionsHandler().handle(cast("Any", ctx))

    options = ctx.proxy.client.options
    assert options is not None
    assert options.overrides == {}
    assert options.max_build_jobs == 1


# ── The build carries the set of the client that asked for it ────────


class FakeScheduler:
    def __init__(self) -> None:
        self.options: SetOptionsRequest | None = None
        self.future: asyncio.Future[BuildDerivationResponse] | None = None
        self.started = anyio.Event()
        # No queued build, so a holder waits on the future alone.
        self.queue = SimpleNamespace(by_id={})

    async def build_derivation(
        self,
        request: BuildDerivationRequest,
        from_goal_path: bool = False,
        goal_request_id: RequestId | None = None,
        options: SetOptionsRequest | None = None,
        delegated: bool = False,
    ) -> tuple[BuildId, asyncio.Future[BuildDerivationResponse]]:
        del request, from_goal_path, goal_request_id, delegated
        self.options = options
        self.future = asyncio.get_running_loop().create_future()
        self.started.set()
        return BuildId(1), self.future


class FakeCtx:
    def __init__(self, scheduler: FakeScheduler) -> None:
        self.scheduler = scheduler


class FakeEngine:
    def __init__(self, scheduler: FakeScheduler) -> None:
        self.ctx = FakeCtx(scheduler)
        # The engine is the request, as far as the build queue is concerned.
        # Issue nixidae/nixidae#325.
        self.request_id = RequestId(1)

    async def subscribe_build(self, build_id: BuildId, client: ClientConn) -> bool:
        del build_id, client
        return True

    async def unsubscribe_build(self, build_id: BuildId, client: ClientConn) -> None:
        del build_id, client


def _build_request() -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path="/nix/store/00000000000000000000000000000001-test.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )


@pytest.mark.anyio
async def test_the_build_carries_the_set_of_the_client_that_asked() -> None:
    """A build runs after the request returned, so the queue keeps the set."""
    scheduler = FakeScheduler()
    goal = BuildDerivationGoal(cast("GoalEngine", FakeEngine(scheduler)), _build_request())
    client = ClientConn(BytesWriter("client"))
    client.options = _options("/hook.sh")
    await goal.subscribe(client)

    task = asyncio.create_task(goal.result())
    await scheduler.started.wait()
    future = scheduler.future
    if future is None:
        raise RuntimeError("the scheduler made no future")
    future.set_result(BuildDerivationResponse(result=BuildResult(status=BuildResultStatus.BUILT)))
    await task

    assert scheduler.options == _options("/hook.sh")


# ── A connection carries one option set for its whole life ───────────


class FakeIo:
    bytes_read = 0
    bytes_written = 0

    async def is_dirty(self) -> bool:
        return False

    def peer_closed(self) -> bool:
        return False

    async def close(self) -> None:
        return None


class PooledConnection:
    """Enough of `Connection` for the rules of the pool."""

    def __init__(self, conn_id: str, options: SetOptionsRequest | None) -> None:
        self.id = conn_id
        self.dirty = False
        self.op_log: list[str] = []
        self.opened_at = 0.0
        self.closed = False
        self.applied_options = options
        self.r = FakeIo()
        self.w = FakeIo()

    async def close(self) -> None:
        self.closed = True


def _pool() -> ConnectionPool:
    async def factory() -> PooledConnection:
        return PooledConnection("fresh", None)

    return ConnectionPool(
        store_id="test",
        factory=cast("Any", factory),
        background=Background("test"),
        max_lifetime=0.0,
        idle_ttl=10_000.0,
    )


def _idle(pool: ConnectionPool, conn: PooledConnection) -> None:
    pool.idle_conns.append((cast("Any", conn), time.monotonic()))
    pool.all_conns.append(cast("Any", conn))


@pytest.mark.anyio
async def test_the_pool_discards_a_connection_that_carries_another_set() -> None:
    """`SetOptions` adds to the set of a connection, and takes nothing away.

    A client built with `--auto-optimise-store`, and the next client got a
    store that optimises when it asked for none. `main:optimise-store` reads
    exactly that.
    """
    pool = _pool()
    stale = PooledConnection("stale", _options("/first.sh"))
    _idle(pool, stale)

    handed = await pool.get_or_create_conn(_options("/second.sh"))

    assert stale.closed
    assert handed is not stale  # type: ignore[comparison-overlap]  # the pool hands back the fake PooledConnection; zuban types it as Connection
    assert stale not in pool.all_conns


@pytest.mark.anyio
async def test_the_pool_reuses_a_connection_that_carries_the_same_set() -> None:
    pool = _pool()
    same = PooledConnection("same", _options("/hook.sh"))
    _idle(pool, same)

    handed = await pool.get_or_create_conn(_options("/hook.sh"))

    assert handed is same  # type: ignore[comparison-overlap]  # the pool hands back the fake PooledConnection; zuban types it as Connection
    assert not same.closed


@pytest.mark.anyio
async def test_the_pool_reuses_a_connection_that_carries_no_set() -> None:
    """A fresh connection takes the set of whoever needs it first."""
    pool = _pool()
    fresh = PooledConnection("fresh", None)
    _idle(pool, fresh)

    handed = await pool.get_or_create_conn(_options("/hook.sh"))

    assert handed is fresh  # type: ignore[comparison-overlap]  # the pool hands back the fake PooledConnection; zuban types it as Connection


@pytest.mark.anyio
async def test_a_second_set_adds_to_the_first() -> None:
    """`ClientSettings::apply` sets only the names it is given (`daemon.cc:221-300`). nixidae/nixidae#1009."""
    ctx = FakeContext(FakeProxy(await _body(_options("/hook.sh"))), Role.ADMIN)
    await SetOptionsHandler().handle(cast("Any", ctx))
    second = _options("/hook.sh").model_copy(update={"overrides": {"timeout": "5"}})
    ctx.proxy.r = BytesReader(await _body(second), identifier="test:set-options-2")

    await SetOptionsHandler().handle(cast("Any", ctx))

    options = ctx.proxy.client.options
    assert options is not None
    assert options.overrides == {"post-build-hook": "/hook.sh", "timeout": "5"}
