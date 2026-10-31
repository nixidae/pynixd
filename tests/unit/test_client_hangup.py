"""A client that hangs up takes its operation and its builds with it. Issue #91.

`daemon.cc:1016` of Nix watches the client with `MonitorFdHup` for the
whole session, and a hangup interrupts whatever operation runs. On a Unix
socket a half-close is not a hangup: `POLLHUP` needs both directions shut.
Over SSH, Nix reads a pipe behind sshd, and the end of the input is the
hangup.

pynixd watched nothing. A client killed during a build left its request
running, and the request kept its reference on the build queue. Issue #87
measured a build that held its derivation for hours after its client died,
and every later request deduplicated onto it. On dynhetz 17 of 20 sessions
belonged to clients that were gone (issue #89).
"""

from __future__ import annotations

import contextlib
import socket
import sys
from typing import TYPE_CHECKING, Any

import anyio
import asyncssh
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import PynixdSettings, ScheduleMode
from pynixd.connection import Connection
from pynixd.context import PynixdContext
from pynixd.goals import engine as engine_module
from pynixd.goals.engine import GoalEngine
from pynixd.hangup import wait_for_socket_hangup
from pynixd.proxy import DaemonProxy
from pynixd.scheduler import Scheduler
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildMode,
    BuildPathsWithResultsRequest,
    IsValidPathRequest,
    StorePath as SerdeStorePath,
)
from pynixd.serde.auth import Role
from pynixd.ssh_server import start_ssh_server
from pynixd.store_path import StorePath
from pynixd.unix_socket import link_socket
from pynixd.wire import SSHNixReader, SSHNixWriter, UnixNixReader, UnixNixWriter
from tests._conftest.ssh_keys import make_ssh_test_keys
from tests.conftest import serde_path
from tests.functional.mock_store import MockStore

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from anyio.abc import TaskGroup

    from pynixd.build_queue import BuildQueue, QueuedBuild

_DRV = StorePath("/nix/store/00000000000000000000000000000001-hung-test.drv")


def _context() -> PynixdContext:
    stores = {
        StoreId("local"): MockStore("local", feature_matrix={"x86_64-linux": set()}),
        StoreId("remote"): MockStore("remote", feature_matrix={"x86_64-linux": set()}),
    }
    ctx = PynixdContext(settings=PynixdSettings(), _stores=dict(stores))
    ctx.scheduler = Scheduler(ctx)
    return ctx


def _queue_that_never_builds(ctx: PynixdContext) -> BuildQueue:
    for store in ctx.stores.values():
        assert isinstance(store, MockStore)
        store.block_build(_DRV)
    assert ctx.scheduler is not None
    return ctx.scheduler.queue


def _build_request() -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=serde_path(_DRV),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )


async def _the_subscribed_build(queue: BuildQueue) -> QueuedBuild:
    with anyio.fail_after(5):
        while not any(b.subscribers for b in queue.by_id.values()):
            await anyio.sleep(0.01)
    (build,) = queue.by_id.values()
    return build


async def test_a_cancelled_request_lets_go_of_its_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    """The request's teardown runs to the end under an anyio cancellation.

    `anyio.Lock.acquire` checks for cancellation before it takes even a free
    lock, so an unshielded `let_go_of_every_build` stops before the release
    and the build keeps a holder that no request owns.
    """
    ctx = _context()
    scheduler = ctx.scheduler
    assert scheduler is not None
    queue = scheduler.queue
    held = anyio.Event()
    taken: dict[str, Any] = {}

    class HoldingGoal:
        def __init__(self, engine: GoalEngine, request: object, client: object) -> None:
            del request, client
            self.engine = engine

        async def result(self) -> None:
            build_id, future = await queue.enqueue(
                _build_request(),
                from_goal_path=True,
                goal_request_id=self.engine.request_id,
            )
            taken.update(build=queue.by_id[build_id], future=future)
            held.set()
            await anyio.sleep_forever()

    monkeypatch.setattr(engine_module, "BuildPathsWithResultsGoal", HoldingGoal)
    engine = GoalEngine(ctx)

    async with anyio.create_task_group() as tg:
        tg.start_soon(
            engine.build_paths_with_results,
            BuildPathsWithResultsRequest(derived_paths=[], build_mode=BuildMode.NORMAL),  # type: ignore[arg-type]  # zuban-bug: start_soon with a function that has defaulted parameters
        )
        await held.wait()
        tg.cancel_scope.cancel()

    build = taken["build"]
    assert build.holders == set()
    assert taken["future"].done()
    assert "no goal waits for it" in taken["future"].result().result.error_msg
    assert engine.request_id not in queue._given_up


@contextlib.contextmanager
def _socketpair() -> Iterator[tuple[socket.socket, socket.socket]]:
    server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        yield server, client
    finally:
        server.close()
        client.close()


class _Session:
    """A `DaemonProxy` on one end of a socket pair, and a client on the other."""

    def __init__(self, server_sock: socket.socket, client_sock: socket.socket) -> None:
        self.server_sock = server_sock
        self.client_sock = client_sock
        self.ctx = _context()
        self.entered = anyio.Event()
        self.release = anyio.Event()
        self.cancelled = False

    def patch_dispatch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def blocking_dispatch(proxy: DaemonProxy, op_num: int) -> None:
            del proxy, op_num
            await self.blocking_dispatch()

        monkeypatch.setattr(DaemonProxy, "dispatch", blocking_dispatch)

    async def blocking_dispatch(self) -> None:
        self.entered.set()
        try:
            await self.release.wait()
        except anyio.get_cancelled_exc_class():
            self.cancelled = True
            raise

    async def connect(self, tg: TaskGroup) -> tuple[anyio.Event, Connection]:
        """Run the session in *tg*; the event is set when it ends."""
        served = await link_socket(self.server_sock)
        proxy = DaemonProxy(
            UnixNixReader(served, identifier="client"),
            UnixNixWriter(served, identifier="client"),
            ctx=self.ctx,
            role=Role.ADMIN,
            schedule_mode=ScheduleMode.scheduler,
            transport="unix",
            hangup=lambda: wait_for_socket_hangup(self.server_sock.fileno()),
        )
        ended = anyio.Event()

        async def session() -> None:
            try:
                await proxy.run()
            finally:
                ended.set()

        tg.start_soon(session)
        client = await link_socket(self.client_sock)
        conn = Connection(UnixNixReader(client, identifier="test"), UnixNixWriter(client, identifier="test"), "test")
        await conn.connect()
        return ended, conn

    async def start(self, tg: TaskGroup) -> tuple[anyio.Event, Connection, anyio.CancelScope]:
        ended, conn = await self.connect(tg)
        op = _call_in(tg, conn, IsValidPathRequest(path=SerdeStorePath(path=str(_DRV))))
        with anyio.fail_after(5):
            await self.entered.wait()
        return ended, conn, op


def _call_in(tg: TaskGroup, conn: Connection, request: Any) -> anyio.CancelScope:  # noqa: ANN401 -- any request `Connection.call` takes
    """Run the call in *tg* until it ends or the answered scope is cancelled."""
    scope = anyio.CancelScope()

    async def call() -> None:
        # The hangup fails the call. The tests read the session, not this.
        with scope, contextlib.suppress(Exception):
            await conn.call(request)

    tg.start_soon(call)
    return scope


async def test_a_hangup_cancels_the_operation_and_ends_the_session(monkeypatch: pytest.MonkeyPatch) -> None:
    with _socketpair() as (server_sock, client_sock):
        s = _Session(server_sock, client_sock)
        s.patch_dispatch(monkeypatch)
        async with anyio.create_task_group() as tg:
            ended, conn, _ = await s.start(tg)

            await conn.close()
            with anyio.fail_after(5):
                await ended.wait()

        assert s.cancelled
        assert len(s.ctx.sessions) == 0


async def test_a_hangup_cancels_the_build_only_that_client_wanted() -> None:
    """The real `BuildDerivation` handler gives its subscription back.

    The build is pending and nobody else subscribes, so the last unsubscribe
    cancels it. Without the watcher the session waits for the build forever.
    """
    with _socketpair() as (server_sock, client_sock):
        s = _Session(server_sock, client_sock)
        queue = _queue_that_never_builds(s.ctx)
        async with anyio.create_task_group() as tg:
            ended, conn = await s.connect(tg)
            _call_in(tg, conn, _build_request())
            build = await _the_subscribed_build(queue)

            await conn.close()
            with anyio.fail_after(5):
                await ended.wait()

        assert build.is_done
        assert not build.subscribers
        assert len(s.ctx.sessions) == 0


@pytest.mark.skipif(sys.platform != "linux", reason="kqueue's EV_EOF fires on a half-close too, as in Nix")
async def test_a_half_close_is_not_a_hangup(monkeypatch: pytest.MonkeyPatch) -> None:
    """`shutdown(SHUT_WR)` raises no `POLLHUP`, so Nix keeps the operation."""
    with _socketpair() as (server_sock, client_sock):
        s = _Session(server_sock, client_sock)
        s.patch_dispatch(monkeypatch)
        async with anyio.create_task_group() as tg:
            ended, conn, op = await s.start(tg)

            client_sock.shutdown(socket.SHUT_WR)
            await anyio.sleep(0.5)
            assert not s.cancelled
            assert not ended.is_set()

            s.release.set()
            with anyio.fail_after(5):
                await ended.wait()
            assert not s.cancelled
            op.cancel()
            await conn.close()


@pytest.mark.parametrize("hang_up", ["eof", "disconnect"])
async def test_an_ssh_client_that_goes_cancels_its_build(hang_up: str, tmp_path: Path) -> None:
    """Over SSH, the end of the channel's input is the hangup.

    `eof`: the client's ssh closes the channel input, which is what a client
    process that dies does to its ssh child. `disconnect`: the connection
    drops, and asyncssh reports the channel close.
    """
    ctx = _context()
    queue = _queue_that_never_builds(ctx)
    keys = make_ssh_test_keys(tmp_path)
    server = await start_ssh_server(ctx, port=0, schedule_mode=ScheduleMode.scheduler, admin_keys=[keys.admin_keys])
    try:
        async with asyncssh.connect(
            "127.0.0.1",
            server.get_port(),
            username="tester",
            known_hosts=None,
            client_keys=[str(keys.admin)],
            agent_path=None,
        ) as ssh:
            process = await ssh.create_process("nix-daemon --stdio", encoding=None)
            conn = Connection(
                SSHNixReader(process.stdout, identifier="test"),
                SSHNixWriter(process.stdin, identifier="test"),
                "test",
            )
            await conn.connect()
            async with anyio.create_task_group() as tg:
                op = _call_in(tg, conn, _build_request())
                build = await _the_subscribed_build(queue)

                if hang_up == "eof":
                    process.stdin.write_eof()
                else:
                    ssh.abort()

                with anyio.fail_after(5):
                    while not build.is_done or len(ctx.sessions) > 0:
                        await anyio.sleep(0.01)
                assert not build.subscribers
                op.cancel()
    finally:
        server.close()
        await server.wait_closed()
