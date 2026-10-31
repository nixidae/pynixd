"""pynixd against a daemon link that breaks. Issue #93.

A `FaultProxy` sits between pynixd's local store and a real nix-daemon and
breaks the link on request. For each fault the client must get an answer --
an error is an answer -- within a bound, and the next operation must work:
a broken connection leaves the pool, and pynixd keeps serving.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

import anyio
import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.connection import Connection
from pynixd.serde import IsValidPathRequest, StorePath
from pynixd.state import upstream_in_flight
from pynixd.store import LocalSocketStore
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests._conftest.fault_proxy import Fault, FaultProxy
from tests.conftest import STORE_PREFIX, make_test_spec, rmtree_robust

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

_ABSENT = "/nix/store/00000000000000000000000000000000-absent"
_ANSWER_WITHIN = 10
"""Seconds a client may wait for any answer, error or not."""
_READ_TIMEOUT = 3
"""The local store's `read_timeout`, short so a stalled read ends inside the bound."""


def _sample(name: str, **labels: str) -> float | None:
    return REGISTRY.get_sample_value(name, labels)


@dataclass
class _Rig:
    server: Server
    proxy: FaultProxy
    client_socket: Path

    async def ask(self) -> bool:
        """One IsValidPath through pynixd, which forwards it to the daemon."""
        r, w = await asyncio.open_unix_connection(str(self.client_socket))
        conn = Connection(UnixNixReader(r, identifier="t"), UnixNixWriter(w, identifier="t"), "t")
        try:
            await conn.connect()
            response = await conn.call(IsValidPathRequest(path=StorePath(path=_ABSENT)), raise_on_error=True)
            return bool(response.valid)
        finally:
            await conn.close()

    async def ask_within_bound(self) -> BaseException | bool:
        with anyio.fail_after(_ANSWER_WITHIN):
            try:
                return await self.ask()
            except Exception as ex:  # noqa: BLE001 -- an error is an answer here
                return ex


@pytest.fixture
async def rig() -> AsyncIterator[_Rig]:
    root = STORE_PREFIX / "upstream-faults"
    rmtree_robust(root)
    root.mkdir(parents=True)
    daemon = LocalSocketStore(make_test_spec(store_id="faults-daemon", store_path=root / "store", no_probe=True))
    async with daemon:
        proxy = FaultProxy(root / "proxy.sock", daemon.socket_path)
        async with proxy.running():
            await proxy.start()
            local = LocalSocketStore(
                make_test_spec(
                    store_id="local",
                    store_path=root / "store",
                    no_probe=True,
                    managed=False,
                    socket_path=root / "proxy.sock",
                    read_timeout=_READ_TIMEOUT,
                )
            )
            client_socket = root / "pynixd.sock"
            async with Server(
                stores={StoreId("local"): local},
                unix_path=client_socket,
                ssh_port=None,
                http_port=None,
            ) as server:
                rig = _Rig(server, proxy, client_socket)
                # Warm the pool, so an armed fault lands mid-operation and not
                # in a handshake.
                assert await rig.ask() is False
                yield rig


# CORRUPT: flipped bytes read as a huge length, and pynixd waits for bytes the
# daemon never sends -- a stall by another road, which `read_timeout` ends.
@pytest.mark.parametrize("fault", [Fault.CLOSE, Fault.RESET, Fault.CORRUPT])
async def test_a_broken_response_fails_the_operation_and_the_next_one_works(rig: _Rig, fault: Fault) -> None:
    rig.proxy.arm(fault)

    answer = await rig.ask_within_bound()

    assert rig.proxy.triggered.is_set(), "the operation never reached the daemon"
    assert isinstance(answer, BaseException), f"{fault.value}: the client got {answer!r} from a broken link"
    assert await rig.ask_within_bound() is False


async def test_links_that_die_while_idle_cost_the_next_client_nothing(rig: _Rig) -> None:
    """A daemon restart closes every pooled link. The pool reconnects."""
    rig.proxy.cut_every_link()
    await anyio.sleep(0.1)

    assert await rig.ask_within_bound() is False


async def test_a_daemon_that_is_gone_fails_fast_and_pynixd_recovers(rig: _Rig) -> None:
    rig.proxy.cut_every_link()
    await rig.proxy.stop_listening()

    answer = await rig.ask_within_bound()
    assert isinstance(answer, BaseException)

    await rig.proxy.start()
    assert await rig.ask_within_bound() is False


_REPLY_BYTES = 16
"""`IsValidPath` of an absent path: `STDERR_LAST` and the answer, 8 bytes each."""


async def test_a_daemon_that_drips_its_answer_inside_the_bound_is_alive(rig: _Rig) -> None:
    """Silence counts from the last byte, so a slow daemon reads as busy, not silent."""
    rig.proxy.drip_delay = 0.15
    rig.proxy.arm(Fault.DRIP)
    answers: list[BaseException | bool] = []
    async with anyio.create_task_group() as tg:

        async def ask() -> None:
            answers.append(await rig.ask_within_bound())

        tg.start_soon(ask)
        await anyio.sleep(1.0)
        (row,) = upstream_in_flight(rig.server.ctx)
        assert row.op_s >= 0.9
        assert row.silent_s < 0.5

    assert rig.proxy.triggered.is_set()
    assert answers == [False]


async def test_a_drip_that_runs_past_the_bound_is_cut_like_a_stall(rig: _Rig) -> None:
    """`read_timeout` bounds the whole read, not the silence: a pure database
    read that takes longer than the bound ends, however lively the daemon."""
    rig.proxy.drip_delay = 2 * _READ_TIMEOUT / _REPLY_BYTES
    rig.proxy.arm(Fault.DRIP)

    answer = await rig.ask_within_bound()

    assert isinstance(answer, BaseException)
    assert f"within {_READ_TIMEOUT} s" in str(answer)
    assert await rig.ask_within_bound() is False


async def test_a_read_the_daemon_stops_answering_shows_then_fails_and_the_next_one_works(rig: _Rig) -> None:
    """`read_timeout` bounds a pure database read. Before it fires, `pynixd
    state` names the operation and how long the daemon has been silent."""
    timeouts_before = _sample("pynixd_upstream_read_timeouts_total", store_id="local") or 0
    rig.proxy.arm(Fault.STALL)
    answers: list[BaseException | bool] = []
    async with anyio.create_task_group() as tg:

        async def ask() -> None:
            answers.append(await rig.ask_within_bound())

        tg.start_soon(ask)
        await anyio.sleep(_READ_TIMEOUT / 2)
        (row,) = upstream_in_flight(rig.server.ctx)
        assert row.op == "IsValidPathRequest"
        assert row.silent_s >= 1
        # The same, as a Prometheus scrape sees it.
        assert _sample("pynixd_upstream_ops_waiting", store_id="local") == 1
        assert (_sample("pynixd_upstream_silence_seconds_max", store_id="local") or 0) >= 1

    assert _sample("pynixd_upstream_read_timeouts_total", store_id="local") == timeouts_before + 1
    assert rig.proxy.triggered.is_set()
    (answer,) = answers
    assert isinstance(answer, BaseException)
    assert f"within {_READ_TIMEOUT} s" in str(answer)
    assert upstream_in_flight(rig.server.ctx) == []
    assert await rig.ask_within_bound() is False
