"""A client that joins somebody else's build is told that is what it is doing.

Nix opens an `actBuildWaiting` activity at `lvlWarn` when it cannot take the
output locks of a derivation, and polls until they are free:
`src/libstore/build/derivation-building-goal.cc:421-426`. The message names
the lock files.

pynixd does not lock; it dedupes. `BuildQueue.enqueue` keys `_by_path` on the
derivation path and hands the second caller the first caller's future, and
`QueuedBuild.add_subscriber` replays the first builder's log to it. So the
second client read the output of a build it did not start, with no line
saying why. Issue #25.

This stands for `build.sh:167` of the Nix functional suite, which counts the
`error:` lines of `nix build -f fod-failing.nix -j1 -L` and expects one. The
activity added here carries no `error:` line, and the count has to stay one.
"""

from __future__ import annotations

import contextlib
import os
import time
from typing import TYPE_CHECKING, cast

import anyio
import pytest

from nix_daemon_protocol.constants import STDERR_START_ACTIVITY, STDERR_STOP_ACTIVITY
from nix_daemon_protocol.ids import BuildId
from nix_daemon_protocol.protocol import ActivityType, Verbosity
from pynixd.activity import next_activity_id
from pynixd.answer import Answer
from pynixd.build_queue import QueuedBuild
from pynixd_store.store_path import StorePath
from tests._conftest.execution import starts_in

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pynixd.connection import ClientConn
    from pynixd.serde import BuildDerivationRequest

OUT = "/nix/store/00000000000000000000000000000000-foo"
DRV = "/nix/store/11111111111111111111111111111111-foo.drv"

# Field positions of `LogStartActivity`, counting its code as zero. Every one
# of them is a 64-bit little-endian integer, and `text` follows them.
ACT_ID = 1
LEVEL = 2
TYPE = 3


class FakeDerivation:
    def __init__(self, outputs: dict[str, StorePath]) -> None:
        self._outputs = outputs

    def output_paths(self) -> dict[str, StorePath]:
        return self._outputs


class FakeRequest:
    def __init__(self, outputs: dict[str, StorePath]) -> None:
        self.drv_path = StorePath(DRV)
        self.derivation = FakeDerivation(outputs)


class FakeClient:
    def __init__(self) -> None:
        self.blocks: list[bytes] = []

    async def send_raw(self, raw: bytes) -> None:
        self.blocks.append(raw)


@contextlib.asynccontextmanager
async def _build(outputs: dict[str, StorePath] | None = None) -> AsyncIterator[QueuedBuild]:
    async with anyio.create_task_group() as tg:
        yield QueuedBuild(
            build_id=BuildId(1),
            request=cast("BuildDerivationRequest", FakeRequest(outputs or {"out": StorePath(OUT)})),
            answer=Answer(),
            start_sender=starts_in(tg),
        )
        tg.cancel_scope.cancel()


def _codes(client: FakeClient) -> list[int]:
    """The message code of each block the client received.

    Every log message puts its code first, as a 64-bit little-endian integer.
    """
    return [_read_int(block, 0) for block in client.blocks if block]


@contextlib.asynccontextmanager
async def _two_clients(
    outputs: dict[str, StorePath] | None = None,
) -> AsyncIterator[tuple[FakeClient, FakeClient, QueuedBuild]]:
    async with _build(outputs) as build:
        first, second = FakeClient(), FakeClient()
        await build.add_subscriber(cast("ClientConn", first))
        await build.add_subscriber(cast("ClientConn", second))
        await build.log_sent(cast("ClientConn", first))
        await build.log_sent(cast("ClientConn", second))
        yield first, second, build


class TestTheSecondClient:
    @pytest.mark.anyio
    async def test_reads_a_build_waiting_activity(self) -> None:
        async with _two_clients() as (_, second, _):
            assert STDERR_START_ACTIVITY in _codes(second)

    @pytest.mark.anyio
    async def test_reads_it_before_the_replay(self) -> None:
        async with _build() as build:
            build.post_log_bytes(b"a line of the first builder\n")
            first, second = FakeClient(), FakeClient()

            await build.add_subscriber(cast("ClientConn", first))
            await build.add_subscriber(cast("ClientConn", second))
            await build.log_sent(cast("ClientConn", second))

            assert _codes(second)[0] == STDERR_START_ACTIVITY
            assert len(second.blocks) == 2, "the activity, then the replay"

    @pytest.mark.anyio
    async def test_the_activity_carries_nixs_type_and_level(self) -> None:
        async with _two_clients() as (_, second, _):
            block = second.blocks[0]
            assert _read_int(block, LEVEL) == Verbosity.WARN
            assert _read_int(block, TYPE) == ActivityType.BUILD_WAITING

    @pytest.mark.anyio
    async def test_it_stops_when_the_build_ends(self) -> None:
        async with _two_clients() as (_, second, build):
            before = len(second.blocks)

            await build.stop_waiting_activities()

            assert _codes(second)[before:] == [STDERR_STOP_ACTIVITY]

    @pytest.mark.anyio
    async def test_the_stop_names_the_activity_that_started(self) -> None:
        async with _two_clients() as (_, second, build):
            started = _read_int(second.blocks[0], ACT_ID)

            await build.stop_waiting_activities()

            assert _read_int(second.blocks[-1], ACT_ID) == started


class TestTheFirstClient:
    @pytest.mark.anyio
    async def test_reads_no_such_activity(self) -> None:
        """It asked for the build. Nix tells nobody they are waiting for a
        lock they already hold."""
        async with _two_clients() as (first, _, _):
            assert STDERR_START_ACTIVITY not in _codes(first)

    @pytest.mark.anyio
    async def test_reads_none_when_its_build_started_before_it_subscribed(self) -> None:
        """A local build can start between `enqueue` and the asker's subscription.

        Nix's `cli-characterisation/nix-env-install-bad-oti-elem.err.exp` and
        `gc-concurrent.sh:19` read no `waiting for lock` there. nixidae/nixidae#1233.
        """
        async with _build() as build:
            first, second = FakeClient(), FakeClient()
            build.askers.add(cast("ClientConn", first))
            build.started_at = time.monotonic()
            build.post_log_bytes(b"building\n")

            await build.add_subscriber(cast("ClientConn", first))
            await build.add_subscriber(cast("ClientConn", second))
            await build.log_sent(cast("ClientConn", first))
            await build.log_sent(cast("ClientConn", second))

            assert STDERR_START_ACTIVITY not in _codes(first)
            assert _codes(second)[0] == STDERR_START_ACTIVITY

    @pytest.mark.anyio
    async def test_gets_no_stop_either(self) -> None:
        async with _two_clients() as (first, _, build):
            await build.stop_waiting_activities()

            assert STDERR_STOP_ACTIVITY not in _codes(first)


class TestTheMessage:
    @pytest.mark.anyio
    async def test_names_the_output_path(self) -> None:
        async with _two_clients() as (_, second, _):
            assert OUT.encode() in second.blocks[0]

    @pytest.mark.anyio
    async def test_falls_back_to_the_drv_and_the_output_name(self) -> None:
        """A floating content-addressed output has no path until it is built.
        `derivation-building-goal.cc:413-416` locks `<drv>.<outputName>`
        there, and this says the same."""
        async with _two_clients({"out": StorePath("")}) as (_, second, _):
            assert f"{DRV}.out".encode() in second.blocks[0]

    @pytest.mark.anyio
    async def test_reads_like_nixs_own(self) -> None:
        async with _two_clients() as (_, second, _):
            assert b"waiting for lock on" in second.blocks[0]


class TestTheActivityId:
    def test_it_carries_this_process(self) -> None:
        """`logging.cc:208` puts the pid in the high half so two daemons
        cannot answer with the same id. pynixd forwards a backend's ids
        unchanged, so its own have to miss that space."""
        assert next_activity_id() >> 32 == os.getpid()

    def test_two_of_them_differ(self) -> None:
        assert next_activity_id() != next_activity_id()


def _read_int(block: bytes, index: int) -> int:
    """Field `index` of a wire message, counting the code as field zero."""
    start = index * 8
    return int.from_bytes(block[start : start + 8], "little")
