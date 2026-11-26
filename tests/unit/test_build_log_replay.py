"""How a build's log reaches its subscribers: each from its own offset, none waiting for another.

One client subscribes many times to one build, and the replay runs once. A client asks for a build as a root goal
and again as the input derivation of another goal; a replay for each printed the error of one build two or three
times. `build.sh:167` of the Nix functional suite counts the `error:` lines of `nix build -f fod-failing.nix -j1 -L`
and expects one. Issue nixidae/nixidae#235.

A post never waits for a client, so a client that stops reading stalls neither the build nor another client. A log
sent only when the build ends gives that too, at a cost: on the Chromium build of nixidae/nixidae#1632 the client
read no line in 70 minutes, and pynixd held 231,489 messages as objects.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.ids import BuildId
from pynixd.answer import Answer
from pynixd.build_queue import QueuedBuild
from tests._conftest.execution import starts_in

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pynixd.connection import ClientConn
    from pynixd.serde import BuildDerivationRequest, BuildDerivationResponse


class FakeClient:
    """A client that keeps every block of bytes that the build sends it."""

    def __init__(self, raises: BaseException | None = None, until: anyio.Event | None = None) -> None:
        self.blocks: list[bytes] = []
        self.raises = raises
        self.until = until

    async def send_raw(self, raw: bytes) -> None:
        if self.raises is not None:
            raise self.raises
        if self.until is not None:
            await self.until.wait()
        self.blocks.append(raw)

    def received(self) -> bytes:
        return b"".join(self.blocks)


class _Derivation:
    def output_paths(self) -> dict[str, str]:
        return {}


class _Request:
    """`add_subscriber` reads the derivation to name what a second client is
    waiting on (issue #25), so `object()` is no longer enough here."""

    drv_path = "/nix/store/00000000000000000000000000000000-foo.drv"
    derivation = _Derivation()


@contextlib.asynccontextmanager
async def _build() -> AsyncIterator[QueuedBuild]:
    async with anyio.create_task_group() as tg:
        yield QueuedBuild(
            build_id=BuildId(1),
            request=cast("BuildDerivationRequest", _Request()),
            answer=Answer(),
            start_sender=starts_in(tg),
        )
        tg.cancel_scope.cancel()


def _as_client(client: FakeClient) -> ClientConn:
    return cast("ClientConn", client)


@pytest.mark.anyio
async def test_the_second_subscription_replays_nothing() -> None:
    async with _build() as build:
        build.post_log_bytes(b"error: hash mismatch\n")
        client: Any = FakeClient()

        await build.add_subscriber(_as_client(client))
        await build.add_subscriber(_as_client(client))
        await build.log_sent(_as_client(client))

        assert client.received() == b"error: hash mismatch\n"


@pytest.mark.anyio
async def test_the_second_subscription_still_counts() -> None:
    """The build must stay subscribed until each goal lets it go."""
    async with _build() as build:
        client: Any = FakeClient()

        await build.add_subscriber(_as_client(client))
        await build.add_subscriber(_as_client(client))

        assert await build.remove_subscriber(_as_client(client)) is True
        assert build.subscribers == [client]
        assert await build.remove_subscriber(_as_client(client)) is True
        assert build.subscribers == []


@pytest.mark.anyio
async def test_the_client_appears_once_in_the_fan_out_list() -> None:
    async with _build() as build:
        client: Any = FakeClient()

        await build.add_subscriber(_as_client(client))
        await build.add_subscriber(_as_client(client))

        assert build.subscribers == [client]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error",
    [
        # uvloop, for a write that starts after the loop dropped the transport.
        RuntimeError("unable to perform operation on <UnixTransport closed=True>; the handler is closed"),
        # A peer that goes away during the write. Both are subclasses of OSError.
        BrokenPipeError(32, "Broken pipe"),
        ConnectionResetError(104, "Connection reset by peer"),
    ],
    ids=["closed-transport", "broken-pipe", "reset"],
)
async def test_a_client_that_is_gone_drops_out_of_the_fan_out(error: BaseException) -> None:
    """A build outlives its client, so a sender meets a closed transport. Issue nixidae/nixidae#235."""
    async with _build() as build:
        good: Any = FakeClient()
        gone: Any = FakeClient(raises=error)
        await build.add_subscriber(_as_client(good))
        await build.add_subscriber(_as_client(gone))

        build.post_log_bytes(b"building\n")
        await build.log_sent(_as_client(good))
        await build.log_sent(_as_client(gone))
        await anyio.wait_all_tasks_blocked()

        assert good.received() == b"building\n"
        assert build.subscribers == [good]


@pytest.mark.anyio
async def test_a_second_client_gets_its_own_replay() -> None:
    async with _build() as build:
        build.post_log_bytes(b"building\n")
        first: Any = FakeClient()
        second: Any = FakeClient()

        await build.add_subscriber(_as_client(first))
        await build.add_subscriber(_as_client(second))
        await build.log_sent(_as_client(first))
        await build.log_sent(_as_client(second))

        assert first.received() == b"building\n"
        # The second client also reads a BUILD_WAITING activity, because it joined
        # a build the first client asked for. Issue #25 and
        # `tests/unit/test_build_waiting_activity.py`. Its replay is what this
        # test is about, and it is the last block.
        assert second.blocks[-1] == b"building\n"
        assert len(second.blocks) == 2


@pytest.mark.anyio
async def test_a_client_that_stops_reading_stalls_neither_the_build_nor_another_client() -> None:
    async with _build() as build:
        reads = anyio.Event()
        stalled: Any = FakeClient(until=reads)
        reading: Any = FakeClient()
        await build.add_subscriber(_as_client(stalled))
        await build.add_subscriber(_as_client(reading))

        with anyio.fail_after(1):
            for line in (b"one\n", b"two\n", b"three\n"):
                build.post_log_bytes(line)
            await build.log_sent(_as_client(reading))

        assert reading.received().endswith(b"one\ntwo\nthree\n")
        assert stalled.received() == b""
        reads.set()
        await build.log_sent(_as_client(stalled))
        assert stalled.received().endswith(b"one\ntwo\nthree\n")


@pytest.mark.anyio
async def test_the_answer_waits_for_the_tail_of_a_slow_client() -> None:
    """`STDERR_LAST` and the answer follow the last line (`daemon.cc:129`); a line after them is misread."""
    async with _build() as build:
        reads = anyio.Event()
        slow: Any = FakeClient(until=reads)
        await build.add_subscriber(_as_client(slow))
        build.post_log_bytes(b"the last line\n")
        sent = anyio.Event()

        async def answer() -> None:
            await build.log_sent(_as_client(slow))
            sent.set()

        async with anyio.create_task_group() as tg:
            tg.start_soon(answer)
            await anyio.wait_all_tasks_blocked()
            assert not sent.is_set()
            reads.set()
        assert slow.received() == b"the last line\n"


@pytest.mark.anyio
async def test_the_log_goes_once_the_build_answered_and_its_last_client_left() -> None:
    async with _build() as build:
        client: Any = FakeClient()
        await build.add_subscriber(_as_client(client))
        build.post_log_bytes(b"building\n")
        build.answer.set(cast("BuildDerivationResponse", object()))
        build.close_log_if_unread()
        assert build.log_bytes() == b"building\n"

        await build.remove_subscriber(_as_client(client))

        assert build.log_bytes() == b""
