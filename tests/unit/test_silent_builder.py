"""A silent build moves to another store only when its builder stops answering handshakes.

nixidae/nixidae#1063: macbook's SSH answered keepalives while its daemon
answered nothing, and held 48 builds for 55 min. gx10-1759 was silent too,
and its builds finished: silence alone does not prove a builder is gone.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus
from tests._conftest.execution import FakeLocal, fake_executor, queued
from tests._conftest.placement import FakeStore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd.build_queue import QueuedBuild

BUILDER = StoreId("builder1")
SILENCE = 0.05


async def _refuses() -> None:
    raise ConnectionRefusedError("daemon socket refused the session")


async def _hangs() -> None:
    await anyio.sleep_forever()


def _silent_for(seconds: float) -> Callable[[WireRequest], Awaitable[BuildDerivationResponse]]:
    async def answer(request: WireRequest) -> BuildDerivationResponse:
        del request
        await anyio.sleep(seconds)
        return BuildDerivationResponse(result=BuildResult(status=int(BuildResultStatus.BUILT)))

    return answer


async def _run(store: FakeStore) -> QueuedBuild:
    queue = BuildQueue()
    build = await queued(queue)
    executor = fake_executor(
        queue, {store.store_id: store}, local=FakeLocal(), silent_build_probe=SILENCE, handshake_timeout=SILENCE
    )
    with anyio.fail_after(10):
        await executor.run(build, store)
    return build


@pytest.mark.parametrize("handshake", [_refuses, _hangs], ids=["refused", "no-answer"])
async def test_a_silent_build_leaves_a_builder_that_does_not_answer(handshake: Callable[[], Awaitable[None]]) -> None:
    store = FakeStore(BUILDER)
    store.conn.answer = _silent_for(3600)
    store.handshake = handshake

    build = await _run(store)

    assert not build.answer.done()
    assert build.is_blacklisted(BUILDER)
    assert "did not answer a new session" in build.store_errors[BUILDER]


async def test_a_silent_build_stays_on_a_builder_that_answers() -> None:
    store = FakeStore(BUILDER)
    store.conn.answer = _silent_for(SILENCE * 6)

    build = await _run(store)

    assert store.handshakes >= 2
    assert build.store_failures == {}
    assert build.answer.result().result.status == BuildResultStatus.BUILT
