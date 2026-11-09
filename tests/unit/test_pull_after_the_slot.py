"""A build gives its slot back before pynixd pulls the outputs.

A pull of 92 s held the slot of a 40 s build for t3code. nixidae/nixidae#1075.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue
from pynixd.serde import BuildResultStatus
from tests._conftest.execution import FakeConn, FakeLocal, Streamed, Triggers, answering, fake_executor, queued
from tests._conftest.placement import FakeStore

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_ops import WireRequest

OUT = "/nix/store/00000000000000000000000000000001-out"


@pytest.mark.anyio
async def test_the_slot_goes_back_before_the_pull_and_the_roots_after_it() -> None:
    steps: list[str] = []
    store = FakeStore(StoreId("sg1-n2"))
    store.events = steps
    store.conn = FakeConn(events=steps)
    answer = answering(BuildResultStatus.BUILT)

    async def build(request: WireRequest) -> Any:
        steps.append("build")
        return await answer(request)

    store.conn.answer = build
    stream = Streamed()
    stream.before = lambda: steps.append("pull")
    queue = BuildQueue()
    queued_build = await queued(queue, outputs={"out": OUT})
    executor = fake_executor(queue, {store.store_id: store}, local=FakeLocal(), stream=stream, trigger=Triggers(steps))

    await executor.run(queued_build, store)

    assert steps == [
        "slot taken",
        "build",
        "transfer taken",
        "hold",
        "slot given back",
        "pull",
        "trigger",
        "transfer given back",
    ]
