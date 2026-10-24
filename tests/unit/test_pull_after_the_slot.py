"""A build gives its slot back before pynixd pulls the outputs.

A pull of 92 s held the slot of a 40 s build for t3code. nixidae/nixidae#1075.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.scheduler import Scheduler
from pynixd.serde import BuildResultStatus
from tests.unit.test_collect_on_every_success import FakeBuild, FakeContext, FakeResponse


class SlotStore:
    store_id = StoreId("sg1-n2")

    def __init__(self, steps: list[str]) -> None:
        self.steps = steps

    @contextlib.asynccontextmanager
    async def build_conn(self, _options: Any = None) -> AsyncIterator[object]:
        self.steps.append("slot taken")
        try:
            yield object()
        finally:
            self.steps.append("slot given back")


class SlotScheduler:
    def __init__(self) -> None:
        self.steps: list[str] = []
        self.ctx = FakeContext()
        self.local_store = None
        self.queue = self

    async def complete(self, _build_id: str, _response: Any) -> None:
        self.steps.append("complete")

    def trigger(self) -> None:
        return

    async def _prepare_build(self, *_args: Any) -> None:
        return

    async def _say_where_it_builds(self, *_args: Any) -> None:
        return

    async def _execute(self, *_args: Any) -> FakeResponse:
        self.steps.append("build")
        return FakeResponse(BuildResultStatus.BUILT)

    async def _hold_outputs(self, held: contextlib.AsyncExitStack, *_args: Any) -> None:
        self.steps.append("outputs rooted")
        held.callback(self.steps.append, "roots released")

    async def _collect_outputs(self, *_args: Any) -> None:
        self.steps.append("pull")


@pytest.mark.anyio
async def test_the_slot_goes_back_before_the_pull_and_the_roots_after_it() -> None:
    scheduler = SlotScheduler()
    store = SlotStore(scheduler.steps)

    await Scheduler.execute_build(cast("Any", scheduler), cast("Any", FakeBuild()), cast("Any", store))

    assert scheduler.steps == [
        "slot taken",
        "build",
        "outputs rooted",
        "slot given back",
        "pull",
        "complete",
        "roots released",
    ]
