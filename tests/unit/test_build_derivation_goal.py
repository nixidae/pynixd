"""Unit tests for BuildDerivationGoal lifecycle behavior."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import anyio
import pytest

from nix_daemon_protocol.ids import BuildId, RequestId
from pynixd.answer import Answer
from pynixd.connection import ClientConn
from pynixd.goals.build_derivation import BuildDerivationGoal
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    DerivationOutput,
    IsValidPathResponse,
    SetOptionsRequest,
    StorePath,
)
from pynixd.wire import BytesWriter
from tests.goal_group import goals_running

if TYPE_CHECKING:
    from pynixd.context import PynixdContext
    from pynixd.goals.engine import GoalEngine

OUTPUT_PATH = "/nix/store/00000000000000000000000000000002-test"
"""The one output that the derivation of these tests declares."""


class FakeScheduler:
    def __init__(self) -> None:
        self.started = anyio.Event()
        self.answer: Answer[BuildDerivationResponse] | None = None
        self.goal_request_id: RequestId | None = None
        # No queued build, so a holder waits on the answer alone.
        self.queue = SimpleNamespace(by_id={})

    async def build_derivation(
        self,
        request: BuildDerivationRequest,
        *,
        from_goal_path: bool = False,
        goal_request_id: RequestId | None = None,
        options: SetOptionsRequest | None = None,
        delegated: bool = False,
    ) -> tuple[BuildId, Answer[BuildDerivationResponse]]:
        del request, options, delegated
        self.goal_request_id = goal_request_id
        if not from_goal_path:
            raise RuntimeError("BuildDerivationGoal must mark scheduler builds as goal-owned")
        self.answer = Answer()
        self.started.set()
        return BuildId(1), self.answer


class FakeEngine:
    def __init__(self, scheduler: FakeScheduler) -> None:
        self.ctx = cast(
            "PynixdContext",
            SimpleNamespace(
                scheduler=scheduler,
                local_store=SimpleNamespace(),
            ),
        )
        # The engine *is* the request, as far as the build queue is
        # concerned, and `BuildDerivationGoal` passes this on. Issue nixidae/nixidae#325.
        self.request_id = RequestId(1)
        self.subscribed: list[tuple[BuildId, ClientConn]] = []
        self.unsubscribed: list[tuple[BuildId, ClientConn]] = []

    async def subscribe_build(self, build_id: BuildId, client: ClientConn) -> bool:
        self.subscribed.append((build_id, client))
        return True

    async def unsubscribe_build(self, build_id: BuildId, client: ClientConn) -> None:
        self.unsubscribed.append((build_id, client))


def _request() -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path="/nix/store/00000000000000000000000000000001-test.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )


@pytest.mark.anyio
async def test_build_derivation_goal_unsubscribes_late_subscriber() -> None:
    scheduler = FakeScheduler()
    engine = FakeEngine(scheduler)
    goal = BuildDerivationGoal(cast("GoalEngine", engine), _request())
    client = ClientConn(BytesWriter("client"))

    async with goals_running(goal.engine):
        await goal.start()
        await scheduler.started.wait()
        await goal.subscribe(client)

        answer = scheduler.answer
        if answer is None:
            raise RuntimeError("scheduler did not create a build answer")
        answer.set(
            BuildDerivationResponse(
                result=BuildResult(status=BuildResultStatus.BUILT),
            )
        )
        await goal.result()

    assert engine.subscribed == [(BuildId(1), client)]
    assert engine.unsubscribed == [(BuildId(1), client)]


class CountingStore:
    """A local store that answers `IsValidPath` and counts every question."""

    def __init__(self, *, valid: bool) -> None:
        self.valid = valid
        self.questions = 0

    async def execute(self, request: object, mark: bool = True) -> IsValidPathResponse:
        del request, mark
        self.questions += 1
        return IsValidPathResponse(valid=self.valid)


def _request_with_an_output() -> BuildDerivationRequest:
    """A derivation that declares an output path, so the wait has work to do."""
    return BuildDerivationRequest(
        drv_path=StorePath(path="/nix/store/00000000000000000000000000000001-test.drv"),
        derivation=BasicDerivation(
            platform="x86_64-linux",
            builder="",
            outputs={"out": DerivationOutput(path=OUTPUT_PATH)},
        ),
        build_mode=BuildMode.NORMAL,
    )


async def _run_with(status: BuildResultStatus, store: CountingStore) -> None:
    """Run one build goal to its answer, with *store* as the local store."""
    scheduler = FakeScheduler()
    engine = FakeEngine(scheduler)
    engine.ctx.local_store = store  # type: ignore[misc]  # SimpleNamespace, and the goal only calls execute
    goal = BuildDerivationGoal(cast("GoalEngine", engine), _request_with_an_output())

    async with goals_running(goal.engine):
        await goal.start()
        await scheduler.started.wait()
        answer = scheduler.answer
        if answer is None:
            raise RuntimeError("scheduler did not create a build answer")
        answer.set(BuildDerivationResponse(result=BuildResult(status=status)))
        await goal.result()


@pytest.mark.anyio
async def test_a_failed_build_asks_the_store_nothing() -> None:
    """A build that failed produced nothing, so it waits for nothing.

    `_wait_for_local_paths` polls for 2.0 s at 0.05 s, and `produced` holds
    the path that the derivation *declares*, filled before anything reads the
    status. So a failure waited the whole deadline for a path that the failure
    means cannot appear.

    Measured through the functional suite, before the correction:
    `nix build -f fod-failing.nix -j1 -L` at `build.sh:164` took 2.0498 s
    between `build_completed` for x1 and the moment its request acted on the
    failure, and the gap held 226 `IsValidPath` queries and nothing else.
    Issue nixidae/nixidae#326, and issue nixidae/nixidae#325 holds what the delay costs the scheduler.
    """
    store = CountingStore(valid=False)

    await _run_with(BuildResultStatus.PERMANENT_FAILURE, store)

    assert store.questions == 0


@pytest.mark.anyio
async def test_a_successful_build_still_waits_for_its_output() -> None:
    """The wait exists for a real reason, and a success keeps it.

    The local store registers an output a moment after the build reports it,
    so a success that asks once and gives up reads a missing output. Issue
    nixidae/nixidae#326 narrows the wait to the successes; it does not remove it.
    """
    store = CountingStore(valid=True)

    await _run_with(BuildResultStatus.BUILT, store)

    assert store.questions == 1
