"""Unit tests for build request root goals."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.exceptions import BackendError
from pynixd.goals.engine import GoalEngine
from pynixd.goals.goal import Goal
from pynixd.goals.requests import BuildPathsWithResultsGoal
from pynixd.goals.results import GoalResult, goal_failure, goal_success
from pynixd.serde import (
    MAX_WIRE_STATUS,
    BuildMode,
    BuildPathsRequest,
    BuildPathsWithResultsRequest,
    BuildPathsWithResultsResponse,
    BuildResultStatus,
    DerivedPath as SerdeDerivedPath,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pynixd.connection import ClientConn
    from pynixd.derived_path import DerivedPath
    from pynixd.goals.dispatch_order import DispatchTurn


class FakeEnsureGoal(Goal[GoalResult]):
    def __init__(
        self,
        engine: GoalEngine,
        result: GoalResult,
        *,
        before_result: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__(engine)
        self._result = result
        self._before_result = before_result
        # `FakeEngine.get_ensure_derived_path_goal` fills this in, the way the
        # real goal carries the path it was made for. `_goal_order` reads it to
        # take the goals in the order that Nix takes them, which is the order
        # of the derivation name. Issue nixidae/nixidae#235.
        self.derived_path: DerivedPath | None = None
        self.subscribers: list[ClientConn] = []

    def take_a_turn(self, turn: DispatchTurn) -> None:
        """Accept the place of this goal in the order, and hold nobody.

        The real goal keeps the turn until its build reaches the queue. This
        fake enqueues no build, so it marks the turn decided at once and the
        goals after it start with no delay. Issue nixidae/nixidae#246.
        """
        turn.decided()

    async def subscribe(self, client: ClientConn | None) -> None:
        if client is not None:
            self.subscribers.append(client)

    async def _run(self) -> GoalResult:
        if self._before_result is not None:
            await self._before_result()
        return self._result


class FakeEngine:
    def __init__(self, goals: dict[str, FakeEnsureGoal]) -> None:
        self.goals = goals
        # A backend is present, so `max-jobs` of the client does not reach the
        # loop of the root goals. `_build_slots` gives the reason. Issue nixidae/nixidae#229.
        # `no_schedule` is the property that separates a builder from a
        # substituter, and `_build_slots` counts the builders alone. Issue nixidae/nixidae#235.
        self.ctx = SimpleNamespace(
            stores={
                LOCAL_STORE_ID: SimpleNamespace(no_schedule=False),
                StoreId("builder"): SimpleNamespace(no_schedule=False),
            }
        )

    def substituter_ids(self) -> tuple[str, ...]:
        return ()

    async def get_ensure_derived_path_goal(
        self,
        path: DerivedPath,
        build_mode: int,
        substituter_ids: tuple[str, ...],
    ) -> FakeEnsureGoal:
        del build_mode, substituter_ids
        goal = self.goals[str(path)]
        goal.derived_path = path
        return goal

    async def build_paths_with_results(
        self,
        request: BuildPathsWithResultsRequest,
        client: ClientConn | None = None,
        hold: Any = None,
    ) -> BuildPathsWithResultsResponse:
        del hold
        return await BuildPathsWithResultsGoal(cast("GoalEngine", self), request, client).result()


def _serde_path(path: str) -> Any:
    return SerdeDerivedPath(value=path)


def _request(paths: set[str]) -> BuildPathsWithResultsRequest:
    derived_paths: list[Any] = [_serde_path(path) for path in sorted(paths)]
    return BuildPathsWithResultsRequest(
        derived_paths=cast("list[SerdeDerivedPath]", derived_paths),
        build_mode=BuildMode.NORMAL,
    )


@pytest.mark.anyio
async def test_build_paths_with_results_keeps_successes_when_one_root_fails() -> None:
    success_path = "/nix/store/11111111111111111111111111111111-success.drv!out"
    failure_path = "/nix/store/22222222222222222222222222222222-failure.drv!out"
    engine = FakeEngine(
        {
            success_path: FakeEnsureGoal(cast("GoalEngine", None), goal_success()),
            failure_path: FakeEnsureGoal(
                cast("GoalEngine", None),
                goal_failure("pynixd: expected test failure", BuildResultStatus.UNKNOWN),
            ),
        }
    )
    for goal in engine.goals.values():
        goal.engine = cast("GoalEngine", engine)

    response = await BuildPathsWithResultsGoal(
        cast("GoalEngine", engine),
        _request({success_path, failure_path}),
    ).result()

    results_by_path = {str(item.path): item.result for item in response.results}
    assert set(results_by_path) == {success_path, failure_path}
    assert BuildResultStatus(results_by_path[success_path].status).is_success

    # A failure, and one a client can read. The goal above produced `UNKNOWN`,
    # and this used to assert that value came back. It cannot: `UNKNOWN` is 102,
    # the wire carries the status as one byte a client looks up in a table of
    # 15, and `BuildPathsWithResultsGoal` now calls `for_the_wire()` on the way
    # out. The subject of this test is that one failing root does not take the
    # successful one with it, and that is unchanged.
    failure_status = BuildResultStatus(results_by_path[failure_path].status)
    assert not failure_status.is_success
    assert failure_status <= MAX_WIRE_STATUS, f"{failure_status.name} cannot be written to the wire"


@pytest.mark.anyio
async def test_build_paths_reports_failure_when_any_root_fails() -> None:
    success_path = "/nix/store/11111111111111111111111111111111-success.drv!out"
    failure_path = "/nix/store/22222222222222222222222222222222-failure.drv!out"
    engine = FakeEngine(
        {
            success_path: FakeEnsureGoal(cast("GoalEngine", None), goal_success()),
            failure_path: FakeEnsureGoal(
                cast("GoalEngine", None),
                goal_failure("pynixd: expected test failure", BuildResultStatus.UNKNOWN),
            ),
        }
    )
    for goal in engine.goals.values():
        goal.engine = cast("GoalEngine", engine)

    derived_paths: list[Any] = [_serde_path(success_path), _serde_path(failure_path)]

    # **An error, and not a value.** `daemon.cc:558` of Nix writes a constant
    # `1` after `buildPaths`, and `buildPaths` throws when a build fails. A
    # client of Nix reads that number and drops it, so a failure carried in
    # the number reached nobody. Issue nixidae/nixidae#216.
    with pytest.raises(BackendError, match="expected test failure"):
        await GoalEngine.build_paths(
            cast("GoalEngine", engine),
            BuildPathsRequest(
                derived_paths=cast("list[SerdeDerivedPath]", derived_paths),
                build_mode=BuildMode.NORMAL,
            ),
        )


@pytest.mark.anyio
async def test_build_paths_answers_one_when_every_root_succeeds() -> None:
    """The same constant that Nix writes, and not a status of its own."""
    success_path = "/nix/store/11111111111111111111111111111111-success.drv!out"
    engine = FakeEngine({success_path: FakeEnsureGoal(cast("GoalEngine", None), goal_success())})
    for goal in engine.goals.values():
        goal.engine = cast("GoalEngine", engine)

    derived_paths: list[Any] = [_serde_path(success_path)]
    response = await GoalEngine.build_paths(
        cast("GoalEngine", engine),
        BuildPathsRequest(
            derived_paths=cast("list[SerdeDerivedPath]", derived_paths),
            build_mode=BuildMode.NORMAL,
        ),
    )

    assert response.value == 1


@pytest.mark.anyio
async def test_build_paths_with_results_runs_root_goals_in_parallel() -> None:
    first_path = "/nix/store/11111111111111111111111111111111-first.drv!out"
    second_path = "/nix/store/22222222222222222222222222222222-second.drv!out"
    second_started = anyio.Event()

    async def wait_for_second() -> None:
        await second_started.wait()

    async def mark_second_started() -> None:
        second_started.set()

    engine = FakeEngine(
        {
            first_path: FakeEnsureGoal(
                cast("GoalEngine", None),
                goal_success(),
                before_result=wait_for_second,
            ),
            second_path: FakeEnsureGoal(
                cast("GoalEngine", None),
                goal_success(),
                before_result=mark_second_started,
            ),
        }
    )
    for goal in engine.goals.values():
        goal.engine = cast("GoalEngine", engine)

    with anyio.fail_after(1):
        response = await BuildPathsWithResultsGoal(
            cast("GoalEngine", engine),
            _request({first_path, second_path}),
        ).result()

    assert {str(item.path) for item in response.results} == {first_path, second_path}


@pytest.mark.anyio
async def test_the_answers_come_back_in_the_order_of_the_request() -> None:
    """The client reads the answers by position, so the order is the contract.

    `DerivedPaths` of Nix is a vector, and `Store::buildPathsWithResults`
    answers one result for each request in that order. pynixd held the request
    in a set and then sorted it, so the answer followed the hash part of each
    store path. `build.sh:8` of the functional suite passed or failed by luck
    of that hash. Issue nixidae/nixidae#219.
    """
    # `z...` sorts after `a...`, and the request asks for it first.
    later = "/nix/store/zz111111111111111111111111111111-later.drv!out"
    earlier = "/nix/store/aa222222222222222222222222222222-earlier.drv!out"
    engine = FakeEngine(
        {
            later: FakeEnsureGoal(cast("GoalEngine", None), goal_success()),
            earlier: FakeEnsureGoal(cast("GoalEngine", None), goal_success()),
        }
    )
    for goal in engine.goals.values():
        goal.engine = cast("GoalEngine", engine)

    derived_paths: list[Any] = [_serde_path(later), _serde_path(earlier)]
    response = await BuildPathsWithResultsGoal(
        cast("GoalEngine", engine),
        BuildPathsWithResultsRequest(
            derived_paths=cast("list[SerdeDerivedPath]", derived_paths),
            build_mode=BuildMode.NORMAL,
        ),
    ).result()

    assert [str(item.path) for item in response.results] == [later, earlier]


@pytest.mark.anyio
async def test_a_repeated_path_gets_one_answer_for_each_request() -> None:
    """A set dropped the second one, and Nix answers both."""
    path = "/nix/store/11111111111111111111111111111111-twice.drv!out"
    engine = FakeEngine({path: FakeEnsureGoal(cast("GoalEngine", None), goal_success())})
    for goal in engine.goals.values():
        goal.engine = cast("GoalEngine", engine)

    derived_paths: list[Any] = [_serde_path(path), _serde_path(path)]
    response = await BuildPathsWithResultsGoal(
        cast("GoalEngine", engine),
        BuildPathsWithResultsRequest(
            derived_paths=cast("list[SerdeDerivedPath]", derived_paths),
            build_mode=BuildMode.NORMAL,
        ),
    ).result()

    assert [str(item.path) for item in response.results] == [path, path]
