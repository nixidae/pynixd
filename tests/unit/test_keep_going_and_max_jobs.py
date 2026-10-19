"""The first failure stops the request, and every root goal runs.

`Worker::removeGoal` at `worker.cc:173` clears `topGoals` when a top goal
fails and `keepGoing` is off, and `Worker::run` then leaves its loop.
`Worker::buildPathsWithResults` at `entry-points.cc:93` skips each goal whose
`exitCode` is still `ecBusy`, so the answer holds fewer entries than the
request. Issue nixidae/nixidae#229.

**`max-jobs` reaches no test here.** It limits the builders of the machine at
`worker.cc:261`, and pynixd holds them in `Scheduler._local_slot_is_full`.
The docstring of `_build_slots` states what a second limit on the goals cost.
Issue nixidae/nixidae#235.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.derived_path import DerivedPath
from pynixd.goals.requests import BuildPathsWithResultsGoal
from pynixd.goals.results import GoalResult, goal_failure, goal_success
from pynixd.serde import (
    BuildMode,
    BuildPathsWithResultsRequest,
    DerivedPath as SerdeDerivedPath,
    SetOptionsRequest,
    TimeSpan,
    Verbosity,
)

if TYPE_CHECKING:
    from pynixd.connection import ClientConn
    from pynixd.goals.dispatch_order import DispatchTurn
    from pynixd.goals.engine import GoalEngine

_PATHS = [f"/nix/store/{str(index) * 32}-x{index}.drv!out" for index in (1, 2, 3, 4)]


def _options(*, max_build_jobs: int, keep_going: bool) -> SetOptionsRequest:
    """A `SetOptions` request with every field, because no field has a default."""
    return SetOptionsRequest(
        keep_failed=False,
        keep_going=keep_going,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=max_build_jobs,
        max_silent_time=TimeSpan(0),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=1,
        use_substitutes=True,
        overrides={},
    )


class FakeEnsureGoal:
    """A goal that records that it ran, and answers success or failure.

    *blocks* makes it never finish, which is the goal that a build slot has
    not reached yet. The request must leave such a goal behind rather than
    wait for it, so `answered` says which goals the client really hears about.
    """

    def __init__(
        self,
        name: str,
        *,
        succeeds: bool,
        started: list[str],
        blocks: bool = False,
        blamed: str | None = None,
    ) -> None:
        self.name = name
        self.derived_path = DerivedPath(name)
        self.succeeds = succeeds
        self.started = started
        self.blocks = blocks
        # The derived path whose own build failed, when that is another goal.
        # `GoalResult.failing_derivation` carries it out of the real goal.
        self.blamed = blamed
        self.unsubscribed: list[Any] = []

    def take_a_turn(self, turn: DispatchTurn) -> None:
        """Accept the place of this goal in the order, and hold nobody.

        The real goal keeps the turn until its build reaches the queue. This
        fake enqueues no build, so it marks the turn decided at once and the
        goals after it start with no delay. Issue nixidae/nixidae#246.
        """
        turn.decided()

    async def subscribe(self, client: Any) -> None:
        del client

    async def unsubscribe(self, client: Any) -> None:
        self.unsubscribed.append(client)

    async def result(self) -> GoalResult:
        self.started.append(self.name)
        if self.blocks:
            await anyio.sleep_forever()
        # One checkpoint, so the order of the starts is the order of the answers.
        await anyio.sleep(0)
        if self.succeeds:
            return goal_success()
        result = goal_failure(f"{self.name} failed")
        blamed = self.blamed if self.blamed is not None else self.name
        result.failing_derivation = DerivedPath(blamed).base_store_path()
        return result


def _store(*, no_schedule: bool) -> SimpleNamespace:
    """A store that the scheduler may send a build to, or may not."""
    return SimpleNamespace(no_schedule=no_schedule)


class FakeEngine:
    def __init__(
        self,
        goals: dict[str, FakeEnsureGoal],
        *,
        has_a_backend: bool,
        has_a_substituter: bool = False,
    ) -> None:
        self.goals = goals
        stores: dict[StoreId, object] = {LOCAL_STORE_ID: _store(no_schedule=False)}
        if has_a_backend:
            stores[StoreId("builder")] = _store(no_schedule=False)
        if has_a_substituter:
            stores[StoreId("http-cache.nixos.org")] = _store(no_schedule=True)
        self.ctx = SimpleNamespace(stores=stores)

    def substituter_ids(self) -> tuple[str, ...]:
        return ()

    async def get_ensure_derived_path_goal(
        self,
        path: DerivedPath,
        build_mode: int,
        substituter_ids: tuple[str, ...],
    ) -> FakeEnsureGoal:
        del build_mode, substituter_ids
        return self.goals[str(path)]


def _client(options: SetOptionsRequest | None) -> ClientConn:
    # `standard_features` is what the client handshake negotiated. Empty is
    # what Nix 2.34 names, and `for_the_wire` reads it. Issue #14.
    return cast("ClientConn", SimpleNamespace(options=options, standard_features=frozenset()))


async def _run(
    *,
    failing: set[str],
    options: SetOptionsRequest | None,
    has_a_backend: bool = False,
    has_a_substituter: bool = False,
    paths: list[str] | None = None,
    blocking: set[str] | None = None,
    blamed: dict[str, str] | None = None,
) -> tuple[list[str], list[str]]:
    """Run the four paths, and answer which goals ran and which got an answer."""
    paths = _PATHS if paths is None else paths
    blocking = blocking or set()
    blamed = blamed or {}
    started: list[str] = []
    goals = {
        path: FakeEnsureGoal(
            path,
            succeeds=path not in failing,
            started=started,
            blocks=path in blocking,
            blamed=blamed.get(path),
        )
        for path in paths
    }
    engine = FakeEngine(goals, has_a_backend=has_a_backend, has_a_substituter=has_a_substituter)
    request = BuildPathsWithResultsRequest(
        derived_paths=[SerdeDerivedPath(value=path) for path in paths],
        build_mode=BuildMode.NORMAL,
    )

    response = await BuildPathsWithResultsGoal(
        cast("GoalEngine", engine),
        request,
        _client(options),
    ).result()

    return started, [str(item.path) for item in response.results]


@pytest.mark.anyio
async def test_max_jobs_does_not_hold_a_root_goal() -> None:
    """`-j1` starts every root goal, and holds the builders elsewhere.

    Nix makes every goal of the request and then counts the builders that run,
    at `worker.cc:261`. A goal that waits for a slot is a goal that has
    started. pynixd counts the builders in `Scheduler._local_slot_is_full`,
    which reads `max_build_jobs` of the same client, so a second count here
    would hold each goal behind the one before it.

    **This test stands for `build.sh:247` and `build.sh:269`.** A request
    answers when its goals answer, so a goal that waited for a build slot held
    every goal behind it. `build.sh:247` read the derivation that the answer
    named, and `build.sh:269` waited for a fifo that nothing opens and reached
    the 300 s cap of the test.

    The four names sort the same way as the four store paths here, so this
    reads the fan-out alone.
    `test_the_goals_run_in_the_order_of_the_derivation_name` reads the order.
    """
    started, answered = await _run(failing=set(), options=_options(max_build_jobs=1, keep_going=False))

    assert started == _PATHS
    assert answered == _PATHS


@pytest.mark.anyio
async def test_the_first_failure_stops_the_request() -> None:
    """x1 fails, so x2, x3 and x4 report nothing, although each one runs.

    This is `Worker::removeGoal` at `worker.cc:173`: the request leaves its
    loop, and a goal that has not reached `amDone` is skipped at
    `entry-points.cc:93`. It is not a statement about how many goals run.
    The three that report nothing block here, which is the goal that is still
    building when the failure ends the request.
    """
    started, answered = await _run(
        failing={_PATHS[0]},
        options=_options(max_build_jobs=1, keep_going=False),
        blocking=set(_PATHS[1:]),
    )

    assert started == _PATHS
    assert answered == [_PATHS[0]]


@pytest.mark.anyio
async def test_keep_going_runs_every_goal_after_a_failure() -> None:
    started, answered = await _run(
        failing={_PATHS[0]},
        options=_options(max_build_jobs=1, keep_going=True),
    )

    assert started == _PATHS
    assert answered == _PATHS


@pytest.mark.anyio
async def test_a_backend_keeps_the_fan_out_that_the_client_asked_to_limit() -> None:
    """A backend changes nothing here, because no limit reads the stores.

    `_build_slots` counted the stores of the context, so a pynixd with a
    backend fanned out and a pynixd without one did not. `worker.cc:261`
    counts the local builders, and `Scheduler._local_slot_is_full` is where
    pynixd asks the same question. Issue nixidae/nixidae#235.
    """
    started, answered = await _run(
        failing=set(),
        options=_options(max_build_jobs=1, keep_going=False),
        has_a_backend=True,
    )

    assert sorted(started) == sorted(_PATHS)
    assert answered == _PATHS


@pytest.mark.anyio
async def test_max_jobs_of_zero_still_runs_every_goal() -> None:
    """`max-jobs = 0` means "no build here", and it holds no goal either."""
    started, _ = await _run(failing=set(), options=_options(max_build_jobs=0, keep_going=False))

    assert started == _PATHS


@pytest.mark.anyio
async def test_a_client_with_no_option_set_keeps_the_fan_out() -> None:
    """A client that sent no `SetOptions` reads the behaviour of before."""
    started, answered = await _run(failing=set(), options=None)

    assert sorted(started) == sorted(_PATHS)
    assert answered == _PATHS


@pytest.mark.anyio
async def test_a_substituter_changes_no_fan_out() -> None:
    """A binary cache in the configuration decides nothing here.

    `_build_slots` counted every store that is not the local one, so one
    `http-cache.nixos.org` lifted its limit for every request, and almost
    every configuration holds one. No limit here reads a store now, so this
    holds the answer steady against a configuration that once changed it.
    Issue nixidae/nixidae#235.
    """
    started, answered = await _run(
        failing=set(),
        options=_options(max_build_jobs=1, keep_going=False),
        has_a_substituter=True,
    )

    assert started == _PATHS
    assert answered == _PATHS


@pytest.mark.anyio
async def test_the_goals_run_in_the_order_of_the_derivation_name() -> None:
    """Nix takes `aardvark` before `baboon`, whatever order the client wrote.

    `DerivationBuildingGoal::key()` at `derivation-building-goal.cc:54` builds
    `"dd$" + name + "$" + path`, and `goal.hh:604` states the rule. The store
    path decides nothing until the names are equal.

    `main:build` measured it. The client sends four failing derivations in
    store-path order, `-j1`, and asserts that the one `error:` line names x1.
    pynixd took the request order, so it built x3 and named x3.
    """
    # The store path order is the reverse of the name order, so a request in
    # store-path order can only pass by reading the name.
    paths = [f"/nix/store/{chr(ord('a') + 4 - index) * 32}-x{index}.drv!out" for index in (4, 3, 2, 1)]
    started, answered = await _run(
        failing={paths[-1]},
        options=_options(max_build_jobs=1, keep_going=False),
        paths=paths,
        blocking=set(paths[:-1]),
    )

    # x1 is last in the request and first by name, and it is the one that
    # fails. Every goal starts, and the **first** one to start is the one the
    # name order picks. The answer holds it alone, because the other three
    # had not finished when its failure ended the request.
    assert started[0] == paths[-1]
    assert answered == [paths[-1]]


@pytest.mark.anyio
async def test_the_answer_keeps_the_order_of_the_request() -> None:
    """The work order is the name order, and the answer order is the request order.

    `nix build --json` reads the answers by position. `_goal_order` decides
    which goal runs first and writes each result to the place of the request,
    so the two orders stay apart. Issue nixidae/nixidae#235.
    """
    paths = [f"/nix/store/{chr(ord('a') + 4 - index) * 32}-x{index}.drv!out" for index in (4, 3, 2, 1)]
    started, answered = await _run(
        failing=set(),
        options=_options(max_build_jobs=1, keep_going=False),
        paths=paths,
    )

    assert started == list(reversed(paths))
    assert answered == paths


@pytest.mark.anyio
async def test_a_root_that_waited_for_a_failed_root_reports_nothing() -> None:
    """The request answers for the build that failed, and not for its waiter.

    `nix build fast-fail^out depends-on-fail^out` names two derivations, and
    the second has the first as an input. Nix answers with the failure of the
    first alone: `Worker::removeGoal` at `worker.cc:173` clears `topGoals`, so
    the goal of the waiter never reaches `amDone` and `entry-points.cc:93`
    skips it.

    pynixd runs its root goals together, so both answer. `failing_derivation`
    names the build that really failed, and a root that names another root of
    the same request adds nothing. `build.sh:279` reads the difference: the
    client wrote one `error:` block more than the control run.
    """
    paths = _PATHS[:2]
    started, answered = await _run(
        failing=set(paths),
        options=_options(max_build_jobs=1, keep_going=True),
        paths=paths,
        # x2 failed because x1 failed.
        blamed={paths[1]: paths[0]},
    )

    assert started == paths
    assert answered == [paths[0]]


@pytest.mark.anyio
async def test_a_root_that_waited_for_a_derivation_outside_the_request_reports() -> None:
    """A failed input that the client did not name is nothing the client heard.

    Nix reports such a goal, because it does reach `amDone`: the request holds
    one top goal, and it is the waiter. So the name decides, and not the kind
    of the failure.
    """
    paths = _PATHS[:2]
    outside = "/nix/store/99999999999999999999999999999999-x9.drv!out"
    started, answered = await _run(
        failing={paths[1]},
        options=_options(max_build_jobs=1, keep_going=True),
        paths=paths,
        blamed={paths[1]: outside},
    )

    assert started == paths
    assert answered == paths
