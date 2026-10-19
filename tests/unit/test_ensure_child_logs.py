"""What a client that watches one derivation reads while pynixd builds it.

Two lines were missing.

`nix-daemon` writes `building '<input>.drv'...` and, when the input fails, the
reason it failed. A client of pynixd read "1 dependency failed" for the
derivation it asked for, and nothing at all about the dependency, because
`_realise_input_derivations` started a goal for each input and subscribed
nobody to it. `_ensure_nested` already subscribed the next level of a dynamic
derivation, so one of the two roads carried the logs and the other did not.

`nix-daemon` also writes `resolved derivation: 'A' -> 'B'...`, which names the
derivation that it really builds. pynixd resolved and said nothing.

Refs nixidae/nixidae#214.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.drv_parser import Derivation
from pynixd.goals.ensure import (
    _ERROR_PREFIX,  # noqa: PLC2701 -- the exact bytes are the unit under test
    EnsureDerivedPathGoal,
)
from pynixd.goals.results import GoalResult, goal_success, result_succeeded
from pynixd.goals.substitute import SubstituteAttempt
from pynixd.serde import (
    BuildMode,
    BuildResult,
    BuildResultStatus,
    IsValidPathResponse,
    QueryRealisationRequest,
    QueryRealisationResponse,
)
from pynixd.store_layout import StoreLayout
from pynixd.store_path import DrvOutput, StorePath

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine
    from pynixd.serde import BuildDerivationRequest

TOP_DRV = "/nix/store/00000000000000000000000000000001-top.drv"
INPUT_DRV = "/nix/store/00000000000000000000000000000002-input.drv"
TOP_OUT = "/nix/store/11111111111111111111111111111111-top"
INPUT_OUT = "/nix/store/22222222222222222222222222222222-input"
RESOLVED_PREFIX = "/nix/store/33333333333333333333333333333333"
RESOLVED = f"{RESOLVED_PREFIX}-top.drv"
GONE = "/nix/store/44444444444444444444444444444444-gone"


def _input() -> Derivation:
    return Derivation(
        outputs=[DrvOutput(hash_algo="", hash_value="", output_name="out", path=INPUT_OUT)],
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", "echo hi > $out"],
        env={"out": INPUT_OUT, "name": "input"},
    )


def _top() -> Derivation:
    """The output is deferred, so `shouldResolve` answers yes for it.

    `Derivation::shouldResolve` at `derivations.cc:1129` gives a resolved
    derivation to an input-addressed derivation only when the output path is
    not known yet. An input of a dynamic derivation is one road to that, and a
    plain deferred output is the shorter one to write here.
    """
    return Derivation(
        outputs=[DrvOutput(hash_algo="", hash_value="", output_name="out", path="")],
        input_drvs={StorePath(INPUT_DRV): ["out"]},  # pyright: ignore[reportArgumentType] -- StorePath is a str
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", f"cat {INPUT_OUT} > $out"],
        env={"out": TOP_OUT, "name": "top"},
    )


class FakeClient:
    """A client that watches the goal, and keeps what the goal said."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.options = None
        """No option set, so this client names no substituter. Issue nixidae/nixidae#226."""

    async def send(self, msg: Any) -> None:
        self.lines.append(str(msg.text))


class FakeLocalStore:
    def __init__(self) -> None:
        self.layout = StoreLayout.chroot(None)

    async def add_text_to_store(self, name: str, text: str, references: set[str]) -> str:
        """Answer the path of the resolved derivation, as the daemon does."""
        del text, references
        return f"{RESOLVED_PREFIX}-{name}"

    async def read_derivation(self, drv_path: str) -> Derivation | None:
        if str(drv_path) == TOP_DRV:
            return _top()
        if str(drv_path) == INPUT_DRV:
            return _input()
        return None

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        """Nothing is valid, so the input needs a goal of its own."""
        del kwargs
        if isinstance(request, QueryRealisationRequest):
            return QueryRealisationResponse(realisations=[])
        return IsValidPathResponse(valid=False)


class FakeBuildGoal:
    may_reach_a_root_goal = False
    """A build goal reaches no root goal, so a caller keeps its place. Issue nixidae/nixidae#246."""

    def __init__(self, request: BuildDerivationRequest) -> None:
        self.request = request

    async def subscribe(self, client: Any) -> None:
        del client

    async def start(self) -> None:
        """`Goal.start` begins the build and does not wait. Issue nixidae/nixidae#246."""

    async def wait_until_it_reached_the_queue(self) -> None:
        """This fake needs no queue, so the build is on it at once."""

    async def result(self) -> GoalResult:
        return goal_success()


class FakeChildGoal:
    """The goal of the input derivation, and who watches it."""

    may_reach_a_root_goal = True
    """The goal of an input can reach a root goal of the request. Issue nixidae/nixidae#246."""

    def __init__(self) -> None:
        self.subscribers: list[Any] = []
        self.has_a_parent = False

    def note_a_parent(self) -> None:
        self.has_a_parent = True

    async def subscribe_many(self, clients: list[Any]) -> None:
        self.subscribers.extend(clients)

    async def result(self) -> GoalResult:
        result = goal_success()
        result.resolved_outputs = {"out": StorePath(INPUT_OUT)}
        return result.with_dynamic_outputs(StorePath(INPUT_DRV))


class FakeSubstituteGoal:
    may_reach_a_root_goal = False
    """A substitute goal reaches no root goal. Issue nixidae/nixidae#246."""

    async def result(self) -> SubstituteAttempt:
        return SubstituteAttempt(found=False, result=goal_success())


class FakeContext:
    def __init__(self) -> None:
        self.local_store = FakeLocalStore()

    def store_for_output_path(self, path: str) -> None:
        """No backend holds the path, so the goal has to fail."""
        del path
        return None


class FakeEngine:
    def __init__(self) -> None:
        self.ctx = FakeContext()
        self.child_goals: list[FakeChildGoal] = []

    async def get_build_derivation_goal(self, request: BuildDerivationRequest) -> FakeBuildGoal:
        return FakeBuildGoal(request)

    async def get_ensure_derived_path_goal(self, dp: Any, build_mode: int, substituter_ids: Any) -> FakeChildGoal:
        del dp, build_mode, substituter_ids
        goal = FakeChildGoal()
        self.child_goals.append(goal)
        return goal

    async def get_substitute_path_goal(self, path: StorePath, substituter_ids: tuple[str, ...]) -> Any:
        del path, substituter_ids
        return FakeSubstituteGoal()


@pytest.mark.anyio
async def test_the_client_watches_the_goal_of_each_input() -> None:
    engine = FakeEngine()
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{TOP_DRV}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    client = FakeClient()
    await goal.subscribe(cast("Any", client))

    await goal.result()

    assert len(engine.child_goals) == 1
    assert engine.child_goals[0].subscribers == [client]


@pytest.mark.anyio
async def test_the_client_reads_which_derivation_pynixd_really_builds() -> None:
    """`DerivationResolutionGoal` says this at `derivation-resolution-goal.cc:150`."""
    engine = FakeEngine()
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{TOP_DRV}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    client = FakeClient()
    await goal.subscribe(cast("Any", client))

    await goal.result()

    assert client.lines == [f"resolved derivation: '{TOP_DRV}' -> '{RESOLVED_PREFIX}-top.drv'...\n"]


@pytest.mark.anyio
async def test_a_failed_resolved_build_says_the_short_sentence() -> None:
    """`DerivationGoal` at `derivation-goal.cc:247` answers this and nothing else."""
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", FakeEngine()),
        derived_path=DerivedPath(f"{TOP_DRV}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    client = FakeClient()
    await goal.subscribe(cast("Any", client))
    failure = GoalResult(
        result=BuildResult(status=BuildResultStatus.PERMANENT_FAILURE, error_msg="Cannot build 'x'.\nReason: no.")
    )

    answer = await goal._name_the_resolved_derivation(  # noqa: SLF001 -- the message is the unit under test
        failure,
        StorePath(path=TOP_DRV),
        StorePath(path=RESOLVED),
    )

    assert str(answer.result.error_msg) == f"build of resolved derivation '{RESOLVED}' failed"
    assert client.lines == [f"{_ERROR_PREFIX}Cannot build 'x'.\n       Reason: no."]


@pytest.mark.anyio
async def test_a_failed_build_of_the_derivation_itself_keeps_its_message() -> None:
    """A goal at the top of the request reports its own failure, at `goal.cc:214`."""
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", FakeEngine()),
        derived_path=DerivedPath(f"{TOP_DRV}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    client = FakeClient()
    await goal.subscribe(cast("Any", client))
    failure = GoalResult(result=BuildResult(status=BuildResultStatus.PERMANENT_FAILURE, error_msg="Cannot build 'x'."))

    answer = await goal._name_the_resolved_derivation(  # noqa: SLF001 -- the message is the unit under test
        failure,
        StorePath(path=TOP_DRV),
        StorePath(path=TOP_DRV),
    )

    assert str(answer.result.error_msg) == "Cannot build 'x'."
    assert client.lines == []


@pytest.mark.anyio
async def test_a_failed_child_goal_writes_its_reason() -> None:
    """`Goal::amDone` at `goal.cc:214` writes this when `waiters` is not empty."""
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", FakeEngine()),
        derived_path=DerivedPath(GONE),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    client = FakeClient()
    await goal.subscribe(cast("Any", client))
    goal.note_a_parent()

    result = await goal.result()

    assert not result_succeeded(result.result)
    assert client.lines == [f"{_ERROR_PREFIX}{result.result.error_msg}"]


@pytest.mark.anyio
async def test_a_failed_top_goal_writes_no_reason() -> None:
    """The caller reports that one through `STDERR_ERROR`, so a line here doubles it."""
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", FakeEngine()),
        derived_path=DerivedPath(GONE),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    client = FakeClient()
    await goal.subscribe(cast("Any", client))

    result = await goal.result()

    assert not result_succeeded(result.result)
    assert client.lines == []


@pytest.mark.anyio
async def test_a_client_that_watches_one_goal_twice_reads_each_line_once() -> None:
    """One goal serves the request and an input of the same request.

    `get_ensure_derived_path_goal` gives one goal for one derived path.
    `BuildPathsWithResultsGoal._run` subscribes the client to it as a root of
    the request, and `_realise_input_derivations` subscribes the same client
    to the same goal as an input of another derivation. `_say` wrote one line
    for each entry of `_watchers`, so the client read every line of that goal
    twice.

    `main:build` counts them. `nix build -f fod-failing.nix -j1 -L` names x1
    to x4, and x4 depends on x2 and x3. `build.sh:167` asserts one `error:`
    line, and pynixd wrote three for the one failure of x2. Issue nixidae/nixidae#235.
    """
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", FakeEngine()),
        derived_path=DerivedPath(GONE),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    client = FakeClient()
    await goal.subscribe(cast("Any", client))
    await goal.subscribe(cast("Any", client))
    goal.note_a_parent()

    result = await goal.result()

    assert not result_succeeded(result.result)
    assert client.lines == [f"{_ERROR_PREFIX}{result.result.error_msg}"]
