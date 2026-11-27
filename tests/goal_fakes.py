"""A fake `GoalEngine` for the tests of `EnsureDerivedPathGoal`, and the goals it hands out.

A test subclasses `FakeGoalEngine` and gives it the store and the context of
its case. It overrides a method only where its case differs: a build that
writes outputs, a child goal that carries a result.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from pynixd.goals.results import GoalResult, goal_success
from pynixd.goals.substitute import SubstituteAttempt

if TYPE_CHECKING:
    from pynixd.serde import BuildDerivationRequest
    from pynixd_store.store_path import StorePath


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
    may_reach_a_root_goal = True
    """The goal of an input can reach a root goal of the request. Issue nixidae/nixidae#246."""

    def note_a_parent(self) -> None:
        """The goal of an input has a goal that waits for it."""

    async def subscribe_many(self, clients: list[Any]) -> None:
        del clients

    async def result(self) -> GoalResult:
        return goal_success()


class FakeSubstituteGoal:
    """A substituter that holds nothing."""

    may_reach_a_root_goal = False
    """A substitute goal reaches no root goal. Issue nixidae/nixidae#246."""

    async def subscribe(self, client: object) -> None:
        del client

    async def unsubscribe(self, client: object) -> None:
        del client

    async def result(self) -> SubstituteAttempt:
        return SubstituteAttempt(found=False, result=goal_success())


class FakeGoalEngine:
    """Enough of `GoalEngine` for `EnsureDerivedPathGoal`: each request gets a fake goal."""

    substitution_queue = None
    """No backend: the goal asks the daemon only for a client that names a substituter."""

    child_goal: ClassVar[type[FakeChildGoal]] = FakeChildGoal
    """The goal of an input derivation, which a test replaces to give it a result."""

    def __init__(self) -> None:
        self.build_goals: list[FakeBuildGoal] = []
        self.child_goals: list[FakeChildGoal] = []

    async def hold_what_it_made(self, result: Any) -> None:
        del result

    async def get_build_derivation_goal(self, request: BuildDerivationRequest) -> FakeBuildGoal:
        goal = FakeBuildGoal(request)
        self.build_goals.append(goal)
        return goal

    async def get_ensure_derived_path_goal(
        self, dp: Any, build_mode: int, substituter_ids: Any, *, keys: object = ()
    ) -> FakeChildGoal:
        del dp, build_mode, substituter_ids, keys
        goal = self.child_goal()
        self.child_goals.append(goal)
        return goal

    async def get_substitute_path_goal(
        self,
        path: StorePath,
        substituter_ids: tuple[str, ...],
        *,
        ca: object = None,
        keys: object = (),
    ) -> Any:  # Any: each test's goal type stands in for `SubstitutePathGoal`
        del path, substituter_ids, ca, keys
        return FakeSubstituteGoal()
