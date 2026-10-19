"""The order in which the root goals of one request may enqueue a build.

**Nix has no race here, and the reason is how its worker steps a goal.**
`Worker::run` at `worker.cc:352` takes a snapshot of `awake`, which is a
`std::set` over `CompareGoalPtrs`, and then calls `goal->work()` on each
member in that order. A store query inside a goal of Nix is an ordinary
blocking call, so one `work()` call carries the goal from its start to
`waitForBuildSlot`. Stepping the goals in the order of the key and asking
for a build slot in the order of the key are therefore the same thing.

pynixd runs its root goals as coroutines that suspend on real I/O, so the
four goals of one request reach "I want a build" in the order that the event
loop gives them. `main:build` measured 26 ms between the first and the last,
and the one slot of `-j1` went to whichever goal arrived first.

**This orders the moment a goal enqueues, and it does not order the
preparation.** Every goal still reads its derivation and asks the store at
the same time as its siblings. A goal may enqueue its build when every goal
before it in the order has decided, and a goal decides when it enqueues a
build or when it ends without one.

Two properties follow, and both are the reason for this shape:

- **The first goal never waits.** It has nothing before it, so it enqueues as
  soon as it is ready and takes the slot. That is the goal Nix would take.
- **The idle time is the time that Nix is idle as well.** Nix does not step
  the second goal until the step of the first one returns, so it waits for
  the same preparation.

**This orders the roots that are ready to build, and not every root.** A goal
gives its place up before it waits for a goal that can reach another root of
the same request, and `EnsureDerivedPathGoal.run_child` holds that rule. The
two would wait for each other without it: a derivation early in the order can
depend on a derivation late in it, so the early goal waits for the late one to
finish while the late one waits for the early one to decide. A root that waits
for its input derivations therefore reaches the queue in no fixed order against
its siblings. Nix keeps the order across such a wait, because `Worker::awake` is
a set over the key and it sorts the goals again each time it wakes them. The
difference reaches a request whose roots each have an input to build, and it
reaches no test of the suite: the four derivations of `fod-failing.nix` that
`build.sh:167` measures have no input derivation.

**The measurement.** `main - nix-functional-tests:build`, 10 runs before and 20
runs after, against Nix 2.34. Before: 7 runs failed. After: 0 runs failed.

Issue nixidae/nixidae#246.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import anyio
import structlog

if TYPE_CHECKING:
    from collections.abc import Sequence

log = structlog.get_logger(__name__)


class DispatchOrder:
    """The places of the root goals of one request, in the order of the key."""

    def __init__(self, count: int) -> None:
        self._decided: list[anyio.Event] = [anyio.Event() for _ in range(count)]

    def __len__(self) -> int:
        return len(self._decided)

    def turn(self, position: int) -> DispatchTurn:
        """The turn of the goal at *position* of the order."""
        return DispatchTurn(self, position)

    async def wait_for_the_goals_before(self, position: int) -> None:
        """Return when every goal before *position* decided."""
        for earlier, event in enumerate(self._decided[:position]):
            if event.is_set():
                continue
            log.debug("dispatch_waits_for_an_earlier_goal", position=position, earlier=earlier)
            await event.wait()

    def note_that_it_decided(self, position: int) -> None:
        """Let the goals after *position* enqueue. This is safe to repeat."""
        self._decided[position].set()

    def turns_in_the_order_of(self, order: Sequence[int]) -> list[DispatchTurn]:
        """A turn for each goal, by the place of that goal in the request.

        *order* holds the indices of the goals, in the order of the key, which
        is what `_goal_order` answers. The answer holds one turn for each goal,
        at the place the client wrote it, so a caller reads it by the same
        index that it reads the goals.
        """
        if len(order) != len(self._decided):
            raise ValueError("the order does not name every goal")
        turns: list[DispatchTurn | None] = [None] * len(order)
        for place, index in enumerate(order):
            if index >= len(turns) or turns[index] is not None:
                raise ValueError("the order does not name every goal once")
            turns[index] = self.turn(place)
        answer: list[DispatchTurn] = []
        for turn in turns:
            if turn is None:
                raise ValueError("the order does not name every goal")
            answer.append(turn)
        return answer

    def release_every_goal(self) -> None:
        """Let every goal enqueue, whatever it decided.

        **A goal that never runs must not hold the goals after it.** The
        request stops taking goals at the first failure, and a goal that it
        never took never decides. `_run_the_root_goals` calls this when it
        ends, so no goal waits for a turn that cannot come.
        """
        for event in self._decided:
            event.set()


@dataclass(frozen=True)
class DispatchTurn:
    """The place of one goal in a `DispatchOrder`."""

    order: DispatchOrder
    position: int

    async def wait(self) -> None:
        """Return when every goal before this one decided."""
        await self.order.wait_for_the_goals_before(self.position)

    def decided(self) -> None:
        """Let the goals after this one enqueue. This is safe to repeat."""
        self.order.note_that_it_decided(self.position)
