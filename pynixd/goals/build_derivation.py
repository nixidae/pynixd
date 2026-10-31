"""BuildDerivation action goal."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

import anyio
import structlog

from ..exceptions import DelegationLoopError
from ..serde import (
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildResult,
    BuildResultStatus,
    IsValidPathRequest,
)
from ..store_path import StorePath
from .goal import ExecutionGoal
from .results import GoalResult, goal_failure, result_succeeded

if TYPE_CHECKING:
    from nix_daemon_protocol.ids import BuildId

    from ..answer import Answer
    from ..build_queue import QueuedBuild
    from ..connection import ClientConn
    from ..serde import SetOptionsRequest
    from .engine import GoalEngine

log = structlog.get_logger(__name__)

_RECHECK_S = 60.0
"""How often a holder that waits on a build not yet started looks again."""


def _timeout_of(options: SetOptionsRequest) -> int:
    """`timeout` reaches the daemon as an override; Nix also reads `build-timeout`."""
    raw = options.overrides.get("timeout", options.overrides.get("build-timeout", "0"))
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _tighter(mine: int, owner: int) -> int:
    """My limit, when the owner's does not already end the build first."""
    return mine if mine and (not owner or mine < owner) else 0


def limits_the_owner_leaves(mine: SetOptionsRequest | None, owner: SetOptionsRequest | None) -> tuple[int, int]:
    """`(max-silent-time, timeout)` in seconds that this holder enforces itself; 0 for none.

    The upstream daemon enforces the owner's limits, so a holder enforces only
    a limit that is tighter than the owner's.
    """
    if mine is None or mine is owner:
        return 0, 0
    if owner is None:
        return int(mine.max_silent_time), _timeout_of(mine)
    return (
        _tighter(int(mine.max_silent_time), int(owner.max_silent_time)),
        _tighter(_timeout_of(mine), _timeout_of(owner)),
    )


def limit_reached(build: QueuedBuild, silent: int, timeout: int, now: float) -> tuple[int | None, float]:
    """The limit in seconds that has passed, or `None`, and how long to wait before the next look.

    The clock starts when the build starts, as in `Worker::waitForInput`: time
    in the queue counts against neither limit.
    """
    started = build.started_at
    if started is None:
        return None, _RECHECK_S
    deadlines: list[tuple[float, int]] = []
    if silent:
        deadlines.append(((build.last_output_at or started) + silent, silent))
    if timeout:
        deadlines.append((started + timeout, timeout))
    deadline, limit = min(deadlines)
    if now >= deadline:
        return limit, 0.0
    return None, deadline - now


def _timed_out(seconds: int) -> BuildDerivationResponse:
    """What Nix answers for a goal it timed out, `goal.cc:23`."""
    return BuildDerivationResponse(
        result=BuildResult(
            status=BuildResultStatus.TIMED_OUT,
            error_msg=f"timed out after {seconds} seconds",
        )
    )


@dataclass
class BuildDerivationGoal(ExecutionGoal[GoalResult]):
    """Execute a single derivation build via the scheduler's build queue."""

    may_reach_a_root_goal: ClassVar[bool] = False
    """A build goal reaches no root goal, so a caller may wait and keep its place.

    `_run_and_let_the_next_goal_enqueue` calls the scheduler and nothing else
    before the build is on the queue. `Goal.may_reach_a_root_goal` gives the
    rule. Issue nixidae/nixidae#246.
    """

    engine: GoalEngine
    request: BuildDerivationRequest
    _subscribers: list[ClientConn] = field(default_factory=list)
    _active_subscribers: list[ClientConn] = field(default_factory=list)
    _build_id: BuildId | None = None
    _finished: bool = False
    _reached_the_queue: anyio.Event = field(default_factory=anyio.Event)
    """Set when the build is on the queue, or when this goal ends without one.

    A root goal of a request waits for this before it lets the next root goal
    enqueue. The end of the build is far later, and the order needs the
    earlier moment. Issue nixidae/nixidae#246.
    """

    def __post_init__(self) -> None:
        """Initialize the ExecutionGoal base with the shared engine."""
        ExecutionGoal.__init__(self, self.engine)

    async def wait_until_it_reached_the_queue(self) -> None:
        """Return when the build is on the queue, or when this goal ended."""
        await self._reached_the_queue.wait()

    async def subscribe(self, client: ClientConn | None) -> None:
        """Register a client for real-time log forwarding during the build."""
        if client is None:
            return
        async with self._lock:
            if self._finished:
                return
            build_id = self._build_id
            if build_id is None:
                self._subscribers.append(client)
                return
        await self._subscribe_active(build_id, client)

    async def unsubscribe(self, client: ClientConn | None) -> None:
        """Stop sending the log of this build to *client*.

        The build runs on for every other client that asked for it. This says
        one client stopped listening, and nothing more. Issue nixidae/nixidae#235.
        """
        if client is None:
            return
        async with self._lock:
            build_id = self._build_id
            while client in self._subscribers:
                self._subscribers.remove(client)
            while client in self._active_subscribers:
                self._active_subscribers.remove(client)
        if build_id is not None:
            await self.engine.unsubscribe_build(build_id, client)

    async def _subscribe_active(self, build_id: BuildId, client: ClientConn) -> None:
        if not await self.engine.subscribe_build(build_id, client):
            return

        should_unsubscribe = False
        async with self._lock:
            if self._finished or self._build_id != build_id:
                should_unsubscribe = True
            else:
                self._active_subscribers.append(client)

        if should_unsubscribe:
            await self.engine.unsubscribe_build(build_id, client)

    async def _run(self) -> GoalResult:
        # **Every road out of this method must let the next goal enqueue.**
        # A goal that fails before it reaches the queue would otherwise hold
        # every root goal after it in the order. Issue nixidae/nixidae#246.
        try:
            return await self._run_and_let_the_next_goal_enqueue()
        finally:
            self._reached_the_queue.set()

    async def _run_and_let_the_next_goal_enqueue(self) -> GoalResult:
        if self.engine.ctx.scheduler is None:
            return goal_failure("pynixd: BuildDerivation requires a configured scheduler")

        # **The build carries the options of the client that asked for it.**
        # A build runs after the request of the client returned, so the
        # connection that runs it must get the set from the queue. Issue nixidae/nixidae#231.
        options = next((c.options for c in self._subscribers if c.options is not None), None)
        try:
            build_id, answer = await self.engine.ctx.scheduler.build_derivation(
                self.request,
                from_goal_path=True,
                goal_request_id=self.engine.request_id,
                options=options,
                delegated=any(c.delegated for c in self._subscribers),
            )
        except DelegationLoopError as exc:
            return goal_failure(str(exc))
        # The build is on the queue now, so the next root goal of the request
        # may enqueue behind it and the order of the queue decides. Issue nixidae/nixidae#246.
        self._reached_the_queue.set()
        async with self._lock:
            self._build_id = build_id
            subscribers = list(self._subscribers)
            self._subscribers.clear()

        for client in subscribers:
            await self._subscribe_active(build_id, client)

        try:
            response = await self._wait_within_my_limits(build_id, answer, options)
        finally:
            async with self._lock:
                self._finished = True
                active_subscribers = list(self._active_subscribers)
                self._active_subscribers.clear()
            for client in active_subscribers:
                await self.engine.unsubscribe_build(build_id, client)

        resolved: dict[str, StorePath] = {}
        produced: set[StorePath] = set()

        for name, output in self.request.derivation.outputs.items():
            if output.path:
                path = StorePath(output.path)
                resolved[name] = path
                produced.add(path)

        # `realised_outputs` keys by the output name, whichever of the two
        # wire shapes the answer carried. Issue #14.
        for output_name, realisation in response.result.realised_outputs().items():
            if realisation.out_path:
                path = StorePath(str(realisation.out_path)).with_store_prefix()
                resolved[output_name] = path
                produced.add(path)

        # **A build that failed produced nothing, so it waits for nothing.**
        # `produced` holds the path that the derivation *declares*, which the
        # loop above fills before anything reads the status, so the set is
        # never empty and the wait always ran. A failure means that path
        # cannot appear, so the wait ended at its deadline every time.
        # Measured: 2.0498 s for each failed build of
        # `nix build -f fod-failing.nix -j1 -L`, holding 226 `IsValidPath`
        # queries and nothing else. Issue nixidae/nixidae#326.
        if result_succeeded(response.result):
            await self._wait_for_local_paths(produced)

        status = response.result.status
        if not produced and status == BuildResultStatus.BUILT:
            for path in self.request.derivation.output_paths().values():
                if path:
                    produced.add(path)

        return GoalResult(
            result=response.result,
            resolved_outputs=resolved,
            produced_paths=produced,
            # `BuildQueue.cancel_unwanted` is the only failure that carries no
            # message, and it says the request that wanted this build had
            # already stopped. The mark travels up so the request leaves the
            # place of that root empty rather than reporting a build it chose
            # not to run. Issue nixidae/nixidae#325.
            abandoned=not result_succeeded(response.result) and not response.result.error_msg,
        )

    async def _wait_within_my_limits(
        self,
        build_id: BuildId,
        answer: Answer[BuildDerivationResponse],
        mine: SetOptionsRequest | None,
    ) -> BuildDerivationResponse:
        """The build's answer, or Nix's timeout once this client's own limit passes.

        NIX-DEVIATION (#27): two clients of Nix that want one derivation meet
        at its output lock (`derivation-building-goal.cc:421`), so the second
        waits while the first builds with its own options. `Worker::waitForInput`
        at `worker.cc:485` applies `max-silent-time` and `timeout` to the
        building goal only. pynixd shares one build, so a client that set a
        limit could wait without end on a build its owner runs without one --
        the shape of the hang in #89. Each holder that is not the owner
        enforces its own limits on its wait instead, and lets go when one
        passes; the build ends only when its last holder lets go. To reverse
        it, measure a client that a later holder's limit failed wrongly.
        Issue nixidae/nixidae#231.
        """
        scheduler = self.engine.ctx.scheduler
        build = None if scheduler is None else scheduler.queue.by_id.get(build_id)
        if build is None:
            return await answer.wait()
        silent, timeout = limits_the_owner_leaves(mine, build.options)
        if not silent and not timeout:
            return await answer.wait()
        while not answer.done():
            passed, wait = limit_reached(build, silent, timeout, time.monotonic())
            if passed is not None:
                log.info("build_holder_timed_out", build_id=int(build_id), after_s=passed)
                return _timed_out(passed)
            with anyio.move_on_after(wait):
                await answer.wait()
        return answer.result()

    async def _wait_for_local_paths(self, paths: set[StorePath]) -> None:
        if not paths:
            return
        deadline = anyio.current_time() + 2.0
        while anyio.current_time() < deadline:
            valid_paths = [await self._is_valid_local_path(path) for path in paths]
            if all(valid_paths):
                return
            await anyio.sleep(0.05)

    async def _is_valid_local_path(self, path: StorePath) -> bool:
        response = await self.engine.ctx.local_store.execute(
            IsValidPathRequest(path=StorePath(path=str(path))), mark=False
        )
        return bool(response.valid)
