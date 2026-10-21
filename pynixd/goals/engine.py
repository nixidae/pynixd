"""Request-local goal registry and daemon request entrypoints."""

from __future__ import annotations

import hashlib
import itertools
import json
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, RequestId

from ..exceptions import BackendError
from ..serde import (
    BuildDerivationRequest,
    BuildMode,
    BuildPathsRequest,
    BuildPathsResponse,
    BuildPathsWithResultsRequest,
    QueryMissingRequest,
    QueryMissingResponse,
)
from ..store_path import StorePath
from .build_derivation import BuildDerivationGoal
from .ensure import EnsureDerivedPathGoal
from .keys import BuildDerivationKey, EnsureDerivedPathKey, SubstitutePathKey
from .query_missing import QueryMissingPlanGoal
from .requests import BuildPathsWithResultsGoal
from .results import result_succeeded
from .substitute import SubstitutePathGoal, substituter_fingerprint

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from nix_daemon_protocol.ids import BuildId

    from ..connection import ClientConn
    from ..context import PynixdContext
    from ..derived_path import DerivedPath
    from ..serde import BuildPathsWithResultsResponse
    from ..store import Store
    from .goal import Goal

    Hold = Callable[[StorePath], Awaitable[None]]
    """Roots one path for the client's session: `DaemonProxy.add_temp_root`."""

log = structlog.get_logger(__name__)

_REQUEST_IDS = itertools.count(1)
"""Names the live goal systems apart, for `BuildQueue`. Issue nixidae/nixidae#325."""


def _realised_paths(response: BuildPathsWithResultsResponse) -> list[StorePath]:
    """Each output path a successful entry names, and each opaque path that succeeded."""
    paths: list[StorePath] = []
    for item in response.results:
        if not result_succeeded(item.result):
            continue
        if "!" not in str(item.path):
            paths.append(StorePath(str(item.path)))
        paths.extend(
            StorePath(str(realisation.out_path))
            for realisation in item.result.realised_outputs().values()
            if realisation.out_path
        )
    return paths


def _build_failure_message(failed: list[Any]) -> str:
    """What `BuildPaths` tells the client when a build did not succeed.

    `Store::buildPaths` of Nix collects the message of each failed goal, so
    this collects the `error_msg` of each failed result.
    """
    parts = [str(item.result.error_msg) for item in failed if str(item.result.error_msg)]
    if not parts:
        return f"{len(failed)} of the requested paths failed to build"
    return "; ".join(parts)


def _derivation_fingerprint(request: BuildDerivationRequest) -> str:
    payload = request.derivation.model_dump(mode="json")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


class GoalEngine:
    """Request-local active-goal registry and request entrypoint."""

    def __init__(self, ctx: PynixdContext) -> None:
        self.ctx = ctx
        # What the build queue calls this request. `proxy.py` makes one engine
        # for one request, so the engine *is* the request, and the queue needs
        # a name for it to answer "does any live request still want this
        # build". A counter is enough: the queue compares the names of live
        # engines and nothing else. Issue nixidae/nixidae#325.
        self.request_id = RequestId(next(_REQUEST_IDS))
        self._lock = anyio.Lock()
        self._goals: dict[Any, Goal[Any]] = {}
        self.substitution_import_limiter = anyio.Semaphore(4)

    async def let_go_of_every_build(self) -> None:
        """Give back every build reference that this request took.

        **The request is the unit that wants a build, and not the goal.** A
        request stops at its first failure with `keep-going` off, and the
        goals it leaves behind keep running: `_the_result_unless_it_stops` in
        `goals/requests.py` states why, and a build of pynixd serves every
        client that asked for the same derivation. So the moment the request
        answers is the moment this request wants nothing more, whatever state
        its goals are in.

        `BuildQueue.release_request` ends each build that no other request
        holds. A second call finds nothing held.
        """
        scheduler = self.ctx.scheduler
        if scheduler is None:
            return
        await scheduler.queue.release_request(self.request_id)

    async def subscribe_build(self, build_id: BuildId, client: ClientConn) -> bool:
        """Subscribe *client* to real-time log output for the given *build_id*."""
        scheduler = self.ctx.scheduler
        if scheduler is None:
            return False
        return await scheduler.queue.subscribe(build_id, client, cancel_on_unsubscribe=True)

    async def unsubscribe_build(self, build_id: BuildId, client: ClientConn) -> None:
        """Unsubscribe *client* from real-time log output for the given *build_id*."""
        scheduler = self.ctx.scheduler
        if scheduler is None:
            return
        await scheduler.queue.unsubscribe(build_id, client)

    def reap(self) -> int:
        """Cancel every unfinished goal of this request.

        One engine serves one request, so when the request ends nobody
        watches its goals: answered, failed, or disconnected. A goal whose
        task already finished answers False and keeps its result. Sync, so
        it runs in a `finally` under cancellation too. Queue builds live and
        die by their own references (`let_go_of_every_build` gives this
        request's back first); this only stops the waiting. Issue #79.
        """
        reaped = sum(1 for goal in self._goals.values() if goal.cancel())
        if reaped:
            log.info("goals_reaped", request_id=int(self.request_id), count=reaped)
        else:
            log.debug("goals_reaped", request_id=int(self.request_id), count=0)
        return reaped

    async def build_paths(
        self, request: BuildPathsRequest, client: ClientConn | None = None, hold: Hold | None = None
    ) -> BuildPathsResponse:
        """Execute a BuildPaths request, returning a simple success/failure response."""
        if request.build_mode != BuildMode.NORMAL:
            return await self._straight_to_the_store(request, client)
        response = await self.build_paths_with_results(
            BuildPathsWithResultsRequest(
                derived_paths=request.derived_paths,
                build_mode=request.build_mode,
            ),
            client=client,
            hold=hold,
        )
        # **The value is always 1, and a failure is an error and not a value.**
        # `daemon.cc:558` of Nix writes `conn.to << 1` after `buildPaths`, and
        # `buildPaths` throws when a build fails. A client of Nix reads the
        # number and drops it, so a value of 0 for success reached no client
        # and no test, and a value of 1 for failure read as success. Issue
        # nixidae/nixidae#216 holds the measurement that found this.
        failed = [item for item in response.results if not result_succeeded(item.result)]
        if failed:
            raise BackendError(_build_failure_message(failed))
        return BuildPathsResponse(value=1)

    async def build_paths_with_results(
        self,
        request: BuildPathsWithResultsRequest,
        client: ClientConn | None = None,
        hold: Hold | None = None,
    ):
        """Execute a BuildPathsWithResults request, returning per-path results.

        *hold* roots each path the request realised for the client's session
        before the answer goes out. Nix's worker holds them as temp roots of
        that client (`derivation-goal.cc:67`, `substitution-goal.cc`); pynixd
        brought them in over a pooled connection, whose roots end when
        `CollectGarbage` retires it. nixidae/nixidae#1007.
        """
        if request.build_mode != BuildMode.NORMAL:
            return await self._straight_to_the_store(request, client)
        try:
            response = await BuildPathsWithResultsGoal(self, request, client).result()
            if hold is not None:
                for path in _realised_paths(response):
                    await hold(path)
            return response
        finally:
            # The answer of this request is the moment it wants nothing more.
            # `let_go_of_every_build` says what the build queue does with that.
            # Shielded: a hung-up client cancels this request, and
            # `anyio.Lock` raises at once inside a cancelled scope, which
            # leaves the reference held. Issue #91.
            with anyio.CancelScope(shield=True):
                try:
                    await self.let_go_of_every_build()
                finally:
                    # Whatever state the goals are in, nobody waits for them now.
                    self.reap()

    async def _straight_to_the_store(self, request: Any, client: ClientConn | None) -> Any:
        """A check or a repair goes to the local store, and the goal system stands aside.

        `nix build --rebuild` sends `BuildMode.CHECK`, and `--repair` sends
        `BuildMode.REPAIR`. `nix-store --realise --check`, `--repair-path` and
        `--verify --repair` send the same two. The goal system raised
        `RuntimeError` for each one, so every such command failed through
        pynixd and succeeded through `nix-daemon`.

        **Neither mode is a build that pynixd can schedule.** A check builds
        the derivation again in the same store and compares the two outputs,
        at `derivation-building-goal.cc:990`. A repair reads the closure and
        rewrites what is corrupt, at `derivation-goal.cc:152`. Both are
        operations on one store, and a second builder answers no part of
        either one. The local store is a whole Nix daemon, so it does the
        work, and pynixd carries the bytes.

        The scheduling of pynixd, the dedup of a build and the fleet are all
        out of the path here, and that is the point: they answer a question
        that a check does not ask.

        A mode that no version of Nix defines takes the same road. The store
        answers what it answers, and pynixd invents no behaviour for a number
        that it does not know.
        """
        return await self.ctx.local_store.call(request, client=client)

    async def query_missing(
        self,
        request: QueryMissingRequest,
        client: ClientConn | None = None,
    ) -> QueryMissingResponse:
        """Execute a read-only QueryMissing request, classifying paths as build/substitute/unknown."""
        try:
            return await QueryMissingPlanGoal(self, request, client).result()
        finally:
            self.reap()

    async def get_ensure_derived_path_goal(
        self,
        path: DerivedPath,
        build_mode: int,
        substituter_ids: tuple[str, ...],
    ) -> EnsureDerivedPathGoal:
        """Return a deduplicated EnsureDerivedPathGoal for the given derived path."""
        key = EnsureDerivedPathKey(str(path), substituter_fingerprint(substituter_ids))
        async with self._lock:
            goal = self._goals.get(key)
            if goal is None:
                goal = EnsureDerivedPathGoal(self, path, build_mode, substituter_ids)
                self._goals[key] = goal
            if not isinstance(goal, EnsureDerivedPathGoal):
                raise TypeError(f"goal key collision for {key}")
            return goal

    async def get_build_derivation_goal(self, request: BuildDerivationRequest) -> BuildDerivationGoal:
        """Return a deduplicated BuildDerivationGoal for the given build request."""
        key = BuildDerivationKey(str(request.drv_path), _derivation_fingerprint(request))
        async with self._lock:
            goal = self._goals.get(key)
            if goal is None:
                goal = BuildDerivationGoal(self, request)
                self._goals[key] = goal
            if not isinstance(goal, BuildDerivationGoal):
                raise TypeError(f"goal key collision for {key}")
            return goal

    async def get_substitute_path_goal(
        self,
        path: StorePath,
        substituter_ids: tuple[str, ...],
    ) -> SubstitutePathGoal:
        """Return a deduplicated SubstitutePathGoal for the given store path."""
        key = SubstitutePathKey(str(path), substituter_fingerprint(substituter_ids))
        async with self._lock:
            goal = self._goals.get(key)
            if goal is None:
                goal = SubstitutePathGoal(self, path, substituter_ids)
                self._goals[key] = goal
            if not isinstance(goal, SubstitutePathGoal):
                raise TypeError(f"goal key collision for {key}")
            return goal

    def substituter_ids(self) -> tuple[str, ...]:
        """Return the store IDs of all configured substituter stores (skipping local)."""
        return tuple(
            str(store_id)
            for store_id, store in self.ctx.stores.items()
            if store_id != LOCAL_STORE_ID and store.no_schedule
        )

    def substituter_stores(self) -> Iterable[Store]:
        """Yield healthy substituter stores, ordered by store ID."""
        ids = set(self.substituter_ids())
        return (
            store
            for store_id, store in sorted(self.ctx.stores.items(), key=lambda item: str(item[0]))
            if store_id != LOCAL_STORE_ID and str(store_id) in ids and store.is_healthy
        )
