"""Request-local goal registry and daemon request entrypoints."""

from __future__ import annotations

import contextlib
import hashlib
import itertools
import json
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, RequestId
from nix_daemon_protocol.protocol import Verbosity
from pynixd_store.background import Background
from pynixd_store.hintfmt import magenta
from pynixd_store.store_path import StorePath
from pynixd_store.verify import RepairError

from ..derived_path import DerivedPath, OutputsAll
from ..exceptions import BackendError
from ..op_routes import OpRoute
from ..serde import (
    BuildDerivationRequest,
    BuildMode,
    BuildPathsRequest,
    BuildPathsResponse,
    BuildPathsWithResultsRequest,
    IsValidPathRequest,
    QueryMissingRequest,
    QueryMissingResponse,
)
from ..store.local_db import LocalDBStore
from .build_derivation import BuildDerivationGoal
from .ensure import EnsureDerivedPathGoal
from .keys import BuildDerivationKey, EnsureDerivedPathKey, SubstitutePathKey
from .query_missing import QueryMissingPlanGoal, daemon_substitutes
from .requests import BuildPathsWithResultsGoal
from .results import goal_failure, goal_success, result_succeeded
from .substitute import SubstituteAttempt, SubstitutePathGoal, substituter_fingerprint

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable

    from nix_daemon_protocol.ids import BuildId
    from pynixd_store.content_address import ContentAddress

    from ..connection import ClientConn
    from ..context import PynixdContext
    from ..serde import BuildPathsWithResultsResponse
    from ..store import Store
    from ..substitution_queue import SubstitutionQueue
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
    if len(failed) == 1 and str(failed[0].result.error_msg):
        return str(failed[0].result.error_msg)
    # `drvReq->to_string` or the substituted path: the derived path without
    # its last output list, `^` between the levels.
    names = sorted({"^".join(str(item.path).split("!")[:-1]) or str(item.path) for item in failed})
    return "build of " + ", ".join(f"'{name}'" for name in names) + " failed"


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
        self._background: Background | None = None
        self.substitution_import_limiter = anyio.Semaphore(4)
        self._contents: dict[str, bool] = {}
        """`Worker::pathContentsGoodCache` (`worker.cc:556`): a path's contents are checked once per request."""

    def _local_db(self) -> LocalDBStore:
        store = self.ctx.local_store
        if not isinstance(store, LocalDBStore):
            raise TypeError("a repair through the goals needs pynixd's own local store")
        return store

    async def closure(self, paths: Iterable[str]) -> set[StorePath]:
        """`computeFSClosure` of *paths* in the local store."""
        return {StorePath(str(p)) for p in await self._local_db().engine.query_closure(sorted(paths))}

    async def contents_good(self, path: StorePath, say: Callable[[int, str], Awaitable[None]]) -> bool:
        """`Worker::pathContentsGood` (`worker.cc:553`): once per path and request, with its two lines."""
        known = self._contents.get(str(path))
        if known is not None:
            return known
        await say(Verbosity.INFO, f"checking path '{path}'...")
        good = self._contents[str(path)] = await self._local_db().contents_good(str(path))
        if not good:
            await say(Verbosity.ERROR, f"path '{path}' is corrupted or missing!")
        return good

    def mark_contents_good(self, path: StorePath) -> None:
        """`Worker::markContentsGood` (`worker.cc:572`): this request substituted or built *path*."""
        self._contents[str(path)] = True

    @property
    def substitution_queue(self) -> SubstitutionQueue | None:
        """The scheduler's race over pynixd's substituter backends, or `None` without a scheduler."""
        scheduler = self.ctx.scheduler
        return scheduler.substitution_queue if scheduler is not None else None

    @property
    def goal_group(self) -> Background:
        """The task group the goals of this request run in, open inside `running`."""
        if self._background is None:
            raise RuntimeError("GoalEngine runs its goals only inside `running`")
        return self._background

    @contextlib.asynccontextmanager
    async def running(self) -> AsyncIterator[None]:
        """Run the goals of this request for the body of this block, and cancel what is left at its end."""
        if self._background is not None:
            raise RuntimeError("GoalEngine is running already")
        # A fresh one each time: a `Background` runs once.
        self._background = Background("GoalEngine")
        try:
            async with self._background.running():
                yield
        finally:
            self._background = None

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
        if request.build_mode != BuildMode.NORMAL and await self._the_store_answers(request, client):
            straight: BuildPathsResponse = await self._straight_to_the_store(request, client)
            return straight
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
    ) -> BuildPathsWithResultsResponse:
        """Execute a BuildPathsWithResults request, returning per-path results.

        *hold* roots each path the request realised for the client's session
        before the answer goes out. Nix's worker holds them as temp roots of
        that client (`derivation-goal.cc:67`, `substitution-goal.cc`); pynixd
        brought them in over a pooled connection, whose roots end when
        `CollectGarbage` retires it. nixidae/nixidae#1007.
        """
        if request.build_mode != BuildMode.NORMAL and await self._the_store_answers(request, client):
            straight: BuildPathsWithResultsResponse = await self._straight_to_the_store(request, client)
            return straight
        async with self.running():
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

    async def _the_store_answers(
        self, request: BuildPathsRequest | BuildPathsWithResultsRequest, client: ClientConn | None
    ) -> bool:
        """Whether the local store's daemon answers *request* whole, and the goal system stands aside.

        A check or a repair goes through the goals when the store holds the
        derivation of each derived path; a nested path checks or repairs only
        the derivation it ends in. `derivation-goal.cc:143` needs its outputs valid, a
        floating output's path is its realisation's, and an impure one builds
        again with no check of validity (`derivation-goal.cc:71`). A
        repair also needs pynixd's own local store, which checks contents and
        builds, and pynixd's substituters: the daemon's
        `EnsurePath` cannot repair. A daemon that builds under repair checks
        each output's contents again, and its client would read the lines
        twice. An opaque path is a substitution, with repair under repair
        and without it under check (`worker.cc:138`). A mode no version of
        Nix defines, and anything else, go to the daemon: pynixd invents no
        behaviour for them.
        """
        if request.build_mode not in (BuildMode.CHECK, BuildMode.REPAIR):
            return True
        if request.build_mode == BuildMode.REPAIR and not await self.repairs_here(client):
            return True
        for wire_path in request.derived_paths:
            path = DerivedPath(wire_path.value)
            if path.is_opaque:
                continue
            parsed = await self.ctx.local_store.read_derivation(path.drv_path)
            if parsed is None:
                return True
        return False

    async def repairs_here(self, client: ClientConn | None) -> bool:
        """Whether pynixd's goals repair for *client*: its own store builds, and its own substituters answer."""
        store = self.ctx.local_store
        return (
            isinstance(store, LocalDBStore)
            and store.routes.get(BuildDerivationRequest.op) is OpRoute.pynixd
            and not await daemon_substitutes(client, self.substitution_queue)
        )

    async def repair_path(self, path: StorePath, client: ClientConn | None) -> None:
        """`Store::repairPath` (`build/entry-points.cc:118`): substitute *path* with repair, else build its deriver.

        The deriver builds every output in repair mode. Raises `RepairError` with Nix's text when *path* has no
        valid deriver. Nix makes a fresh `Worker` for each path, so each call takes a fresh engine.
        """
        async with self.running():
            try:
                ids = await self.substituter_ids_for(client)
                substitute = await self.get_substitute_path_goal(path, ids, repair=True)
                attempt = await substitute.result()
                if attempt.found and result_succeeded(attempt.result.result):
                    return
                engine = self._local_db().engine
                info = await engine.query_path_info(str(path))
                deriver = str(info.info.deriver) if info is not None else ""
                if not deriver or not await engine.is_valid_path_uncached(deriver):
                    raise RepairError(f"cannot repair path '{magenta(str(path))}'")
                every = DerivedPath._from_components(drv_path=StorePath(deriver), chain=(), outputs=OutputsAll())
                goal = await self.get_ensure_derived_path_goal(every, BuildMode.REPAIR, ids)
                await goal.subscribe(client)
                await goal.result()
            finally:
                with anyio.CancelScope(shield=True):
                    try:
                        await self.let_go_of_every_build()
                    finally:
                        self.reap()

    async def _straight_to_the_store(self, request: Any, client: ClientConn | None) -> Any:
        """The local store's daemon does the work, and pynixd carries the bytes.

        A repair `_the_store_answers` names, and a check of what pynixd's
        goals do not build, go to the daemon whole.
        """
        return await self.ctx.local_store.call(request, client=client)

    async def query_missing(
        self,
        request: QueryMissingRequest,
        client: ClientConn | None = None,
    ) -> QueryMissingResponse:
        """Execute a read-only QueryMissing request, classifying paths as build/substitute/unknown."""
        async with self.running():
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
        key = EnsureDerivedPathKey(str(path), substituter_fingerprint(substituter_ids), int(build_mode))
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
        key = BuildDerivationKey(str(request.drv_path), _derivation_fingerprint(request), int(request.build_mode))
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
        *,
        repair: bool = False,
        ca: ContentAddress | None = None,
    ) -> SubstitutePathGoal:
        """Return a deduplicated SubstitutePathGoal for the given store path.

        The first caller's *ca* holds, as in `Worker::makePathSubstitutionGoal` (`worker.cc:110`).
        """
        key = SubstitutePathKey(str(path), substituter_fingerprint(substituter_ids), repair)
        async with self._lock:
            goal = self._goals.get(key)
            if goal is None:
                goal = SubstitutePathGoal(self, path, substituter_ids, repair=repair, ca=ca)
                self._goals[key] = goal
            if not isinstance(goal, SubstitutePathGoal):
                raise TypeError(f"goal key collision for {key}")
            return goal

    async def substitute_paths(
        self,
        paths: Iterable[StorePath],
        hold: Hold | None = None,
        *,
        substitutes: bool = True,
        ids: tuple[str, ...] | None = None,
    ) -> dict[StorePath, SubstituteAttempt]:
        """Substitute each path with its closure, all at once: one goal for each, as `Store::buildPaths` makes.

        *hold* roots each path the goals made valid for the client's session,
        as the substitution goal of Nix does (`substitution-goal.cc:33`).
        nixidae/nixidae#1007. Without *substitutes* a valid path still
        succeeds: the goal asks the store before any substituter
        (`substitution-goal.cc:36`), and `use-substitutes` off leaves it none
        (`worker.cc:32`).
        """
        if not substitutes:
            return await self._valid_or_missed(paths, hold)
        if ids is None:
            ids = self.substituter_ids()
        async with self.running():
            try:
                goals = {path: await self.get_substitute_path_goal(path, ids) for path in paths}
                attempts: dict[StorePath, SubstituteAttempt] = {}

                async def run_one(path: StorePath, goal: SubstitutePathGoal) -> None:
                    attempts[path] = await goal.result()

                async with anyio.create_task_group() as tg:
                    for path, goal in goals.items():
                        tg.start_soon(run_one, path, goal)
                if hold is not None:
                    for attempt in attempts.values():
                        for produced in attempt.result.produced_paths:
                            await hold(produced)
                return attempts
            finally:
                self.reap()

    async def _valid_or_missed(
        self, paths: Iterable[StorePath], hold: Hold | None
    ) -> dict[StorePath, SubstituteAttempt]:
        attempts: dict[StorePath, SubstituteAttempt] = {}
        for path in paths:
            response = await self.ctx.local_store.execute(IsValidPathRequest(path=StorePath(str(path))), mark=False)
            if response.valid:
                if hold is not None:
                    await hold(path)
                result = goal_success()
                result.produced_paths.add(path)
                attempts[path] = SubstituteAttempt(found=True, result=result)
            else:
                attempts[path] = SubstituteAttempt(
                    found=False, result=goal_failure(f"pynixd: substitution is off for {path}")
                )
        return attempts

    async def substituter_ids_for(self, client: ClientConn | None) -> tuple[str, ...]:
        """The ids of the backends that stand for *client*'s substituters; the daemon's list where only the daemon reaches them.

        In that case the goals still ask pynixd's backends first, then the
        daemon (`daemon_substitutes`).
        """
        queue = self.substitution_queue
        stores = await queue.substituters_for(client) if queue is not None else None
        return self.substituter_ids() if stores is None else tuple(str(store.store_id) for store in stores)

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
