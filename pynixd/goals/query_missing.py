"""Read-only QueryMissing planning goals."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import anyio
import structlog

from nix_daemon_protocol.exceptions import DaemonProtocolError

from ..derived_path import DerivedPath
from ..exceptions import BackendError
from ..serde import (
    ContentAddress,
    DerivedPath as SerdeDerivedPath,
    IsValidPathRequest,
    LogNext,
    QueryMissingRequest,
    QueryMissingResponse,
    QuerySubstitutablePathInfosRequest,
)
from ..store_path import StorePath
from ..substitution_queue import SubstitutionAvailability
from .goal import ExecutionGoal
from .realisations import realisations_of

if TYPE_CHECKING:
    from anyio.abc import TaskGroup

    from ..connection import ClientConn
    from ..drv_parser import ChildMapNode, Derivation
    from .engine import GoalEngine

log = structlog.get_logger(__name__)


@dataclass
class QueryMissingPlan:
    """Accumulated classification of paths into build/substitute/unknown buckets."""

    will_build: set[StorePath]
    will_substitute: set[StorePath]
    unknown: set[StorePath]
    download_size: int = 0
    nar_size: int = 0

    def add_substitute(self, path: StorePath, availability: SubstitutionAvailability) -> None:
        """Record that *path* can be substituted and accumulate its download size."""
        self.will_substitute.add(StorePath(path=str(path)))
        self.download_size += availability.download_size or 0
        self.nar_size += availability.nar_size or 0


_SUBSTITUTER_OPTIONS = ("substituters", "extra-substituters", "trusted-substituters")
"""The names that make a client ask for a substituter of its own choosing."""


def client_names_a_substituter(client: ClientConn | None) -> bool:
    """True when the option set of *client* names a substituter."""
    options = client.options if client is not None else None
    if options is None:
        return False
    return any(name in options.overrides for name in _SUBSTITUTER_OPTIONS)


@dataclass
class _Walk:
    """The state of one walk over the derived paths of a request.

    `Store::queryMissing` runs a thread pool over a work list, and each item
    may add more. This holds the same three things: the answer, the set of
    derived paths that the walk already read, and the group that runs a task
    for each new one.
    """

    plan: QueryMissingPlan
    task_group: TaskGroup
    seen: set[str] = field(default_factory=set)
    warnings: list[str] = field(default_factory=list)


@dataclass
class QueryMissingPlanGoal(ExecutionGoal[QueryMissingResponse]):
    """Read-only root goal for QueryMissing.

    `Store::queryMissing` at `misc.cc:102` is what this answers. It walks the
    input derivations of each derivation that must be built, and it walks the
    references of each path that a substituter holds.

    **The client can name a substituter that pynixd has no store for.**
    `--substituters file:///...` is one, and so is any cache that the
    configuration of pynixd does not list. The daemon behind pynixd speaks to
    every kind of substituter already, so this asks that daemon rather than
    building a second binary-cache client. Issue nixidae/nixidae#226.
    """

    engine: GoalEngine
    request: QueryMissingRequest
    client: ClientConn | None = None

    def __post_init__(self) -> None:
        ExecutionGoal.__init__(self, self.engine)

    async def _run(self) -> QueryMissingResponse:
        plan = QueryMissingPlan(will_build=set(), will_substitute=set(), unknown=set())

        # Bound before the block, because an anyio task group may swallow
        # what its body raised and `__aexit__` is typed as able to. The read
        # of `walk.warnings` below is then past a name that never got one.
        walk: _Walk | None = None
        async with anyio.create_task_group() as tg:
            walk = _Walk(plan=plan, task_group=tg)
            for wire_path in self.request.derived_paths:
                self._enqueue(walk, wire_path.value)

        log.debug(
            "query_missing_goal_plan",
            requested=len(self.request.derived_paths),
            will_build=len(plan.will_build),
            will_substitute=len(plan.will_substitute),
            unknown=len(plan.unknown),
        )
        response = QueryMissingResponse(
            will_build=plan.will_build,
            will_substitute=plan.will_substitute,
            unknown=plan.unknown,
            download_size=plan.download_size,
            nar_size=plan.nar_size,
        )
        for text in walk.warnings if walk is not None else ():
            response.logs.add(LogNext(text=f"warning: {text}\n"))
        return response

    def _enqueue(self, walk: _Walk, wire_path: str) -> None:
        """Classify one derived path, unless the walk did that already.

        `doPath` of `Store::queryMissing` keeps a `done` set of the string form
        of each derived path, at `misc.cc:188`. A closure that reaches one
        derivation by two roads then reads it once.

        The check and the insertion take no await between them, so two tasks
        cannot both pass it.
        """
        if wire_path in walk.seen:
            return
        walk.seen.add(wire_path)
        walk.task_group.start_soon(self._classify_wire_path, wire_path, walk)

    async def _classify_wire_path(self, wire_path: str, walk: _Walk) -> None:
        derived_path = DerivedPath(wire_path)
        base_path = derived_path.base_store_path()
        # **The question is the shape of the derived path, and not the name of
        # the store path.** `Store::queryMissing` at `misc.cc:102` reads the
        # two cases of `DerivedPath` apart, and the opaque case asks about the
        # path alone. A `.drv` name in an opaque path means nothing there.
        if not derived_path.is_opaque:
            await self._classify_derivation(derived_path, walk)
            return

        await self._classify_opaque_path(base_path, walk.plan)

    def _must_build(self, drv_path: StorePath, parsed: Derivation | None, walk: _Walk) -> None:
        """The derivation builds, and so does each input that it needs.

        `mustBuildDrv` at `misc.cc:139` puts the derivation in `willBuild` and
        then enqueues each input derivation with the outputs that this one
        wants. pynixd classified the derived paths of the request alone, so
        `nix build` printed one derivation where `nix-daemon` printed the
        whole list. `impure-derivations.sh` is where that showed: the daemon
        named `impure.drv` and `impure-on-impure.drv`, and pynixd named the
        second one only.

        A dynamic input takes part as well. `enqueueDerivedPaths` at
        `misc.cc:130` walks the tree of one and enqueues a derived path for
        each level that names an output. `doPath` then warns for each of those
        and puts it in no bucket, so the answer holds nothing for one. The
        warning is the part that a reader sees, and pynixd wrote none.
        """
        walk.plan.will_build.add(StorePath(path=str(drv_path)))
        if parsed is None:
            return
        for input_drv_path, output_names in parsed.input_drvs.items():
            if not output_names:
                continue
            self._enqueue(walk, f"{input_drv_path}!{','.join(sorted(output_names))}")
        for input_drv_path, node in parsed.dynamic_input_drvs.items():
            self._enqueue_child_map(walk, str(input_drv_path), node)

    def _enqueue_child_map(self, walk: _Walk, prefix: str, node: ChildMapNode) -> None:
        """Enqueue one derived path for each level of a dynamic input.

        `enqueueDerivedPaths` at `misc.cc:130` takes the direct outputs of the
        level first, and then goes one level deeper for each child. The prefix
        grows by one output name at each step, which is
        `SingleDerivedPath::Built` of Nix.

        The separator is `!`, which is the one that `DerivedPath.__str__` and
        the rest of this walk use. Nix prints `^` and reads both.
        """
        if node.outputs:
            self._enqueue(walk, f"{prefix}!{','.join(sorted(node.outputs))}")
        for output_name, child in node.children.items():
            self._enqueue_child_map(walk, f"{prefix}!{output_name}", child)

    async def _classify_derivation(self, derived_path: DerivedPath, walk: _Walk) -> None:
        drv_path = derived_path.base_store_path()
        if derived_path.is_nested:
            # **A dynamic derived path goes in no bucket, and it gets a
            # warning.** `doPath` at `misc.cc:196` reads the derivation path of
            # the request, finds a `Built` and not an `Opaque`, and returns.
            # The subject of the warning is that inner path, which is this one
            # with the last output name removed.
            #
            # NIX-DEFECT (#23): Nix walks the whole request twice, so the
            # client reads this warning twice. `nix-build` calls
            # `queryMissing` to print the build plan, and `Worker::run` at
            # `worker.cc:340` calls it again inside the daemon. The second
            # walk answers the same question about the same paths, and the
            # daemon holds no result of the first one. pynixd walks once, so
            # it writes one warning, and issue nixidae/nixidae#228 holds that difference.
            walk.warnings.append(
                f"Ignoring dynamic derivation {derived_path.outer.to_string()} "
                "while querying missing paths; not yet implemented",
            )
            return

        parsed = await self.engine.ctx.local_store.read_derivation(str(drv_path))
        if parsed is None:
            walk.plan.unknown.add(StorePath(path=str(drv_path)))
            return
        if parsed.is_dynamic:
            self._must_build(drv_path, parsed, walk)
            return

        # **NIX-DEFECT (#23): `queryMissing` decides `knownOutputPaths` from
        # every output of the derivation, and it ignores the outputs that the
        # caller wants.** The loop at `misc.cc:217-225` breaks on the first
        # output with no path. The line under that break reads `bfd.outputs`,
        # and the substituter loop at `misc.cc:250` reads it as well, so two
        # of the three tests honour the wanted outputs and the first one does
        # not. A content-addressed derivation reaches that state after one
        # ordinary build, because `DerivationGoal` holds one `wantedOutput`
        # and registers a realisation for that output alone, at
        # `derivation-goal.cc:228-236`. `nix build rootCA^out --dry-run` then
        # announces a build of a derivation whose `out` is already realised.
        # pynixd could copy that and read every output here. It does not:
        # `selected_output_paths` reads the wanted outputs, so pynixd leaves
        # the derivation out of `willBuild` and `nix-daemon` puts it in.
        # `Lillecarl/nix#312` reports the defect, `docs/notes/querymissing.md`
        # holds the measurement, and issue #26 holds the difference.
        output_paths = parsed.selected_output_paths(derived_path.output_names)
        if not output_paths:
            # **A request that names no output of this derivation plans
            # nothing.** `Store::queryMissing` reads `bfd.outputs` only to
            # decide whether an output path is invalid, at `misc.cc:222`. A
            # name that the derivation does not carry matches nothing there,
            # so `invalid` stays empty and the walk returns at `misc.cc:225`
            # with no derivation to build. `build.sh:98` sends
            # `multiple-outputs-a.drv!not-an-output`, and `build.sh:102` sends
            # an empty output list. Each one must fail the build, and neither
            # one is a derivation that a plan can name.
            #
            # pynixd answered `willBuild` here. The client then asked for the
            # path of that derivation and tried to build it, so it sent one
            # operation more than it sends to `nix-daemon`. Issue #26.
            log.debug(
                "query_missing_no_wanted_output",
                drv_path=str(drv_path),
                wanted=sorted(derived_path.output_names),
            )
            return

        unnamed = [name for name, path in output_paths.items() if not str(path)]
        realised = await self._realised_paths(parsed, unnamed, drv_path) if unnamed else {}
        if realised is None:
            if await self._substituter_holds_a_realisation(derived_path, walk):
                return
            self._must_build(drv_path, parsed, walk)
            return

        needs_build = False
        for output_name, output_path in output_paths.items():
            path = output_path if str(output_path) else realised[output_name]
            if not await self._classify_output_path(path, walk.plan):
                needs_build = True
        if needs_build:
            self._must_build(drv_path, parsed, walk)

    async def _substituter_holds_a_realisation(self, derived_path: DerivedPath, walk: _Walk) -> bool:
        """Ask the daemon behind pynixd whether a substituter can realise this.

        **Nix asks the substituters for a realisation, and it asks them in the
        plan.** `Store::queryMissing` at `misc.cc:250` loops over
        `getDefaultSubstituters()` and calls `sub->queryRealisation({drvPath,
        outputName})` for each wanted output. When every one answers,
        `knownOutputPaths` becomes true again and the outputs take the
        substitution road; when one does not, the derivation is
        `mustBuildDrv`.

        pynixd has no substituter of its own, so it cannot run that loop. The
        daemon behind it has them all, and `QueryMissing` upstream with the
        option set of the client makes that daemon run the loop over the
        substituters that the client named.

        **The plan and the work have to give the same answer.**
        `_try_realise_upstream` of `goals/ensure.py` now substitutes such an
        output through the same daemon, and a plan that still said "will be
        built" made pynixd announce a build that never happened.
        `build-cache.sh:39` reads exactly that: it greps the output of
        `nix build` for the absence of ` will be built:`. Issues nixidae/nixidae#226 and
        nixidae/nixidae#237.

        Answers True when the upstream plan does not name this derivation as
        one to build, and it then merges what that plan says. Answers False
        for a client that named no substituter, for an upstream error, and
        for an upstream plan that agrees the derivation must be built.
        """
        if not client_names_a_substituter(self.client):
            return False
        wire_path = SerdeDerivedPath(value=str(derived_path))
        try:
            response = await self.engine.ctx.local_store.execute(
                QueryMissingRequest(derived_paths={wire_path}), client=self.client, mark=False
            )
        except (BackendError, DaemonProtocolError, OSError, EOFError) as ex:
            # The plan is a question, and a question that fails is not a
            # failure of the request. The build road stays open.
            log.debug("upstream_plan_miss", derived_path=str(derived_path), reason=str(ex))
            return False

        drv_path = StorePath(path=str(derived_path.base_store_path()))
        if drv_path in response.will_build:
            return False
        walk.plan.will_substitute.update(response.will_substitute)
        walk.plan.unknown.update(response.unknown)
        log.debug(
            "upstream_plan_substitutes",
            derived_path=str(derived_path),
            will_substitute=len(response.will_substitute),
        )
        return True

    async def _realised_paths(
        self,
        parsed: Derivation,
        wanted: list[str],
        drv_path: StorePath,
    ) -> dict[str, StorePath] | None:
        """The path of each output that the derivation does not name.

        `Store::queryMissing` reads `queryPartialDerivationOutputMap` at
        `misc.cc:217`, which answers the path of a content-addressed output
        from the realisation that the build registered. When every wanted
        output has a path, and every one of those paths is valid, the loop at
        `misc.cc:225` returns and the derivation is in no bucket at all.

        pynixd read the derivation alone. A content-addressed derivation names
        no output path, so pynixd answered `willBuild` for one that it had
        already built, and the client then took a different code path from the
        one it takes with `nix-daemon`. Issue nixidae/nixidae#214.

        Answers `None` when a wanted output has no realisation, and also for
        an impure derivation, which Nix never treats as built. That is the
        `knownOutputPaths = false` of Nix, and the derivation must be built.
        """
        if parsed.is_impure:
            return None
        found = await realisations_of(parsed, wanted, self.engine.ctx.local_store, drv_path)
        if found is None:
            return None
        return {key.rpartition("!")[2]: StorePath(str(realisation.out_path)) for key, realisation in found.items()}

    async def _classify_opaque_path(self, path: StorePath, plan: QueryMissingPlan) -> None:
        if not await self._classify_output_path(path, plan):
            plan.unknown.add(StorePath(path=str(path)))

    async def _classify_output_path(self, path: StorePath, plan: QueryMissingPlan) -> bool:
        response = await self.engine.ctx.local_store.execute(
            IsValidPathRequest(path=StorePath(path=str(path))), mark=False
        )
        if response.valid:
            return True
        availability = await self._can_substitute(path)
        if availability.available:
            plan.add_substitute(path, availability)
            return True
        return False

    async def _can_substitute(self, path: StorePath) -> SubstitutionAvailability:
        scheduler = self.engine.ctx.scheduler
        if scheduler is not None:
            availability = await scheduler.substitution_queue.can_substitute(path)
            if availability.available:
                return availability
        return await self._can_substitute_upstream(path)

    async def _can_substitute_upstream(self, path: StorePath) -> SubstitutionAvailability:
        """Ask the daemon behind pynixd, when the client named a substituter.

        **This runs only when the client names one.** A client that names none
        gets the behaviour of before, so pynixd asks its own backends and
        nothing else. A round trip for every missing path, against every
        substituter of the machine, is not a cost to pay for a client that did
        not ask for it.

        `ca:build-cache`, `ca:issue-13247` and `ca:new-build-cmd` pass
        `--option substituters file:///...` and then read the plan: a build
        there is a defect, because the cache holds every path. Issue nixidae/nixidae#226.

        **The question is `QuerySubstitutablePathInfos`, and not
        `QuerySubstitutablePaths`.** `Store::querySubstitutablePaths` at
        `store-api.cc:517` skips each substituter whose `want-mass-query` is
        off, and a `file://` cache has it off. `Store::queryMissing` asks the
        other operation, which reads every substituter and also answers the
        two sizes that the plan reports.
        """
        if not client_names_a_substituter(self.client):
            return SubstitutionAvailability.unavailable()
        wire_path = StorePath(path=str(path))
        response = await self.engine.ctx.local_store.execute(
            QuerySubstitutablePathInfosRequest(paths={wire_path: ContentAddress("")}), client=self.client, mark=False
        )
        info = next((one for one in response.infos if one.path == wire_path), None)
        if info is None:
            return SubstitutionAvailability.unavailable()
        return SubstitutionAvailability(
            available=True,
            download_size=info.download_size,
            nar_size=info.nar_size,
        )
