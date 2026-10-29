"""Store ranking and build allocation for distributed builds."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from .config import StoreRankingSettings
from .system_features import PYNIXD_HANDLED_FEATURES, effective_required_features

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from nix_daemon_protocol.ids import StoreId

    from .build_queue import QueuedBuild
    from .build_stats import StoreEstimate
    from .config import PynixdSettings
    from .store import DaemonStore

log = structlog.get_logger(__name__)

# Duration threshold for tiny-build heuristics (2.5s).
TINY_BUILD_THRESHOLD_MS = 2500


@dataclass(frozen=True)
class ScoreBreakdown:
    """Each term of one store's score for one build. Penalties are negative points."""

    cpu_utilization: float | None
    cpu_idle_points: float
    cpu_psi: float | None
    cpu_psi_points: float
    io_psi: float | None
    io_psi_points: float
    unmonitored_points: float
    in_flight: int
    concurrency_points: float
    assigned_this_pass: int
    herd_points: float
    score_penalty: float
    load_score: float
    """The sum of the terms above: what `min_schedule_score` reads."""
    threshold: float
    passed: bool
    transfer_seconds: float | None
    transfer_points: float
    priority: float
    final_score: float
    cpu_scale: float = 1.0
    """The share of the CPU points the order counts; `cpu_reference_seconds`."""


def score_store(
    store: DaemonStore,
    settings: StoreRankingSettings,
    in_flight: int,
    assigned_this_pass: int,
    transfer_seconds: float | None,
    expected_seconds: float | None = None,
    expected_build_seconds: float | None = None,
) -> ScoreBreakdown:
    """Score *store* for one build, term by term.

    *expected_seconds* is the whole build on this store, *expected_build_seconds*
    the build alone wherever it ran; both from history, `None` when unknown.
    """
    cpu = store.cpu_util.utilization if store.cpu_util else None
    cpu_idle_points = (1.0 - cpu / 100.0) * settings.cpu_idle_weight if cpu is not None else 0.0
    psi = store.monitor.health.psi if store.monitor else None
    cpu_psi = psi.cpu.some_avg10 if psi else None
    io_psi = psi.io.some_avg10 if psi else None
    # PSI `avg10` is a percent; the division puts it on the CPU figure's scale.
    # Undivided, 0.77 % of IO stall costs 38.5 of the 100 points an idle CPU
    # earns (nixidae/nixidae#1125).
    cpu_psi_points = -cpu_psi / 100.0 * settings.cpu_pressure_penalty if cpu_psi is not None else 0.0
    io_psi_points = -io_psi / 100.0 * settings.io_pressure_penalty if io_psi is not None else 0.0
    monitored = cpu is not None or psi is not None
    unmonitored_points = 0.0 if monitored else -settings.unmonitored_penalty
    cores = store.monitor.health.cores if store.monitor else None
    if settings.per_core_load and cores:
        concurrency_points = -in_flight / cores * settings.cpu_idle_weight
        herd_points = -assigned_this_pass / cores * settings.cpu_idle_weight
    else:
        concurrency_points = -in_flight * settings.concurrency_penalty
        herd_points = -assigned_this_pass * settings.thundering_herd_penalty
    load_score = (
        cpu_idle_points
        + cpu_psi_points
        + io_psi_points
        + unmonitored_points
        + concurrency_points
        + herd_points
        - store.score_penalty
    )
    # The time cost orders the stores and never drops one: it is the cost of
    # this build, not the load of the store. Under the threshold, a 366 MB
    # input put every idle builder below 0, and the build waited 20 minutes
    # for a cost that never falls.
    if settings.time_penalty_per_second is not None and expected_seconds is not None:
        cost_seconds: float | None = expected_seconds
        cost_points = -expected_seconds * settings.time_penalty_per_second
    else:
        cost_seconds = transfer_seconds
        cost_points = -(transfer_seconds or 0.0) * settings.transfer_penalty_per_second
    cpu_scale = (
        min(1.0, expected_build_seconds / settings.cpu_reference_seconds)
        if settings.cpu_reference_seconds and expected_build_seconds is not None
        else 1.0
    )
    ordered_load = load_score - (1.0 - cpu_scale) * (cpu_idle_points + cpu_psi_points)
    return ScoreBreakdown(
        cpu_utilization=cpu,
        cpu_idle_points=cpu_idle_points,
        cpu_psi=cpu_psi,
        cpu_psi_points=cpu_psi_points,
        io_psi=io_psi,
        io_psi_points=io_psi_points,
        unmonitored_points=unmonitored_points,
        in_flight=in_flight,
        concurrency_points=concurrency_points,
        assigned_this_pass=assigned_this_pass,
        herd_points=herd_points,
        score_penalty=store.score_penalty,
        load_score=load_score,
        threshold=settings.min_schedule_score,
        passed=load_score >= settings.min_schedule_score or not monitored,
        transfer_seconds=cost_seconds,
        transfer_points=cost_points,
        priority=store.priority,
        final_score=(ordered_load + cost_points) * store.priority,
        cpu_scale=cpu_scale,
    )


@dataclass
class RankedStore:
    """A store with its allocation score."""

    store_id: StoreId
    score: float
    store: DaemonStore
    breakdown: ScoreBreakdown | None = None


@dataclass(frozen=True)
class ExcludedStore:
    """A store that is no candidate for a build, and why."""

    store_id: StoreId
    reason: str
    breakdown: ScoreBreakdown | None = None


class RankedStores:
    """Ordered collection of ranked stores, and the stores the ranking left out."""

    def __init__(self, stores: list[RankedStore], excluded: list[ExcludedStore] | None = None) -> None:
        self._stores = stores
        self.excluded = excluded or []

    def __iter__(self) -> Iterator[RankedStore]:
        return iter(self._stores)

    def __len__(self) -> int:
        return len(self._stores)

    def __bool__(self) -> bool:
        return bool(self._stores)

    def sort(self) -> RankedStores:
        """Return a new instance sorted by score descending."""
        return RankedStores(sorted(self._stores, key=lambda s: s.score, reverse=True), self.excluded)


def pick_store(
    ranked: list[RankedStore],
    local_id: StoreId,
    *,
    local_slot_full: bool,
    prefer_local: PreferLocal | None = None,
) -> RankedStore | None:
    """The store a build goes to, from the ranked stores with room; `None` waits.

    NIX-DEVIATION (#27): `DerivationBuildingGoal` at
    `derivation-building-goal.cc:606` builds a `preferLocalBuild` derivation
    locally whenever the machine can, and `acquireResources` waits for a
    local slot however long that takes. pynixd waits too, unless the expected
    wait passes `PreferLocal.weight` times the best remote's measured
    delegation seconds; then it delegates. A busy machine keeps its trivial
    builds moving, and a local copy of 550 MB still stays local (#947). With
    either number unknown it waits, as Nix does. To reverse it, measure
    `preferLocalBuild` builds that went remote and cost more than the wait.
    """
    usable = [candidate for candidate in ranked if candidate.store_id != local_id or not local_slot_full]
    if prefer_local is None:
        return usable[0] if usable else None
    local = next((candidate for candidate in usable if candidate.store_id == local_id), None)
    if local is not None:
        return local
    remote = next((candidate for candidate in usable if candidate.store_id != local_id), None)
    if remote is None:
        return None
    remote_seconds = prefer_local.transfer_seconds.get(remote.store_id)
    if remote_seconds is None or prefer_local.wait_seconds is None:
        return None
    return remote if prefer_local.wait_seconds > remote_seconds * prefer_local.weight else None


def explore_store(
    ranked: list[RankedStore],
    chosen: RankedStore,
    local_id: StoreId,
    *,
    local_slot_full: bool,
    share: float,
    draw: float,
    pick: float,
) -> RankedStore | None:
    """A candidate other than `chosen` when `draw` falls under `share`; `pick` in [0, 1) selects which."""
    if draw >= share:
        return None
    others = [
        candidate
        for candidate in ranked
        if candidate.store_id != chosen.store_id and (candidate.store_id != local_id or not local_slot_full)
    ]
    return others[int(pick * len(others))] if others else None


@dataclass(frozen=True)
class PreferLocal:
    """What `pick_store` weighs for a `preferLocalBuild` derivation the local store can build."""

    wait_seconds: float | None
    """Until the soonest running local build is expected to free its slot."""
    transfer_seconds: Mapping[StoreId, float]
    weight: float


class StoreRanker(ABC):
    """Abstract base for store ranking strategies."""

    @abstractmethod
    def rank_stores(
        self,
        build: QueuedBuild,
        stores: list[DaemonStore],
        assigned_this_pass: Mapping[StoreId, int],
        running_builds: Mapping[StoreId, int] | None = None,
        transfer_seconds: Mapping[StoreId, float] | None = None,
        estimates: Mapping[StoreId, StoreEstimate] | None = None,
        expected_build_seconds: float | None = None,
    ) -> RankedStores:
        """Score and sort available stores for the given build."""
        ...


class TelemetryStoreRanker(StoreRanker):
    """Ranks stores using telemetry-driven heuristic scoring."""

    def __init__(self, settings: PynixdSettings) -> None:
        self.settings = settings.ranking

    def rank_stores(
        self,
        build: QueuedBuild,
        stores: list[DaemonStore],
        assigned_this_pass: Mapping[StoreId, int],
        running_builds: Mapping[StoreId, int] | None = None,
        transfer_seconds: Mapping[StoreId, float] | None = None,
        estimates: Mapping[StoreId, StoreEstimate] | None = None,
        expected_build_seconds: float | None = None,
    ) -> RankedStores:
        """Score stores by CPU, pressure, running builds, herd and transfer penalties."""
        ranked: list[RankedStore] = []
        excluded: list[ExcludedStore] = []
        for store in stores:
            estimate = estimates.get(store.store_id) if estimates else None
            terms = score_store(
                store,
                self.settings,
                running_builds.get(store.store_id, 0) if running_builds else 0,
                assigned_this_pass.get(store.store_id, 0),
                transfer_seconds.get(store.store_id) if transfer_seconds else None,
                expected_seconds=estimate.total_seconds() if estimate else None,
                expected_build_seconds=expected_build_seconds,
            )
            if terms.passed:
                ranked.append(RankedStore(store.store_id, terms.final_score, store, terms))
            else:
                excluded.append(ExcludedStore(store.store_id, "score under min_schedule_score", terms))

        return RankedStores(ranked, excluded).sort()


def short_of_disk(store: DaemonStore, settings: StoreRankingSettings, now: float) -> bool:
    """Whether *store* has too little disk for a build. nixidae/nixidae#1013.

    A measured figure decides when the store's monitor has one. A store that
    reports none is held back for a while after a build failed there with
    "No space left on device".
    """
    monitor = getattr(store, "monitor", None)
    disk = monitor.health.disk if monitor is not None else None
    if disk is not None:
        enough: bool = disk.free_bytes < settings.min_free_bytes
        return enough
    return now < getattr(store, "disk_full_until", 0.0)


def incapable_reason(store: DaemonStore, build: QueuedBuild, features: set[str]) -> str | None:
    """Why *store* cannot take *build* at all, or `None` when it can."""
    if not store.is_healthy:
        return "unhealthy"
    if store.draining:
        return "draining"
    if store.no_schedule:
        return "no_schedule"
    if not store.supports_derivation(build.request.derivation.platform, features):
        return "lacks the system or a required feature"
    if build.is_blacklisted(store.store_id):
        return "failed this build too often"
    return None


class BuildAllocator:
    """Orchestrates ranking and selection of stores for builds."""

    def __init__(
        self,
        stores: dict[StoreId, DaemonStore],
        local_store: DaemonStore,
        ranker: StoreRanker,
    ) -> None:
        self.stores = stores
        self.local_store = local_store
        self.ranker = ranker

    def add_store(self, store: DaemonStore) -> None:
        """Register a store that arrived after construction.

        The mapping passed in holds the stores of scheduler start; a dynamic
        store (a reverse builder dialling in) never lands in it unless
        somebody puts it there. Without this the allocator ranks a snapshot
        while the scheduler filters the live set, so a compatible builder
        reads as "compatible, keep waiting" for ever. Issue #79.
        """
        self.stores[store.store_id] = store

    def discard_store(self, store_id: StoreId) -> None:
        """Forget a removed store, so no pass ranks a closed connection."""
        self.stores.pop(store_id, None)

    def rank_stores(
        self,
        build: QueuedBuild,
        assigned_this_pass: Mapping[StoreId, int],
        running_builds: Mapping[StoreId, int] | None = None,
        transfer_seconds: Mapping[StoreId, float] | None = None,
        estimates: Mapping[StoreId, StoreEstimate] | None = None,
        expected_build_seconds: float | None = None,
    ) -> RankedStores:
        """Rank stores for a build using the injected ranker."""
        build_features = effective_required_features(build.request.derivation)

        # The local store is a candidate like the others. The sort is stable,
        # so its place in this list decides a tie: last by default, as when it
        # was only a fallback (nixidae/nixidae#1001), first with
        # `local_wins_ties`.
        remotes = [store for store_id, store in self.stores.items() if store_id != self.local_store.store_id]
        excluded: list[ExcludedStore] = []
        capable: list[DaemonStore] = []
        settings = getattr(self.ranker, "settings", StoreRankingSettings())
        order = [self.local_store, *remotes] if settings.local_wins_ties else [*remotes, self.local_store]
        for store in order:
            reason = incapable_reason(store, build, build_features)
            if reason is None:
                capable.append(store)
            else:
                excluded.append(ExcludedStore(store.store_id, reason))
        # A last-resort store is a candidate only when no other store can
        # build this at all; load and disk do not count. nixidae/nixidae#1018.
        if any(not getattr(store, "last_resort", False) for store in capable):
            excluded += [
                ExcludedStore(store.store_id, "last resort, and another store can build it")
                for store in capable
                if getattr(store, "last_resort", False)
            ]
            capable = [store for store in capable if not getattr(store, "last_resort", False)]
        now = time.monotonic()
        candidates: list[DaemonStore] = []
        for store in capable:
            if short_of_disk(store, settings, now):
                excluded.append(ExcludedStore(store.store_id, "short of disk"))
            else:
                candidates.append(store)

        ranked = self.ranker.rank_stores(
            build,
            candidates,
            assigned_this_pass,
            running_builds,
            transfer_seconds,
            estimates,
            expected_build_seconds,
        )
        return RankedStores(list(ranked), [*excluded, *ranked.excluded])

    def incompatibility_reasons(
        self,
        platform: str,
        features: set[str] | None,
    ) -> list[str]:
        """Build per-store incompatibility explanations for error reporting."""
        reasons: list[str] = []
        fm = self.local_store._feature_matrix
        if fm is not None and platform not in fm:
            reasons.append(f"local: system {platform} not in feature_matrix")
        elif fm is not None and features:
            local_feats = fm.get(platform, set())
            missing = features - local_feats
            if missing:
                reasons.append(
                    f"local: missing features {', '.join(sorted(missing))} for {platform}",
                )
        elif fm is None:
            reasons.append("local: no feature_matrix (not probed)")
        else:
            reasons.append("local: compatible")

        for store in self.stores.values():
            sfm = store._feature_matrix
            if sfm is not None and platform not in sfm:
                reasons.append(f"{store.store_id}: system {platform} not in feature_matrix")
            elif sfm is not None and features:
                store_feats = sfm.get(platform, set())
                missing = features - store_feats
                if missing:
                    reasons.append(
                        f"{store.store_id}: missing features {', '.join(sorted(missing))} for {platform}",
                    )
                else:
                    reasons.append(
                        f"{store.store_id}: compatible but excluded (unhealthy/saturated/failed)",
                    )
            elif sfm is None:
                reasons.append(f"{store.store_id}: no feature_matrix (not probed)")
            else:
                reasons.append(
                    f"{store.store_id}: compatible but excluded (unhealthy/saturated/failed)",
                )

        return reasons

    @staticmethod
    def strip_handled_features(build: QueuedBuild) -> None:
        """Remove pynixd-handled features from requiredSystemFeatures.

        Only strips features that pynixd has actually resolved. If the
        derivation still has CA or dynamic outputs, those features are
        kept so the allocator can correctly match stores.

        This prevents sending unresolved CA/dynamic derivations to stores
        whose builders don't support the corresponding protocol operations.
        """

        features = build.request.derivation.required_system_features
        stripped = features & PYNIXD_HANDLED_FEATURES
        if not stripped:
            return

        # Only strip features that the derivation no longer needs
        # (i.e., they were actually resolved by the resolver).
        still_ca = any(o.is_ca for o in build.request.derivation.outputs.values())
        still_dynamic = any(o.is_dynamic_output for o in build.request.derivation.outputs.values())
        if still_ca:
            stripped.discard("ca-derivations")
        if still_dynamic:
            stripped.discard("dynamic-derivations")

        if not stripped:
            return
        remaining = features - stripped
        build.request.derivation.set_required_system_features(remaining)
        log.debug(
            "stripped_handled_features",
            build_id=build.build_id,
            stripped=sorted(stripped),
            remaining=sorted(remaining),
        )
