"""Store ranking and build allocation for distributed builds."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from .system_features import PYNIXD_HANDLED_FEATURES, effective_required_features

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from nix_daemon_protocol.ids import StoreId

    from .build_queue import QueuedBuild
    from .config import PynixdSettings
    from .store import DaemonStore

log = structlog.get_logger(__name__)

# Duration threshold for tiny-build heuristics (2.5s).
TINY_BUILD_THRESHOLD_MS = 2500


@dataclass
class RankedStore:
    """A store with its allocation score."""

    store_id: StoreId
    score: float
    store: DaemonStore


class RankedStores:
    """Ordered collection of ranked stores."""

    def __init__(self, stores: list[RankedStore]) -> None:
        self._stores = stores

    def __iter__(self) -> Iterator[RankedStore]:
        return iter(self._stores)

    def __len__(self) -> int:
        return len(self._stores)

    def __bool__(self) -> bool:
        return bool(self._stores)

    def sort(self) -> RankedStores:
        """Return a new instance sorted by score descending."""
        return RankedStores(sorted(self._stores, key=lambda s: s.score, reverse=True))


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
        override_in_flight: Mapping[StoreId, int] | None = None,
        transfer_seconds: Mapping[StoreId, float] | None = None,
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
        override_in_flight: Mapping[StoreId, int] | None = None,
        transfer_seconds: Mapping[StoreId, float] | None = None,
    ) -> RankedStores:
        """Score stores by CPU, pressure, concurrency, herd and transfer penalties."""
        ranked = []
        for store in stores:
            score = 0.0

            # 1. CPU Availability (+ points)
            if store.cpu_util:
                idle_ratio = 1.0 - (store.cpu_util.utilization / 100.0)
                score += idle_ratio * self.settings.cpu_idle_weight
            # else: no monitor data → neutral (no bonus, no penalty)

            # 2. System Pressure (- points)
            if store.monitor and store.monitor.health.psi:
                psi = store.monitor.health.psi
                score -= psi.cpu.some_avg10 * self.settings.cpu_pressure_penalty
                score -= psi.io.some_avg10 * self.settings.io_pressure_penalty
            # else: no monitor data → neutral (no penalty)

            has_resource_signal = store.cpu_util is not None or (
                store.monitor is not None and store.monitor.health.psi is not None
            )

            # 3. Concurrency Penalty (- points)
            in_flight = store.in_flight
            if override_in_flight and store.store_id in override_in_flight:
                in_flight = override_in_flight[store.store_id]
            score -= in_flight * self.settings.concurrency_penalty

            # 4. Predicted Load Penalty (- points)
            # (Requires build duration estimation, placeholder for now)

            # 5. Thundering Herd Penalty (- points)
            assigned = assigned_this_pass.get(store.store_id, 0)
            score -= assigned * self.settings.thundering_herd_penalty

            # 6. Delegation Penalty (- points): the measured seconds of
            # moving inputs and outputs and of the dispatch round trip.
            if transfer_seconds:
                score -= transfer_seconds.get(store.store_id, 0.0) * self.settings.transfer_penalty_per_second

            score -= store.score_penalty
            if score >= self.settings.min_schedule_score or not has_resource_signal:
                ranked.append(RankedStore(store.store_id, score * store.priority, store))
            else:
                log.debug(
                    "store_ranking_below_threshold",
                    store_id=store.store_id,
                    score=score,
                    threshold=self.settings.min_schedule_score,
                )

        return RankedStores(ranked).sort()


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
        override_in_flight: Mapping[StoreId, int] | None = None,
        transfer_seconds: Mapping[StoreId, float] | None = None,
    ) -> RankedStores:
        """Rank stores for a build using the injected ranker."""
        build_features = effective_required_features(build.request.derivation)
        candidates = []

        # The local store is a candidate like the others, and the last one:
        # the sort is stable, so a remote wins a tie, as when the local store
        # was only a fallback. nixidae/nixidae#1001.
        remotes = [store for store_id, store in self.stores.items() if store_id != self.local_store.store_id]
        for store in [*remotes, self.local_store]:
            if not store.is_healthy or store.draining or store.no_schedule:
                continue
            if not store.supports_derivation(build.request.derivation.platform, build_features):
                continue
            if build.is_blacklisted(store.store_id):
                continue

            candidates.append(store)

        return self.ranker.rank_stores(
            build,
            candidates,
            assigned_this_pass,
            override_in_flight,
            transfer_seconds,
        )

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
