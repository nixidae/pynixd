"""Fakes of what placement reads, held to the Protocols of `pynixd/allocator.py` and `pynixd/placement.py`.

A test passes them with no cast, so the type checker fails a fake that lacks
a member placement reads. nixidae/nixidae#1244.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import anyio

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.allocator import RankedStore, RankedStores
from pynixd.config import PlacementTraceSettings, PressureSettings, StoreRankingSettings
from pynixd.placement import Placer
from pynixd.placement_trace import PlacementTrace
from pynixd.pressure import BuildLimit
from pynixd.psi import CpuUtil, SystemHealth
from pynixd.serde import BasicDerivation, SetOptionsRequest, TimeSpan, Verbosity

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from pynixd.build_queue import BuildQueue, QueuedBuild
    from pynixd.placement import Estimates, PlacedStore, Ranks
    from pynixd_store.build_stats import StoreEstimate


@dataclass
class FakeMonitor:
    health: SystemHealth = field(default_factory=SystemHealth)


def cpu(utilization: float) -> CpuUtil:
    return CpuUtil(utilization=utilization, cores=1.0, throttled_pct=0.0)


class FakeStore:
    """A healthy, unmonitored store that builds every system unless *systems* names them."""

    def __init__(
        self,
        store_id: StoreId,
        *,
        systems: set[str] | None = None,
        session_only: bool = False,
        last_resort: bool = False,
        monitor: FakeMonitor | None = None,
    ) -> None:
        self.store_id = store_id
        self.systems = systems
        self.cpu_util: CpuUtil | None = None
        self.monitor = monitor
        self.in_flight = 0
        self.score_penalty = 0.0
        self.priority = 1.0
        self.is_healthy = True
        self.draining = False
        self.no_schedule = False
        self.last_resort = last_resort
        self.session_only = session_only
        self.disk_full_until = 0.0
        self.build_limit = BuildLimit(str(store_id), 1_000, PressureSettings())
        self.feature_matrix: dict[str, set[str]] = {}
        # The scheduler builds the local store's builds on its `daemon_client`.
        self.daemon_client = self

    def supports_derivation(self, system: str, features: set[str]) -> bool:
        del features
        return self.systems is None or system in self.systems


@dataclass
class FakeRequest:
    derivation: BasicDerivation = field(
        default_factory=lambda: BasicDerivation(platform="x86_64-linux", builder="/bin/sh")
    )


@dataclass
class FakeBuild:
    request: FakeRequest = field(default_factory=FakeRequest)
    builders: frozenset[StoreId] | None = None
    failed_on: frozenset[StoreId] = frozenset()

    @classmethod
    def on(cls, platform: str, *, builders: frozenset[StoreId] | None = None) -> FakeBuild:
        return cls(FakeRequest(BasicDerivation(platform=platform, builder="/bin/sh")), builders)

    def is_blacklisted(self, store_id: StoreId) -> bool:
        return store_id in self.failed_on


class FixedRanking:
    """A ranking that answers what *rank* answers for each build."""

    def __init__(self, rank: Callable[[QueuedBuild], list[RankedStore[FakeStore]]]) -> None:
        self.rank = rank

    @classmethod
    def of(cls, *stores: FakeStore) -> FixedRanking:
        """The same ranking for every build: *stores*, best first."""
        return cls(lambda _build: [RankedStore(store.store_id, 0.0, store) for store in stores])

    def rank_stores(
        self,
        build: QueuedBuild,
        assigned_this_pass: Mapping[StoreId, int],
        running_builds: Mapping[StoreId, int] | None = None,
        transfer_seconds: Mapping[StoreId, float] | None = None,
        estimates: Mapping[StoreId, StoreEstimate] | None = None,
        expected_build_seconds: float | None = None,
    ) -> RankedStores[FakeStore]:
        del assigned_this_pass, running_builds, transfer_seconds, estimates, expected_build_seconds
        return RankedStores(self.rank(build))


class NoEstimates:
    """No store has a measured cost."""

    async def transfer_seconds(self, build: QueuedBuild) -> dict[StoreId, float]:
        del build
        return {}


class FixedRng(random.Random):
    """Every draw returns *value*."""

    def __init__(self, value: float) -> None:
        super().__init__()
        self.value = value

    def random(self) -> float:
        return self.value


class Spawned:
    """The stores the placer started builds on, in order. It runs nothing."""

    def __init__(self) -> None:
        self.stores: list[StoreId] = []

    def __call__(self, build: QueuedBuild, store: FakeStore) -> None:
        build.worker = anyio.CancelScope()
        self.stores.append(store.store_id)


def _conforms(store: FakeStore, ranking: FixedRanking) -> tuple[PlacedStore, Ranks[FakeStore], Estimates]:
    """Never called. A fake that lacks a member fails `checks.types` here, and the error names the member."""
    return store, ranking, NoEstimates()


def fake_placer(
    queue: BuildQueue,
    ranking: FixedRanking,
    spawned: Spawned,
    *,
    local: FakeStore | None = None,
    stores: Mapping[StoreId, FakeStore] | None = None,
    settings: StoreRankingSettings | None = None,
    estimator: Estimates | None = None,
    rng: random.Random | None = None,
) -> Placer[FakeStore]:
    """A placer over fake stores: a trace with no database, and no platform a builder may still register for."""
    return Placer(
        queue=queue,
        local=local or FakeStore(LOCAL_STORE_ID),
        stores=lambda: stores or {},
        ranking=ranking,
        settings=settings or StoreRankingSettings(),
        trace=PlacementTrace(None, PlacementTraceSettings()),
        estimator=estimator or NoEstimates(),
        rng=rng or FixedRng(0.5),
        dynamic={},
        spawn=spawned,
    )


def options(max_build_jobs: int) -> SetOptionsRequest:
    """A client's `SetOptions` with every field, because no field has a default."""
    return SetOptionsRequest(
        keep_failed=False,
        keep_going=False,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=max_build_jobs,
        max_silent_time=TimeSpan(0),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=0,
        use_substitutes=True,
        overrides={},
    )
