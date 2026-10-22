"""A borrowed builder builds what no other store can, and nothing else.

User, 2026-10-22: the nix-community aarch64 builders are "only used as a
last resort (for capability not offloading)". nixidae/nixidae#1018.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.allocator import BuildAllocator, TelemetryStoreRanker
from pynixd.config import StoreRankingSettings
from pynixd.psi import DiskSpace, SystemHealth
from pynixd.serde import BasicDerivation

DARWIN = StoreId("darwin-build-box")
GIB = 1024**3


class Store:
    def __init__(
        self, store_id: StoreId, systems: set[str], *, last_resort: bool = False, free: int = 100 * GIB
    ) -> None:
        self.store_id = store_id
        self.systems = systems
        self.last_resort = last_resort
        self.cpu_util = None
        self.monitor = SimpleNamespace(health=SystemHealth(disk=DiskSpace(free_bytes=free, total_bytes=100 * GIB)))
        self.disk_full_until = 0.0
        self.in_flight = 0
        self.score_penalty = 0
        self.priority = 1.0
        self.is_healthy = True
        self.draining = False
        self.no_schedule = False

    def supports_derivation(self, platform: str, features: set[str]) -> bool:
        del features
        # Every machine takes `builtin`, as `machines.cc:42` says.
        return platform == "builtin" or platform in self.systems


def _rank(platform: str, *, local_free: int = 100 * GIB, failed_on: set[StoreId] | None = None) -> list[StoreId]:
    local = Store(LOCAL_STORE_ID, {"x86_64-linux"}, free=local_free)
    darwin = Store(DARWIN, {"aarch64-darwin"}, last_resort=True)
    build = SimpleNamespace(
        request=SimpleNamespace(derivation=BasicDerivation(platform=platform, builder="/bin/sh")),
        is_blacklisted=lambda store_id: store_id in (failed_on or set()),
    )
    ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=StoreRankingSettings())))
    allocator = BuildAllocator(cast("Any", {LOCAL_STORE_ID: local, DARWIN: darwin}), cast("Any", local), ranker)
    return [candidate.store_id for candidate in allocator.rank_stores(cast("Any", build), {})]


def test_builds_what_only_it_can():
    assert _rank("aarch64-darwin") == [DARWIN]


def test_takes_no_builtin_work_from_a_store_that_can_do_it():
    assert _rank("builtin") == [LOCAL_STORE_ID]


def test_a_full_regular_store_means_wait_not_move():
    """Capability, not load: the local store can build it, so the build waits."""
    assert _rank("builtin", local_free=GIB) == []


def test_takes_a_build_every_regular_store_failed():
    assert _rank("builtin", failed_on={LOCAL_STORE_ID}) == [DARWIN]
