"""A borrowed builder builds what no other store can, and nothing else.

User, 2026-10-22: the nix-community aarch64 builders are "only used as a
last resort (for capability not offloading)". nixidae/nixidae#1018.
"""

from __future__ import annotations

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.allocator import BuildAllocator, TelemetryStoreRanker
from pynixd.config import StoreRankingSettings
from pynixd.psi import DiskSpace, SystemHealth
from tests._conftest.placement import FakeBuild, FakeMonitor, FakeStore

DARWIN = StoreId("darwin-build-box")
GIB = 1024**3


class Store(FakeStore):
    def __init__(
        self, store_id: StoreId, systems: set[str], *, last_resort: bool = False, free: int = 100 * GIB
    ) -> None:
        disk = DiskSpace(free_bytes=free, total_bytes=100 * GIB)
        super().__init__(
            store_id, systems=systems, last_resort=last_resort, monitor=FakeMonitor(SystemHealth(disk=disk))
        )

    def supports_derivation(self, system: str, features: set[str]) -> bool:
        # Every machine takes `builtin`, as `machines.cc:42` says.
        return system == "builtin" or super().supports_derivation(system, features)


def _rank(platform: str, *, local_free: int = 100 * GIB, failed_on: set[StoreId] | None = None) -> list[StoreId]:
    local = Store(LOCAL_STORE_ID, {"x86_64-linux"}, free=local_free)
    darwin = Store(DARWIN, {"aarch64-darwin"}, last_resort=True)
    build = FakeBuild.on(platform)
    build.failed_on = frozenset(failed_on or ())
    ranker = TelemetryStoreRanker(StoreRankingSettings())
    allocator = BuildAllocator({LOCAL_STORE_ID: local, DARWIN: darwin}, local, ranker)
    return [candidate.store_id for candidate in allocator.rank_stores(build, {})]


def test_builds_what_only_it_can() -> None:
    assert _rank("aarch64-darwin") == [DARWIN]


def test_takes_no_builtin_work_from_a_store_that_can_do_it() -> None:
    assert _rank("builtin") == [LOCAL_STORE_ID]


def test_a_full_regular_store_means_wait_not_move() -> None:
    """Capability, not load: the local store can build it, so the build waits."""
    assert _rank("builtin", local_free=GIB) == []


def test_takes_a_build_every_regular_store_failed() -> None:
    assert _rank("builtin", failed_on={LOCAL_STORE_ID}) == [DARWIN]
