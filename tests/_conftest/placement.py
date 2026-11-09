"""Fakes of what the allocator reads, held to `Candidate` and `Placeable` in `pynixd/allocator.py`.

A test passes them with no cast, so the type checker fails a fake that lacks
a member the allocator reads. nixidae/nixidae#1244.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from nix_daemon_protocol.ids import StoreId
from pynixd.psi import CpuUtil, SystemHealth
from pynixd.serde import BasicDerivation


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
