"""ScheduledStore: a store the scheduler places builds on, with a daemon or without one.

The scheduler reads what a store builds, its host's pressure, its build limit
and its health, through `daemon_client`. `DaemonStore` adds the wire to a Nix
daemon; a daemonless local store has the facts and no wire. nixidae/nixidae#1325.
"""

from __future__ import annotations

import time
from enum import IntEnum
from typing import TYPE_CHECKING

import anyio
import structlog

from .. import metrics, wire
from ..config import PressureSettings
from ..pressure import BuildLimit
from ..system_features import BUILTIN_SYSTEM
from .base import Store

if TYPE_CHECKING:
    from collections.abc import Callable, Set as AbstractSet

    from pynixd_store.store_layout import StoreLayout

    from ..config import StoreSpecBase
    from ..monitor import ResourceMonitor
    from ..psi import CpuUtil, MemInfo, SystemHealth

__all__ = ["ProbeState", "ScheduledStore"]

log = structlog.get_logger(__name__)

_CB_THRESHOLD: int = 3
_CB_MAX_COOLDOWN: float = 300.0


class ProbeState(IntEnum):
    """Tracks the protocol probing state of a DaemonStore connection."""

    NOT_PROBED = 0
    PROBING = 1
    PROBED = 2


class ScheduledStore(Store):
    """What the scheduler reads of a store, with or without a daemon under it."""

    runs_daemon: bool = False
    """Whether a Nix daemon answers this store; `DaemonStore` sets it."""

    MAX_CONNECTIONS: int = 64
    """How many connections the pool may hold at once.

    A connection to a Unix-socket daemon costs a process on this machine and
    nothing on a far one, so 64 is a bound on memory rather than a limit
    somebody else imposes. A subclass whose transport has its own ceiling
    lowers this; `SSHSubprocessStore` does. Issue #43."""

    TRANSFER_CONNECTIONS: int | None = None
    """How many of `MAX_CONNECTIONS` the transfer pool takes; the build pool
    takes the rest. `None` is a quarter, at least 2.

    Two pools, because a build holds its connection for the whole build: on
    an SSH builder with 8 slots, 8 running builds held every read to it until
    `read_timeout`. Issue #97."""

    def _pool_sizes(self, spec: StoreSpecBase) -> tuple[int, int]:
        """`(build, transfer)` slots: the spec's numbers, or this class's."""
        total = max(2, spec.max_connections or self.MAX_CONNECTIONS)
        transfer = spec.transfer_connections or self.TRANSFER_CONNECTIONS or max(2, total // 4)
        transfer = max(1, min(transfer, total - 1))
        return total - transfer, transfer

    def __init__(self, spec: StoreSpecBase) -> None:
        super().__init__(spec)
        self.store_path = spec.store_path
        self.layout: StoreLayout = spec.layout()
        """The three directories of this store. Issue nixidae/nixidae#215.

        A store that is not local has no layout of its own, so it takes the
        chroot layout of its root. Nothing reads the state directory of such
        a store: the temporary roots and the SQLite fast paths belong to the
        local store that pynixd serves.
        """
        self.scheduleable = spec.scheduleable
        self.priority = spec.priority
        self.score_penalty = spec.score_penalty
        self.last_resort = spec.last_resort
        self.mandatory_features = frozenset(spec.mandatory_features)
        self.gc_enabled = spec.gc_enabled
        self.gc_max_age = spec.gc_max_age
        self.gc_target_usage = spec.gc_target_usage
        self.gc_allow_execute = spec.gc_allow_execute
        self.no_schedule = spec.no_schedule
        self.version: int = wire.PROTOCOL_VERSION
        self.nix_version: str = ""

        fm = spec._effective_feature_matrix()
        self._feature_matrix: dict[str, set[str]] | None = fm
        if spec.probe is not None:
            self._probe = spec.probe
        else:
            self._probe = fm is None

        build_slots, _ = self._pool_sizes(spec)
        self.pressure_settings: PressureSettings = spec.pressure or (
            spec.settings.pressure if spec.settings else PressureSettings()
        )
        self.build_limit = BuildLimit(str(self.store_id), build_slots, self.pressure_settings)
        self.on_capacity: Callable[[], None] | None = None
        """Called when the build limit rises; the scheduler sets it to its trigger."""
        metrics.STORE_PRESSURE.register(self)
        self.monitor: ResourceMonitor | None = None
        self.consecutive_failures: int = 0
        self.cooldown_until: float = 0.0
        self.disk_full_until: float = 0.0
        """Until when a build failed here for want of disk; read when no disk sample is known."""
        self.sample_listeners: set[Callable[[SystemHealth], None]] = set()
        """Each hears every sample of this store's host: a `PynixdHostHealthStream` session. nixidae/nixidae#1016."""
        self._features: set[str] = set()
        self.probe_state: ProbeState = ProbeState.NOT_PROBED
        self._probe_event: anyio.Event = anyio.Event()
        self.draining: bool = False

    # ── What the store builds ─────────────────────────────────────────────

    @property
    def features(self) -> AbstractSet[str]:
        """Set of daemon-advertised protocol features."""
        return self._features

    @property
    def feature_matrix(self) -> dict[str, set[str]]:
        """Mapping of system → feature-set probed from the daemon."""
        if self._feature_matrix is not None:
            return self._feature_matrix
        return {}

    @feature_matrix.setter
    def feature_matrix(self, value: dict[str, set[str]]) -> None:
        """Override the probed feature matrix."""
        self._feature_matrix = value

    @property
    def systems(self) -> set[str] | None:
        """Set of systems this store can build for, or None if unrestricted."""
        fm = self._feature_matrix
        if fm is not None:
            return set(fm.keys()) if fm else None
        return None

    @systems.setter
    def systems(self, value: set[str] | None) -> None:
        """Override the set of systems this store can build for."""
        if self._feature_matrix is not None:
            if value is None:
                self._feature_matrix = {}
                return
            old = self._feature_matrix
            self._feature_matrix = {s: old.get(s, set()) for s in value}
        elif value is not None:
            self._feature_matrix = {s: set() for s in value}

    def supports_system(self, system: str) -> bool:
        """Check if this store can build for the given system."""
        fm = self._feature_matrix
        if fm is None:
            return True
        return system in fm

    def supports_derivation(self, system: str, features: set[str] | None = None) -> bool:
        if not self.mandatory_features <= (features or set()):
            return False
        fm = self._feature_matrix
        if fm is not None:
            sys_features = fm.get(system)
            if sys_features is None and system == BUILTIN_SYSTEM and fm:
                # Every machine takes `builtin`: `Machine::systemSupported`
                # at `machines.cc:42`, and `derivation-building-goal.cc:421`
                # for the local one. The daemon itself runs the builder.
                sys_features = set().union(*fm.values())
            if sys_features is None:
                return False
            if not features:
                return True
            return features.issubset(sys_features)
        if not features:
            return True
        _platform_specific = frozenset({"kvm", "apple-virt"})
        return features.isdisjoint(_platform_specific)

    # ── Resource metrics ────────────────────────────────────────────

    def on_pressure_sample(self, health: SystemHealth) -> None:
        """Move the build limit for one sample of this store's host, and pass the sample on."""
        if self.build_limit.observe(health, self._building(), time.monotonic()) and self.on_capacity is not None:
            self.on_capacity()
        for listener in tuple(self.sample_listeners):
            listener(health)

    @property
    def pressure(self) -> float | None:
        return None

    @property
    def meminfo(self) -> MemInfo | None:
        """System memory info, or None if unavailable."""
        return self.monitor.health.meminfo if self.monitor else None

    @property
    def cpu_util(self) -> CpuUtil | None:
        """CPU utilization from cgroupv2, or None if unavailable."""
        return self.monitor.health.cpu_util if self.monitor else None

    # ── Circuit breaker ─────────────────────────────────────────────

    @property
    def is_healthy(self) -> bool:
        """Whether the store is not in cooldown."""
        return time.monotonic() >= self.cooldown_until

    def record_success(self) -> None:
        """Reset circuit breaker after a successful operation."""
        if self.consecutive_failures > 0:
            log.info("store_recovered", store_id=self.store_id, consecutive_failures=self.consecutive_failures)
        self.consecutive_failures = 0
        self.cooldown_until = 0.0

    def record_failure(self) -> None:
        """Record a failure and optionally enter circuit-breaker cooldown."""
        self.consecutive_failures += 1
        if self.consecutive_failures >= _CB_THRESHOLD:
            cooldown = min(30 * 2 ** (self.consecutive_failures - _CB_THRESHOLD), _CB_MAX_COOLDOWN)
            self.cooldown_until = time.monotonic() + cooldown
            log.warning(
                "store_cooldown",
                store_id=self.store_id,
                consecutive_failures=self.consecutive_failures,
                cooldown=cooldown,
            )
        self._schedule_reconnect()

    def _building(self) -> int:
        """Builds running on this store, as the build limit counts them."""
        return 0

    def _schedule_reconnect(self) -> None:
        """A failure here asks nothing to reconnect: there is no connection."""

    # ── Lifecycle ───────────────────────────────────────────────────

    async def probe(self) -> None:
        """There is nothing to probe: the spec or nix.conf says what the store builds."""
        self.probe_state = ProbeState.PROBED
        self._probe_event.set()

    async def start(self, sync_paths: bool = True) -> None:
        if self._started:
            return
        await self.probe()
        if self.monitor:
            self.monitor.start(self._background)
        self._started = True

    async def close(self) -> None:
        if self.monitor is not None:
            # A monitor that outlives its store asks a closed store for ever.
            self.monitor.stop()
            self.monitor = None
