"""The build limit of a store, moved by the pressure of its host. Issue #98.

The pool size of a store is a ceiling, and it can be high. What the host can
take now is lower, and it changes: a link step takes 20 GiB, a test suite
none. The limit follows it the way TCP follows a link: slow start doubles it
until the first sign of pressure, a fall cuts it by `decrease_factor`, and
each quiet interval after that adds `increase_step`.

The limit gates the start of a build only. A running build is never stopped,
and transfers and reads never wait for it: they add no build to the host.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

import structlog

from . import metrics

if TYPE_CHECKING:
    from .config import PressureSettings
    from .psi import SystemHealth

log = structlog.get_logger(__name__)


class LimitReason(StrEnum):
    """Why the limit has its value now."""

    unmonitored = "unmonitored"
    """No sample says anything; the limit is the ceiling."""
    slow_start = "slow_start"
    steady = "steady"
    memory_pressure = "memory_pressure"
    io_pressure = "io_pressure"
    low_memory = "low_memory"
    cpu_pressure = "cpu_pressure"
    load = "load"


@dataclass
class BuildLimit:
    """How many builds a store may run now."""

    store_id: str
    ceiling: int
    settings: PressureSettings
    limit: int = 0
    reason: LimitReason = LimitReason.unmonitored
    slow_start: bool = True
    sampled_at: float | None = None
    _next_fall: float = 0.0
    _next_rise: float = 0.0

    def __post_init__(self) -> None:
        self.limit = self.ceiling

    def has_room(self, running: int) -> bool:
        """Whether a build may start next to *running* builds of this store."""
        return running < self.limit

    def observe(self, health: SystemHealth | None, running: int, now: float) -> bool:
        """Move the limit for one sample. Answers whether it rose."""
        if not self.settings.enabled or health is None or not health.has_signal:
            rose = self.limit < self.ceiling
            self._set(self.ceiling, LimitReason.unmonitored)
            self.sampled_at = None
            return rose
        if self.reason == LimitReason.unmonitored:
            self._set(self._initial(health), LimitReason.slow_start)
            self.slow_start = True
            self._next_rise = now + self.settings.settle
        self.sampled_at = now

        verdict, falls = self._verdict(health)
        if falls and now >= self._next_fall:
            cut = int(min(self.limit, max(running, 1)) * self.settings.decrease_factor)
            self._set(max(1, cut), verdict)
            self.slow_start = False
            self._next_fall = self._next_rise = now + self.settings.settle
            return False
        if verdict is not LimitReason.steady:
            self.reason = verdict
            return False

        self.reason = LimitReason.slow_start if self.slow_start else LimitReason.steady
        if running < self.limit or self.limit >= self.ceiling or now < self._next_rise:
            return False
        grown = self.limit * 2 if self.slow_start else self.limit + self.settings.increase_step
        self._set(min(self.ceiling, grown), self.reason)
        self._next_rise = now + self.settings.settle
        return True

    def _initial(self, health: SystemHealth) -> int:
        start = self.settings.initial_limit
        if start is None:
            start = int(health.cores) if health.cores else 1
        return max(1, min(self.ceiling, start))

    def _verdict(self, health: SystemHealth) -> tuple[LimitReason, bool]:
        """The reason, and whether it lowers the limit. `steady` lets it rise."""
        s = self.settings
        psi = health.psi
        mem = health.meminfo if health.meminfo and health.meminfo.mem_total else None
        min_kb = s.min_available_memory_mb * 1024
        if psi is not None and psi.memory.full_avg10 >= s.memory_full_high:
            return LimitReason.memory_pressure, True
        if psi is not None and psi.io.full_avg10 >= s.io_full_high:
            return LimitReason.io_pressure, True
        if mem is not None and mem.mem_available < min_kb:
            return LimitReason.low_memory, True
        if psi is not None and psi.memory.full_avg10 > s.memory_full_low:
            return LimitReason.memory_pressure, False
        if psi is not None and psi.io.full_avg10 > s.io_full_low:
            return LimitReason.io_pressure, False
        if mem is not None and mem.mem_available < 2 * min_kb:
            return LimitReason.low_memory, False
        if psi is not None and psi.cpu.some_avg10 >= s.cpu_some_high:
            return LimitReason.cpu_pressure, False
        if (
            psi is None
            and health.load1 is not None
            and health.cores
            and health.load1 / health.cores >= s.load_per_core_high
        ):
            return LimitReason.load, False
        return LimitReason.steady, False

    def _set(self, limit: int, reason: LimitReason) -> None:
        if limit != self.limit:
            direction = "up" if limit > self.limit else "down"
            log.info(
                "build_limit_changed",
                store_id=self.store_id,
                limit=limit,
                was=self.limit,
                reason=str(reason),
            )
            metrics.BUILD_LIMIT_CHANGES.labels(store_id=self.store_id, direction=direction, reason=str(reason)).inc()
        self.limit = limit
        self.reason = reason
