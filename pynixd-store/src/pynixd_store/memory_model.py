"""A build's peak memory as a function of its cores, fitted from its history (nixidae/nixidae#1558).

`peak(cores) = base + per_core * cores`. Placement reads it to give a builder the most cores whose peak fits
the builder's free memory, so the same derivation gets 64 cores on a roomy host and 8 on a starved one, and
nothing sticky is stored.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True, slots=True)
class MemoryPoint:
    """One attempt: the cores it ran with (0 when unknown) and its peak memory. A killed attempt's peak is a lower
    bound."""

    cores: int
    peak_bytes: int
    killed: bool = False


@dataclass(frozen=True, slots=True)
class MemoryModel:
    base_bytes: float
    per_core_bytes: float

    def peak(self, cores: int) -> int:
        return math.ceil(self.base_bytes + self.per_core_bytes * cores)

    def most_cores_within(self, room_bytes: int, ceiling: int) -> int | None:
        """The most cores, at most *ceiling*, whose peak fits *room_bytes*; `None` when not even 1 core fits."""
        if self.peak(1) > room_bytes:
            return None
        if self.per_core_bytes <= 0:
            return ceiling
        return max(1, min(ceiling, math.floor((room_bytes - self.base_bytes) / self.per_core_bytes)))

    def killed_at(self, cores: int, peak_bytes: int) -> MemoryModel:
        """This model, raised so that *cores* needs more than *peak_bytes*: the attempt died holding that much."""
        if self.peak(cores) > peak_bytes:
            return self
        return MemoryModel(self.base_bytes, (peak_bytes + 1 - self.base_bytes) / cores)


def fit(points: Iterable[MemoryPoint]) -> MemoryModel | None:
    """The model of *points*, or `None` when no attempt measured its memory.

    The median peak of each core count is one point. Two or more core counts give the least-squares slope; one
    gives a line through zero, which overestimates above that count. A slope down is noise: the highest peak
    stands for every count. A kill then raises the line above its peak. An attempt that names no cores (0, a row
    from before pynixd stored them) counts only when no other does: its median peak, at any cores.
    """
    measured = [p for p in points if p.peak_bytes > 0]
    by_cores: dict[int, list[int]] = {}
    for point in measured:
        if not point.killed:
            by_cores.setdefault(point.cores, []).append(point.peak_bytes)
    uncounted = by_cores.pop(0, [])
    medians = {cores: statistics.median(peaks) for cores, peaks in by_cores.items()}
    model = _line(medians) if medians else MemoryModel(statistics.median(uncounted), 0.0) if uncounted else None
    for point in measured:
        if point.killed and point.cores > 0:
            model = (model or MemoryModel(0.0, 0.0)).killed_at(point.cores, point.peak_bytes)
    return model


def _line(medians: dict[int, float]) -> MemoryModel:
    steepest = max(peak / cores for cores, peak in medians.items())
    if len(medians) < 2:
        return MemoryModel(0.0, steepest)
    slope, _ = statistics.linear_regression(list(medians), list(medians.values()))
    if slope < 0:
        return MemoryModel(max(medians.values()), 0.0)
    # Raised to the highest point, not through the middle: a point above the line would place a build that dies.
    intercept = max(peak - slope * cores for cores, peak in medians.items())
    if intercept < 0:
        return MemoryModel(0.0, steepest)
    return MemoryModel(intercept, slope)
