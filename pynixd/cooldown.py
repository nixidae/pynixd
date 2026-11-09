"""A wait that doubles after each failure and ends at the first success."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable

OPEN_COOLDOWN_BASE: Final[float] = 60.0
"""The first wait: `maybeDisable` (`http-binary-cache-store.cc:94`) disables a cache for 60 s."""

OPEN_COOLDOWN_CEILING: Final[float] = 1800.0


@dataclass
class Cooldown:
    """How long to leave a store alone after it failed, by consecutive failures.

    The first failure waits *base* seconds, each further one doubles the
    wait up to *ceiling*, and one success clears it.
    """

    base: float = OPEN_COOLDOWN_BASE
    ceiling: float = OPEN_COOLDOWN_CEILING
    now: Callable[[], float] = field(default=time.monotonic, repr=False)
    failures: int = 0
    until: float = 0.0

    def failed(self) -> float:
        """Count one failure and return the wait it starts."""
        self.failures += 1
        wait = min(self.base * 2.0 ** (self.failures - 1), self.ceiling)
        self.until = self.now() + wait
        return wait

    def succeeded(self) -> None:
        self.failures = 0
        self.until = 0.0

    @property
    def cooling(self) -> bool:
        return self.now() < self.until

    @property
    def remaining(self) -> float:
        return max(0.0, self.until - self.now())
