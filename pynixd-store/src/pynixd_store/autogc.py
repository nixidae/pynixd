"""`LocalStore::autoGC` (`gc.cc:1012`): collect when free space drops under `min-free`.

The decision follows Nix: at most one free-space reading per
`min-free-check-interval`, nothing while the space is at or over `min-free` or
`max-free`, nothing while it is still over 97 % of what the last collection
left, and a collection that frees `max-free` minus the free space. The
collection is the engine's, so it finds Nix's dead set.

**The state belongs to one client session.** Nix keeps it in each
`LocalStore` (`local-store.hh:218`), and each daemon worker opens its own, so
each client has its own clock, its own last result and its own collection.
`gc-auto.sh` depends on that: its two clients collect once each, and only
then is one of its three dead paths left. A state for the whole store
collects once, and two are left.
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import anyio
import structlog

from .errors import PynixdError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from typing import Any

FREE_SPACE_FILE: Final = "_NIX_TEST_FREE_SPACE_FILE"
"""Nix reads the free space from the file this variable names, in place of `statvfs` (`gc.cc:1020`)."""

_UNIT_PREFIXES: Final = {"K": 10, "M": 20, "G": 30, "T": 40}

log = structlog.get_logger(__name__)


def parse_size(value: str) -> int:
    """`string2IntWithUnitPrefix` (`util.hh:105`): a number, with a `K`, `M`, `G` or `T` suffix of 1024."""
    shift = 0
    if value and value[-1].isalpha():
        unit = value[-1].upper()
        if unit not in _UNIT_PREFIXES:
            raise PynixdError(f"invalid unit specifier '{unit}'")
        shift = _UNIT_PREFIXES[unit]
        value = value[:-1]
    if not value.isdigit():
        raise PynixdError(f"'{value}' is not an integer")
    return int(value) << shift


_DEFAULT_MIN_FREE: Final = 0
_DEFAULT_MAX_FREE: Final = 2**63 - 1
_DEFAULT_CHECK_INTERVAL: Final = 5


@dataclass(frozen=True, slots=True)
class AutoGCSettings:
    """`min-free`, `max-free` and `min-free-check-interval` (`local-settings.hh:105`), the session's overrides applied."""

    min_free: int = _DEFAULT_MIN_FREE
    max_free: int = _DEFAULT_MAX_FREE
    check_interval: int = _DEFAULT_CHECK_INTERVAL

    @classmethod
    def from_nix_config(cls, config: Mapping[str, Any], overrides: Mapping[str, str]) -> AutoGCSettings:
        def value(name: str, default: int) -> int:
            if name in overrides:
                try:
                    return parse_size(overrides[name])
                # `ClientSettings::apply` warns and keeps the setting (`daemon.cc:330`).
                except PynixdError as exc:
                    log.warning("auto_gc_setting_ignored", setting=name, error=str(exc))
            entry = config.get(name)
            return int(entry["value"]) if entry is not None else default

        return cls(
            min_free=value("min-free", _DEFAULT_MIN_FREE),
            max_free=value("max-free", _DEFAULT_MAX_FREE),
            check_interval=value("min-free-check-interval", _DEFAULT_CHECK_INTERVAL),
        )

    @property
    def enabled(self) -> bool:
        """`worker.cc:395` wakes the build loop only while `min-free` is set."""
        return self.min_free != 0


def free_space(real_store_dir: str) -> int:
    """The bytes free for the store, as `autoGC` reads them (`gc.cc:1019-1028`). It blocks on the file system."""
    if path := os.environ.get(FREE_SPACE_FILE):
        with open(path, encoding="ascii") as handle:  # noqa: PTH123 -- one read, on a worker thread
            return int(handle.read())
    st = os.statvfs(real_store_dir)
    return st.f_bavail * st.f_frsize


def bytes_to_free(settings: AutoGCSettings, avail: int, avail_after_gc: float) -> int | None:
    """What one auto-GC frees with *avail* bytes free, or `None` when none runs (`gc.cc:1050-1078`)."""
    if avail >= settings.min_free or avail >= settings.max_free:
        return None
    if avail > avail_after_gc * 0.97:
        return None
    return settings.max_free - avail


class AutoGC:
    """The auto-GC state of one client session: the last check, what the last collection left, the running one."""

    def __init__(
        self,
        *,
        free: Callable[[], Awaitable[int]],
        collect: Callable[[int, Mapping[str, str]], Awaitable[None]],
        spawn: Callable[[Callable[[], Awaitable[None]]], object],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._free = free
        self._collect = collect
        self._spawn = spawn
        self._clock = clock
        self._last_check = -math.inf
        self._avail_after_gc: float = math.inf
        self._running: anyio.Event | None = None

    async def check(self, settings: AutoGCSettings, overrides: Mapping[str, str], *, wait: bool) -> None:
        """`autoGC(sync)`: start a collection when one is due, and with *wait* wait for a running one."""
        running = self._running
        if running is None:
            now = self._clock()
            if now < self._last_check + settings.check_interval:
                return
            avail = await self._free()
            self._last_check = now
            to_free = bytes_to_free(settings, avail, self._avail_after_gc)
            if to_free is None:
                return
            running = self._running = anyio.Event()
            self._spawn(lambda: self._run(to_free, overrides, running))
        if wait:
            await running.wait()

    async def _run(self, to_free: int, overrides: Mapping[str, str], done: anyio.Event) -> None:
        try:
            log.info("auto_gc", to_free=to_free)
            await self._collect(to_free, overrides)
            self._avail_after_gc = await self._free()
        # `gc.cc:1091` ignores a failed auto-GC; a raise here would end the store's background loops.
        except Exception:
            log.exception("auto_gc_failed", to_free=to_free)
        finally:
            self._running = None
            self._last_check = self._clock()
            done.set()
