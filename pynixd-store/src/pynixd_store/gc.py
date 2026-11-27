"""The collection's reads over a local store: its roots, its database, its disk.

The functions are pure over their arguments or read only the file system, so
a pass's order and bound are asserted without a store. `GarbagePlanner`
reads the store's state. The collector in pynixd chooses, streams to the
client and deletes through the daemon.
"""

from __future__ import annotations

import ctypes
import os
import stat
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from anyio.to_thread import run_sync

from .liveness import RootsTracker, walk_volatile
from .local_store_db import LocalStoreDB

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from .store_layout import StoreLayout

log = structlog.get_logger(__name__)

_LIBC = ctypes.CDLL(None, use_errno=True)

FREED_RATIO_FLOOR = 0.001
"""The smallest share of its NAR bytes a pass is taken to free: past it, the next pass plans the whole plan anyway."""


def sync_filesystem(directory: str | Path) -> None:
    """Commit what the file system holding *directory* still owes, so `statvfs` counts freed blocks.

    btrfs returns a deleted file's extents at the next transaction commit,
    30 s after by default, and ZFS at the next transaction group. `syncfs`
    commits one file system; `sync` (macOS has no `syncfs`) commits all.
    """
    syncfs = getattr(_LIBC, "syncfs", None)
    if syncfs is None:
        os.sync()
        return
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        if syncfs(fd) != 0:
            errno = ctypes.get_errno()
            raise OSError(errno, os.strerror(errno), str(directory))
    finally:
        os.close(fd)


def freed_ratio(freed: int, planned_nar_bytes: int) -> float | None:
    """The share of its NAR bytes a pass freed on disk, or `None` when the pass freed nothing.

    A NAR size overstates what a delete frees: a compressing file system
    stores less, and a file `.links` shares with a live path frees nothing.
    On dynhetz (btrfs, zstd), passes planned 1-2 paths to reach the target
    and freed 72-144 MB. More than the NAR bytes reads as one: blocks freed
    by someone else in the same window.
    """
    if freed <= 0 or planned_nar_bytes <= 0:
        return None
    return max(FREED_RATIO_FLOOR, min(1.0, freed / planned_nar_bytes))


def pressure_usage(directory: str | Path) -> tuple[int, int]:
    """`(used, total)` bytes with pressure measured the way `df` measures it.

    `shutil.disk_usage` counts `bfree` (blocks free to anyone, root
    included) as free, but a writer that is not root can only spend
    `bavail`. On btrfs the two diverge by tens of percent of the disk,
    so a `bfree` gauge reads relief long before `df` -- and kubelet --
    agree. Read `statvfs` directly and count everything but `bavail` as
    used. Issue nixidae/nixidae#11.
    """
    vfs = os.statvfs(directory)
    total = vfs.f_blocks * vfs.f_frsize
    return ((vfs.f_blocks - vfs.f_bavail) * vfs.f_frsize, total)


def over_target(usage: tuple[int, int] | None, target: float | None) -> bool:
    """Whether the disk holds more than the pass's target allows: the age rule yields then."""
    if target is None or usage is None:
        return False
    used, total = usage
    return total > 0 and used / total > target


@dataclass(frozen=True, slots=True)
class DeleteOrder:
    """The score a pass deletes by, highest first: one rule in three spellings.

    `score` takes `(nar_size, age_seconds)`. `sqlite` and `postgres` are SQL
    expressions over `w.size` and `w.age` that must compute the same double:
    the SQL plan and the in-memory plan cut the same prefix only when they
    agree to the bit. Equal scores fall to the bigger path, then to the path
    string. `tests/unit/test_gc_sql_plan.py` holds every order to that.
    """

    name: str
    score: Callable[[int, int], float]
    sqlite: str
    postgres: str


SPACE_TIME = DeleteOrder(
    name="space-time",
    score=lambda size, age: float(size) * float(age),
    # A float product, not an integer one: 100 GB idle for 3 years passes
    # int64, where SQLite turns to REAL and Postgres raises.
    sqlite="1.0 * w.size * w.age",
    postgres="w.size::float8 * w.age::float8",
)
"""Bytes times seconds since a client last referenced the path: what keeping it has cost.

A big idle path goes first and a big path in use goes last. Disk pressure
decides how much a pass deletes, not the order: under a size-led order a
full disk deleted vllm on sg1 minutes after its last use. nixidae/nixidae#1650.
"""


def by_weight(weights: Mapping[str, tuple[int, int]], order: DeleteOrder = SPACE_TIME) -> list[str]:
    """The paths in delete order. `weights` maps the path string to `(nar_size, age_seconds)`."""
    scored = {path: order.score(size, age) for path, (size, age) in weights.items()}
    return sorted(scored, key=lambda path: (-scored[path], -weights[path][0], path))


def take_until_below_target(
    ordered: Sequence[tuple[str, int]], used: int, total: int, target: float, ratio: float = 1.0
) -> list[str]:
    """The leading paths whose removal projects usage under `target`.

    Pure, for the same reason. `ordered` is `(path, size)` heaviest first;
    the walk stops at the first prefix whose freed bytes, `size * ratio`
    each (`freed_ratio`), bring `used / total` to or under the fraction. A
    target already met takes nothing; a target no prefix meets takes
    everything.
    """
    batch: list[str] = []
    before = 0
    for path, size in ordered:
        # `ratio * before`, as the SQL plan computes it, so both cut at the same row.
        if (used - ratio * before) / total <= target:
            break
        batch.append(path)
        before += size
    return batch


def physical_batch_total(top_paths: Iterable[str], store_dir: Path | None) -> int | None:
    """Block bytes the batch holds, hardlinks counted once, or `None`.

    The daemon counts `st.st_size` per deleted file (`deletePath` in
    `file-system.cc`), so sparse files count their holes and two-link
    files count twice -- the number `nix store gc` prints has the same
    fiction. An operator deciding by `df` needs blocks, so this walks
    the batch with `lstat` (never following symlinks), dedupes by
    `(st_dev, st_ino)`, and sums `st_blocks * 512`. `None` on anything
    unmeasurable -- no layout, a path that vanished, an unreadable
    directory -- and the caller falls back to the daemon number.
    """
    if store_dir is None:
        return None
    total = 0
    seen: set[tuple[int, int]] = set()
    queue: deque[Path] = deque()
    for top in top_paths:
        if store_dir != Path("/nix/store") and top.startswith("/nix/store/"):
            queue.append(store_dir / Path(top).relative_to("/nix/store"))
        else:
            queue.append(Path(top))
    try:
        while queue:
            current = queue.popleft()
            try:
                info = current.lstat()
            except OSError:
                return None
            key = (info.st_dev, info.st_ino)
            if key in seen:
                continue
            seen.add(key)
            total += info.st_blocks * 512
            if stat.S_ISDIR(info.st_mode):
                try:
                    queue.extend(current.iterdir())
                except OSError:
                    return None
    except OSError:
        return None
    return total


class GarbagePlanner:
    """The reads a collection pass makes of one local store's roots, database and disk.

    *layout* and *db* may be absent: a store served without its state
    gathers no evidence, and each read then answers what the pass treats
    as unknown. *db* is a `LocalStoreDB` or anything with the same queries.
    """

    def __init__(self, layout: StoreLayout | None, db: Any) -> None:
        self.layout = layout
        self.db = db

    @property
    def real_store_dir(self) -> Path | None:
        return None if self.layout is None else self.layout.real_store_dir

    def plan_database(self) -> LocalStoreDB | None:
        """The database a `SqlPlan` opens, or `None` when the store lacks pynixd's tables."""
        db = self.db
        if not isinstance(db, LocalStoreDB) or not db.active or not db.schema.usable:
            return None
        return db

    def volatile_roots(self) -> set[str]:
        """The living roots, read synchronously for a worker thread.

        `/proc` and `temproots` move constantly and no watch covers them,
        so every pass reads them fresh just before planning. A store
        without a layout has no state to read, and vetoes nothing.
        """
        if self.layout is None:
            return set()
        return walk_volatile(self.layout.state_dir, str(self.layout.store_dir))

    async def live(self) -> set[str] | None:
        """The tracker's live set, refreshed just now, or `None` with no mirror.

        The tracker reads roots and SQLite in a worker thread.
        Missing state or a failed refresh returns `None`; callers then ask Nix.
        """
        try:
            db = self.db
            if self.layout is None or not isinstance(db, LocalStoreDB) or not db.active:
                return None
            tracker = RootsTracker(self.layout.state_dir, str(self.layout.store_dir), db)
            return await run_sync(tracker.refresh)
        except Exception:
            log.warning("gc_mirror_refresh_failed", exc_info=True)
            return None

    def sync(self) -> None:
        """`sync_filesystem` on the store's file system; a failure only leaves the next reading stale."""
        directory = getattr(self.layout, "real_store_dir", None)
        if directory is None:
            return
        try:
            sync_filesystem(directory)
        except OSError:
            log.warning("gc_sync_failed", exc_info=True)

    def disk_usage(self) -> tuple[int, int] | None:
        """`(used, total)` bytes of pressure on the filesystem holding the store."""
        try:
            directory = getattr(self.layout, "real_store_dir", None)
            if directory is None:
                return None
            return pressure_usage(directory)
        except Exception:
            log.warning("gc_disk_usage_failed", exc_info=True)
            return None

    async def weigh(self, paths: Iterable[str]) -> dict[str, tuple[int, int]] | None:
        """`(nar_size, age_seconds)` per planned path, or `None` when unknown.

        Sizes come from `ValidPaths`, ages from the access table; a path
        with no date weighs age zero, which deprioritises it without
        excluding it. `None` fails the pass closed: a pass that cannot see
        the whole state deletes nothing, and shows nothing either.
        """
        paths = set(paths)
        if not paths:
            return {}
        # A database read, not a store request: it marks nothing, so weighing
        # never freshens the judged, and it loads no references.
        query = getattr(self.db, "query_weights", None)
        if query is None:
            log.warning("gc_no_access_tracking")
            return None
        try:
            rows = await query(paths)
        except Exception:
            log.warning("gc_weigh_failed", exc_info=True)
            return None
        if rows is None:
            return None
        now = time.time()
        return {path: (size, 0 if seen is None else max(0, int(now - seen))) for path, (size, seen) in rows.items()}

    async def stale_since(self, max_age: int) -> set[str] | None:
        """The paths unreferenced for `max_age` seconds, or `None` when unknown."""
        query = getattr(self.db, "query_paths_not_referenced_since", None)
        if query is None:
            log.warning("gc_no_access_tracking")
            return None
        try:
            result = await query(max_age)
        except Exception:
            log.warning("gc_stale_query_failed", exc_info=True)
            return None
        if result is None:
            return None
        return {str(path) for path in result}

    async def referrer_closure(self, seeds: set[str]) -> set[str] | None:
        """`seeds` plus everything that references them, transitively."""
        query = getattr(self.db, "query_referrer_closure", None)
        if query is None:
            log.warning("gc_no_referrer_closure")
            return None
        try:
            closure: set[str] | None = await query(seeds)
            return closure
        except Exception:
            log.warning("gc_referrer_query_failed", exc_info=True)
            return None

    async def close_batch(self, batch: list[str], planned: set[str]) -> list[str]:
        """The batch closed under referrers again, against the plan.

        Each seed brings its transitive referrers; a seed whose closure
        escapes the plan cannot be deleted now, so it drops instead of
        poisoning the request. Membership in the plan is the liveness
        answer: the planners closed the plan, so every referrer the
        database knows is either planned or live. Seeds first in batch
        order, joined referrers after. An unanswerable database plans
        nothing rather than half a batch.
        """
        query = getattr(self.db, "query_referrer_closure", None)
        if query is None:
            log.warning("gc_batch_close_unavailable")
            return []
        kept: dict[str, None] = {}
        for seed in batch:
            try:
                closed = await query({seed})
            except Exception:
                log.warning("gc_batch_close_failed", exc_info=True)
                return []
            if closed is None:
                log.warning("gc_batch_close_unavailable")
                return []
            outside = {path for path in closed if path not in planned}
            if outside:
                log.info("gc_batch_seed_dropped", seed=seed, live_referrers=len(outside))
                continue
            for path in closed:
                kept.setdefault(path)
        order = {path: index for index, path in enumerate(batch)}
        return sorted(kept, key=lambda path: (0, order[path]) if path in order else (1, path))


class LivenessWatch:
    """Refresh Python liveness without taking the daemon GC lock."""

    def __init__(self, tracker: RootsTracker) -> None:
        self.tracker = tracker

    @classmethod
    def of(cls, layout: StoreLayout | None, db: LocalStoreDB) -> LivenessWatch | None:
        """A watch over the store at *layout*, or `None` when it cannot host a mirror.

        The mirror lives in the store's own database beside Nix's tables,
        and reads the store's own roots directories: without a layout or a
        database there is neither. `None` is not an error -- a store served
        without its state simply gathers no evidence.
        """
        if layout is None or not db.active:
            return None
        return cls(RootsTracker(layout.state_dir, str(layout.store_dir), db))

    async def refresh(self) -> set[str]:
        """Refresh the mirror without asking Nix anything.

        Stable-root changes and the configured interval trigger refreshes.
        """
        return await run_sync(self.tracker.refresh)
