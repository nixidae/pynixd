"""What the collector weighs a store by: disk pressure, path weights, the bytes a delete frees.

Each function is pure over its arguments or reads only the file system, so
a pass's order and bound are asserted without a store.
"""

from __future__ import annotations

import os
import stat
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence


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


def by_weight(weights: Mapping[str, tuple[int, int]], pressure: float) -> list[str]:
    """The paths in delete order: size matters more the fuller the disk is.

    Pure, so the order a pass deletes in is asserted without a daemon.
    `weights` maps the path string to `(nar_size, age_seconds)`; `pressure`
    is the disk usage fraction. Both axes normalise against the candidate
    set, and the score blends them by pressure:

        score = pressure * size_norm + (1 - pressure) * age_norm

    An empty disk collects oldest first, a full disk biggest first, and
    every pressure between moves continuously: no threshold flips the order
    between two passes. Descending score, path string breaking ties so the
    order is stable run to run.
    """
    sizes = {path: size for path, (size, _) in weights.items()}
    ages = {path: age for path, (_, age) in weights.items()}
    biggest = max(sizes.values(), default=0)
    oldest = max(ages.values(), default=0)
    scored = {
        path: (
            pressure * (size / biggest if biggest else 0.0)
            + (1.0 - pressure) * (ages[path] / oldest if oldest else 0.0)
        )
        for path, size in sizes.items()
    }
    return [path for path, _ in sorted(scored.items(), key=lambda item: (-item[1], item[0]))]


def take_until_below_target(ordered: Sequence[tuple[str, int]], used: int, total: int, target: float) -> list[str]:
    """The leading paths whose removal projects usage under `target`.

    Pure, for the same reason. `ordered` is `(path, size)` heaviest first;
    the walk stops at the first prefix whose freed bytes bring `used / total`
    to or under the fraction. A target already met takes nothing; a target
    no prefix meets takes everything.
    """
    batch: list[str] = []
    freed = 0
    for path, size in ordered:
        if (used - freed) / total <= target:
            break
        batch.append(path)
        freed += size
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
