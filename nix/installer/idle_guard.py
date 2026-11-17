"""Power a builder off after a quiet spell, whatever its controller does: nixidae/nixidae#1198.

The controller ends an idle builder itself. This guard ends one that a dead,
unreachable or wrong controller never ends. A systemd timer runs it each
minute. Quiet means no build process runs and the store database has not
changed. A cloud machine launched to terminate on shutdown then costs nothing.
"""

from __future__ import annotations

import argparse
import contextlib
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterable

DB_FILES: Final = (Path("/nix/var/nix/db/db.sqlite"), Path("/nix/var/nix/db/db.sqlite-wal"))
"""A build's outputs are registered here; a query writes neither file."""
SEEN: Final = Path("/run/pynixd-idle/build-seen")
"""Touched while a build runs. /run empties at boot, so a stamp never outlives the machine's uptime."""
# `start-id` and `id-count`'s defaults (`globals.hh`): the installer sets neither.
FIRST_BUILD_UID: Final = 0x34000000
BUILD_UIDS: Final = 128 << 16


def building(uids: Iterable[int]) -> bool:
    """Whether a process runs as an auto-allocated build user: a build in progress writes nothing until it ends."""
    return any(FIRST_BUILD_UID <= uid < FIRST_BUILD_UID + BUILD_UIDS for uid in uids)


def quiet_for(now: float, booted: float, changes: Iterable[float]) -> float:
    """Seconds since boot or the last change, whichever came later."""
    return now - max([booted, *changes])


def process_uids(proc: Path = Path("/proc")) -> list[int]:
    uids = []
    for entry in proc.iterdir():
        if entry.name.isdigit():
            # A process that ended since the listing.
            with contextlib.suppress(FileNotFoundError, ProcessLookupError):
                uids.append(entry.stat().st_uid)
    return uids


def _mtimes(paths: Iterable[Path]) -> list[float]:
    times = []
    for path in paths:
        with contextlib.suppress(FileNotFoundError):
            times.append(path.stat().st_mtime)
    return times


def main() -> None:
    parser = argparse.ArgumentParser(prog="pynixd-idle-guard", description=__doc__.splitlines()[0])
    parser.add_argument("seconds", type=float, help="power off after this many quiet seconds")
    args = parser.parse_args()
    now = time.time()
    if building(process_uids()):
        SEEN.parent.mkdir(parents=True, exist_ok=True)
        SEEN.touch()
        return
    booted = now - float(Path("/proc/uptime").read_text().split()[0])
    quiet = quiet_for(now, booted, _mtimes([*DB_FILES, SEEN]))
    if quiet < args.seconds:
        return
    print(f"pynixd-idle-guard: quiet for {quiet:.0f} s, powering off", file=sys.stderr)
    systemctl = shutil.which("systemctl") or "/usr/bin/systemctl"
    subprocess.run([systemctl, "poweroff"], check=True)


if __name__ == "__main__":
    main()
