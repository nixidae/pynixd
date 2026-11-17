"""Power a builder off after a quiet spell, whatever its controller does: nixidae/nixidae#1198.

The controller ends an idle builder itself. This guard ends one that a dead,
unreachable or wrong controller never ends. A systemd timer runs it each
minute. Quiet means no build process runs and the store database has not
changed. A cloud machine launched to terminate on shutdown then costs nothing.

A controller that admits the builder sends its idle limit (`pynixd-lease`),
and the builder's pynixd writes it to `LEASE`. The guard then ends the
machine `LEASE_MARGIN` after that limit, in place of its own argument, and
counts quiet from the lease's arrival, as the controller counts idle from
registration.

`--wait` holds a CI job instead (nixidae/nixidae#1473): a runner has no
poweroff that ends its job cleanly, so the guard returns once quiet, and the
job's last step ends with it.
"""

from __future__ import annotations

import argparse
import contextlib
import json
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
WAIT_INTERVAL: Final = 15.0
LEASE: Final = Path("/run/pynixd-idle/lease.json")
"""`ReverseInitiatorSettings.idle_lease` of a builder the installer sets up."""
LEASE_MARGIN: Final = 60.0
"""Past the controller's own end: its step comes each `heartbeat` (30 s by default), the guard's each minute."""


def building(uids: Iterable[int]) -> bool:
    """Whether a process runs as an auto-allocated build user: a build in progress writes nothing until it ends."""
    return any(FIRST_BUILD_UID <= uid < FIRST_BUILD_UID + BUILD_UIDS for uid in uids)


def quiet_for(now: float, booted: float, changes: Iterable[float]) -> float:
    """Seconds since boot or the last change, whichever came later."""
    return now - max([booted, *changes])


def limit(seconds: float, lease: str | None) -> float:
    """The quiet seconds that end this machine: the lease's idle limit and margin, else *seconds*."""
    if lease is None:
        return seconds
    try:
        idle = json.loads(lease)["idle_seconds"]
    except (ValueError, TypeError, KeyError):
        return seconds
    if isinstance(idle, bool) or not isinstance(idle, int | float) or idle < 0:
        return seconds
    return float(idle) + LEASE_MARGIN


def read_lease(path: Path = LEASE) -> str | None:
    try:
        return path.read_text()
    except (FileNotFoundError, UnicodeDecodeError):
        return None


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


def quiet() -> float:
    """Seconds this machine has been quiet; 0 while a build runs."""
    now = time.time()
    if building(process_uids()):
        SEEN.parent.mkdir(parents=True, exist_ok=True)
        SEEN.touch()
        return 0.0
    booted = now - float(Path("/proc/uptime").read_text().split()[0])
    return quiet_for(now, booted, _mtimes([*DB_FILES, SEEN, LEASE]))


def main() -> None:
    parser = argparse.ArgumentParser(prog="pynixd-idle-guard", description=__doc__.splitlines()[0])
    parser.add_argument("seconds", type=float, help="power off after this many quiet seconds, until a lease comes")
    parser.add_argument("--wait", action="store_true", help="return once quiet, instead of powering off")
    args = parser.parse_args()
    if args.wait:
        # Neither a build's start nor its end notifies anyone, so the guard looks again, as its timer does.
        while (spell := quiet()) < (seconds := limit(args.seconds, read_lease())):
            time.sleep(WAIT_INTERVAL)
        print(f"pynixd-idle-guard: quiet for {spell:.0f} s of {seconds:.0f} s, ending", file=sys.stderr)
        return
    spell = quiet()
    seconds = limit(args.seconds, read_lease())
    if spell < seconds:
        return
    print(f"pynixd-idle-guard: quiet for {spell:.0f} s of {seconds:.0f} s, powering off", file=sys.stderr)
    systemctl = shutil.which("systemctl") or "/usr/bin/systemctl"
    subprocess.run([systemctl, "poweroff"], check=True)


if __name__ == "__main__":
    main()
