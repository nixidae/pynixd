"""The Nix functional suite, both arms, inside the guest.

Runs the nixft harness's `all` (setup, control, pynixd, compare) off a work
directory on the guest's disk. The full log lands in `/artifacts/nixft.log`
-- thousands of lines, and the way back is a serial line -- and the verdict
line plus the compare summary are printed, so the run's outcome reads
without opening the log. Issue #45.
"""

from __future__ import annotations

from vivarium_runner import Machines

TIMEOUT = 21600
"""Seconds for both arms. The suite starts a daemon per test twice over, and
a timeout that fires is a result nobody wants from the clock rather than from
the tests."""

JOBS = "$(( $(nproc) / 2 ))"
"""Tests at a time: half the processors. pynixd starts slower than `nix-daemon`,
and the suite waits 6 s for a daemon's socket. Measured on 8 processors: one
test per processor failed two tests of the pynixd arm that pass alone
(`ca:build-cache`, `main:fetchTree-file`); four at a time failed none, at
about 4.5 minutes for the pynixd arm."""


async def test(vms: Machines) -> None:
    [vm] = vms.values()
    nixft = vms.settings["nixft"]
    command = f"mkdir -p /work-nixft && JOBS={JOBS} NIXFT_WORK=/work-nixft {nixft} all > /artifacts/nixft.log 2>&1"
    rc, _ = await vm.execute(command, timeout=TIMEOUT, label="nixft all")
    tail = (await vm.succeed("tail -n 30 /artifacts/nixft.log")).strip()
    print(f"[test] nixft all: exit {rc}\n[test]   {tail}")
    if rc != 0:
        raise AssertionError(f"nixft all exited {rc}: {tail.splitlines()[-1] if tail else ''}")
