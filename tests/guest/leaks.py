"""Count the guest's processes again, and fail on one the suites left.

A guest and not a derivation, because these suites start nix daemons,
pynixd daemons, ssh servers and http servers, and build into stores they
make themselves. A build sandbox cleans up files and nothing else: a
process that outlives its test keeps running on the machine that ran it.
Measured while writing the first version of this: a UML guest leaked by
an earlier session had been using a whole core for thirty-one hours.

`always`, so it runs after a suite that failed -- a leak is most likely
exactly then.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from vivarium_runner import Machines

if TYPE_CHECKING:
    from vivarium_runner.agent import ProcessInfo

ALLOWED_LEAKS = 0
"""The one leak this check has found belongs to `tests/parity`: a
per-connection worker of a managed daemon, which `daemon.cc` gives its own
session, so no group signal at either end reaches it. Issue #36. Raise this
only with an issue number beside it, never to make a run pass."""

LEAK_MARKERS = ("/tmp/pynixd-", "pynixd")
"""A test store, or pynixd itself. Matched against the command line and
the process name both, because `/proc/<pid>/stat` cuts a name at 15
characters.

Not `nix-daemon`: the guest's own is socket-activated, so it reads `0
before, 1 after` every time and none of it is a leak."""


async def test(vms: Machines) -> None:
    # `shared` holds Any: the census is `prepare`'s, and `prepare` is the one that writes it.
    census: dict[str, list[ProcessInfo]] | None = vms.shared.get("before")
    if census is None:
        raise RuntimeError("no census from `prepare`, so there is nothing to compare against")

    leaked: list[ProcessInfo] = []
    for name, vm in vms.items():
        before = census[name]
        after = await vm.processes()

        # By pid, not by count: a count that happens to match hides one
        # process exiting while another leaks.
        was_running = {p["pid"] for p in before}
        mine = [p for p in after if p["pid"] not in was_running and _is_a_leak(p)]
        leaked += mine

        # What the host pays for this guest's RAM now, after its suite. The
        # measurement that tells the two backends apart: UML returns freed
        # pages to the host, and `memory` is a ceiling, not a cost.
        host_kib = vm.host_memory_kib()
        print(f"[test] {name}: {len(before)} processes before, {len(after)} after; host pays {host_kib // 1024} MiB")
        for p in mine:
            print(f"[test] {name}: LEAKED {p['pid']} (parent {p['ppid']}) {p['cmdline'][:120]}")

        # Written whether or not anything leaked: a clean census is what a
        # later one is read against.
        (vms.artifacts / name / "processes.json").write_text(
            json.dumps({"before": before, "after": after, "leaked": mine, "host_kib": host_kib}, indent=2) + "\n"
        )

    assert len(leaked) <= ALLOWED_LEAKS, (
        f"the suites left {len(leaked)} processes holding a test store, and "
        f"{ALLOWED_LEAKS} is what issue #36 accounts for. Each guest's census "
        "is in its processes.json beside this run's logs."
    )


def _is_a_leak(process: ProcessInfo) -> bool:
    return any(marker in process["cmdline"] or marker in process["name"] for marker in LEAK_MARKERS)
