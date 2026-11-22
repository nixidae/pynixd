"""What each way to give a build its inputs costs, for each closure size. nixidae/nixidae#1560, spike.

`input_primitives.py` measures in the guest; this phase names the closures and prints a table. Results go to
`<artifacts>/input-primitives.json`. Measures; asserts nothing.
"""

from __future__ import annotations

import json
import shlex
import statistics
from collections.abc import Mapping

import anyio
from vivarium_runner import Machine, Machines

REPEATS = 3
SCRATCH = "/var/lib/pynixd-bench"

type Seconds = dict[str, dict[str, list[float]]]
"""Per closure, per way: seconds of each repeat."""


async def closure_size(cp: Machine, path: str) -> tuple[int, int]:
    """The closure's path count and its bytes."""
    query = f"nix-store --query --requisites {path}"
    count = int((await cp.succeed(f"{query} | wc -l")).stdout)
    sizes = (await cp.succeed(f"{query} | xargs du -sb --apparent-size | cut -f1")).stdout
    return count, sum(int(n) for n in sizes.split())


async def measure(cp: Machine, settings: Mapping[str, str], mode: str, labels: Mapping[str, str]) -> Seconds:
    pairs = shlex.join(f"{label}={path}" for label, path in labels.items())
    command = f"{settings['venvPython']} {settings['inputPrimitives']} {mode} {REPEATS} {pairs}"
    if mode == "namespace":
        command = (
            f"setpriv --reuid tester --regid users --clear-groups unshare --user --map-root-user --mount {command}"
        )
    found: Seconds = json.loads((await cp.succeed(command, timeout=1800)).stdout)
    return found


async def test(vms: Machines) -> None:
    cp, settings = vms.cp, vms.settings
    labels = {"busybox": settings["busybox"], "python3": settings["probePython"], "pynixd": settings["benchLarge"]}
    await cp.succeed(f"install -d -m 0755 -o tester {SCRATCH}")
    load = (await cp.succeed("cat /proc/loadavg")).stdout.strip()

    found = await measure(cp, settings, "namespace", labels)
    for label, row in (await measure(cp, settings, "copy", labels)).items():
        found[label] |= row

    report = {}
    for label, ways in found.items():
        paths, size = await closure_size(cp, labels[label])
        report[label] = {"paths": paths, "bytes": size, "seconds": ways}
        for way, seconds in ways.items():
            median = statistics.median(seconds)
            print(
                f"[test] {label} ({paths} paths, {size / 2**20:.0f} MiB), {way}: median {median * 1000:.1f} ms, "
                f"{median / paths * 1e6:.1f} us per path, all {[round(s * 1000, 1) for s in seconds]} ms"
            )
    report_file = anyio.Path(vms.artifacts) / "input-primitives.json"
    await report_file.write_text(json.dumps({"load before": load, "closures": report}, indent=2) + "\n")
