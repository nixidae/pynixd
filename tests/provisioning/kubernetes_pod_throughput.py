"""How long a build pod takes, and where the time goes. nixidae/nixidae#1560, spike.

Two measurements, printed as tables and written to `<artifacts>/pod-throughput.json`:

  floor   a pod that runs `true`: from the create request to the container's start and to its end, one
          at a time and several at once. What any build pod pays before its build starts.
  inputs  one build through the build pod store for each input closure size, a few times each: from the
          request to the built output. The pod copies its inputs from central over SSH, so the slope
          against the closure's bytes is that copy's cost.

Times are the host's monotonic clock around kr8s and agent calls; a pod's state is polled every 0.1 s, so a
figure is good to about that. The node is one UML guest: a ranking between figures holds, not the figures.
"""

from __future__ import annotations

import json
import shlex
import statistics
import time
from collections.abc import Mapping
from typing import Any, cast

import anyio
from daemon_helpers import as_tester, store_path
from k8s_api import cluster
from kr8s.asyncio.objects import Pod
from vivarium_runner import Machine, Machines

NAMESPACE = "default"
REPEATS = 3
AT_ONCE = 5
POLL = 0.1
POD_TIMEOUT = 300
PARTS_LINES = {"pynixd build-one: seconds:": "pod", "pynixd: build pod seconds:": "central"}
"""`POD_SECONDS` and `CENTRAL_SECONDS` of `pynixd/build_job.py`, which the phase cannot import."""

type Pending = dict[str, Any]
"""A pod spec, as the API server takes it."""


def floor_pod(image: str, busybox: str, name: str) -> Pending:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": NAMESPACE, "labels": {"pynixd.nixidae.org/bench": "floor"}},
        "spec": {
            "restartPolicy": "Never",
            "terminationGracePeriodSeconds": 0,
            "containers": [
                {
                    "name": "true",
                    "image": image,
                    "imagePullPolicy": "Never",
                    "command": [f"{busybox}/bin/true"],
                    "volumeMounts": [{"name": "nix-store", "mountPath": "/nix/store", "readOnly": True}],
                }
            ],
            "volumes": [{"name": "nix-store", "hostPath": {"path": "/nix/store", "type": "Directory"}}],
        },
    }


async def timed_pod(pod: Pod) -> dict[str, float]:
    """Seconds from the create request to the container's start, and to its end."""
    started = time.monotonic()
    await pod.create()
    seen: dict[str, float] = {}
    with anyio.fail_after(POD_TIMEOUT):
        while "ended" not in seen:
            await pod.refresh()
            # kr8s types `raw` as a Box, whose methods carry no types.
            raw = cast(Pending, pod.raw)
            for status in raw.get("status", {}).get("containerStatuses", []):
                state = status.get("state", {})
                if ("running" in state or "terminated" in state) and "started" not in seen:
                    seen["started"] = time.monotonic() - started
                if "terminated" in state:
                    seen.setdefault("started", time.monotonic() - started)
                    seen["ended"] = time.monotonic() - started
            await anyio.sleep(POLL)
    await pod.delete()
    return seen


async def floor(vms: Machines, image: str, busybox: str) -> dict[str, list[dict[str, float]]]:
    api = await cluster(vms)
    alone = [await timed_pod(Pod(floor_pod(image, busybox, f"floor-alone-{i}"), api=api)) for i in range(REPEATS)]
    together: list[dict[str, float]] = []

    async def one(i: int) -> None:
        together.append(await timed_pod(Pod(floor_pod(image, busybox, f"floor-together-{i}"), api=api)))

    async with anyio.create_task_group() as tg:
        for i in range(AT_ONCE):
            tg.start_soon(one, i)
    return {"alone": alone, f"{AT_ONCE} at once": together}


def expression(settings: Mapping[str, str], nonce: str, closure: str) -> str:
    """A build that only copies its input's name to its output: the input closure is the variable."""
    return (
        f'let bb = builtins.storePath "{settings["busybox"]}"; in derivation {{'
        ' name = "pod-throughput";'
        f' system = "{settings["system"]}";'
        ' builder = "${bb}/bin/sh";'
        ' args = [ "-c" "echo $input > $out" ];'
        f' input = builtins.storePath "{closure}";'
        f' nonce = "{nonce}";'
        f' requiredSystemFeatures = [ "{settings["podBuildFeature"]}" ];'
        " }"
    )


async def closure_bytes(cp: Machine, path: str) -> int:
    out = (await cp.succeed(f"nix-store -q --requisites {path} | xargs du -sb --apparent-size | cut -f1")).stdout
    return sum(int(n) for n in out.split())


def parts(log: str) -> dict[str, float]:
    """The seconds of each part of a pod's fixed cost, from the pod's and central's lines in the build log."""
    found: dict[str, float] = {}
    for line in log.splitlines():
        for prefix, side in PARTS_LINES.items():
            if prefix in line:
                words = line.partition(prefix)[2].split()
                found |= {f"{side} {name}": float(value) for name, _, value in (w.partition("=") for w in words)}
    return found


async def inputs(cp: Machine, settings: Mapping[str, str], run: str) -> dict[str, dict[str, Any]]:
    sizes = {"busybox": settings["busybox"], "python3": settings["probePython"], "pynixd": settings["benchLarge"]}
    found: dict[str, dict[str, Any]] = {}
    for label, closure in sizes.items():
        seconds = []
        split: list[dict[str, float]] = []
        for i in range(REPEATS):
            text = expression(settings, f"{run}-{label}-{i}", closure)
            drv = store_path(await as_tester(cp, f"nix-instantiate --expr {shlex.quote(text)}"))
            started = time.monotonic()
            log = await as_tester(cp, f"nix-store --realise {drv} 2>&1", timeout=900)
            seconds.append(time.monotonic() - started)
            split.append(parts(log))
        found[label] = {"bytes": await closure_bytes(cp, closure), "seconds": seconds, "parts": split}
    return found


def summary(values: list[float]) -> str:
    return f"median {statistics.median(values):.2f} s, min {min(values):.2f}, max {max(values):.2f}"


async def test(vms: Machines) -> None:
    cp, settings = vms.cp, vms.settings
    await cp.wait_for_unit("pynixd.service")
    config = json.loads((await cp.succeed("cat /etc/pynixd/pynixd.json")).stdout)
    image = config["provisioning"]["kubernetes"]["templates"]["buildpod"]["pod"]["spec"]["containers"][0]["image"]
    run = (await cp.succeed("cat /proc/sys/kernel/random/uuid")).stdout.strip()
    load = (await cp.succeed("cat /proc/loadavg")).stdout.strip()

    floors = await floor(vms, image, settings["busybox"])
    for label, pods in floors.items():
        print(f"[test] floor, {label}: started {summary([p['started'] for p in pods])}")
        print(f"[test] floor, {label}: ended {summary([p['ended'] for p in pods])}")

    builds = await inputs(cp, settings, run)
    for label, row in builds.items():
        print(
            f"[test] a build pod with the {label} closure ({row['bytes'] / 2**20:.0f} MiB): {summary(row['seconds'])}"
        )
        names = sorted({name for split in row["parts"] for name in split})
        for name in names:
            print(f"[test]   {label}, {name}: {summary([split[name] for split in row['parts'] if name in split])}")
    # The pod and central each say what their parts took (nixidae/nixidae#1560: the fixed cost, measured).
    if not all({"pod build", "central total"} <= split.keys() for row in builds.values() for split in row["parts"]):
        raise AssertionError(f"a build log lacks the pod's or central's seconds: {builds}")

    report = {"load before": load, "floor": floors, "inputs": builds}
    await (anyio.Path(vms.artifacts) / "pod-throughput.json").write_text(json.dumps(report, indent=2) + "\n")
