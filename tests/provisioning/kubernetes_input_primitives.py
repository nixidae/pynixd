"""What each way to give a build its inputs costs, and where each one may run. nixidae/nixidae#1560, spike.

`input_primitives.py` measures; this phase runs it on the node and in both build pod templates, and prints a
table. Results go to `<artifacts>/input-primitives.json`. Measures; asserts nothing.

The node's store reaches a pod by a `hostPath` volume in this test. The Pod Security levels refuse that, so
the phase also asks the API server which level admits each way to mount a store: `hostPath`, or a CSI volume
(nixkube's). NRI binds inside the runtime, where no admission sees it.
"""

from __future__ import annotations

import json
import statistics
from collections.abc import Mapping
from typing import Any, cast

import anyio
import kr8s
from k8s_api import cluster
from kr8s.asyncio.objects import ConfigMap, Namespace, Pod
from vivarium_runner import Machine, Machines

REPEATS = 3
SCRATCH = "/var/lib/pynixd-bench"
NAMESPACE = "default"
POD_TIMEOUT = 600
POLL = 0.5
LEVELS = ("restricted", "baseline")

type Seconds = dict[str, dict[str, list[float]]]
"""Per closure, per way: seconds of each repeat."""
type Manifest = dict[str, Any]
"""A Kubernetes object, as the API server takes it."""


async def closure(cp: Machine, path: str) -> list[str]:
    return (await cp.succeed(f"nix-store --query --requisites {path}")).stdout.split()


async def closure_bytes(cp: Machine, path: str) -> int:
    sizes = (await cp.succeed(f"nix-store --query --requisites {path} | xargs du -sb --apparent-size | cut -f1")).stdout
    return sum(int(n) for n in sizes.split())


async def on_node(cp: Machine, command: str) -> Seconds | str:
    """The bench's result, or its error: one way failing says something about where it can run."""
    done = await cp.execute(command, timeout=1800)
    if done.returncode != 0:
        return done.stderr.strip().splitlines()[-1] if done.stderr.strip() else f"exit {done.returncode}"
    found: Seconds = json.loads(done.stdout)
    return found


def bench_pod(template: Manifest, name: str, command: list[str]) -> Manifest:
    """The template's sandbox container alone, running the bench: the store and the user namespace as the
    helper sees them."""
    spec = template["spec"]
    (sandbox,) = (c for c in spec["containers"] if c["name"] == "sandbox")
    volumes = [v for v in spec["volumes"] if v["name"] in ("nix-store", "build")]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            **{k: v for k, v in spec.items() if k not in ("containers", "volumes", "activeDeadlineSeconds")},
            "restartPolicy": "Never",
            "containers": [
                {
                    **sandbox,
                    "command": command,
                    "volumeMounts": [*sandbox["volumeMounts"], {"name": "bench", "mountPath": "/bench"}],
                }
            ],
            "volumes": [*volumes, {"name": "bench", "configMap": {"name": "input-primitives"}}],
        },
    }


async def in_pod(api: kr8s.asyncio.Api, manifest: Manifest) -> Seconds | str:
    pod = Pod(manifest, api=api)
    await pod.create()
    # A pod's end has no condition that `wait` can name: Ready is False before the start too.
    with anyio.fail_after(POD_TIMEOUT):
        while True:
            await pod.refresh()
            phase = cast(Manifest, pod.raw).get("status", {}).get("phase")
            if phase in ("Succeeded", "Failed"):
                break
            await anyio.sleep(POLL)
    text = "\n".join([line async for line in pod.logs()])
    await pod.delete()
    if phase == "Failed":
        return text.strip().splitlines()[-1] if text.strip() else "failed with no log"
    found: Seconds = json.loads(text)
    return found


def store_volume(kind: str) -> Manifest:
    if kind == "hostPath":
        return {"name": "nix-store", "hostPath": {"path": "/nix/store", "type": "Directory"}}
    return {"name": "nix-store", "csi": {"driver": "nixkube", "readOnly": True, "volumeAttributes": {}}}


async def admitted(api: kr8s.asyncio.Api, templates: Mapping[str, Manifest]) -> dict[str, str]:
    """For each template, Pod Security level and store volume: admitted, or the API server's reason."""
    found: dict[str, str] = {}
    for level in LEVELS:
        name = f"pynixd-{level}"
        labels = {"pod-security.kubernetes.io/enforce": level}
        try:
            await Namespace({"metadata": {"name": name, "labels": labels}}, api=api).create()
        except kr8s.ServerError as error:
            if "already exists" not in str(error):
                raise
        for template_name, template in templates.items():
            for kind in ("hostPath", "csi"):
                spec = template["spec"]
                volumes = [
                    store_volume(kind) if v["name"] == "nix-store" else {"name": v["name"], "emptyDir": {}}
                    for v in spec["volumes"]
                ]
                manifest = {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": template_name, "namespace": name},
                    "spec": {**spec, "volumes": volumes},
                }
                key = f"{template_name}, {level}, store by {kind}"
                try:
                    await Pod(manifest, api=api).create(dry_run="server")
                except kr8s.ServerError as error:
                    found[key] = str(error).split(": ", 1)[-1]
                else:
                    found[key] = "admitted"
    return found


def report_lines(where: str, found: Seconds | str, sizes: Mapping[str, tuple[int, int]]) -> list[str]:
    if isinstance(found, str):
        return [f"[test] {where}: failed: {found}"]
    lines = []
    for label, ways in found.items():
        paths, size = sizes[label]
        for way, seconds in ways.items():
            median = statistics.median(seconds)
            lines.append(
                f"[test] {where}, {label} ({paths} paths, {size / 2**20:.0f} MiB), {way}: median "
                f"{median * 1000:.1f} ms, {median / paths * 1e6:.1f} us per path, "
                f"all {[round(s * 1000, 1) for s in seconds]} ms"
            )
    return lines


async def test(vms: Machines) -> None:
    cp, settings = vms.cp, vms.settings
    api = await cluster(vms)
    await cp.wait_for_unit("pynixd.service")
    await cp.wait_for_unit("pynixd-podstore.service")
    labels = {"busybox": settings["busybox"], "python3": settings["probePython"], "pynixd": settings["benchLarge"]}
    closures = {label: await closure(cp, path) for label, path in labels.items()}
    sizes = {label: (len(paths), await closure_bytes(cp, labels[label])) for label, paths in closures.items()}
    await cp.succeed(f"install -d -m 0755 -o tester {SCRATCH}")
    load = (await cp.succeed("cat /proc/loadavg")).stdout.strip()

    bench = f"{settings['venvPython']} -I {settings['inputPrimitives']}"
    pairs = " ".join(f"{label}={path}" for label, path in labels.items())
    common = f"--repeats {REPEATS} --scratch {SCRATCH} {pairs}"
    found: dict[str, Seconds | str] = {
        "node, unprivileged": await on_node(
            cp, f"setpriv --reuid tester --regid users --clear-groups {bench} namespace {common}"
        ),
        "node, copy": await on_node(cp, f"{bench} copy {common}"),
        "node, hardlinks": await on_node(cp, f"{bench} hardlink --store-root /var/lib/pynixd-podstore {common}"),
        "node, composefs": await on_node(cp, f"{bench} composefs {common}"),
    }

    config = json.loads((await cp.succeed("cat /etc/pynixd/pynixd.json")).stdout)
    templates = {
        name: config["provisioning"]["kubernetes"]["templates"][name]["pod"] for name in ("buildpod", "buildpod-userns")
    }
    await ConfigMap(
        {
            "metadata": {"name": "input-primitives", "namespace": NAMESPACE},
            "data": {"closures.json": json.dumps(closures)},
        },
        api=api,
    ).create()
    in_sandbox = [
        settings["venvPython"],
        "-I",
        settings["inputPrimitives"],
        "namespace",
        "--closures",
        "/bench/closures.json",
        "--repeats",
        str(REPEATS),
        "--scratch",
        "/build",
    ]
    for name, template in templates.items():
        found[f"pod {name}"] = await in_pod(api, bench_pod(template, f"input-primitives-{name}", in_sandbox))

    for where, result in found.items():
        for line in report_lines(where, result, sizes):
            print(line)
    admission = await admitted(api, templates)
    for key, verdict in admission.items():
        print(f"[test] admission: {key}: {verdict}")

    report = {"load before": load, "closures": {k: {"paths": p, "bytes": b} for k, (p, b) in sizes.items()}}
    report |= {"seconds": found, "admission": admission}
    await (anyio.Path(vms.artifacts) / "input-primitives.json").write_text(json.dumps(report, indent=2) + "\n")
