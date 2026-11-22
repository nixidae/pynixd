"""A build pod gives way to an ordinary pod that needs its room. nixidae/nixidae#1560, spike.

Build pods run in a PriorityClass below the default (`kubernetes_cluster.py`). The phase starts a
build that runs for a while, then asks for an ordinary pod that fits on the node only without the
build pod. The scheduler preempts the build pod, and central queues the build again without counting
a failure. The next build pod waits for room, and once the ordinary pod is gone the build finishes.
"""

from __future__ import annotations

import json
from typing import Any

import anyio
import kr8s
from cloud_helpers import feature_drv
from daemon_helpers import attempt
from k8s_api import BUILD_PODS, cluster
from kr8s.asyncio.objects import APIObject, Node, Pod
from vivarium_runner import Machine, Machines

SCRIPT = "${bb}/bin/sleep 45; echo built > $out"
ORDINARY = "ordinary"
PREEMPTED = "PreemptionByScheduler"


def millicores(quantity: str) -> int:
    return int(quantity[:-1]) if quantity.endswith("m") else round(float(quantity) * 1000)


async def requested(api: kr8s.asyncio.Api, node: str) -> int:
    """The CPU every pod still on *node* requests, in millicores: what the scheduler counts."""
    total = 0
    pods = api.get("pods", namespace=kr8s.ALL, field_selector=f"spec.nodeName={node}")
    async for pod in pods:
        assert isinstance(pod, APIObject), pod
        if pod.raw["status"].get("phase") in {"Succeeded", "Failed"}:
            continue
        for container in pod.raw["spec"]["containers"]:
            total += millicores(container.get("resources", {}).get("requests", {}).get("cpu", "0"))
    return total


async def running_build_pod(api: kr8s.asyncio.Api) -> APIObject:
    # A watch with no resourceVersion starts with every pod there is.
    async for event, pod in api.watch("pods", label_selector=BUILD_PODS):
        raw: dict[str, Any] = pod.raw  # Any: a Kubernetes pod
        if event != "DELETED" and raw.get("status", {}).get("phase") == "Running":
            return pod
    raise AssertionError("the watch of the build pods ended")


async def ordinary_pod(cp: Machine, api: kr8s.asyncio.Api, cpu: int, busybox: str) -> Pod:
    """A pod of the default priority that requests *cpu* millicores, on the build pods' image and store."""
    config = json.loads((await cp.succeed("cat /etc/pynixd/pynixd.json")).stdout)
    template = config["provisioning"]["kubernetes"]["templates"]["buildpod"]["pod"]["spec"]
    store: dict[str, Any] = next(v for v in template["volumes"] if v["name"] == "nix-store")  # Any: a pod volume
    container = template["containers"][0]
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": ORDINARY},
        "spec": {
            "terminationGracePeriodSeconds": 1,
            "containers": [
                {
                    "name": ORDINARY,
                    "image": container["image"],
                    "imagePullPolicy": "Never",
                    "command": [f"{busybox}/bin/sleep", "3600"],
                    "resources": {"requests": {"cpu": f"{cpu}m"}},
                    "volumeMounts": [{"name": "nix-store", "mountPath": "/nix/store", "readOnly": True}],
                }
            ],
            "volumes": [store],
        },
    }
    pod = Pod(manifest, api=api)
    await pod.create()
    return pod


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    api = await cluster(vms)
    await cp.wait_for_unit("pynixd.service")
    drv, out = await feature_drv(cp, settings, "preempted", SCRIPT, settings["podBuildFeature"])
    realised: list[tuple[int, str]] = []

    async def realise() -> None:
        realised.append(await attempt(cp, f"nix-store --realise {drv}", timeout=900))

    async with anyio.create_task_group() as tg:
        tg.start_soon(realise)
        with anyio.fail_after(300):
            first = await running_build_pod(api)
        [node] = [n async for n in api.get(Node)]
        assert isinstance(node, APIObject), node
        allocatable = millicores(node.raw["status"]["allocatable"]["cpu"])
        used = await requested(api, node.name)
        # It fits once the build pod's request is free again, and not before.
        room = allocatable - used + millicores(settings["buildPodCpu"])
        print(f"[test] the node has {allocatable}m, pods request {used}m: the ordinary pod requests {room}m")
        ordinary = await ordinary_pod(cp, api, room, settings["busybox"])
        with anyio.fail_after(120):
            await first.wait("delete")
            await ordinary.wait("condition=Ready")
        print(f"[test] {first.name} gave way to the ordinary pod")
        await ordinary.delete()

    [(code, stderr)] = realised
    print(f"[test] nix-store --realise exited {code}:\n{stderr}")
    assert code == 0, stderr
    assert PREEMPTED in stderr and "queued again" in stderr, stderr
    assert (await cp.succeed(f"cat {out}")).stdout.strip() == "built"
    print("[test] the build ran again once there was room, and finished")
