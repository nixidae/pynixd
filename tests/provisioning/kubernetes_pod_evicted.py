"""A nested build pod preempts its own outer build pod on a full node. nixidae/nixidae#1576.

The store's pods each request `evictMemory`, and the node has room for one. The outer
build's recursive Nix asks for an inner build on the same store. The inner pod's class
preempts lower ones, so the scheduler takes the outer pod. Central queues the outer
build again (`PodDisrupted`), the inner build goes on with no client left, and the outer
build's next pod finds the inner output at central.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field
from typing import Any

import anyio
import kr8s
from daemon_helpers import as_tester
from k8s_api import cluster
from kr8s.asyncio.objects import APIObject, Node, Pod
from kubernetes_pod_nested import CLASSES, nested_expression
from vivarium_runner import MachineError, Machines

STORE = "buildpods-evict"
PREEMPTED = "PreemptionByScheduler"
_UNITS = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "k": 1000, "M": 1000**2, "G": 1000**3}


def memory_bytes(quantity: str) -> int:
    for suffix, factor in _UNITS.items():
        if quantity.endswith(suffix):
            return int(quantity.removesuffix(suffix)) * factor
    return int(quantity)


async def room(api: kr8s.asyncio.Api) -> int:
    """The memory the one node can still give requests, in bytes: allocatable less what live pods request."""
    [node] = [n async for n in api.get(Node)]
    if not isinstance(node, APIObject):
        raise MachineError(f"listing nodes answered {type(node).__name__}")
    free = memory_bytes(node.raw["status"]["allocatable"]["memory"])
    async for pod in api.get("pods", namespace=kr8s.ALL, field_selector=f"spec.nodeName={node.name}"):
        if not isinstance(pod, APIObject):
            raise MachineError(f"listing pods answered {type(pod).__name__}")
        if pod.raw["status"].get("phase") in {"Succeeded", "Failed"}:
            continue
        for container in pod.raw["spec"]["containers"]:
            free -= memory_bytes(container.get("resources", {}).get("requests", {}).get("memory", "0"))
    return free


@dataclass
class Seen:
    """Each pod of the store: its PriorityClass, and why Kubernetes took it, if it did."""

    classes: dict[str, str] = field(default_factory=dict)
    disrupted: dict[str, str] = field(default_factory=dict)


async def _watch(api: kr8s.asyncio.Api, seen: Seen) -> None:
    async for _event, pod in api.watch("pods", label_selector=f"pynixd.nixidae.org/build-store={STORE}"):
        if not isinstance(pod, Pod):
            raise MachineError(f"watching pods answered {type(pod).__name__}")
        raw: dict[str, Any] = pod.raw  # Any: a Kubernetes pod
        seen.classes[pod.name] = str(raw["spec"].get("priorityClassName"))
        for condition in raw.get("status", {}).get("conditions", []):
            if condition.get("type") == "DisruptionTarget" and condition.get("status") == "True":
                seen.disrupted[pod.name] = str(condition.get("reason"))


async def test(vms: Machines) -> None:
    cp, settings = vms.cp, vms.settings
    api = await cluster(vms)
    await cp.wait_for_unit("pynixd.service")
    pod_bytes = memory_bytes(settings["evictMemory"])
    free = await room(api)
    # The phase proves nothing unless one pod fits and two do not.
    if not pod_bytes <= free < 2 * pod_bytes:
        raise MachineError(f"[cp] {free} bytes free for requests; a pod requests {pod_bytes}: not room for one alone")
    print(f"[test] {free} bytes free for requests, {pod_bytes} per build pod: room for one")

    seen = Seen()
    async with anyio.create_task_group() as watching:
        watching.start_soon(_watch, api, seen)
        expression = nested_expression(settings, "podEvictFeature")
        out = (await as_tester(cp, f"nix-build --no-out-link --expr {shlex.quote(expression)}", timeout=600)).strip()
        watching.cancel_scope.cancel()
    said = (await cp.succeed(f"cat {out}")).stdout.strip()
    if said != "inner":
        raise MachineError(f"[cp] the outer build's output holds {said!r}")
    print(f"[test] the store's pods and their PriorityClass: {seen.classes}")
    print(f"[test] the pods Kubernetes took: {seen.disrupted}")
    if sorted(set(seen.classes.values())) != CLASSES:
        raise MachineError(f"the store's pods took {sorted(set(seen.classes.values()))}, not {CLASSES}")
    preempted = [name for name, reason in seen.disrupted.items() if reason == PREEMPTED]
    if [seen.classes[name] for name in preempted] != [CLASSES[0]]:
        raise MachineError(f"expected one outer pod preempted, saw {seen.disrupted} of {seen.classes}")
    outer = [name for name, kind in seen.classes.items() if kind == CLASSES[0]]
    if len(outer) != 2:
        raise MachineError(f"expected the outer build in two pods, the preempted one and the next: {outer}")
    print(f"[test] the nested pod preempted {preempted[0]}, and the outer build passed in {out}")
