"""The one-node cluster the Kubernetes provider starts builder pods in. nixidae/nixidae#1560."""

from __future__ import annotations

from k8s_api import BUILD_PRIORITY, cluster
from kr8s.asyncio.objects import new_class
from vivarium_k8s import bring_up
from vivarium_runner import Machines

# kr8s guesses a plural by adding "s" to a kind it does not know.
PriorityClass = new_class("PriorityClass", version="scheduling.k8s.io/v1", namespaced=False, plural="priorityclasses")


async def test(vms: Machines) -> None:
    await bring_up(vms)
    # What nixkube's build-pod mode creates on a real cluster: a pod of any class above it takes a build
    # pod's place, and a build pod takes no one's.
    build_priority = {
        "metadata": {"name": BUILD_PRIORITY},
        "value": -1,
        "preemptionPolicy": "Never",
        "description": "pynixd's build pods: the first to go when another pod needs the room.",
    }
    await PriorityClass(build_priority, api=await cluster(vms)).create()
