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
        "value": -2,
        "preemptionPolicy": "Never",
        "description": "pynixd's build pods: the first to go when another pod needs the room.",
    }
    # A nested build's pod, one higher: on a full node it evicts a shallower build pod, and no ordinary pod,
    # which all sit above it (nixidae/nixidae#1576).
    nested_priority = {
        "metadata": {"name": f"{BUILD_PRIORITY}-nested"},
        "value": -1,
        "preemptionPolicy": "PreemptLowerPriority",
        "description": "pynixd's nested build pods: ahead of the build pods they are nested in.",
    }
    api = await cluster(vms)
    await PriorityClass(build_priority, api=api).create()
    await PriorityClass(nested_priority, api=api).create()
