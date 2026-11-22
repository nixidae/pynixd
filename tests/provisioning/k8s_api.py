"""The test cluster's API, from the host. nixidae/nixidae#1560.

The `cluster` phase writes `<out>/access/kubeconfig`, aimed at the host's
forward of the API server (`vivarium_k8s.host_kubeconfig`).
"""

from __future__ import annotations

import kr8s.asyncio
from kr8s.asyncio.objects import APIObject, Namespace
from vivarium_runner import Machines

NAMESPACE = "default"
MANAGED = "app.kubernetes.io/managed-by=pynixd"
"""The label the provider sets on each pod and Secret it makes."""
BUILD_PODS = "pynixd.nixidae.org/build=true"
"""The label of a pod central makes for one build."""
BUILD_PRIORITY = "pynixd-build"
"""The PriorityClass of the build pod templates (`provisioning-kubernetes/default.nix`)."""


async def cluster(vms: Machines) -> kr8s.asyncio.Api:
    return await kr8s.asyncio.api(kubeconfig=str(vms.access / "kubeconfig"), namespace=NAMESPACE)


async def level_namespace(api: kr8s.asyncio.Api, level: str) -> str:
    """`pynixd-<level>`, a namespace that enforces that Pod Security level. More than one phase asks."""
    name = f"pynixd-{level}"
    labels = {"pod-security.kubernetes.io/enforce": level}
    namespace = Namespace({"metadata": {"name": name, "labels": labels}}, api=api)
    if not await namespace.exists():
        await namespace.create()
    return name


async def managed(api: kr8s.asyncio.Api, kind: str) -> list[APIObject]:
    """The objects of *kind* that pynixd's provider made."""
    found: list[APIObject] = []
    async for item in api.get(kind, label_selector=MANAGED):
        assert isinstance(item, APIObject), item
        found.append(item)
    return found
