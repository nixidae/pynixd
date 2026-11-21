"""The one-node cluster the Kubernetes provider starts builder pods in. nixidae/nixidae#1560."""

from __future__ import annotations

from vivarium_k8s import bring_up
from vivarium_runner import Machines


async def test(vms: Machines) -> None:
    await bring_up(vms)
