"""A central that starts deletes the build pods an earlier process left, and only its own. nixidae/nixidae#1560.

A pod outlives the central that made it, and its key died with that
process, so it can never report. `BuildPodStore.arm` deletes the store's
pods at start. A pod with another store's label belongs to another central
and stays.
"""

from __future__ import annotations

import copy
import json
from typing import Any

import anyio
from k8s_api import NAMESPACE, cluster
from kr8s.asyncio.objects import Pod
from vivarium_runner import Machines

STORE = "buildpods"
LEFT = "pynixd-build-left"
OTHERS = "pynixd-build-others"


# A pod manifest as JSON: the API server's schema, not Python's.
def _stray(template: dict[str, Any], name: str, store: str) -> dict[str, Any]:
    """The store's template with the labels of a build pod, and no job: it waits for its Secret for ever."""
    pod = copy.deepcopy(template)
    pod["apiVersion"], pod["kind"] = "v1", "Pod"
    pod["metadata"] = {
        "name": name,
        "namespace": NAMESPACE,
        "labels": {
            "app.kubernetes.io/managed-by": "pynixd",
            "pynixd.nixidae.org/build": "true",
            "pynixd.nixidae.org/build-store": store,
        },
    }
    pod["spec"]["restartPolicy"] = "Never"
    pod["spec"].setdefault("volumes", []).append({"name": "pynixd-job", "secret": {"secretName": name}})
    return pod


async def test(vms: Machines) -> None:
    cp = vms.cp
    api = await cluster(vms)
    await cp.wait_for_unit("pynixd.service")
    config = json.loads((await cp.succeed("cat /etc/pynixd/pynixd.json")).stdout)
    template = config["provisioning"]["kubernetes"]["templates"]["buildpod"]["pod"]
    left = Pod(_stray(template, LEFT, STORE), api=api)
    others = Pod(_stray(template, OTHERS, "another-central"), api=api)
    await left.create()
    await others.create()

    await cp.succeed("systemctl restart pynixd.service")
    await cp.wait_for_unit("pynixd.service")
    with anyio.fail_after(60):
        await left.wait("delete")
    print(f"[test] central deleted {LEFT}, which its store's label marks, at start")
    assert await others.exists(), f"{OTHERS} has another store's label, and central deleted it"
    print(f"[test] {OTHERS}, of another store, stays")
    await others.delete()
