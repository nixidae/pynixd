"""Central's own pod owns each build pod, so the build pods go when it does. nixidae/nixidae#1560.

Central runs on the node in this test, not in a pod. The phase makes a pod
that stands in for central's, and gives pynixd its name and uid as the
downward API would. A build pod then names that pod as its owner, and
deleting the stand-in makes the garbage collector delete the build pod.
"""

from __future__ import annotations

import json
from typing import Any

import anyio
import kr8s.asyncio
from cloud_helpers import feature_drv
from daemon_helpers import attempt
from k8s_api import BUILD_PODS, NAMESPACE, cluster
from kr8s.asyncio.objects import APIObject, Pod
from vivarium_runner import Machine, MachineError, Machines

STORE = "buildpods"
STORE_LABEL = "pynixd.nixidae.org/build-store"
STAND_IN = "central-stand-in"
DROP_IN = "/run/systemd/system/pynixd.service.d/owner-pod.conf"
SCRIPT = "${bb}/bin/sleep 600; echo built > $out"


async def _stand_in(cp: Machine, busybox: str, api: kr8s.asyncio.Api) -> Pod:
    """A pod on the build pods' image and store that sleeps: central's pod, for the garbage collector."""
    config = json.loads((await cp.succeed("cat /etc/pynixd/pynixd.json")).stdout)
    template = config["provisioning"]["kubernetes"]["templates"]["buildpod"]["pod"]["spec"]
    store: dict[str, Any] = next(v for v in template["volumes"] if v["name"] == "nix-store")  # Any: a pod volume
    pod = Pod(
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": STAND_IN, "namespace": NAMESPACE},
            "spec": {
                "terminationGracePeriodSeconds": 1,
                "containers": [
                    {
                        "name": STAND_IN,
                        "image": template["containers"][0]["image"],
                        "imagePullPolicy": "Never",
                        "command": [f"{busybox}/bin/sleep", "3600"],
                        "volumeMounts": [{"name": "nix-store", "mountPath": "/nix/store", "readOnly": True}],
                    }
                ],
                "volumes": [store],
            },
        },
        api=api,
    )
    await pod.create()
    return pod


async def _restart(cp: Machine, environment: str | None) -> None:
    if environment is None:
        await cp.succeed(f"rm -f {DROP_IN}")
    else:
        await cp.succeed(f"mkdir -p {DROP_IN.rsplit('/', 1)[0]} && printf '[Service]\\n{environment}\\n' > {DROP_IN}")
    await cp.succeed("systemctl daemon-reload && systemctl restart pynixd.service")
    await cp.wait_for_unit("pynixd.service")


async def _build_pod(api: kr8s.asyncio.Api) -> APIObject:
    """The store's running build pod. Not one that goes: pod-left's stray of another store may still."""
    async for event, pod in api.watch("pods", label_selector=f"{BUILD_PODS},{STORE_LABEL}={STORE}"):
        raw: dict[str, Any] = pod.raw  # Any: a Kubernetes pod
        going = event == "DELETED" or "deletionTimestamp" in raw["metadata"]
        if not going and raw.get("status", {}).get("phase") == "Running":
            return pod
    raise MachineError("the watch of the build pods ended")


async def test(vms: Machines) -> None:
    cp, settings = vms.cp, vms.settings
    api = await cluster(vms)
    await cp.wait_for_unit("pynixd.service")
    stand_in = await _stand_in(cp, settings["busybox"], api)
    uid = str(stand_in.raw["metadata"]["uid"])
    await _restart(
        cp,
        f"Environment=PYNIXD_POD_NAME={STAND_IN} PYNIXD_POD_UID={uid} PYNIXD_POD_NAMESPACE={NAMESPACE}",
    )
    drv, _out = await feature_drv(cp, settings, "owned", SCRIPT, settings["podBuildFeature"])
    ended: list[tuple[int, str]] = []

    async def realise() -> None:
        ended.append(await attempt(cp, f"nix-store --realise {drv}", timeout=300))

    async with anyio.create_task_group() as tg:
        tg.start_soon(realise)
        with anyio.fail_after(300):
            pod = await _build_pod(api)
        owners: list[dict[str, str]] = pod.raw["metadata"].get("ownerReferences") or []
        if {"apiVersion": "v1", "kind": "Pod", "name": STAND_IN, "uid": uid} not in owners:
            raise MachineError(f"{pod.name} names the owners {owners}, not {STAND_IN}")
        print(f"[test] {pod.name} names {STAND_IN} as its owner")
        await stand_in.delete()
        with anyio.fail_after(120):
            await Pod(pod.raw, api=api).wait("delete")
        print(f"[test] the garbage collector deleted {pod.name} once {STAND_IN} was gone")
        # The restart ends the build, which has no owner to make a pod under any more.
        await _restart(cp, None)

    [(code, stderr)] = ended
    print(f"[test] the build's client exited {code} at the restart:\n{stderr}")
