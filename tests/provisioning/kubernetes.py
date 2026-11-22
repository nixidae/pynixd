"""A Kubernetes pod as a builder: the real provider against a kubeadm node. nixidae/nixidae#1560.

A build that needs `k8s-pod` makes the controller's `KubernetesProvider`
create a pod and the Secret that holds its join ticket. The pod's pynixd
registers by the ticket and builds. Once the pool finds it idle for 15 s, the
controller deletes the pod, and the API server deletes the Secret it owns.
"""

from __future__ import annotations

import json
import time

from cloud_helpers import feature_drv, rows, until
from daemon_helpers import as_tester
from k8s_api import cluster, managed
from kr8s.asyncio.objects import Pod
from vivarium_runner import Machines

TAGS = "pynixd.nixidae.org/tags"
NOTE = "built in a pod"


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    api = await cluster(vms)
    await cp.wait_for_unit("pynixd.service")

    drv, out = await feature_drv(cp, settings, "k8s-pod", f'echo "{NOTE}" > $out')
    started = time.monotonic()
    await as_tester(cp, f"nix-store --realise {drv}", timeout=600)
    built = (await cp.succeed(f"cat {out}")).stdout.strip()
    assert built == NOTE, built
    print(f"[test] {out} built in {time.monotonic() - started:.0f} s")

    [pod] = await managed(api, "pods")
    assert isinstance(pod, Pod), pod
    name = pod.name
    tags = json.loads(pod.annotations[TAGS])
    assert tags["pynixd-pool"] == "pods", tags
    assert tags["pynixd-instance"].startswith("pods-"), tags
    assert pod.status.phase == "Running", pod.status
    [secret] = await managed(api, "secrets")
    assert secret.name == name, secret.metadata
    [owner] = secret.metadata.ownerReferences
    assert (owner["kind"], owner["name"], owner["uid"]) == ("Pod", name, pod.metadata.uid), owner
    ticket = await pod.exec(["cat", "/run/pynixd-join/ticket"])
    token = json.loads(ticket.stdout)["token"]
    assert token not in json.dumps(pod.raw), "the join token is in the pod's spec"
    await pod.exec(["nix-store", "--store", "/build", "--check-validity", out])
    print(f"[test] pod {name} registered by the ticket in Secret {name}, which it owns, and built {out}")

    [row] = await rows(cp)
    assert row["state"] == "running", row
    assert row["registeredAtMs"] is not None, row
    print(f"[test] the pod booted in {(row['registeredAtMs'] - row['launchedAtMs']) / 1000:.1f} s")

    idle_from = time.monotonic()

    async def gone() -> bool:
        return not await managed(api, "pods") and not await managed(api, "secrets")

    await until(gone, 120)
    [row] = await rows(cp)
    assert (row["state"], row["endReason"]) == ("ended", "idle"), row
    print(f"[test] the controller deleted the idle pod {time.monotonic() - idle_from:.0f} s after the build: {row}")

    # pynixd starts before kubeadm: its first calls meet no API server, then one that refuses them.
    restarts = (await cp.succeed("systemctl show --property NRestarts --value pynixd.service")).stdout.strip()
    assert restarts == "0", f"pynixd restarted {restarts} times while the cluster came up"
    print("[test] pynixd outlived the cluster's bring-up with no restart")
