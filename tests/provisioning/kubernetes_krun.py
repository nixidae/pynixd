"""The `krun` kind: a pod that builds in pynixd's build VM. nixidae/nixidae#1560.

The pod gets the node's `/dev/kvm`, and its pynixd builds with `build_vm`.
The build writes the release of the kernel it ran under. That is libkrunfw's
kernel, not the node's: the build ran in the VM, not in the pod's namespaces.
"""

from __future__ import annotations

import time

from cloud_helpers import feature_drv, rows, until
from daemon_helpers import as_tester
from k8s_api import cluster, managed
from vivarium_runner import Machines


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    await cp.wait_for_unit("pynixd.service")
    node = (await cp.succeed("uname -r")).stdout.strip()

    drv, out = await feature_drv(
        cp, settings, "krun", "read r < /proc/sys/kernel/osrelease; echo $r > $out", settings["krunFeature"]
    )
    started = time.monotonic()
    await as_tester(cp, f"nix-store --realise {drv}", timeout=600)
    built = (await cp.succeed(f"cat {out}")).stdout.strip()
    print(f"[test] {out} built in {time.monotonic() - started:.0f} s, under kernel {built}; the node runs {node}")
    assert built, "the build wrote no kernel release"
    assert built != node, f"the build ran under the node's kernel {node}, not in the build VM"

    api = await cluster(vms)

    async def ended() -> bool:
        return not await managed(api, "pods") and all(row["state"] == "ended" for row in await rows(cp))

    await until(ended, 120)
    [row] = await rows(cp)
    assert row["id"].startswith("vms-"), row
    assert row["endReason"] == "used", row
    print(f"[test] the single-use VM pod ended after its build: {row['id']}")
