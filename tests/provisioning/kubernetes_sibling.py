"""A pod of two containers builds: pynixd in one, the sandbox in the other. nixidae/nixidae#1560, spike.

The daemon container is not privileged and the sandbox container has no
capability. The builder writes the command line of its pid 1 and its own pid:
the sandbox keeps the sandbox container's pid namespace, so pid 1 is the
sibling runner, and the builder is not pid 1. Its network is its own: no
`eth0` of the pod.
"""

from __future__ import annotations

from cloud_helpers import feature_drv, rows, until
from daemon_helpers import as_tester
from k8s_api import cluster, managed
from vivarium_runner import Machines

SCRIPT = "${bb}/bin/tr '\\0' ' ' < /proc/1/cmdline > $out; echo >> $out; echo $$ >> $out; ${bb}/bin/cat /proc/net/dev >> $out"


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    await cp.wait_for_unit("pynixd.service")
    drv, out = await feature_drv(cp, settings, "sibling", SCRIPT, settings["siblingFeature"])
    await as_tester(cp, f"nix-store --realise {drv}", timeout=300)
    cmdline, pid, _header, _columns, *devices = (await cp.succeed(f"cat {out}")).stdout.splitlines()
    interfaces = [line.split(":")[0].strip() for line in devices]
    print(f"[test] pid 1 in the sandbox: {cmdline}; the builder's pid: {pid}; interfaces: {interfaces}")
    assert "pynixd_store.builder.sibling run" in cmdline, cmdline
    assert pid != "1", pid
    # A new network namespace also gets the kernel's fallback tunnels (`sit0`, `ip_vti0`) when their modules are
    # loaded, as they are on the node; the pod's `eth0` is what must be absent.
    assert "lo" in interfaces, interfaces
    assert not [name for name in interfaces if name.startswith("eth")], interfaces

    api = await cluster(vms)

    async def ended() -> bool:
        return not await managed(api, "pods") and all(row["state"] == "ended" for row in await rows(cp))

    await until(ended, 120)
    sibling = [row for row in await rows(cp) if row["id"].startswith("sibling-")]
    assert [row["endReason"] for row in sibling] == ["used"], sibling
    print(f"[test] the two-container pod ended after its build: {sibling[0]['id']}")
