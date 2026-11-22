"""Central makes a pod for one build; the pod dials central, builds, and ends. nixidae/nixidae#1560, spike.

The pod's pynixd is a client of central only: it registers nowhere and
listens on no port. Central grants the pod's key the one derivation, follows
the pod's log, and reads the result from the pod's termination message. The
build runs in the pod's sandbox container, so pid 1 there is the sibling
runner.
"""

from __future__ import annotations

from cloud_helpers import feature_drv, rows, until
from daemon_helpers import as_tester
from vivarium_k8s import get_json
from vivarium_runner import Machines

BUILD_PODS = "pynixd.nixidae.org/build=true"
SCRIPT = (
    "${bb}/bin/tr '\\0' ' ' < /proc/1/cmdline > $out; echo >> $out; ${bb}/bin/cat /proc/self/uid_map >> $out;"
    " ${bb}/bin/grep Seccomp_filters /proc/self/status >> $out"
)
BUILDER_UID = 1000


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    await cp.wait_for_unit("pynixd.service")
    drv, out = await feature_drv(cp, settings, "podbuild", SCRIPT, settings["podBuildFeature"])
    await as_tester(cp, f"nix-store --realise {drv}", timeout=300)
    cmdline, uid_map, filters = (await cp.succeed(f"cat {out}")).stdout.strip().splitlines()
    print(f"[test] pid 1 in the build pod's sandbox: {cmdline}")
    assert "pynixd_store.builder.sibling run" in cmdline, cmdline
    # The second field is the uid outside the sandbox: the pod's, which has no root.
    print(f"[test] the build's uid map: {uid_map}")
    assert int(uid_map.split()[1]) == BUILDER_UID, uid_map
    # The helper's own filter, and the container's profile under it: Unconfined leaves one.
    print(f"[test] the build's {filters}")
    assert int(filters.split()[1]) >= 2, filters

    async def gone() -> bool:
        return not (await get_json(cp, f"get pods --selector {BUILD_PODS}"))["items"]

    await until(gone, 60)
    # The pod never registered: the provisioner holds no row for it.
    assert not [row for row in await rows(cp) if row["id"].startswith("buildpod")], await rows(cp)
    print("[test] the build pod ended, and central deleted it")
