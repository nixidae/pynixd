"""Central makes a pod for one build; the pod dials central, builds, and ends. nixidae/nixidae#1560, spike.

The pod's pynixd is a client of central only: it registers nowhere and
listens on no port. Central grants the pod's key the one derivation, follows
the pod's log, and reads the result from the pod's termination message. The
build runs in the pod's sandbox container, so pid 1 there is the sibling
runner.
"""

from __future__ import annotations

import json
from typing import Any

import anyio
import kr8s
from cloud_helpers import feature_drv, rows
from daemon_helpers import as_tester
from k8s_api import cluster, level_namespace
from kr8s.asyncio.objects import Pod
from vivarium_runner import Machine, Machines

BUILD_PODS = "pynixd.nixidae.org/build=true"
RESTRICTED = "pynixd-restricted"
BASELINE = "pynixd-baseline"
SCRIPT = (
    "${bb}/bin/tr '\\0' ' ' < /proc/1/cmdline > $out; echo >> $out; ${bb}/bin/cat /proc/self/uid_map >> $out;"
    " ${bb}/bin/grep Seccomp_filters /proc/self/status >> $out"
)
BUILDER_UID = 1000


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    api = await cluster(vms)
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

    with anyio.fail_after(60):
        async for pod in api.get(Pod, label_selector=BUILD_PODS):
            assert isinstance(pod, Pod), pod
            await pod.wait("delete")
    # The pod never registered: the provisioner holds no row for it.
    assert not [row for row in await rows(cp) if row["id"].startswith("buildpod")], await rows(cp)
    print("[test] the build pod ended, and central deleted it")
    await admitted_by_level(cp, api)


async def admitted_by_level(cp: Machine, api: kr8s.asyncio.Api) -> None:
    """Which Pod Security level admits each template.

    `buildpod` passes `restricted`; the privileged `nested` one passes neither level. `buildpod-userns`
    sets `procMount: Unmasked`, which `restricted` refuses always and `baseline` admits for a pod with
    `hostUsers: false` (Kubernetes 1.35 and later, `check_procMount_baseline.go`).

    The levels refuse `hostPath`, and the test's pods take the node's store and passwd from it: the
    check gives them `emptyDir` instead. A real pod has its store in its image, or in nixkube's `csi`
    volume, which the levels allow.
    """
    config = json.loads((await cp.succeed("cat /etc/pynixd/pynixd.json")).stdout)
    templates = config["provisioning"]["kubernetes"]["templates"]
    for level, admitted in (
        (RESTRICTED, {"buildpod": True, "buildpod-userns": False, "nested": False}),
        (BASELINE, {"buildpod-userns": True, "nested": False}),
    ):
        await level_namespace(api, level.removeprefix("pynixd-"))
        for template, wanted in admitted.items():
            pod = Pod(level_copy(templates[template]["pod"], template, level), api=api)
            try:
                await pod.create(dry_run="server")
            except kr8s.ServerError as error:
                print(f"[test] the {template} template in a {level} namespace: {error}")
                assert not wanted, error
                assert "violates PodSecurity" in str(error), error
            else:
                print(f"[test] the {template} template in a {level} namespace: admitted")
                assert wanted, (template, level)


def level_copy(template: dict[str, Any], name: str, namespace: str) -> dict[str, Any]:  # Any: a Kubernetes pod
    spec = template["spec"]
    volumes = [{"name": v["name"], "emptyDir": {}} if "hostPath" in v else v for v in spec.get("volumes", [])]
    return {
        **template,
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {**spec, "volumes": volumes},
    }
