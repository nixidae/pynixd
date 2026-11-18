"""A builder that dies mid-build, with no notice, loses the build to a replacement. nixidae/nixidae#1198.

The host ends the machine outright, as a crashed or reclaimed machine
ends: no notice, no drain. The reverse connection drops, the store goes,
the build goes back to the queue, and the provisioner launches a machine
for it.
"""

from __future__ import annotations

import json
import time

from cloud_helpers import exists, feature_drv, pool_instances, rows, until
from vivarium_runner import Machines

BUILD_SECONDS = 60


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    drv, out = await feature_drv(
        controller, vms.settings, "provisioned-crashed", f"${{bb}}/bin/sleep {BUILD_SECONDS}; echo crashed > $out"
    )
    await controller.succeed(
        "systemd-run --quiet --unit=crash --uid=tester -p StandardOutput=file:/tmp/crash.out"
        " -p StandardError=append:/tmp/crash.out"
        f" /bin/sh -c '/run/current-system/sw/bin/nix-store --realise {drv}; echo $? > /tmp/crash.rc'"
    )

    async def building() -> bool:
        state = json.loads((await controller.succeed("pynixd state")).stdout)
        return any(
            row["state"] == "building" and (row["store"] or "").startswith("provisioned-") for row in state["builds"]
        )

    await until(building, 300)
    [first] = [one for one in await pool_instances(controller) if one["state"] == "running"]
    crashed = time.monotonic()
    await cloud.terminate(first["id"])
    print(f"[test] {first['id']} ended mid-build, with no notice")

    await until(lambda: exists(controller, "/tmp/crash.rc"), 600)
    rc = (await controller.succeed("cat /tmp/crash.rc")).stdout.strip()
    assert rc == "0", (await controller.succeed("tail -n 20 /tmp/crash.out")).stdout
    assert (await controller.succeed(f"cat {out}")).stdout.strip() == "crashed"
    print(f"[test] {out} built {time.monotonic() - crashed:.0f} s after the crash")

    *_, lost, replacement = await rows(controller)
    assert lost["endReason"] in ("gone", "disconnected"), lost
    assert replacement["storeId"] is not None, replacement
    print(f"[test] {lost['id']} ended as {lost['endReason']}; {replacement['id']} built it")
