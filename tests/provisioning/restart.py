"""A pynixd that dies with machines live ends them at its next start. nixidae/nixidae#1198.

The builder's token was for the process that died, so the builder cannot
register again: the new pynixd ends its row as `orphan` and terminates the
machine. A build asked after the restart launches a machine of its own.
"""

from __future__ import annotations

import json
import time

from cloud_helpers import exists, feature_drv, pool_instances, rows, until
from daemon_helpers import as_tester
from vivarium_runner import Machines

BUILD_SECONDS = 60


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    drv, _ = await feature_drv(
        controller, vms.settings, "provisioned-orphaned", f"${{bb}}/bin/sleep {BUILD_SECONDS}; echo orphaned > $out"
    )
    await controller.succeed(
        "systemd-run --quiet --unit=orphan --uid=tester -p StandardOutput=file:/tmp/orphan.out"
        " -p StandardError=append:/tmp/orphan.out"
        f" /bin/sh -c '/run/current-system/sw/bin/nix-store --realise {drv}; echo $? > /tmp/orphan.rc'"
    )

    async def building() -> bool:
        state = json.loads((await controller.succeed("pynixd state")).stdout)
        return any(
            row["state"] == "building" and (row["store"] or "").startswith("provisioned-") for row in state["builds"]
        )

    await until(building, 300)
    [live] = [one for one in await pool_instances(controller) if one["state"] == "running"]
    killed = time.monotonic()
    await controller.succeed("systemctl kill --signal=KILL pynixd.service")
    print(f"[test] pynixd killed while {live['id']} builds")

    await cloud.wait_ended(live["id"], timeout=180)
    print(f"[test] {live['id']} ended {time.monotonic() - killed:.0f} s after the kill")

    async def orphan_ended() -> bool:
        return any(row["endReason"] == "orphan" for row in await rows(controller))

    await until(orphan_ended, 60)
    await until(lambda: exists(controller, "/tmp/orphan.rc"), 120)
    print(
        f"[test] the client asked before the kill exited {(await controller.succeed('cat /tmp/orphan.rc')).stdout.strip()}"
    )

    drv, out = await feature_drv(controller, vms.settings, "provisioned-after-restart", "echo after > $out")
    await as_tester(controller, f"nix-store --realise {drv}", timeout=600)
    assert (await controller.succeed(f"cat {out}")).stdout.strip() == "after"
    newest = (await rows(controller))[-1]
    assert newest["storeId"] is not None, newest
    print(f"[test] a build after the restart launched {newest['id']} and built {out}")
