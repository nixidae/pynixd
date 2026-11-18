"""A bootstrap that fails powers its builder off, and the instance ends: nixidae/nixidae#1420.

The `broken` template's installer URL names no file. Its user data fails
to unpack, powers the guest off, and the cloud ends the instance, as EC2
does with `InstanceInitiatedShutdownBehavior=terminate`. The machine never
registers, and the provisioner records it as gone, long before its
launch timeout.
"""

from __future__ import annotations

import json
import shlex

from cloud_helpers import DB, TABLE, call, feature_drv, until
from vivarium_runner import Machines

WAIT_SECONDS = 60


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    settings = vms.settings

    drv, out = await feature_drv(controller, settings, "ec2-broken", "echo never > $out", settings["broken"])
    # No store ever takes it, so the client gives up.
    rc = (
        await controller.execute(f"su - tester -c {shlex.quote(f'timeout {WAIT_SECONDS} nix-store --realise {drv}')}")
    ).returncode
    assert rc == 124, f"a build no store can take ended with {rc}"
    await controller.fail(f"test -e {out}")

    _, listed = await call(controller, "GET", "/v1/instances?tag:pynixd-pool=broken")
    first, *later = listed["instances"]
    await cloud.wait_ended(first["id"], timeout=120)
    # The bootstrap may power off before vivarium's agent is up: vivarium then records a failed boot.
    _, view = await call(controller, "GET", f"/v1/instances/{first['id']}")
    assert view["state"] in ("terminated", "failed"), view
    console = (vms.artifacts.parent / "console" / f"{first['id']}.log").read_text()
    said = [line for line in console.splitlines() if "pynixd-bootstrap:" in line]
    assert said and "powering off" in said[0], said
    print(f"[test] {first['id']} failed its bootstrap and powered itself off: {said[0].strip()}")

    rows = json.loads(
        (
            await controller.succeed(
                f"sqlite3 -json {DB} \"SELECT providerId, state, storeId, endReason FROM {TABLE} WHERE pool = 'broken'\""
            )
        ).stdout
    )
    [row] = [one for one in rows if one["providerId"] == first["id"]]
    assert (row["state"], row["storeId"], row["endReason"]) == ("ended", None, "gone"), row
    print(f"[test] the provisioner ended it as gone, never registered; {len(later)} relaunch(es) followed: {rows}")

    async def none_live() -> bool:
        _, now = await call(controller, "GET", "/v1/instances?tag:pynixd-pool=broken")
        return all(one["state"] in ("terminated", "failed") for one in now["instances"])

    await until(none_live, 120)
    print("[test] with no build waiting, no broken builder is left")
