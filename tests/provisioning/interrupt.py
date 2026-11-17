"""A builder under an interruption notice drains, and its build finishes on a replacement. nixidae/nixidae#1198.

The build sleeps past the notice, so the drain cannot wait it out: the
build goes back to the queue before the machine ends, and the replacement
the notice launched builds it from the start.
"""

from __future__ import annotations

import json
import time

from cloud_helpers import feature_drv, pool_instances, rows, until
from vivarium_runner import Machine, Machines

BUILD_SECONDS = 60
NOTICE_SECONDS = 40.0


async def _exists(vm: Machine, path: str) -> bool:
    rc, _ = await vm.execute(f"test -e {path}")
    return rc == 0


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    drv, out = await feature_drv(
        controller, vms.settings, "provisioned-interrupted", f"${{bb}}/bin/sleep {BUILD_SECONDS}; echo long > $out"
    )
    await controller.succeed(
        "systemd-run --quiet --unit=long --uid=tester -p StandardOutput=file:/tmp/long.out"
        f" -p StandardError=append:/tmp/long.out /bin/sh -c '/run/current-system/sw/bin/nix-store --realise {drv}; echo $? > /tmp/long.rc'"
    )

    async def one_running() -> bool:
        return any(one["state"] == "running" for one in await pool_instances(controller))

    await until(one_running, 300)
    [first] = [one for one in await pool_instances(controller) if one["state"] == "running"]

    async def building() -> bool:
        state = json.loads(await controller.succeed("pynixd state"))
        return any(
            row["state"] == "building" and (row["store"] or "").startswith("provisioned-") for row in state["builds"]
        )

    await until(building, 300)
    noticed = time.monotonic()
    await cloud.interrupt(first["id"], NOTICE_SECONDS)
    print(f"[test] {first['id']} builds; it has a {NOTICE_SECONDS:g} s notice")

    await until(lambda: _exists(controller, "/tmp/long.rc"), 600)
    rc = (await controller.succeed("cat /tmp/long.rc")).strip()
    assert rc == "0", await controller.succeed("tail -n 20 /tmp/long.out")
    assert (await controller.succeed(f"cat {out}")).strip() == "long"
    print(f"[test] {out} built {time.monotonic() - noticed:.0f} s after the notice")

    states = {one["id"]: one["state"] for one in await pool_instances(controller)}
    assert states[first["id"]] == "terminated", states
    *_, interrupted, replacement = await rows(controller)
    assert interrupted["endReason"] in ("gone", "disconnected"), interrupted
    assert replacement["storeId"] is not None, replacement
    print(f"[test] {interrupted['id']} ended as {interrupted['endReason']}; {replacement['id']} built it")
