"""The EC2 builder's boot path: the real provider's user data on a guest with no Nix. nixidae/nixidae#1420.

A build that needs `ec2-spot` makes the controller's `Ec2Provider` call
RunInstances; the fake endpoint boots the `ec2-builder` template with the
rendered bootstrap as its user data. The bootstrap unpacks the installer
tarball, installs pynixd with no daemon and registers. Then the build runs
there through stdenv, and the builder ends once idle.
"""

from __future__ import annotations

import json
import time

from cloud_helpers import pool_instances, rows, stdenv_drv, until
from daemon_helpers import as_tester
from vivarium_runner import Machines

INSTALLED_NIX = "/nix/var/nix/profiles/default/bin"
NOTE = "stdenv on an EC2 builder"


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    settings = vms.settings
    await controller.wait_for_unit("fake-ec2.service")
    await controller.wait_for_unit("pynixd.service")

    drv, out = await stdenv_drv(controller, settings, NOTE)
    started = time.monotonic()
    await as_tester(controller, f"nix-store --realise {drv}", timeout=600)
    built = (await controller.succeed(f"cat {out}")).strip()
    print(f"[test] {out} built in {time.monotonic() - started:.0f} s: {built}")
    assert built == NOTE, built

    [launched] = await pool_instances(controller)
    assert launched["template"] == "ec2-builder", launched
    assert launched["tags"].get("pynixd-cloud-builder") == "true", launched["tags"]
    builder = await cloud.wait_running(launched["id"])
    await builder.succeed("systemctl is-active pynixd.service")
    await builder.succeed(f"{INSTALLED_NIX}/nix-store --check-validity {out}")
    assert await builder.count_processes("nix-daemon") == 0, "a nix-daemon runs on the builder"
    print(f"[test] {launched['id']} booted with no Nix, installed pynixd from the tarball and built it")

    assert (await builder.succeed("stat -c %a /etc/pynixd/join-ticket.json")).strip() == "600"
    token = json.loads(await builder.succeed("cat /etc/pynixd/join-ticket.json"))["token"]
    rc, _ = await builder.execute(f"journalctl --no-pager | grep -F -q {token}")
    assert rc != 0, "the join token reached the builder's journal, which the console carries"
    print("[test] the join ticket is 0600 and its token is in no log")

    lease = json.loads(await builder.succeed("cat /run/pynixd-idle/lease.json"))
    [registered] = await rows(controller)
    boot = (registered["registeredAtMs"] - registered["launchedAtMs"]) / 1000
    assert abs(lease["boot_seconds"] - boot) < 0.01, (lease, registered)
    # The `big` pool's `idle_seconds` and `idle_max_seconds`.
    assert lease["idle_seconds"] == min(max(boot, 15.0), 30.0), lease
    print(f"[test] the builder booted in {boot:.1f} s, and its guard holds a lease of {lease['idle_seconds']:.1f} s")

    idle_from = time.monotonic()
    await cloud.wait_ended(launched["id"], timeout=180)

    async def row_ended() -> bool:
        return all(row["state"] == "ended" for row in await rows(controller))

    await until(row_ended, 30)
    [row] = await rows(controller)
    assert (row["state"], row["endReason"]) == ("ended", "idle"), row
    print(f"[test] {launched['id']} ended {time.monotonic() - idle_from:.0f} s after the build: {row}")
