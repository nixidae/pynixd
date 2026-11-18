"""A trusted client that names a pool in `builders` gets that pool's builder. nixidae/nixidae#1198.

The build needs only `spare`, so the pool's mandatory `provisioned` keeps it off the pool unless the
client names the pool (the `provision` phase shows that). Two controls, each failed as Nix fails a
build no machine takes: an untrusted client's `builders` is dropped (`trust.py`), so naming the pool
launches nothing; and while the steered builder is up, another client's unsteered build stays off it.
"""

from __future__ import annotations

import shlex
import time

from cloud_helpers import feature_drv, pool_instances
from vivarium_runner import Machines

STEER = "--option builders pynixd-pool://big"


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    settings = vms.settings
    before = {one["id"] for one in await pool_instances(controller)}

    ignored, ignored_out = await feature_drv(
        controller, settings, "steer-untrusted", "echo u > $out", settings["spare"]
    )
    command = f"timeout 30 nix-store --realise {ignored} {STEER}"
    done = await controller.execute(f"su - tester -c {shlex.quote(command)} 2>&1")
    rc, out = done.returncode, done.stdout
    assert rc not in (0, 124), f"an untrusted client's steered build ended with {rc}"
    assert "missing system features" in out, out
    assert {one["id"] for one in await pool_instances(controller)} == before, "an untrusted `builders` launched one"
    print("[test] an untrusted client that names the pool launches nothing")

    # A misspelt pool fails the build with pynixd's reason. The steered build below proves pynixd lived on.
    typo, _ = await feature_drv(controller, settings, "steer-typo", "echo t > $out", settings["spare"])
    done = await controller.execute(
        f"timeout 30 nix-store --store daemon --realise {typo} --option builders pynixd-pool://bgi 2>&1"
    )
    rc, out = done.returncode, done.stdout
    assert rc not in (0, 124), f"a build that names no known pool ended with {rc}"
    assert "no provisioning pool 'bgi'" in out, out
    print("[test] a misspelt pool fails its build at once")

    steered, steered_out = await feature_drv(controller, settings, "steered", "echo steered > $out", settings["spare"])
    started = time.monotonic()
    await controller.succeed(f"nix-store --store daemon --realise {steered} {STEER}", timeout=600)
    built = (await controller.succeed(f"cat {steered_out}")).stdout.strip()
    assert built == "steered", built
    [launched] = [one for one in await pool_instances(controller) if one["id"] not in before]
    builder = await cloud.wait_running(launched["id"])
    await builder.succeed(f"nix-store --check-validity {steered_out}")
    print(f"[test] the steered build launched {launched['id']} and built there in {time.monotonic() - started:.0f} s")

    # Within the pool's 15 s idle: the builder is up, and an unsteered build that needs only `spare` stays off it.
    other, other_out = await feature_drv(controller, settings, "unsteered", "echo o > $out", settings["spare"])
    done = await controller.execute(f"su - tester -c {shlex.quote(f'timeout 30 nix-store --realise {other}')} 2>&1")
    rc, out = done.returncode, done.stdout
    assert rc not in (0, 124), f"an unsteered build ended with {rc}"
    assert "missing system features" in out, out
    rc = (await builder.execute(f"nix-store --check-validity {other_out}")).returncode
    assert rc != 0, "an unsteered build ran on the builder a steered one launched"
    rc = (await builder.execute(f"nix-store --check-validity {ignored_out}")).returncode
    assert rc != 0, "the untrusted client's build ran on the builder"
    print("[test] another client's unsteered build stayed off the steered builder")

    await cloud.wait_ended(launched["id"], timeout=180)
