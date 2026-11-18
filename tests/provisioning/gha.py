"""A GitHub Actions job as a builder: the real provider and the real workflow's steps. nixidae/nixidae#1473.

A build that needs `gha-job` makes the controller's `GhaProvider` dispatch
the builder workflow. The fake boots a runner guest with no Nix, which runs
the workflow's steps: it installs pynixd from the tarball, opens the sealed
ticket and registers. At registration the controller tells the builder its
idle limit, the boot time within the pool's 15 to 60 s. The build runs there;
once the pool finds the builder idle that long, the controller cancels the
run. The job's guard ends it a margin later, so the cancel comes first.
"""

from __future__ import annotations

import json
import time

from cloud_helpers import rows, stdenv_drv, until
from daemon_helpers import as_tester
from gha_helpers import RUN_NAME_PREFIX, completed, pool_runs
from vivarium_runner import Machines

INSTALLED_NIX = "/nix/var/nix/profiles/default/bin"
NOTE = "stdenv on a GitHub Actions job"
LEASE = "/run/pynixd-idle/lease.json"
IDLE_MIN, IDLE_MAX = 15.0, 60.0
"""The `big` pool's `idle_seconds` and `idle_max_seconds`."""


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    settings = vms.settings
    await controller.wait_for_unit("fake-gha.service")
    await controller.wait_for_unit("pynixd.service")

    drv, out = await stdenv_drv(controller, settings, NOTE)
    started = time.monotonic()
    await as_tester(controller, f"nix-store --realise {drv}", timeout=600)
    built = (await controller.succeed(f"cat {out}")).stdout.strip()
    print(f"[test] {out} built in {time.monotonic() - started:.0f} s: {built}")
    assert built == NOTE, built

    [run] = await pool_runs(controller, settings, "big")
    tags = json.loads(run["display_title"].removeprefix(RUN_NAME_PREFIX))
    assert tags["pynixd-instance"].startswith("big-"), run
    assert run["inputs"]["ticket"].startswith("pynixd-ticket-v1:"), run["inputs"]
    assert json.loads(run["inputs"]["runs_on"]) == ["gha-runner"], run["inputs"]
    builder = await cloud.wait_running(run["instance"])
    await builder.succeed("systemctl is-active pynixd.service")
    await builder.succeed(f"{INSTALLED_NIX}/nix-store --check-validity {out}")
    assert await builder.count_processes("nix-daemon") == 0, "a nix-daemon runs on the builder"
    print(f"[test] run {run['id']} installed pynixd on a runner with no Nix and built it")

    assert (await builder.succeed("stat -c %a /etc/pynixd/join-ticket.json")).stdout.strip() == "600"
    token = json.loads((await builder.succeed("cat /etc/pynixd/join-ticket.json")).stdout)["token"]
    assert token not in json.dumps(run), "the join token is in the dispatch, which a run's page shows"
    rc = (await builder.execute(f"journalctl --no-pager | grep -F -q {token}")).returncode
    assert rc != 0, "the join token reached the runner's journal, which the job's log carries"
    print("[test] the ticket travelled sealed, opened 0600, and its token is in no input and no log")

    lease = json.loads((await builder.succeed(f"cat {LEASE}")).stdout)
    [registered] = await rows(controller)
    boot = (registered["registeredAtMs"] - registered["launchedAtMs"]) / 1000
    assert abs(lease["boot_seconds"] - boot) < 0.01, (lease, registered)
    assert lease["idle_seconds"] == min(max(boot, IDLE_MIN), IDLE_MAX), lease
    print(f"[test] the job booted in {boot:.1f} s, and the controller gave it {lease['idle_seconds']:.1f} s to idle")

    idle_from = time.monotonic()
    ended = await completed(controller, settings, run["id"], timeout=180)
    assert ended["conclusion"] == "cancelled", ended

    async def row_ended() -> bool:
        return all(row["state"] == "ended" for row in await rows(controller))

    await until(row_ended, 30)
    [row] = await rows(controller)
    assert (row["state"], row["endReason"]) == ("ended", "idle"), row
    print(
        f"[test] the controller cancelled run {run['id']} {time.monotonic() - idle_from:.0f} s after the build: {row}"
    )
