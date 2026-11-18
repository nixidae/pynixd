"""A build holds a builder up past its guard's limit, and a builder with no controller powers itself off.
nixidae/nixidae#1422.

The `guard` pool leases its builder 20 s of idle, so the guard's limit is
80 s. The build sleeps longer than that limit and the guard's one-minute
timer together: the guard sees it at least once, and must not end the
machine. Then the controller's pynixd freezes, so only the guard can end the
builder: a clean stop would end it, as a shutdown ends every machine. It
powers off once quiet past its limit, and the cloud ends the instance. The
controller, thawed, ends the row: `disconnected` or `gone`, by whether the
dropped connection or the provider's listing reaches it first.
"""

from __future__ import annotations

import json
import shlex
import time
from typing import TYPE_CHECKING

from cloud_helpers import call, feature_drv, rows, until
from daemon_helpers import as_tester
from vivarium_runner import Machines

if TYPE_CHECKING:
    from pathlib import Path

IDLE = 20.0
"""The `guard` pool's `idle_seconds` and `idle_max_seconds`."""
LEASE_MARGIN = 60.0
"""`idle_guard.LEASE_MARGIN`."""
TIMER = 60.0
"""The guard's timer period."""
BUILD_SECONDS = int(IDLE + LEASE_MARGIN + TIMER + 10)
SEEN = "/run/pynixd-idle/build-seen"
"""`idle_guard.SEEN`: the guard touches it when it finds a build running."""


def guard_lines(journal: Path) -> list[str]:
    """What the guard's unit said, from the journal the builder streamed to the host: its timer's output
    reaches no console."""
    entries = (json.loads(line) for line in journal.read_text().splitlines())
    return [str(entry["MESSAGE"]) for entry in entries if entry.get("_SYSTEMD_UNIT") == "pynixd-idle.service"]


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    settings = vms.settings
    limit = IDLE + LEASE_MARGIN

    drv, out = await feature_drv(
        controller,
        settings,
        "ec2-guard-check",
        f"${{bb}}/bin/sleep {BUILD_SECONDS}; echo held > $out",
        settings["guard"],
    )
    started = time.monotonic()
    await as_tester(controller, f"nix-store --realise {shlex.quote(drv)}", timeout=600)
    built = time.monotonic()
    await controller.succeed("systemctl kill --signal=SIGSTOP pynixd.service")
    assert (await controller.succeed(f"cat {out}")).stdout.strip() == "held"

    _, listed = await call(controller, "GET", "/v1/instances?tag:pynixd-pool=guard")
    [instance] = listed["instances"]
    builder = await cloud.wait_running(instance["id"])
    seen = float((await builder.succeed(f"stat -c %Y {SEEN}")).stdout.strip())
    lease = float((await builder.succeed("stat -c %Y /run/pynixd-idle/lease.json")).stdout.strip())
    assert seen - lease > limit, (
        f"the guard saw the build only {seen - lease:.0f} s after the lease, under {limit:.0f} s"
    )
    print(
        f"[test] the guard found the build {seen - lease:.0f} s after the lease, past its {limit:.0f} s, and held off"
    )
    print(f"[test] the build took {built - started:.0f} s; the controller is frozen")

    await cloud.wait_ended(instance["id"], timeout=limit + TIMER + 60)
    quiet = time.monotonic() - built
    assert quiet >= limit, f"{instance['id']} ended {quiet:.0f} s after the build, under its {limit:.0f} s"
    _, view = await call(controller, "GET", f"/v1/instances/{instance['id']}")
    assert view["state"] == "terminated", view
    said = guard_lines(vms.artifacts.parent / "artifacts" / instance["id"] / "journal.jsonl")
    assert said and f"of {limit:.0f} s, powering off" in said[-1], said
    print(f"[test] {instance['id']} powered itself off {quiet:.0f} s after the build: {said[-1].strip()}")

    await controller.succeed("systemctl kill --signal=SIGCONT pynixd.service")

    mine = instance["tags"]["pynixd-instance"]

    async def lost() -> bool:
        return any(row["id"] == mine and row["endReason"] in {"gone", "disconnected"} for row in await rows(controller))

    await until(lost, 60)
    print("[test] the thawed controller found the machine lost")
