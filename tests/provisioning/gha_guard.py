"""A job its controller cannot end ends itself at the idle limit the controller sent: nixidae/nixidae#1473.

The fake refuses every cancel. The `selfend` pool lets a builder idle 15 to
20 s, so the controller ends the row by then and its cancel fails. The
template's guard argument is an hour; the job ends because its guard takes
the lease's 20 s plus the margin instead. The job succeeds.
"""

from __future__ import annotations

import json
import shlex
import time

from cloud_helpers import feature_drv, rows, until
from daemon_helpers import as_tester
from gha_helpers import RUN_NAME_PREFIX, completed, pool_runs, refuse_cancel
from vivarium_runner import Machines

IDLE_MIN, IDLE_MAX = 15.0, 20.0
"""The `selfend` pool's `idle_seconds` and `idle_max_seconds`."""
LEASE_MARGIN = 60.0
"""`idle_guard.LEASE_MARGIN`."""


async def test(vms: Machines) -> None:
    controller = vms.controller
    settings = vms.settings
    await refuse_cancel(controller, settings, refuse=True)
    drv, out = await feature_drv(controller, settings, "gha-selfend-check", "echo self > $out", settings["selfend"])
    await as_tester(controller, f"nix-store --realise {shlex.quote(drv)}", timeout=600)
    assert (await controller.succeed(f"cat {out}")).stdout.strip() == "self"

    [run] = await pool_runs(controller, settings, "selfend")
    built = time.monotonic()
    ended = await completed(controller, settings, run["id"], timeout=240)
    assert ended["conclusion"] == "success", ended
    console = (vms.artifacts.parent / "console" / f"{run['instance']}.log").read_text()
    instance = json.loads(run["display_title"].removeprefix(RUN_NAME_PREFIX))["pynixd-instance"]
    [mine] = [row for row in await rows(controller) if row["id"] == instance]
    boot = (mine["registeredAtMs"] - mine["launchedAtMs"]) / 1000
    limit = min(max(boot, IDLE_MIN), IDLE_MAX) + LEASE_MARGIN
    said = [line for line in console.splitlines() if "pynixd-idle-guard:" in line]
    assert said and f"of {limit:.0f} s, ending" in said[0], f"the guard took no lease of {limit:.0f} s: {said}"
    print(f"[test] run {run['id']} ended by itself {time.monotonic() - built:.0f} s after the build: {said[0].strip()}")

    async def row_ended() -> bool:
        return any(row["id"] == instance and row["state"] == "ended" for row in await rows(controller))

    await until(row_ended, 60)
    [row] = [row for row in await rows(controller) if row["id"] == instance]
    assert row["endReason"] == "idle", row
    print(f"[test] the provisioner ended the row as {row['endReason']}, and the job ended itself: {row}")
    await refuse_cancel(controller, settings, refuse=False)
