"""A job whose controller never ends it ends itself once idle: nixidae/nixidae#1473.

The `selfend` pool waits ten minutes before it ends an idle builder, and its
template's guard waits 20 s. So the job's hold step returns first, the job
succeeds, and the runner goes; the provisioner then finds the run completed.
"""

from __future__ import annotations

import json
import shlex
import time

from cloud_helpers import feature_drv, rows, until
from daemon_helpers import as_tester
from gha_helpers import RUN_NAME_PREFIX, completed, pool_runs
from vivarium_runner import Machines


async def test(vms: Machines) -> None:
    controller = vms.controller
    settings = vms.settings
    drv, out = await feature_drv(controller, settings, "gha-selfend-check", "echo self > $out", settings["selfend"])
    await as_tester(controller, f"nix-store --realise {shlex.quote(drv)}", timeout=600)
    assert (await controller.succeed(f"cat {out}")).strip() == "self"

    [run] = await pool_runs(controller, settings, "selfend")
    built = time.monotonic()
    ended = await completed(controller, settings, run["id"], timeout=240)
    assert ended["conclusion"] == "success", ended
    console = (vms.artifacts.parent / "console" / f"{run['instance']}.log").read_text()
    said = [line for line in console.splitlines() if "pynixd-idle-guard:" in line]
    assert said and "ending" in said[0], said
    print(f"[test] run {run['id']} ended by itself {time.monotonic() - built:.0f} s after the build: {said[0].strip()}")

    instance = json.loads(run["display_title"].removeprefix(RUN_NAME_PREFIX))["pynixd-instance"]

    async def row_ended() -> bool:
        return any(row["id"] == instance and row["state"] == "ended" for row in await rows(controller))

    await until(row_ended, 60)
    [row] = [row for row in await rows(controller) if row["id"] == instance]
    assert row["endReason"] in ("gone", "disconnected"), row
    print(f"[test] the provisioner ended it as {row['endReason']}: {row}")
