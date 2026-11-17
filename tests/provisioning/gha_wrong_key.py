"""A ticket sealed under another key fails the job, and the provisioner ends it: nixidae/nixidae#1473.

The fake's secret changes, so the job's key no longer opens what pynixd
sealed. The install step stops before anything registers, the job fails, and
the provisioner finds the run completed and records the machine as gone.
"""

from __future__ import annotations

import base64
import json
import shlex

from cloud_helpers import DB, TABLE, feature_drv, until
from gha_helpers import RUN_NAME_PREFIX, completed, pool_runs, runs, set_ticket_key
from vivarium_runner import Machines

WAIT_SECONDS = 60


async def test(vms: Machines) -> None:
    controller = vms.controller
    settings = vms.settings
    await set_ticket_key(controller, settings, base64.b64encode(bytes(range(100, 132))).decode())
    before = {run["id"] for run in await runs(controller, settings)}

    drv, out = await feature_drv(controller, settings, "gha-wrong-key", "echo never > $out")
    rc, _ = await controller.execute(
        f"su - tester -c {shlex.quote(f'timeout {WAIT_SECONDS} nix-store --realise {drv}')}"
    )
    assert rc == 124, f"a build no store can take ended with {rc}"
    await controller.fail(f"test -e {out}")

    first, *later = [run for run in await pool_runs(controller, settings, "big") if run["id"] not in before]
    ended = await completed(controller, settings, first["id"], timeout=120)
    assert ended["conclusion"] == "failure", ended
    console = (vms.artifacts.parent / "console" / f"{first['instance']}.log").read_text()
    said = [line for line in console.splitlines() if "pynixd-install:" in line and "does not open" in line]
    assert said, "the install step did not refuse the ticket"
    print(f"[test] run {first['id']} refused the ticket and failed: {said[0].strip()}")

    instance = json.loads(first["display_title"].removeprefix(RUN_NAME_PREFIX))["pynixd-instance"]
    query = f"SELECT id, state, storeId, endReason FROM {TABLE} WHERE id = '{instance}'"
    row: dict[str, object] = {}

    async def row_ended() -> bool:
        nonlocal row
        [row] = json.loads(await controller.succeed(f'sqlite3 -json {DB} "{query}"'))
        return bool(row["state"] == "ended")

    await until(row_ended, 60)
    assert (row["state"], row["storeId"], row["endReason"]) == ("ended", None, "gone"), row
    print(f"[test] the provisioner ended it as gone, never registered; {len(later)} relaunch(es) followed")

    async def none_live() -> bool:
        return all(run["status"] == "completed" for run in await pool_runs(controller, settings, "big"))

    await until(none_live, 180)
    print("[test] with no build waiting, no run is left")
