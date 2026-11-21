"""A single-use pool gives each build a pod of its own; an ordinary pool reuses one. nixidae/nixidae#1560.

Two builds go to each pool at once, and each pool holds one pod at most. The
single-use pool's pod takes one build: the scheduler stops placing on it, and
the controller deletes it once that build ends, though the second build still
waits. A new pod takes the second. The ordinary pool, the control, builds both
on one pod and deletes it only when it has been idle.
"""

from __future__ import annotations

import anyio
from cloud_helpers import feature_drv, rows, until
from daemon_helpers import as_tester
from vivarium_runner import Machines

ROUNDS = 2


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    before = {row["id"] for row in await rows(cp)}
    drvs = [
        await feature_drv(cp, settings, f"{pool}-{n}", f'echo "{pool} {n}" > $out', feature)
        for pool, feature in (("once", settings["onceFeature"]), ("pods", settings["feature"]))
        for n in range(ROUNDS)
    ]

    async def realise(drv: str) -> None:
        await as_tester(cp, f"nix-store --realise {drv}", timeout=600)

    async with anyio.create_task_group() as group:
        for drv, _ in drvs:
            group.start_soon(realise, drv)
    for _, out in drvs:
        await cp.succeed(f"test -s {out}")
    print(f"[test] {len(drvs)} builds done, {ROUNDS} on each pool at once")

    async def settled() -> bool:
        return all(row["state"] == "ended" for row in await rows(cp) if row["id"] not in before)

    await until(settled, 120)
    new = [row for row in await rows(cp) if row["id"] not in before]
    once = [row for row in new if row["id"].startswith("once-")]
    pods = [row for row in new if row["id"].startswith("pods-")]
    assert len(once) == ROUNDS, f"the single-use pool ran {len(once)} pods for {ROUNDS} builds: {once}"
    assert all(row["endReason"] == "used" for row in once), once
    assert len(pods) == 1, f"the ordinary pool ran {len(pods)} pods for {ROUNDS} builds: {pods}"
    assert pods[0]["endReason"] == "idle", pods
    print(f"[test] single-use: {ROUNDS} pods, each ended after its build: {[row['id'] for row in once]}")
    print(f"[test] the control pool built both on one pod, ended idle: {pods[0]['id']}")
