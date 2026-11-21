"""A builder pod that fails at start is named in the journal. nixidae/nixidae#1560.

The broken pool's pod exits 1. The provider reads the failure from the pod's
status. The provisioner ends the row `gone` and warns with that failure:
without the warning, the journal shows nothing while the pool launches the
pod again and again.
"""

from __future__ import annotations

import json
import shlex

from cloud_helpers import feature_drv, logged, rows, until
from vivarium_runner import Machines


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    drv, _ = await feature_drv(cp, settings, "broken", "echo never > $out", settings["brokenFeature"])
    realise = shlex.quote(f"nix-store --realise {drv} > /dev/null 2>&1 &")
    await cp.succeed(f"su - tester -c {realise}")

    line = json.loads(await logged(cp, "provisioning_machine_failed", 180))
    assert line["pool"] == "broken", line
    assert line["error"] == "container pynixd Error with exit 1", line
    print(f"[test] the journal names the broken pod's failure: {line['instance']}: {line['error']}")

    await cp.succeed("pkill -x nix-store")

    async def settled() -> bool:
        return all(row["state"] == "ended" for row in await rows(cp))

    await until(settled, 120)
    ended = [row for row in await rows(cp) if row["id"].startswith("broken-")]
    assert ended, "the broken pool launched no pod"
    assert all(row["endReason"] == "gone" and row["registeredAtMs"] is None for row in ended), ended
    print(f"[test] {len(ended)} broken pods ended gone, none registered")
