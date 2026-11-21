"""A build pod may add only its derivation's outputs to central. nixidae/nixidae#1560, spike.

The negative control of `kubernetes_pod_build.py`. The `rogue` template's
pod (`kubernetes_rogue_pod.py`) copies a path of its own to central with its
granted key, then claims the build succeeded. Central refuses the copy, and
fails the build because the output never reached its store.
"""

from __future__ import annotations

import re

from cloud_helpers import feature_drv
from daemon_helpers import attempt
from vivarium_runner import Machines

SCRIPT = "echo never > $out"


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    await cp.wait_for_unit("pynixd.service")
    drv, out = await feature_drv(cp, settings, "rogue", SCRIPT, settings["rogueFeature"])
    code, stderr = await attempt(cp, f"nix-store --realise {drv}", timeout=300)
    print(f"[test] nix-store --realise exited {code}:\n{stderr}")
    assert code != 0, stderr
    assert "may not add" in stderr, stderr
    assert "did not reach the store" in stderr, stderr

    added = re.search(r"rogue: added (/nix/store/\S+)", stderr)
    assert added is not None, stderr
    for path in (added.group(1), out):
        assert (await cp.execute(f"nix-store --check-validity {path}")).returncode != 0, path
    print(f"[test] central refused {added.group(1)}, and holds no {out}")
