"""A build pod may add only its derivation's outputs and content-addressed paths to central. nixidae/nixidae#1560.

The negative control of `kubernetes_pod_build.py`. The `rogue` template's
pod (`kubernetes_rogue_pod.py`) copies two paths of its own to central with
its granted key, then claims the build succeeded. Central takes the
content-addressed path and refuses the input-addressed one, and fails the
build because the output never reached its store.
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
    assert "did not reach the store" in stderr, stderr

    content_addressed, forged = dict.fromkeys(re.findall(r"rogue: added (/nix/store/\S+)", stderr))
    assert f"may not add {forged}" in stderr, stderr
    for path in (forged, out):
        assert (await cp.execute(f"nix-store --check-validity {path}")).returncode != 0, path
    print(f"[test] central refused {forged}, and holds no {out}")
    # The positive case beside the control: a path its content address names vouches for itself.
    await cp.succeed(f"nix-store --check-validity {content_addressed}")
    print(f"[test] central took {content_addressed}, which is content-addressed")
