"""A build pod over a farm builds from the farm's paths and copies none of them in. nixidae/nixidae#1560, spike.

The farm template's pod mounts a store Nix filled as its whole `/nix`, read-only, in both containers. Its
pynixd reads it as the lower store of its own store in `/build` (`lower_store`). The same build on the plain
build pod is the control: that pod copies the input from central.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from cloud_helpers import feature_drv
from daemon_helpers import attempt

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vivarium_runner import Machine, Machines

SCRIPT = "${bb}/bin/echo built > $out"
FARM_DB = "file:/var/lib/pynixd-farm/nix/var/nix/db/db.sqlite?immutable=1"


def field(log: str, event: str, name: str) -> int:
    """The integer *name* of the relayed log's line for *event*, in either of structlog's renderings."""
    line = next((line for line in log.splitlines() if event in line), None)
    assert line is not None, f"no {event} line in:\n{log}"
    found = re.search(rf"\b{name}\W+(\d+)", line)
    assert found is not None, line
    return int(found.group(1))


async def built(cp: Machine, settings: Mapping[str, str], name: str, feature: str) -> str:
    """The relayed log of a build of a derivation with busybox as its input, on the pod for *feature*."""
    drv, _out = await feature_drv(cp, settings, name, SCRIPT, feature)
    code, stderr = await attempt(cp, f"nix-store --realise {drv}", timeout=300)
    print(f"[test] {feature}: nix-store --realise exited {code}:\n{stderr}")
    assert code == 0, stderr
    return stderr


async def test(vms: Machines) -> None:
    cp = vms.cp
    settings = vms.settings
    await cp.wait_for_unit("pynixd.service")
    await cp.wait_for_unit("pynixd-farm.service")
    rows = int((await cp.succeed(f"sqlite3 '{FARM_DB}' 'select count(*) from ValidPaths'")).stdout)
    entries = int((await cp.succeed("ls /var/lib/pynixd-farm/nix/store | grep -vcx .links")).stdout)
    print(f"[test] the farm's database holds {rows} paths, its store {entries}")
    # An immutable reader sees no frame a `-wal` holds: every path must be in the database file.
    assert rows == entries, (rows, entries)

    control = await built(cp, settings, "podfarm-control", settings["podBuildFeature"])
    farm = await built(cp, settings, "podfarm", settings["podFarmFeature"])
    counts = {
        arm: (field(log, "build_one_inputs", "paths"), field(log, "build_one_inputs", "bytes"))
        for arm, log in (("control", control), ("farm", farm))
    }
    print(f"[test] input paths and bytes copied in from central: {counts}")
    assert counts["control"][0] >= 1 and counts["control"][1] > 0, counts
    assert counts["farm"][0] == counts["control"][0], counts
    assert counts["farm"][1] == 0, counts
    assert field(farm, "lower_store_loaded", "paths") == rows, farm
