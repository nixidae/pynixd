"""A nested build that needs the store its outer build pod holds the only slot of. nixidae/nixidae#1576.

The outer build's recursive Nix asks central for a derivation that only the
same one-slot store takes. Central frees the outer build's slot while it
waits, so the nested build gets a pod of its own, one PriorityClass higher.
Without that, both wait for ever, and the phase's deadline fails it.
"""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

import anyio
from cloud_helpers import nix_string
from daemon_helpers import as_tester
from k8s_api import cluster
from kr8s.asyncio.objects import Pod
from vivarium_runner import MachineError, Machines

if TYPE_CHECKING:
    from collections.abc import Mapping

    import kr8s.asyncio

STORE = "buildpods-nested"
CLASSES = ["pynixd-build", "pynixd-build-nested"]
"""The store's `priority_classes`: the outer pod's, then the nested pod's."""


def nested_expression(settings: Mapping[str, str], feature_setting: str) -> str:
    """An outer build whose recursive Nix builds an inner one; both need the feature *feature_setting* names."""
    feature = nix_string(settings[feature_setting])
    nested = (
        'let b = builtins.storePath "${bb}"; in derivation { name = "podnested-inner"; system = builtins.currentSystem;'
        f' builder = b + "/bin/sh"; args = [ "-c" "echo inner > $out" ]; requiredSystemFeatures = [ {feature} ]; }}'
    )
    script = (
        f"export HOME=$TMPDIR; echo {shlex.quote(nested)} > e.nix; d=$(${{nix}}/bin/nix-instantiate e.nix) || exit 3;"
        " o=$(${nix}/bin/nix-store --realise $d) || exit 4; ${bb}/bin/cat $o > $out"
    )
    return (
        f'let bb = builtins.storePath "{settings["busybox"]}"; nix = builtins.storePath "{settings["nix"]}"; in'
        ' derivation { name = "podnested-outer";'
        f' system = "{settings["system"]}"; builder = "${{bb}}/bin/sh"; args = [ "-c" {nix_string(script)} ];'
        f' requiredSystemFeatures = [ {feature} "recursive-nix" ]; }}'
    )


async def _classes(api: kr8s.asyncio.Api, seen: dict[str, str]) -> None:
    """Each pod of the store, and the PriorityClass it was made with."""
    async for _event, pod in api.watch("pods", label_selector=f"pynixd.nixidae.org/build-store={STORE}"):
        if not isinstance(pod, Pod):
            raise MachineError(f"watching pods answered {type(pod).__name__}")
        seen[pod.name] = str(pod.raw["spec"].get("priorityClassName"))


async def test(vms: Machines) -> None:
    cp, settings = vms.cp, vms.settings
    api = await cluster(vms)
    await cp.wait_for_unit("pynixd.service")
    seen: dict[str, str] = {}
    async with anyio.create_task_group() as watching:
        watching.start_soon(_classes, api, seen)
        expression = nested_expression(settings, "podNestedFeature")
        out = (await as_tester(cp, f"nix-build --no-out-link --expr {shlex.quote(expression)}", timeout=420)).strip()
        watching.cancel_scope.cancel()
    said = (await cp.succeed(f"cat {out}")).stdout.strip()
    if said != "inner":
        raise MachineError(f"[cp] the outer build's output holds {said!r}")
    print(f"[test] the outer build pod got its nested build's output through a slot it held: {out}")
    print(f"[test] the store's pods and their PriorityClass: {seen}")
    if sorted(set(seen.values())) != CLASSES:
        raise MachineError(f"the store's pods took {sorted(set(seen.values()))}, not {CLASSES}")
