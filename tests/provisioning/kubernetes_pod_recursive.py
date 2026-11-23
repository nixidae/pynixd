"""Recursive Nix in a build pod: what the shared socket carries, and what central accepts. nixidae/nixidae#1560.

The builder reaches `.nix-socket`, which the daemon container serves in the
build directory both containers share.

  - added: the builder adds a path, and the output refers to it. The pod
    pushes both; central takes the added path because its content address
    names it (`build_grant.py`). Without that, central refused it (measured).
  - nested: the builder instantiates a derivation and realises it. The pod
    sends the build to central, which builds it as for any untrusted client,
    and copies the output back (`RestrictedProxy.builds_at`).
  - floating: a floating content-addressed output, whose path the grant
    cannot name before the build; central takes the path by its address and
    the realisation by the output's name (`checked_result`).
"""

from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

from cloud_helpers import nix_string
from daemon_helpers import as_tester
from vivarium_runner import Machine, MachineError, Machines

if TYPE_CHECKING:
    from collections.abc import Mapping

ADDED = (
    "export HOME=$TMPDIR; echo recursive > f; p=$(${nix}/bin/nix-store --add f) || exit 3;"
    " ${bb}/bin/cat $p > $out; echo $p >> $out"
)
NESTED_EXPRESSION = (
    'let b = builtins.storePath "${bb}"; in derivation { name = "podnested"; system = builtins.currentSystem;'
    ' builder = b + "/bin/sh"; args = [ "-c" "echo nested > $out" ]; }'
)
NESTED = (
    f"export HOME=$TMPDIR; echo '{NESTED_EXPRESSION}' > e.nix; d=$(${{nix}}/bin/nix-instantiate e.nix) || exit 3;"
    " o=$(${nix}/bin/nix-store --realise $d) || exit 4; ${bb}/bin/cat $o > $out; echo $o >> $out"
)


FLOATING = "echo floating > $out; echo none >> $out"
FLOATING_ATTRS = ' __contentAddressed = true; outputHashMode = "recursive"; outputHashAlgo = "sha256";'


async def _realise(
    cp: Machine, settings: Mapping[str, str], feature: str, name: str, script: str, attrs: str = ""
) -> tuple[str, str, str]:
    """Build *script* as a recursive-nix derivation in a build pod: its output, first line, and the path it names."""
    expression = (
        f'let bb = builtins.storePath "{settings["busybox"]}"; nix = builtins.storePath "{settings["nix"]}"; in'
        " derivation {"
        f" name = {nix_string(name)};"
        f' system = "{settings["system"]}";'
        ' builder = "${bb}/bin/sh";'
        f' args = [ "-c" {nix_string(script)} ];'
        f' requiredSystemFeatures = [ {nix_string(feature)} "recursive-nix" ];'
        f"{attrs} }}"
    )
    out = (await as_tester(cp, f"nix-build --no-out-link --expr {shlex.quote(expression)}", timeout=600)).strip()
    said, named = (await cp.succeed(f"cat {out}")).stdout.split()
    return out, said, named


async def test(vms: Machines) -> None:
    cp, settings = vms.cp, vms.settings
    await cp.wait_for_unit("pynixd.service")
    # The farm pod has no Nix daemon, and reads its inputs from a lower store: the socket and the
    # restriction's binds run on pynixd's engine alone there (`kubernetes_pod_farm.py`).
    for arm, feature in (("pod", settings["podBuildFeature"]), ("farm", settings["podFarmFeature"])):
        await _cases(cp, settings, arm, feature)


async def _cases(cp: Machine, settings: Mapping[str, str], arm: str, feature: str) -> None:
    # Each arm adds its own file: the same content would be a path central already holds.
    added_script = ADDED.replace("echo recursive >", f"echo {arm}-recursive >")
    if added_script == ADDED:
        raise MachineError("the added script writes no recursive file to rename")
    _, said, added = await _realise(cp, settings, feature, f"{arm}recursive", added_script)
    if said != f"{arm}-recursive":
        raise MachineError(f"[cp] {arm}: the added path holds {said!r}")
    await cp.succeed(f"nix-store --verify-path {shlex.quote(added)}")
    print(f"[test] {arm}: a path the builder added reached central with the output: {added}")

    nested_script = NESTED.replace('"podnested"', f'"{arm}-nested"')
    if nested_script == NESTED:
        raise MachineError("the nested expression names no podnested derivation to rename")
    _, said, nested = await _realise(cp, settings, feature, f"{arm}recursive-nested", nested_script)
    if said != "nested":
        raise MachineError(f"[cp] {arm}: the nested build's output holds {said!r}")
    await cp.succeed(f"nix-store --verify-path {shlex.quote(nested)}")
    print(f"[test] {arm}: a nested build's output reached the builder through central: {nested}")

    out, said, _ = await _realise(cp, settings, feature, f"{arm}floating", FLOATING, FLOATING_ATTRS)
    if said != "floating":
        raise MachineError(f"[cp] {arm}: the floating output holds {said!r}")
    infos = json.loads((await cp.succeed(f"nix path-info --json {shlex.quote(out)}")).stdout)
    # Nix 2.19 and later key the document by path; older ones list the infos.
    (info,) = infos.values() if isinstance(infos, dict) else infos
    if not info.get("ca"):
        raise MachineError(f"[cp] {arm}: the floating output is not content-addressed at central: {info}")
    print(f"[test] {arm}: a floating output reached central, content-addressed, and realised: {out}")
