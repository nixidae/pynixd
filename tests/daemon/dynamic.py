"""A dynamic derivation built through pynixd, on a builder that already holds its produced `.drv`.

`delegator` runs pynixd with `control` as its only build store. `control`
builds `<producer>^out^out` first, so it holds the produced `.drv` and its
output. pynixd then sends the same work there, and `control` answers
`ALREADY_VALID` for both steps. pynixd must still copy the produced `.drv`
back before it reads it. When it pulled for `BUILT` alone, the build failed
with "pynixd: derivation not found". nixidae/nixidae#998.

The producer writes a `.drv` that no store has seen: its text is built in
the builder, so evaluation on `delegator` never adds it to the local store,
as Nix's `dyn-drv/text-hashed-output.nix` does.
"""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

from daemon_helpers import as_tester, attempt, store_path

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vivarium_runner import Machine, Machines


def producer(settings: Mapping[str, str]) -> str:
    """An expression for a derivation whose text-hashed output is a floating content-addressed `.drv`.

    The produced `.drv`'s `out` is Nix's placeholder. In the producer's own
    environment Nix would replace it with the producer's output path, so it
    travels in two halves.
    """
    system = settings["system"]
    return f"""
      let
        bb = builtins.storePath "{settings["busybox"]}";
        sh = "${{bb}}/bin/sh";
        ph = builtins.placeholder "out";
      in derivation {{
        name = "pynixd-dynamic.drv";
        system = "{system}";
        builder = sh;
        args = [ "-c" "printf %s \\"$drv\\" | ${{bb}}/bin/sed \\"s|@OUT@|$ph1$ph2|\\" > $out" ];
        ph1 = builtins.substring 0 8 ph;
        ph2 = builtins.substring 8 100 ph;
        drv = ''Derive([("out","","r:sha256","")],[],["${{bb}}"],"{system}","${{sh}}",["-c","echo dynamic > $out"],[("builder","${{sh}}"),("name","pynixd-dynamic"),("out","@OUT@"),("outputHashAlgo","sha256"),("outputHashMode","recursive"),("system","{system}")])'';
        __contentAddressed = true;
        outputHashMode = "text";
        outputHashAlgo = "sha256";
      }}
    """


async def build(vm: Machine, drv: str) -> str:
    out = store_path(await as_tester(vm, f"nix build --no-link --print-out-paths {shlex.quote(drv + '^out^out')}"))
    content = (await vm.succeed(f"cat {out}")).strip()
    if content != "dynamic":
        raise AssertionError(f"{out} holds {content!r}, not 'dynamic'")
    return out


async def produced(vm: Machine, drv: str) -> str:
    return store_path(await as_tester(vm, f"nix build --no-link --print-out-paths {shlex.quote(drv + '^out')}"))


async def test(vms: Machines) -> None:
    await vms.delegator.wait_for_unit("pynixd.service")
    expression = shlex.quote(producer(vms.settings))
    drvs = {
        name: store_path(await as_tester(vms[name], f"nix-instantiate --expr {expression}"))
        for name in ("control", "delegator")
    }
    if drvs["control"] != drvs["delegator"]:
        raise AssertionError(f"the two guests instantiated different producers: {drvs}")
    drv = drvs["control"]

    out = await build(vms.control, drv)
    inner = await produced(vms.control, drv)
    print(f"[test] control: {drv}^out = {inner}, ^out^out = {out}")

    rc, _ = await attempt(vms.delegator, f"nix-store --check-validity {inner}")
    if rc == 0:
        raise AssertionError(f"delegator already holds the produced {inner}, so the test proves nothing")

    delegated = await build(vms.delegator, drv)
    if delegated != out:
        raise AssertionError(f"delegator built {delegated}, control {out}")
    await as_tester(vms.delegator, f"nix-store --check-validity {inner}")
    print(f"[test] delegator: built {delegated} through control, and holds the produced {inner}")
