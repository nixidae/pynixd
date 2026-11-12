"""Nix's `ca/substitute.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

Through pynixd, with nix-daemon and with none, `require-sigs` is off in the store's own configuration,
as the client cannot turn it off there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest

from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, ArmStore, arm_store, clear_store, file_url
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.nix_config import NixConfig

__all__ = ["_keep_store_dirs"]

# `ca/content-addressed.nix`'s first four, on the builder's shell and the guest's own tools: no sandbox.
# `buildCommand` runs from a script, as `config.nix.in` runs it. Under `sh -c` the shell execs the last
# command, and `ln` closes its output before it exits: Nix then kills it at the log's end and fails
# the build (`unprepareBuild`, `derivation-builder.cc:187`). Measured: 1 of 6 builds of `rootCA`.
BUILDS = """let
  mk = args: derivation ({
    system = builtins.currentSystem;
    builder = "/bin/sh";
    PATH = "/run/current-system/sw/bin";
    args = [ "-e" (builtins.toFile "builder.sh" "eval \\"$buildCommand\\"") ];
  } // args);
  mkCA = args: mk ({ __contentAddressed = true; outputHashMode = "recursive"; outputHashAlgo = "sha256"; } // args);
in rec {
  rootLegacy = mk {
    name = "simple-input-addressed";
    buildCommand = ''
      mkdir -p $out
      echo "Hello World" > $out/hello
    '';
  };
  rootCA = mkCA {
    name = "rootCA";
    outputs = [ "out" "dev" "foo" ];
    buildCommand = ''
      mkdir -p $out
      echo ${rootLegacy}/hello > $out/dep
      ln -s $out $out/self
      ln -s $out $dev
      ln -s $out $foo
    '';
  };
  dependentCA = mkCA {
    name = "dependent";
    buildCommand = ''
      mkdir -p $out
      cat ${rootCA}/self/dep
      echo ${rootCA}/self/dep > $out/dep
    '';
  };
  transitivelyDependentCA = mkCA {
    name = "transitively-dependent";
    buildCommand = ''
      cat ${dependentCA}/dep
      echo ${dependentCA} > $out
    '';
  };
}"""

FEATURES = ("--extra-experimental-features", "ca-derivations")


def _config() -> NixConfig:
    config = for_test_store(substituters=(), experimental_features=("ca-derivations",))
    config.sandbox = "false"
    return config


async def _nix(store: ArmStore, *args: str | Path, expected: int | None = 0) -> tuple[int, str]:
    with anyio.fail_after(60):
        rc, _out, err, _both = await run_subproc(
            [NIX_BIN, *args[:1], "--store", store.url, *FEATURES, *args[1:]],
            expected_retcode=expected,
            env=dict(store.env),
            nix_config=store.config,
        )
    return rc, err


@pytest.mark.parametrize("arm", list(Arm))
async def test_ca_outputs_substitute_with_their_realisations(tmp_path: Path, arm: Arm) -> None:
    """`ca/substitute.sh:16`, `:23`, `:25`, `:28`: `-j0` fetches the outputs, and each keeps its realisation."""
    builds = tmp_path / "content-addressed.nix"
    builds.write_text(BUILDS)
    cache = file_url(tmp_path / "cache")
    attrs = ("transitivelyDependentCA", "dependentCA", "rootCA", "rootLegacy")
    async with arm_store(tmp_path, Arm.local, _config()) as store:
        await _nix(store, "copy", "--to", cache, "--file", builds, *attrs)
    clear_store(tmp_path)

    sigs = ["--no-require-sigs"] if arm is Arm.local else []
    async with arm_store(tmp_path, arm, _config()) as store:
        await _nix(
            store, "build", "--file", builds, "-L", "--no-link", "--substitute", "--substituters", cache, *sigs, "-j0",
            "transitivelyDependentCA", "dependentCA",
        )  # fmt: skip
        await _nix(store, "store", "build-trace", "info", "--file", builds, "transitivelyDependentCA")
        await _nix(store, "realisation", "info", "--file", builds, "dependentCA")
