"""Nix's `eval-store.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

The arm is the build store, and a chroot store in `<root>/eval-store` is the eval store.

Through a daemon the client copies the closure of each derivation it builds to the build store
(`RemoteStore::copyDrvsFromEvalStore`, `remote-store.cc:550`), because the daemon cannot read the eval
store. So on the two daemon arms the build store holds `.drv` files where a local store holds none; that
is the script's reason to skip, and pynixd cannot change what the client sends.
"""

from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING

import pytest

from tests._conftest.nix_config import for_test_store
from tests.functional._nixft import Arm, arm_store
from tests.functional.test_build_without_sandbox import _keep_store_dirs
from tests.functional.test_nixft_binary_cache import BUILDS
from tests.functional.test_nixft_repair import NO_CA, _ok, _refused

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.nix_config import NixConfig

__all__ = ["_keep_store_dirs"]

# `ifd.nix`, with `dependencies.nix` as an input it does not use.
IFD = """let
  mk = args: derivation ({
    system = builtins.currentSystem;
    builder = "/bin/sh";
    args = [ "-c" "eval \\"$buildCommand\\"" ];
  } // args);
in import (mk {
  name = "foo";
  bla = import ./dependencies.nix;
  buildCommand = "echo '\\"hi\\"' > $out";
})"""


def _config() -> NixConfig:
    config = for_test_store(substituters=())
    config.sandbox = "false"
    return config


def _eval_store(root: Path) -> tuple[Path, str]:
    """The eval store's root, and its URL: `local?root=`, quoted, as `[` in a URL fails Nix's parser."""
    path = root / "eval-store"
    return path, f"local?root={urllib.parse.quote(str(path))}"


def _drvs(store_dir: Path) -> list[str]:
    return sorted(p.name for p in store_dir.glob("*.drv")) if store_dir.is_dir() else []


@pytest.mark.parametrize("arm", list(Arm))
@pytest.mark.parametrize(("line", "program"), [(14, "nix"), (31, "nix-instantiate"), (38, "nix-build")])
async def test_derivations_stay_in_the_eval_store(tmp_path: Path, arm: Arm, line: int, program: str) -> None:
    """`eval-store.sh:14` to `:44`: `nix build`, `nix-instantiate` and `nix-build` write `.drv` files to the eval store.

    A local build store gets none of them (`:24`, `:32`, `:42`). A build puts its output in the build store.
    """
    (tmp_path / "dependencies.nix").write_text(BUILDS)
    eval_root, eval_url = _eval_store(tmp_path)
    result = tmp_path / "result"
    commands: dict[str, tuple[str | Path, ...]] = {
        "nix": ("nix", "build", "--file", tmp_path / "dependencies.nix", "--out-link", result),
        "nix-instantiate": ("nix-instantiate", tmp_path / "dependencies.nix"),
        "nix-build": ("nix-build", tmp_path / "dependencies.nix", "--out-link", result),
    }
    program_name, *args = commands[program]
    async with arm_store(tmp_path, arm, _config(), server_lines=NO_CA) as store:
        await _ok(store, line, str(program_name), *args, "--eval-store", eval_url)
    builds = program != "nix-instantiate"
    assert (result / "foobar").exists() is builds
    assert _drvs(eval_root / "nix/store")
    in_build_store = _drvs(tmp_path / "store")
    if arm is Arm.local or not builds:
        assert in_build_store == []
    else:
        assert in_build_store == _drvs(eval_root / "nix/store")


@pytest.mark.parametrize("arm", list(Arm))
async def test_import_from_derivation_builds_in_the_build_store(tmp_path: Path, arm: Arm) -> None:
    """`eval-store.sh:50`, `:51`, `:52`: an IFD evaluates to "hi", and its input is built in the build store only."""
    (tmp_path / "dependencies.nix").write_text(BUILDS)
    (tmp_path / "ifd.nix").write_text(IFD)
    eval_root, eval_url = _eval_store(tmp_path)
    async with arm_store(tmp_path, arm, _config(), server_lines=NO_CA) as store:
        out = await _ok(
            store,
            50,
            "nix",
            "eval",
            "--eval-store",
            f"{eval_url}&require-sigs=false",
            "--impure",
            "--raw",
            "--file",
            tmp_path / "ifd.nix",
        )
    assert out == "hi"
    assert list((tmp_path / "store").glob("*dependencies-top/foobar"))
    assert not list((eval_root / "nix/store").glob("*dependencies-top/foobar"))


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_dummy_eval_store_takes_derivations_only_when_writable(tmp_path: Path, arm: Arm) -> None:
    """`eval-store.sh:55`, `:56`: `dummy://` refuses a `.drv`, and `dummy://?read-only=false` takes it."""
    (tmp_path / "dependencies.nix").write_text(BUILDS)
    async with arm_store(tmp_path, arm, _config(), server_lines=NO_CA) as store:
        await _refused(store, 55, "nix-instantiate", tmp_path / "dependencies.nix", "--eval-store", "dummy://")
        await _ok(
            store, 56, "nix-instantiate", tmp_path / "dependencies.nix", "--eval-store", "dummy://?read-only=false"
        )
