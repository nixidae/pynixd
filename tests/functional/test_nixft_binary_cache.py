"""Nix's `binary-cache.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`): a cache that fails.

The script needs `--no-require-sigs`, which only a store the client opens itself takes. Through pynixd,
with nix-daemon and with none, `require-sigs` is off in the store's own configuration (`for_test_store`).
Each step has its own `XDG_CACHE_HOME`, so the client's narinfo cache starts empty (`clearCacheCache`,
`common/functions.sh:61`).
"""

from __future__ import annotations

import lzma
import re
import shutil
from typing import TYPE_CHECKING

import anyio
import pytest

from pynixd.nar import NarDirectory, write_nar
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, ArmStore, arm_store, clear_store, file_url
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.nix_config import NixConfig

__all__ = ["_keep_store_dirs"]

# `dependencies.nix`, on the builder's shell and the guest's own tools: no sandbox. `buildCommand` runs
# from a script, as `config.nix.in` runs it (Lillecarl/nix#428).
BUILDS = """let
  mk = args: derivation ({
    system = builtins.currentSystem;
    builder = "/bin/sh";
    PATH = "/run/current-system/sw/bin";
    args = [ "-e" (builtins.toFile "builder.sh" "eval \\"$buildCommand\\"") ];
  } // args);
  input0 = mk {
    name = "dependencies-input-0";
    buildCommand = "mkdir $out; echo foo > $out/bar";
  };
  input1 = mk {
    name = "dependencies-input-1";
    buildCommand = "mkdir $out; echo FOO > $out/foo";
  };
  input2 = mk {
    name = "dependencies-input-2";
    buildCommand = ''
      mkdir $out
      echo BAR > $out/bar
      echo ${input0} > $out/input0
    '';
  };
  fod_input = mk {
    name = "fod-input";
    buildCommand = "printf %s fod-input > $out";
    outputHashMode = "flat";
    outputHashAlgo = "sha256";
    outputHash = builtins.hashString "sha256" "fod-input";
  };
in mk {
  name = "dependencies-top";
  input0_drv = input0;
  input1_drv = input1;
  input2_drv = input2;
  fod_input_drv = fod_input;
  buildCommand = ''
    mkdir $out
    echo "$(cat ${input1}/foo)$(cat ${input2}/bar)" > $out/foobar
    ln -s ${input2} $out/reference-to-input-2
    ln -s $out $out/self
    echo program > $out/program
    chmod +x $out/program
    echo '1 + 2' > $out/foo.nix
    echo FOO
  '';
}"""


def _config(*, require_sigs: bool = False) -> NixConfig:
    config = for_test_store(substituters=(), require_sigs=require_sigs)
    config.sandbox = "false"
    return config


async def _run(
    store: ArmStore, cache_home: Path, *cmd: str | Path, sigs: bool = True, force_http: bool = False
) -> tuple[int, str, str]:
    """*cmd* against *store*: the program, then `--store`, then the rest.

    *sigs* drops signatures on the local arm. *force_http* reads a `file://` cache as an HTTP one, which
    keeps its narinfos in the client's disk cache, as `binary-cache.sh:86` sets it.
    """
    unsigned = ["--no-require-sigs"] if sigs and store.url == "local" else []
    with anyio.fail_after(60):
        rc, out, err, _both = await run_subproc(
            [cmd[0], "--store", store.url, *cmd[1:], *unsigned],
            expected_retcode=None,
            env=dict(store.env)
            | {"XDG_CACHE_HOME": str(cache_home)}
            | ({"_NIX_FORCE_HTTP": "1"} if force_http else {}),
            nix_config=store.config,
        )
    return rc, out, err


def _build(builds: Path, cache: Path, *extra: str) -> tuple[str | Path, ...]:
    cmd: tuple[str | Path, ...] = (NIX_BIN.with_name("nix-build"), builds, "--no-out-link")
    return (*cmd, "--substituters", file_url(cache), *extra)


async def _make_cache(root: Path) -> tuple[Path, Path, str]:
    """`binary-cache.sh:16`, `:20`: build `dependencies.nix`, copy its closure to a cache, clear the store."""
    builds = root / "dependencies.nix"
    builds.write_text(BUILDS)
    cache = root / "cache"
    async with arm_store(root, Arm.local, _config()) as store:
        rc, out, err = await _run(store, root / "xdg-make", NIX_BIN.with_name("nix-build"), builds, "--no-out-link")
        assert rc == 0, err
        out_path = out.strip()
        rc, _out, err = await _run(store, root / "xdg-make", NIX_BIN, "copy", "--to", file_url(cache), out_path)
        assert rc == 0, err
    clear_store(root)
    return builds, cache, out_path


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_nar_that_does_not_match_its_narinfo_fails(tmp_path: Path, arm: Arm) -> None:
    """`binary-cache.sh:102`: a NAR replaced by an empty directory's fails the build with "hash mismatch"."""
    builds, cache, _out_path = await _make_cache(tmp_path)
    nar = min((cache / "nar").glob("*.nar.xz"))
    nar.write_bytes(lzma.compress(write_nar(NarDirectory())))
    async with arm_store(tmp_path, arm, _config()) as store:
        rc, _out, err = await _run(store, tmp_path / "xdg", *_build(builds, cache))
    assert (rc, "hash mismatch" in err) == (1, True), err


@pytest.mark.parametrize("arm", list(Arm))
async def test_an_unsigned_cache_is_refused_when_signatures_are_required(tmp_path: Path, arm: Arm) -> None:
    """`binary-cache.sh:116`: with `require-sigs`, `nix-store -r` does not take a path from an unsigned cache."""
    _builds, cache, out_path = await _make_cache(tmp_path)
    async with arm_store(tmp_path, arm, _config(require_sigs=True)) as store:
        rc, _out, err = await _run(
            store, tmp_path / "xdg", NIX_BIN.with_name("nix-store"), "-r", out_path, "--substituters", file_url(cache),
            sigs=False,
        )  # fmt: skip
    assert rc != 0, err


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_missing_nar_falls_back_to_a_build(tmp_path: Path, arm: Arm) -> None:
    """`binary-cache.sh:126`: with the NARs gone, the build succeeds and warns "does not exist in binary cache"."""
    builds, cache, _out_path = await _make_cache(tmp_path)
    shutil.rmtree(cache / "nar")
    async with arm_store(tmp_path, arm, _config()) as store:
        rc, _out, err = await _run(store, tmp_path / "xdg", *_build(builds, cache))
    assert (rc, "does not exist in binary cache" in err) == (0, True), err


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_corrupt_nar_needs_fallback(tmp_path: Path, arm: Arm) -> None:
    """`binary-cache.sh:142`: with every NAR empty the build fails and names `--fallback`, and succeeds with it."""
    builds, cache, _out_path = await _make_cache(tmp_path)
    for nar in (cache / "nar").iterdir():
        nar.write_bytes(b"")
    async with arm_store(tmp_path, arm, _config()) as store:
        failed, _out, failed_err = await _run(store, tmp_path / "xdg", *_build(builds, cache))
        rc, _out, err = await _run(store, tmp_path / "xdg", *_build(builds, cache, "--fallback"))
    assert (failed, "try '--fallback'" in failed_err, rc) == (1, True, 0), f"{failed_err}\n{err}"


def _drop_input_2(cache: Path) -> None:
    """`binary-cache.sh:161`: the cache loses `input-2`'s narinfo, and keeps its NAR."""
    (narinfo,) = (
        p for p in cache.glob("*.narinfo") if re.search(r"^StorePath:.*-dependencies-input-2$", p.read_text(), re.M)
    )
    narinfo.unlink()


def _what_ran(err: str, verb: str) -> list[str]:
    """The names, of `dependencies.nix`'s, that a line `<verb> '...` names."""
    lines = [line for line in err.splitlines() if line.startswith(f"{verb} '")]
    return [name for name in ("input-0", "input-1", "input-2", "top") if any(f"-{name}" in line for line in lines)]


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_narinfo_the_store_read_before_the_cache_lost_it(tmp_path: Path, arm: Arm) -> None:
    """`binary-cache.sh:157`: the store read `input-2`'s narinfo, then the cache deleted it.

    `_NIX_FORCE_HTTP` is still set at that line of the script, so the client keeps the narinfo in its disk
    cache, and copies all three paths. pynixd starts the copy of `input-2`, reads its narinfo again, finds
    none, warns once, and builds `input-2`: the deviation that `BinaryCacheStore.read_nar` names. Then
    `top` builds, or, once `input-2` is valid, comes from the cache: measured, the no-daemon store
    builds it and the daemon store fetches it. Either is correct.
    """
    builds, cache, _out_path = await _make_cache(tmp_path)
    async with arm_store(tmp_path, arm, _config()) as store:
        rc, _out, err = await _run(store, tmp_path / "xdg", *_build(builds, cache, "--dry-run"), force_http=True)
        assert rc == 0, err
        _drop_input_2(cache)
        rc, _out, err = await _run(store, tmp_path / "xdg", *_build(builds, cache), force_http=True)
    copied, built = set(_what_ran(err, "copying path")), set(_what_ran(err, "building"))
    if arm is Arm.local:
        expected = ({"input-0", "input-2", "top"}, set[str](), 0)
    else:
        top = {"top"} if "top" in copied else set[str]()
        expected = ({"input-0", "input-2"} | top, {"input-1", "input-2", "top"} - top, 1)
    got = (copied, built, err.count("does not exist in binary cache"))
    assert (rc, got) == (0, expected), err


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_cache_with_an_incomplete_closure_builds_the_rest(tmp_path: Path, arm: Arm) -> None:
    """`binary-cache.sh:169`: with no narinfo cached, `top`'s inputs build, and `input-2` is a path no one can build."""
    builds, cache, _out_path = await _make_cache(tmp_path)
    _drop_input_2(cache)
    async with arm_store(tmp_path, arm, _config()) as store:
        rc, _out, err = await _run(store, tmp_path / "xdg", *_build(builds, cache), force_http=True)
    built = _what_ran(err, "building")
    assert (rc, "don't know how to build" in err, "input-1" in built, "input-2" in built) == (0, True, True, True), err
