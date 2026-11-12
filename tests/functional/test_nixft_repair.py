"""Nix's `repair.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

A daemon refuses `--repair` to a client it does not trust (`daemon.cc:888`), and the script's client is
untrusted. Here the client is trusted, so every step goes through the arm. Only the corruptions write the
store's files directly.
"""

from __future__ import annotations

import stat
from pathlib import PurePath
from typing import TYPE_CHECKING

import anyio
import pytest

from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, ArmStore, arm_store, file_url
from tests.functional.test_build_without_sandbox import _keep_store_dirs
from tests.functional.test_nixft_binary_cache import BUILDS

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["_keep_store_dirs"]


# Nix's suite runs `repair.sh` without `ca-derivations`, and the guest's `nix.conf` names it.
NO_CA = "experimental-features = nix-command"
FEATURES = {"NIX_CONFIG": NO_CA}


async def _run(store: ArmStore, program: str, *args: str | Path) -> tuple[int, str, str]:
    with anyio.fail_after(60):
        rc, out, err, _both = await run_subproc(
            [NIX_BIN.with_name(program), "--store", store.url, *args],
            expected_retcode=None,
            env=FEATURES | store.env,
            nix_config=store.config,
        )
    return rc, out, err


async def _ok(store: ArmStore, line: int, program: str, *args: str | Path) -> str:
    rc, out, err = await _run(store, program, *args)
    assert rc == 0, f"repair.sh:{line}: {err}"
    return out


async def _refused(store: ArmStore, line: int, program: str, *args: str | Path) -> None:
    rc, _out, err = await _run(store, program, *args)
    assert rc != 0, f"repair.sh:{line} succeeded: {err}"


async def _nix_hash(path: Path) -> str:
    _rc, out, _err, _both = await run_subproc([NIX_BIN.with_name("nix-hash"), path])
    return out.strip()


async def _repair_path(store: ArmStore, arm: Arm, line: int, path2: Path, *extra: str, rebuild: bool = False) -> None:
    """`nix-store --repair-path` at `repair.sh:<line>`, or through a daemon the operation that does the same.

    NIX-DEFECT (#23): `RemoteStore::repairPath` (`remote-store.hh:147`) is `unsupported`, and the worker
    protocol has no operation for it. So `--repair-path` fails through any daemon, though `BuildPaths` in
    `bmRepair` repairs the same path. pynixd cannot change the client; the fix is a client that sends
    `BuildPaths`. Lillecarl/nix#430.
    """
    if arm is Arm.local:
        await _ok(store, line, "nix-store", "--repair-path", path2, *extra)
        return
    rc, _out, err = await _run(store, "nix-store", "--repair-path", path2, *extra)
    assert (rc, "operation 'repairPath' is not supported" in err) == (1, True), err
    target = (await _ok(store, line, "nix-store", "-qd", path2)).strip() if rebuild else str(path2)
    await _ok(store, line, "nix-store", "--realise", "--repair", target, *extra)


def _writable(path: Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IWUSR)


def _corrupt(path2: Path) -> None:
    _writable(path2)
    (path2 / "bad").touch()


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_corrupt_path_is_repaired_as_a_local_store_repairs_it(tmp_path: Path, arm: Arm) -> None:
    """`repair.sh:9` to `:130`: `--verify --repair`, `--repair-path` and `nix-build --repair` repair a path.

    They rebuild it, or substitute it, and refuse when neither is possible. A repair also refuses an
    optimised path's corrupt `.links` file.
    """
    config = for_test_store(substituters=())
    config.sandbox = "false"
    builds = tmp_path / "dependencies.nix"
    builds.write_text(BUILDS)
    cache = tmp_path / "cache"
    no_sigs = ("--substituters", file_url(cache), "--no-require-sigs")
    async with arm_store(tmp_path, arm, config, server_lines=NO_CA) as store:
        path = (await _ok(store, 9, "nix-build", builds, "--no-out-link")).strip()
        closure = (await _ok(store, 10, "nix-store", "-qR", path)).split()
        p2 = next(tmp_path / "store" / PurePath(p).name for p in closure if "input-2" in p)
        await _ok(store, 12, "nix-store", "--verify", "--check-contents", "-v")
        hash_ = await _nix_hash(p2)

        _corrupt(p2)
        await _refused(store, 20, "nix-store", "--verify", "--check-contents", "-v")
        await _ok(store, 23, "nix-store", "--verify", "--check-contents", "--repair")
        assert (not (p2 / "bad").exists(), p2.stat().st_mode & stat.S_IWUSR) == (True, 0)
        await _ok(store, 30, "nix-store", "--verify-path", p2)

        _corrupt(p2)
        deriver = (await _ok(store, 38, "nix-store", "-qd", p2)).strip()
        referrers = (await _ok(store, 38, "nix-store", "-q", "--referrers-closure", deriver)).split()
        await _ok(store, 38, "nix-store", "--delete", *referrers)
        await _refused(store, 40, "nix-store", "--verify", "--check-contents", "--repair")
        await _ok(store, 42, "nix-build", builds, "--no-out-link", "--repair")
        assert (await _nix_hash(p2), (p2 / "bad").exists()) == (hash_, False), "repair.sh:45"

        await _ok(store, 54, "nix", "copy", "--to", file_url(cache), path)
        _writable(p2)
        rmtree_robust(p2)
        await _ok(store, 59, "nix-store", "--verify", "--check-contents", "--repair", *no_sigs)
        assert (await _nix_hash(p2), (p2 / "bad").exists()) == (hash_, False), "repair.sh:62"

        await _ok(store, 68, "nix-store", "--verify-path", p2)
        _writable(p2)
        rmtree_robust(p2)
        await _refused(store, 73, "nix-store", "--verify-path", p2)
        await _repair_path(store, arm, 78, p2, *no_sigs)
        assert (await _nix_hash(p2), (p2 / "bad").exists()) == (hash_, False), "repair.sh:81"

        await _ok(store, 87, "nix-store", "--verify-path", p2)
        await _ok(store, 89, "nix-store", "--optimize")
        bar = p2 / "bar"
        _writable(bar)
        bar.write_text("rabrab\n")
        await _refused(store, 96, "nix-store", "--verify-path", p2)
        await _repair_path(store, arm, 101, p2, "--option", "auto-optimise-store", "true", rebuild=True)
        assert (await _nix_hash(p2), bar.read_text()) == (hash_, "BAR\n"), "repair.sh:104"

        await _ok(store, 110, "nix-store", "--verify-path", p2)
        await _ok(store, 112, "nix-store", "--optimize")
        _writable(p2)
        _writable(bar)
        bar.write_text("XXX\n")
        await _refused(store, 122, "nix-store", "--verify-path", p2)
        await _repair_path(store, arm, 127, p2, *no_sigs, "--option", "auto-optimise-store", "true")
        assert (await _nix_hash(p2), bar.read_text()) == (hash_, "BAR\n"), "repair.sh:130"


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_repair_with_ca_derivations_on_rebuilds_a_corrupt_path(tmp_path: Path, arm: Arm) -> None:
    """`repair.sh:23` with `ca-derivations` on: `--verify --check-contents --repair` rebuilds the corrupt path.

    NIX-DEFECT (#23): after it takes the output lock, `DerivationBuildingGoal::checkPathValidity`
    (`derivation-building-goal.cc:1355`) reads the output's realisation and calls the path valid when it
    exists, so the `checkHash` of a repair is lost. The first build registered that realisation, so Nix
    skips the repair build ("someone beat us to it") and exits 0 with the path still corrupt. pynixd
    repairs it. Through nix-daemon the daemon repairs, so that arm copies the defect. Lillecarl/nix#429.
    """
    config = for_test_store(substituters=())
    config.sandbox = "false"
    builds = tmp_path / "dependencies.nix"
    builds.write_text(BUILDS)
    features = {"NIX_CONFIG": "experimental-features = nix-command ca-derivations"}
    async with arm_store(tmp_path, arm, config) as store:
        ca = ArmStore(store.url, dict(store.env) | features, store.config)
        path = (await _ok(ca, 9, "nix-build", builds, "--no-out-link")).strip()
        closure = (await _ok(ca, 10, "nix-store", "-qR", path)).split()
        p2 = next(tmp_path / "store" / PurePath(p).name for p in closure if "input-2" in p)
        _corrupt(p2)
        await _ok(ca, 23, "nix-store", "--verify", "--check-contents", "--repair")
    assert (p2 / "bad").exists() is (arm is not Arm.pynixd)
