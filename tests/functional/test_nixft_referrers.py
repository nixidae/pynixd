"""Nix's `referrers.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

The script registers paths with `nix-store --register-validity`, which needs a store the client opens
itself (`ensureLocalStore`, `nix-store.cc:573`). So each arm registers through its store's own files,
and collects garbage through the arm.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import anyio
import pytest

from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, ArmStore, arm_store
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.nix_config import NixConfig

__all__ = ["_keep_store_dirs"]

MAX = 500
HASH = "a" * 32


def direct(root: Path, config: NixConfig) -> ArmStore:
    """The store in *root*, opened by the client itself: what the script's low-level steps use."""
    state = root / "var/nix"
    env = {"NIX_STORE_DIR": str(root / "store"), "NIX_STATE_DIR": str(state), "NIX_LOG_DIR": str(root / "var/log/nix")}
    return ArmStore("local", env, config)


async def nix_store(store: ArmStore, *args: str | Path, stdin: bytes | None = None) -> tuple[int, str, str]:
    with anyio.fail_after(60):
        rc, out, err, _both = await run_subproc(
            [NIX_BIN.with_name("nix-store"), "--store", store.url, *args],
            expected_retcode=None,
            env=dict(store.env),
            nix_config=store.config,
            stdin=stdin,
        )
    return rc, out, err


def _registration(store_dir: Path) -> tuple[Path, str]:
    """`referrers.sh:11` to `:30`: *MAX* empty paths in a chain, each referring to the first, `reference`."""
    reference = store_dir / f"{HASH}-bla"
    reference.touch()
    lines = []
    for n in range(MAX):
        path = store_dir / f"{HASH}-{n}"
        path.touch()
        ref2 = reference if n + 1 == MAX else store_dir / f"{HASH}-{n + 1}"
        lines += [str(path), "", "2", str(reference), str(ref2)]
    return reference, "".join(f"{line}\n" for line in lines)


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_collected_path_leaves_no_referrer_row(tmp_path: Path, arm: Arm) -> None:
    """`referrers.sh:36`, `:39`, `:42`: a garbage collection that deletes 500 paths deletes their `Refs` rows."""
    config = for_test_store(substituters=())
    async with arm_store(tmp_path, arm, config) as store:
        own = direct(tmp_path, config)
        store_dir = tmp_path / "store"
        store_dir.mkdir(parents=True, exist_ok=True)
        reference, reg_info = _registration(store_dir)
        rc, _out, err = await nix_store(own, "--register-validity", stdin=f"{reference}\n\n0\n".encode())
        assert rc == 0, err
        rc, _out, err = await nix_store(own, "--register-validity", stdin=reg_info.encode())
        assert rc == 0, err
        roots = tmp_path / "var/nix/gcroots"
        roots.mkdir(parents=True, exist_ok=True)
        (roots / "ref").symlink_to(reference)
        rc, _out, err = await nix_store(store, "--gc")
        assert rc == 0, err
    with sqlite3.connect(tmp_path / "var/nix/db/db.sqlite") as db:
        (refs,) = db.execute("select count(*) from Refs").fetchone()
        (valid,) = db.execute("select path from ValidPaths").fetchall()
    assert (refs, valid) == (0, (str(reference),))
    assert sorted(p.name for p in store_dir.iterdir() if p.name.startswith(HASH)) == [reference.name]
