"""Nix's `dump-db.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

`nix-store --load-db` needs a store the client opens itself (`ensureLocalStore`, `nix-store.cc:573`), so
each arm loads its dump through its store's own files, with the arm's server stopped. The rest goes
through the arm.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest

from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, ArmStore, arm_store
from tests.functional.test_build_without_sandbox import _keep_store_dirs
from tests.functional.test_nixft_binary_cache import BUILDS
from tests.functional.test_nixft_referrers import direct, nix_store

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["_keep_store_dirs"]


async def _closure(store: ArmStore, path: str) -> str:
    rc, out, err = await nix_store(store, "-qR", path)
    assert rc == 0, err
    return out


async def _dump(store: ArmStore) -> str:
    rc, out, err = await nix_store(store, "--dump-db")
    assert rc == 0, err
    return out


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_loaded_dump_gives_back_the_same_database(tmp_path: Path, arm: Arm) -> None:
    """`dump-db.sh:9` to `:24`: `--load-db` of a `--dump-db` into an empty database gives back the same dump.

    The dump through the arm also equals the dump of the store's own files: pynixd answers
    `QueryAllValidPaths` and `QueryPathInfo` as the database holds them.
    """
    config = for_test_store(substituters=())
    config.sandbox = "false"
    builds = tmp_path / "dependencies.nix"
    builds.write_text(BUILDS)
    async with arm_store(tmp_path, arm, config) as store:
        with anyio.fail_after(60):
            _rc, out, _err, _both = await run_subproc(
                [NIX_BIN.with_name("nix-build"), "--store", store.url, builds, "--no-out-link"],
                env=dict(store.env),
                nix_config=store.config,
            )
        result = out.strip()
        deps = await _closure(store, result)
        dump = await _dump(store)
    own = direct(tmp_path, config)
    assert dump == await _dump(own)
    rmtree_robust(tmp_path / "var/nix/db")
    rc, _out, err = await nix_store(own, "--load-db", stdin=dump.encode())
    assert rc == 0, err
    async with arm_store(tmp_path, arm, config) as store:
        assert (await _closure(store, result), await _dump(store)) == (deps, dump)
