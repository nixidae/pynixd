"""pynixd opens a new local store as Nix's `LocalStore` constructor does (`local-store.cc:122`).

Nix opens one chroot root and pynixd another, with the same settings. The
directories, their modes, the schema file, the tables and the migrations must
then be the same.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from typing import TYPE_CHECKING

import anyio.to_thread
import pytest

from pynixd_store.create_store import open_local_store
from pynixd_store.nix_config import nix_environment, read_nix_config
from pynixd_store.store_layout import StoreLayout
from tests.conftest import NIX_BIN, run_subproc

if TYPE_CHECKING:
    from pathlib import Path


def _tree(root: Path) -> dict[str, int]:
    """Each directory and file under *root*, with its mode, but the database's own files."""
    found = {}
    for path in root.rglob("*"):
        if path.name.startswith("db.sqlite") or path.name == "big-lock":
            continue
        found[str(path.relative_to(root))] = path.lstat().st_mode
    return found


def _db(root: Path) -> tuple[list[tuple[str, str, str]], set[str], str]:
    db = root / "nix" / "var" / "nix" / "db"
    with contextlib.closing(sqlite3.connect(f"file:{db / 'db.sqlite'}?mode=ro", uri=True)) as conn:
        master = conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY type, name").fetchall()
        migrations = {row[0] for row in conn.execute("SELECT migration FROM SchemaMigrations")}
        mode = conn.execute("pragma journal_mode").fetchone()[0]
    return master, migrations, mode + ":" + (db / "schema").read_text()


@pytest.mark.parametrize("features", ["nix-command", "nix-command ca-derivations"])
async def test_pynixd_opens_a_new_store_as_nix_does(tmp_path: Path, features: str) -> None:
    by_nix, by_pynixd = tmp_path / "nix", tmp_path / "pynixd"
    env = {**os.environ, "NIX_CONFIG": f"experimental-features = {features}"}
    await run_subproc([NIX_BIN, "store", "info", "--store", str(by_nix)], env=env)

    layout = StoreLayout.chroot(by_pynixd)
    config = await read_nix_config(str(NIX_BIN), nix_environment(layout, {"NIX_CONFIG": env["NIX_CONFIG"]}))
    lock = await anyio.to_thread.run_sync(open_local_store, layout, config)
    os.close(lock)

    assert _tree(by_pynixd) == _tree(by_nix)
    assert _db(by_pynixd) == _db(by_nix)


async def test_a_second_open_changes_nothing(tmp_path: Path) -> None:
    layout = StoreLayout.chroot(tmp_path)
    config = await read_nix_config(str(NIX_BIN), nix_environment(layout))
    os.close(await anyio.to_thread.run_sync(open_local_store, layout, config))
    first = _tree(tmp_path), _db(tmp_path)
    os.close(await anyio.to_thread.run_sync(open_local_store, layout, config))
    assert (_tree(tmp_path), _db(tmp_path)) == first
