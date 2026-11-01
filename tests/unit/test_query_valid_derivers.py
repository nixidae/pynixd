"""pynixd's answer to `QueryValidDerivers`, from the store database.

Nix answers with one statement (`local-store.cc:371`), and pynixd runs the
same one: every valid derivation whose `DerivationOutputs` row names the path.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_valid_derivers import QueryValidDeriversRequest
from pynixd.config import LocalSocketStoreSpec
from pynixd.local_store_db import LocalStoreDB
from pynixd.serde import StorePath
from pynixd.store.local_db import LocalDBStore
from pynixd.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path

HELLO_DRV = "/nix/store/00000000000000000000000000000001-hello.drv"
HELLO_FIXED_DRV = "/nix/store/00000000000000000000000000000002-hello-fixed.drv"
HELLO = "/nix/store/00000000000000000000000000000003-hello"
DOCS = "/nix/store/00000000000000000000000000000004-hello-doc"
GONE_DRV = "/nix/store/00000000000000000000000000000005-gone.drv"
LONELY = "/nix/store/00000000000000000000000000000006-lonely"


def _store_db(root: Path) -> None:
    """Two derivations name `HELLO`; `DOCS` is named only by a deriver that is not valid."""
    db_path = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        conn.execute("CREATE TABLE DerivationOutputs (drv INTEGER NOT NULL, id TEXT NOT NULL, path TEXT NOT NULL)")
        for path_id, path in ((1, HELLO_DRV), (2, HELLO_FIXED_DRV), (3, LONELY)):
            conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (?, ?, 0)", (path_id, path))
        conn.executemany(
            "INSERT INTO DerivationOutputs (drv, id, path) VALUES (?, ?, ?)",
            [(1, "out", HELLO), (1, "doc", DOCS), (2, "out", HELLO), (99, "out", DOCS)],
        )


async def _derivers(root: Path, path: str) -> set[str]:
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=root))
    async with await LocalStoreDB.open(StoreLayout.chroot(root)) as store.db:
        answer = await store.own_query_valid_derivers(QueryValidDeriversRequest(path=StorePath(path=path)))
    return {str(found) for found in answer.paths}


@pytest.mark.anyio
class TestQueryValidDerivers:
    async def test_every_derivation_that_names_it(self, tmp_path: Path) -> None:
        """The output itself need not be valid: `HELLO` has no `ValidPaths` row."""
        _store_db(tmp_path)
        assert await _derivers(tmp_path, HELLO) == {HELLO_DRV, HELLO_FIXED_DRV}

    async def test_a_deriver_that_is_not_valid_does_not_count(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _derivers(tmp_path, DOCS) == {HELLO_DRV}

    async def test_a_path_no_derivation_names(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _derivers(tmp_path, LONELY) == set()
        assert await _derivers(tmp_path, GONE_DRV) == set()
