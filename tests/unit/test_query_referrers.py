"""pynixd's answer to `QueryReferrers`, from the store database.

Nix answers with one statement (`local-store.cc:364`), and pynixd runs the
same one. `tests/functional/test_queries.py` asks both through
`nix-store -q --referrers`.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_referrers import QueryReferrersRequest
from pynixd.config import LocalSocketStoreSpec
from pynixd.local_store_db import LocalStoreDB
from pynixd.serde import StorePath
from pynixd.store.local_db import LocalDBStore
from pynixd.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path

HELLO = "/nix/store/00000000000000000000000000000001-hello"
LIBC = "/nix/store/00000000000000000000000000000002-libc"
BASH = "/nix/store/00000000000000000000000000000003-bash"
GONE = "/nix/store/00000000000000000000000000000004-gone"


def _store_db(root: Path) -> None:
    """`HELLO` and `BASH` refer to `LIBC`, and `LIBC` refers to itself."""
    db_path = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
        for path_id, path in ((1, HELLO), (2, LIBC), (3, BASH)):
            conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (?, ?, 0)", (path_id, path))
        conn.executemany("INSERT INTO Refs (referrer, reference) VALUES (?, ?)", [(1, 2), (3, 2), (2, 2)])


async def _referrers(root: Path, path: str) -> set[str]:
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=root))
    async with await LocalStoreDB.open(StoreLayout.chroot(root)) as store.db:
        answer = await store.own_query_referrers(QueryReferrersRequest(path=StorePath(path=path)))
    return {str(found) for found in answer.paths}


@pytest.mark.anyio
class TestQueryReferrers:
    async def test_every_path_that_refers_to_it(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _referrers(tmp_path, LIBC) == {HELLO, BASH, LIBC}

    async def test_a_path_nothing_refers_to(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _referrers(tmp_path, HELLO) == set()

    async def test_a_path_the_store_does_not_hold(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _referrers(tmp_path, GONE) == set()
