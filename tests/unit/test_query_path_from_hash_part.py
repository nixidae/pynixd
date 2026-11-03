"""pynixd's answer to `QueryPathFromHashPart`, from the store database.

Nix looks up the first valid path at or after `<store>/<hash part>` and keeps
it when it starts with that prefix (`local-store.cc:958`). A miss is the empty
path. A hash part that is not 32 characters long is an error in Nix, so
pynixd leaves it to the daemon.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_path_from_hash_part import QueryPathFromHashPartRequest
from pynixd.config import LocalSocketStoreSpec
from pynixd.store.local_db import LocalDBStore
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path

    from nix_daemon_protocol.query_path_from_hash_part import QueryPathFromHashPartResponse

HASH = "00000000000000000000000000000002"
HELLO = f"/nix/store/{HASH}-hello"
BEFORE = "/nix/store/00000000000000000000000000000001-before"
AFTER = "/nix/store/00000000000000000000000000000003-after"


def _store_db(root: Path) -> None:
    db_path = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        for path_id, path in enumerate((BEFORE, HELLO, AFTER), start=1):
            conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (?, ?, 0)", (path_id, path))


async def _ask(root: Path, hash_part: str) -> str | None:
    """The path pynixd answers, as text; `None` where the daemon answers."""
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=root))
    async with await LocalStoreDB.open(StoreLayout.chroot(root)) as store.db:
        answer: QueryPathFromHashPartResponse | None = await store.own_query_path_from_hash_part(
            QueryPathFromHashPartRequest(path=hash_part)
        )
    return None if answer is None else str(answer.value)


@pytest.mark.anyio
class TestQueryPathFromHashPart:
    async def test_a_hit(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _ask(tmp_path, HASH) == HELLO

    async def test_a_miss_is_the_empty_path(self, tmp_path: Path) -> None:
        """The first path at or after the prefix is `BEFORE`, which has another hash part."""
        _store_db(tmp_path)
        assert await _ask(tmp_path, "0" * 32) == ""

    @pytest.mark.parametrize("hash_part", [HASH[:31], HASH + "0", ""])
    async def test_another_length_goes_to_the_daemon(self, tmp_path: Path, hash_part: str) -> None:
        """Nix throws "invalid hash part"; a prefix match here would answer what Nix refuses."""
        _store_db(tmp_path)
        assert await _ask(tmp_path, hash_part) is None
