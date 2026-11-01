"""pynixd's answer to `QueryDerivationOutputMap`, from the store database.

Without `ca-derivations`, Nix answers from `DerivationOutputs` alone
(`store-api.cc:471`, `local-store.cc:922`). With it, Nix also reads the
derivation and its realisations, and the daemon answers.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_derivation_output_map import QueryDerivationOutputMapRequest
from pynixd.config import LocalSocketStoreSpec
from pynixd.local_store_db import LocalStoreDB
from pynixd.serde import StorePath
from pynixd.store.local_db import LocalDBStore
from pynixd.store_layout import StoreLayout
from pynixd.trust import TrustPolicy

if TYPE_CHECKING:
    from pathlib import Path

    from nix_daemon_protocol.query_derivation_output_map import QueryDerivationOutputMapResponse

HELLO_DRV = "/nix/store/00000000000000000000000000000001-hello.drv"
EMPTY_DRV = "/nix/store/00000000000000000000000000000002-empty.drv"
HELLO = "/nix/store/00000000000000000000000000000003-hello"
DOC = "/nix/store/00000000000000000000000000000004-hello-doc"
GONE_DRV = "/nix/store/00000000000000000000000000000005-gone.drv"


def _store_db(root: Path) -> None:
    """`HELLO_DRV` has two outputs; `EMPTY_DRV` is valid and has no row; `GONE_DRV`'s rows have no valid drv."""
    db_path = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        conn.execute("CREATE TABLE DerivationOutputs (drv INTEGER NOT NULL, id TEXT NOT NULL, path TEXT NOT NULL)")
        for path_id, path in ((1, HELLO_DRV), (2, EMPTY_DRV)):
            conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (?, ?, 0)", (path_id, path))
        conn.executemany(
            "INSERT INTO DerivationOutputs (drv, id, path) VALUES (?, ?, ?)",
            [(1, "out", HELLO), (1, "doc", DOC), (99, "out", HELLO)],
        )


async def _ask(root: Path, drv: str, *features: str) -> dict[str, str] | None:
    """The map pynixd answers, as text; `None` where the daemon answers."""
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=root))
    store._trust_policy = TrustPolicy(experimental_features=features)
    async with await LocalStoreDB.open(StoreLayout.chroot(root)) as store.db:
        answer: QueryDerivationOutputMapResponse | None = await store.own_query_derivation_output_map(
            QueryDerivationOutputMapRequest(path=StorePath(path=drv))
        )
    return None if answer is None else {name: str(path) for name, path in answer.items.items()}


@pytest.mark.anyio
class TestQueryDerivationOutputMap:
    async def test_every_output_the_table_holds(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _ask(tmp_path, HELLO_DRV) == {"out": HELLO, "doc": DOC}

    async def test_a_valid_derivation_with_no_row(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _ask(tmp_path, EMPTY_DRV) == {}

    async def test_a_derivation_that_is_not_valid_goes_to_the_daemon(self, tmp_path: Path) -> None:
        """`queryValidPathId` throws "path is not valid"; the daemon says so with Nix's words."""
        _store_db(tmp_path)
        assert await _ask(tmp_path, GONE_DRV) is None

    async def test_ca_derivations_goes_to_the_daemon(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _ask(tmp_path, HELLO_DRV, "nix-command", "ca-derivations") is None
