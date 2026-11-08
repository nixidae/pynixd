"""The HTTP cache finds a path by its NAR hash in each database of an overlay store.

`LocalOverlayStore` reads the upper database, then the lower one
(`local-overlay-store.cc:180`). nixidae/nixidae#1221.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec, OverlaySpec
from pynixd.http_server import PynixdHttpServer
from pynixd.store.local_db import LocalDBStore
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.overlay_engine import OverlayEngine

if TYPE_CHECKING:
    from pathlib import Path

NAR_HASH = "1" * 64
LOWER = "/nix/store/00000000000000000000000000000001-lower"


def _store_db(db_path: Path, paths: dict[str, str]) -> None:
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, hash TEXT NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        for path, nar_hash in paths.items():
            conn.execute("INSERT INTO ValidPaths (path, hash, registrationTime) VALUES (?, ?, 0)", (path, nar_hash))


@pytest.mark.anyio
async def test_a_nar_hash_only_the_lower_store_holds_resolves(tmp_path: Path) -> None:
    spec = LocalSocketStoreSpec(
        store_id=StoreId("local"),
        store_path=tmp_path / "merged",
        overlay=OverlaySpec(lower_store=tmp_path / "lower", upper_layer=tmp_path / "upper", check_mount=False),
    )
    store = LocalDBStore(spec)
    engine = store.engine
    layout = spec.layout()
    assert isinstance(engine, OverlayEngine)
    assert layout.overlay is not None
    _store_db(layout.db_path, {})
    _store_db(layout.overlay.lower.db_path, {LOWER: f"sha256:{NAR_HASH}"})
    server = PynixdHttpServer(store, enable_metrics=False)
    async with (
        await LocalStoreDB.open(layout) as engine.db,
        await LocalStoreDB.open(layout.overlay.lower, read_only=True) as engine.lower.db,
    ):
        assert str(await server.resolve_path(NAR_HASH)) == LOWER
