"""`fsync-metadata` sets `pragma synchronous` (`local-store.cc:536`) on the database the engine holds.

A store with no daemon reads its settings before it opens its database
(`LocalDBStore.start`), so the database set later takes them too.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pynixd_store.engine import StoreEngine
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path


def _engine(tmp_path: Path, fsync_metadata: bool) -> StoreEngine:
    async def config() -> dict[str, Any]:
        return {"fsync-metadata": {"value": fsync_metadata}}

    return StoreEngine(
        StoreLayout.chroot(tmp_path), writes=True, read_config=config, on_change=lambda _: None, name="test"
    )


@pytest.mark.parametrize(("fsync_metadata", "mode"), [(True, "NORMAL"), (False, "OFF")])
async def test_a_database_set_after_the_settings_takes_their_mode(
    tmp_path: Path, fsync_metadata: bool, mode: str
) -> None:
    engine = _engine(tmp_path, fsync_metadata)
    await engine.nix_config()
    engine.db = LocalStoreDB.inactive(engine.layout)
    assert engine.db.synchronous == mode


async def test_a_database_set_before_the_settings_takes_their_mode(tmp_path: Path) -> None:
    engine = _engine(tmp_path, fsync_metadata=False)
    engine.db = LocalStoreDB.inactive(engine.layout)
    await engine.nix_config()
    assert engine.db.synchronous == "OFF"
