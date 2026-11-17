"""The provisioner's instance table, on SQLite and on PostgreSQL. nixidae/nixidae#1198."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from pynixd_store.create_store import open_local_store
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.postgres_db import PostgresBackend
from pynixd_store.provisioning_tables import PENDING, RUNNING, ProvisioningTables
from pynixd_store.sql import POSTGRES
from pynixd_store.store_layout import StoreLayout

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


@pytest.fixture(params=["sqlite", "postgres"])
async def tables(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[ProvisioningTables]:
    layout = StoreLayout.chroot(tmp_path)
    if request.param == "sqlite":
        os.close(open_local_store(layout, {}, "2.35.0"))
        db = await LocalStoreDB.open(layout)
    else:
        conninfo: str = request.getfixturevalue("postgres_conninfo")
        db = await LocalStoreDB.open_backend(PostgresBackend(conninfo, POSTGRES), layout)
    assert db.schema.usable, db.schema.reason
    async with db:
        yield db.provisioned


async def test_a_launch_is_live_until_it_ends(tables: ProvisioningTables) -> None:
    await tables.record_launch("p-1", "arm", "vivarium", "aa" * 32, at_ms=10)
    await tables.record_launch("p-2", "arm", "vivarium", "bb" * 32, at_ms=20)
    await tables.set_provider_id("p-1", "i-0001")
    assert [(row.id, row.state, row.provider_id) for row in await tables.live()] == [
        ("p-1", PENDING, "i-0001"),
        ("p-2", PENDING, None),
    ]

    assert await tables.registered("p-1", "builder-p-1", at_ms=30)
    assert not await tables.registered("p-1", "builder-p-1", at_ms=35), "a token admitted twice"
    found = await tables.by_token("aa" * 32)
    assert found is not None
    assert (found.id, found.state, found.store_id, found.registered_at_ms) == ("p-1", RUNNING, "builder-p-1", 30)

    await tables.ended("p-1", "idle", at_ms=40)
    assert [row.id for row in await tables.live()] == ["p-2"]
    assert await tables.by_token("aa" * 32) is None
    assert not await tables.registered("p-1", "builder-p-1", at_ms=50), "an ended instance registered again"
