"""The engine keeps a path's provenance where the store database has Determinate Nix's column.

Determinate Nix adds `ValidPaths.provenance` with its `provenance` experimental feature (`local-store.cc:679` of
v3.24.0), writes it in `RegisterValidPath` (`:763`) and reads it in `queryPathInfoInternal` (`:862`). A database
without the column holds no provenance, and every read answers an empty one.
"""

from __future__ import annotations

import os
import sqlite3
from typing import TYPE_CHECKING, Any

import anyio.to_thread
import pytest

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd_store import register
from pynixd_store.create_store import PROVENANCE_MIGRATION, open_local_store
from pynixd_store.engine import StoreEngine
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path

PATH = "/nix/store/00000000000000000000000000000001-hello"
PROVENANCE = '{"type":"build","drv":"00000000000000000000000000000002-hello.drv","output":"out"}'


async def _no_config() -> dict[str, Any]:
    raise AssertionError("a registration of prepared rows reads no Nix setting")


def _info() -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(path=PATH),
        info=UnkeyedValidPathInfo(
            nar_hash=NARHash(hash="0" * 64),
            references=set(),
            registration_time=Time(ts=1),
            nar_size=8,
            ultimate=False,
            sigs=set(),
            ca=ContentAddress(value=""),
            provenance=PROVENANCE,
        ),
    )


async def _registered(tmp_path: Path, features: list[str]) -> tuple[bool, ValidPathInfo | None, ValidPathInfo | None]:
    """Create a store under *features*, register `_info()`, and read it back through the pool and the session reader."""
    layout = StoreLayout.chroot(tmp_path)
    config = {"experimental-features": {"value": features}}
    os.close(await anyio.to_thread.run_sync(open_local_store, layout, config, "2.35.0"))
    async with await LocalStoreDB.open(layout) as db:
        row = await register.prepare(_info(), layout, ())
        async with db.acquire_conn() as conn:
            await register.register_valid_paths(conn, [row], db.statements)
        engine = StoreEngine(layout, writes=True, read_config=_no_config, on_change=lambda _: None, name="test")
        engine.db = db
        pooled = await engine.query_path_info(PATH)
        reader = db.sync_reader()
        try:
            from_reader = await engine.query_path_info(PATH, reader=reader)
        finally:
            if reader is not None:
                reader.close()
        return db.statements.provenance, pooled, from_reader


@pytest.mark.anyio
async def test_a_store_with_the_column_keeps_the_provenance(tmp_path: Path) -> None:
    column, pooled, from_reader = await _registered(tmp_path, ["nix-command", "provenance"])
    assert column
    assert pooled is not None and pooled.info.provenance == PROVENANCE
    assert from_reader is not None and from_reader.info.provenance == PROVENANCE


@pytest.mark.anyio
async def test_a_store_without_the_column_reads_no_provenance(tmp_path: Path) -> None:
    column, pooled, from_reader = await _registered(tmp_path, ["nix-command"])
    assert not column
    assert pooled is not None and pooled.info.provenance == ""
    assert from_reader is not None and from_reader.info.provenance == ""


@pytest.mark.anyio
async def test_the_closure_and_batch_reads_carry_the_provenance(tmp_path: Path) -> None:
    layout = StoreLayout.chroot(tmp_path)
    config = {"experimental-features": {"value": ["provenance"]}}
    os.close(await anyio.to_thread.run_sync(open_local_store, layout, config, "2.35.0"))
    async with await LocalStoreDB.open(layout) as db:
        async with db.acquire_conn() as conn:
            await register.register_valid_paths(conn, [await register.prepare(_info(), layout, ())], db.statements)
        engine = StoreEngine(layout, writes=True, read_config=_no_config, on_change=lambda _: None, name="test")
        engine.db = db
        assert [info.info.provenance for info in await engine.query_closure_with_info([PATH])] == [PROVENANCE]
        assert [info.info.provenance for info in await engine.query_path_infos([PATH])] == [PROVENANCE]


@pytest.mark.anyio
async def test_the_migration_runs_with_the_experimental_feature_alone(tmp_path: Path) -> None:
    for features, expected in ((["provenance"], True), (["nix-command"], False)):
        root = tmp_path / features[0]
        os.close(
            await anyio.to_thread.run_sync(
                open_local_store, StoreLayout.chroot(root), {"experimental-features": {"value": features}}, "2.35.0"
            )
        )
        conn = sqlite3.connect(root / "nix" / "var" / "nix" / "db" / "db.sqlite")
        try:
            migrations = {row[0] for row in conn.execute("SELECT migration FROM SchemaMigrations")}
        finally:
            conn.close()
        assert (PROVENANCE_MIGRATION in migrations) is expected
