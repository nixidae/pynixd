"""The engine records a build trace as Nix does (`LocalStore::registerDrvOutput`).

`registerDrvOutput` (`local-store.cc:664`) reads the realisation of an id, and
either merges the new signatures into the set it holds, keeps the old one when
the output path agrees, or raises when it does not. The feature
`realisation-with-path-not-hash` picks the table: Nix 2.35 keys `BuildTraceV3`
by the derivation's base name; Nix 2.34 keys `Realisations` by its hash, with
the output path held as a `ValidPaths` id.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.signature import Signature
from pynixd_store import register
from pynixd_store.engine import StoreEngine
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

DRV = "zzz1111111111111111111111111111-hello.drv"
HASH = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
OUT = "/nix/store/00000000000000000000000000000001-hello"
OTHER = "/nix/store/00000000000000000000000000000002-hello"
SIG_A = Signature("cache-a:YWJj")
SIG_B = Signature("cache-b:ZGVm")

_SCHEMA = """
create table ValidPaths (
    id integer primary key autoincrement not null,
    path text unique not null
);
create table BuildTraceV3 (
    id integer primary key autoincrement not null,
    drvPath text not null,
    outputName text not null,
    outputPath text not null,
    signatures text
);
create table Realisations (
    id integer primary key autoincrement not null,
    drvPath text not null,
    outputName text not null,
    outputPath integer not null,
    signatures text,
    foreign key (outputPath) references ValidPaths(id) on delete cascade
);
"""


_OPENED: list[LocalStoreDB] = []


@pytest.fixture(autouse=True)
async def _close_opened() -> AsyncIterator[None]:
    """Close what `_db` opened: an open aiosqlite connection keeps a thread that stops the interpreter from exiting."""
    yield
    while _OPENED:
        await _OPENED.pop().close_db_pool()


def _db(tmp_path: Path) -> LocalStoreDB:
    """A store database whose two output paths are valid."""
    with closing(sqlite3.connect(tmp_path / "db.sqlite")) as conn, conn:
        conn.executescript(_SCHEMA)
        conn.executemany("insert into ValidPaths (path) values (?)", [(OUT,), (OTHER,)])
    db = LocalStoreDB(db_path=tmp_path / "db.sqlite", store_path=None, read_only=False, reference_flush_interval=5.0)
    _OPENED.append(db)
    return db


async def _trace(db: LocalStoreDB, *, by_path: bool) -> tuple[str, str | None] | None:
    """The one build-trace row's output path and signatures, or `None`."""
    if by_path:
        query = "select outputPath, signatures from BuildTraceV3"
    else:
        query = "select Output.path, signatures from Realisations join ValidPaths as Output on Output.id = outputPath"
    row = await db.fetchone(query)
    return None if row is None else (row[0], row[1])


@pytest.mark.parametrize("by_path", [True, False])
async def test_a_realisation_with_no_signature_writes_an_empty_string(tmp_path: Path, by_path: bool) -> None:
    db = _db(tmp_path)
    key = DRV if by_path else HASH
    await db.nix.register_drv_output(key, "out", OUT, [], by_path=by_path)
    assert await _trace(db, by_path=by_path) == (OUT, "")


@pytest.mark.parametrize("by_path", [True, False])
async def test_a_realisation_is_written_with_its_signature(tmp_path: Path, by_path: bool) -> None:
    db = _db(tmp_path)
    key = DRV if by_path else HASH
    await db.nix.register_drv_output(key, "out", OUT, [SIG_A], by_path=by_path)
    assert await _trace(db, by_path=by_path) == (OUT, "cache-a:YWJj")


@pytest.mark.parametrize("by_path", [True, False])
async def test_the_signatures_merge_in_nix_s_order(tmp_path: Path, by_path: bool) -> None:
    db = _db(tmp_path)
    key = DRV if by_path else HASH
    await db.nix.register_drv_output(key, "out", OUT, [SIG_B], by_path=by_path)
    await db.nix.register_drv_output(key, "out", OUT, [SIG_A], by_path=by_path)
    assert await _trace(db, by_path=by_path) == (OUT, "cache-a:YWJj cache-b:ZGVm")


@pytest.mark.parametrize("by_path", [True, False])
async def test_a_second_output_path_for_one_id_is_refused(tmp_path: Path, by_path: bool) -> None:
    db = _db(tmp_path)
    key = DRV if by_path else HASH
    await db.nix.register_drv_output(key, "out", OUT, [], by_path=by_path)
    with pytest.raises(register.RegistrationError) as refused:
        await db.nix.register_drv_output(key, "out", OTHER, [], by_path=by_path)
    message = str(refused.value)
    assert "we already have another one locally" in message
    assert OUT in message
    assert OTHER in message
    separator = "^" if by_path else "!"
    assert f"{key}{separator}out" in message
    # The refusal rolls back, so the first row stands unchanged.
    assert await _trace(db, by_path=by_path) == (OUT, "")


async def test_an_output_path_that_is_not_valid_is_refused_by_name(tmp_path: Path) -> None:
    """Nix 2.34's `Realisations.outputPath` is a `ValidPaths` id, and Nix lets the insert fail on it.

    The NIX-DEFECT of `register.register_drv_output`: pynixd refuses first and names the path, where Nix
    shows the SQLite statement.
    """
    db = _db(tmp_path)
    missing = "/nix/store/00000000000000000000000000000003-gone"
    with pytest.raises(register.RegistrationError) as refused:
        await db.nix.register_drv_output(HASH, "out", missing, [], by_path=False)
    message = str(refused.value)
    assert f"{HASH}!out" in message
    assert f"{missing}" in message
    assert "is not valid" in message
    assert "SQLite" not in message
    assert await _trace(db, by_path=False) is None


async def _no_config() -> dict[str, Any]:
    raise AssertionError("registerDrvOutput reads no Nix setting")


def _engine(tmp_path: Path, *, writes: bool = True) -> StoreEngine:
    return StoreEngine(
        StoreLayout.chroot(tmp_path), writes=writes, read_config=_no_config, on_change=lambda _: None, name="test"
    )


async def test_the_engine_records_a_build_trace(tmp_path: Path) -> None:
    db = _db(tmp_path)
    engine = _engine(tmp_path)
    engine.db = db
    await engine.register_drv_output(DRV, "out", OUT, [SIG_A], by_path=True)
    assert await _trace(db, by_path=True) == (OUT, "cache-a:YWJj")


async def test_the_engine_refuses_when_pynixd_writes_is_off(tmp_path: Path) -> None:
    engine = _engine(tmp_path, writes=False)
    with pytest.raises(RuntimeError, match="pynixd_writes is off"):
        await engine.register_drv_output(DRV, "out", OUT, [SIG_A], by_path=True)
