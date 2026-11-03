"""The engine records a build trace as Nix does (`LocalStore::registerDrvOutput`).

`registerDrvOutput` (`local-store.cc:664`) reads the realisation of an id, and
either merges the new signatures into the set it holds, keeps the old one when
the output path agrees, or raises when it does not. The feature
`realisation-with-path-not-hash` picks the table: Nix 2.35 keys `BuildTraceV3`
by the derivation's base name; Nix 2.34 keys `Realisations` by its hash, with
the output path held as a `ValidPaths` id.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import aiosqlite
import pytest

from nix_daemon_protocol.signature import Signature
from pynixd_store import register
from pynixd_store.engine import StoreEngine
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


async def _db(tmp_path: Path) -> aiosqlite.Connection:
    """A store database whose two output paths are valid."""
    conn = await aiosqlite.connect(tmp_path / "db.sqlite")
    await conn.executescript(_SCHEMA)
    await conn.executemany("insert into ValidPaths (path) values (?)", [(OUT,), (OTHER,)])
    await conn.commit()
    return conn


async def _trace(conn: aiosqlite.Connection, *, by_path: bool) -> tuple[str, str | None] | None:
    """The one build-trace row's output path and signatures, or `None`."""
    if by_path:
        query = "select outputPath, signatures from BuildTraceV3"
    else:
        query = "select Output.path, signatures from Realisations join ValidPaths as Output on Output.id = outputPath"
    async with conn.execute(query) as cursor:
        row = await cursor.fetchone()
    return None if row is None else (row[0], row[1])


@pytest.mark.parametrize("by_path", [True, False])
async def test_a_realisation_with_no_signature_writes_an_empty_string(tmp_path: Path, by_path: bool) -> None:
    conn = await _db(tmp_path)
    key = DRV if by_path else HASH
    await register.register_drv_output(conn, key, "out", OUT, [], by_path=by_path)
    assert await _trace(conn, by_path=by_path) == (OUT, "")


@pytest.mark.parametrize("by_path", [True, False])
async def test_a_realisation_is_written_with_its_signature(tmp_path: Path, by_path: bool) -> None:
    conn = await _db(tmp_path)
    key = DRV if by_path else HASH
    await register.register_drv_output(conn, key, "out", OUT, [SIG_A], by_path=by_path)
    assert await _trace(conn, by_path=by_path) == (OUT, "cache-a:YWJj")


@pytest.mark.parametrize("by_path", [True, False])
async def test_the_signatures_merge_in_nix_s_order(tmp_path: Path, by_path: bool) -> None:
    conn = await _db(tmp_path)
    key = DRV if by_path else HASH
    await register.register_drv_output(conn, key, "out", OUT, [SIG_B], by_path=by_path)
    await register.register_drv_output(conn, key, "out", OUT, [SIG_A], by_path=by_path)
    assert await _trace(conn, by_path=by_path) == (OUT, "cache-a:YWJj cache-b:ZGVm")


@pytest.mark.parametrize("by_path", [True, False])
async def test_a_second_output_path_for_one_id_is_refused(tmp_path: Path, by_path: bool) -> None:
    conn = await _db(tmp_path)
    key = DRV if by_path else HASH
    await register.register_drv_output(conn, key, "out", OUT, [], by_path=by_path)
    with pytest.raises(register.RegistrationError) as refused:
        await register.register_drv_output(conn, key, "out", OTHER, [], by_path=by_path)
    message = str(refused.value)
    assert "we already have another one locally" in message
    assert OUT in message
    assert OTHER in message
    separator = "^" if by_path else "!"
    assert f"{key}{separator}out" in message
    # The refusal rolls back, so the first row stands unchanged.
    assert await _trace(conn, by_path=by_path) == (OUT, "")


async def test_an_output_path_that_is_not_valid_fails_the_hash_insert(tmp_path: Path) -> None:
    """Nix 2.34's `Realisations.outputPath` is a `ValidPaths` id; an invalid path makes the insert fail."""
    conn = await _db(tmp_path)
    missing = "/nix/store/00000000000000000000000000000003-gone"
    with pytest.raises(aiosqlite.IntegrityError):
        await register.register_drv_output(conn, HASH, "out", missing, [], by_path=False)
    assert await _trace(conn, by_path=False) is None


class _EngineDB:
    """A stand-in store database that yields *conn*, writable and active."""

    def __init__(self, conn: aiosqlite.Connection, *, active: bool = True, read_only: bool = False) -> None:
        self._conn = conn
        self.active = active
        self.read_only = read_only

    @contextlib.asynccontextmanager
    async def acquire_conn(self) -> AsyncIterator[aiosqlite.Connection]:
        yield self._conn


async def _no_config() -> dict[str, Any]:
    raise AssertionError("registerDrvOutput reads no Nix setting")


def _engine(tmp_path: Path, *, writes: bool = True) -> StoreEngine:
    return StoreEngine(
        StoreLayout.chroot(tmp_path), writes=writes, read_config=_no_config, on_change=lambda _: None, name="test"
    )


async def test_the_engine_records_a_build_trace(tmp_path: Path) -> None:
    conn = await _db(tmp_path)
    engine = _engine(tmp_path)
    engine.db = _EngineDB(conn)  # type: ignore[assignment]  # a stand-in database
    await engine.register_drv_output(DRV, "out", OUT, [SIG_A], by_path=True)
    assert await _trace(conn, by_path=True) == (OUT, "cache-a:YWJj")


async def test_the_engine_refuses_when_pynixd_writes_is_off(tmp_path: Path) -> None:
    engine = _engine(tmp_path, writes=False)
    with pytest.raises(RuntimeError, match="pynixd_writes is off"):
        await engine.register_drv_output(DRV, "out", OUT, [SIG_A], by_path=True)
