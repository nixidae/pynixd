"""The engine merges signatures into a path as Nix does (`LocalStore::addSignatures`).

`addSignatures` (`local-store.cc:1583`) reads the path, inserts the new
signatures into its set, and writes the set back. `Signature::toStrings` of a
`std::set` orders the set, so one path signed twice holds each signature once,
in Nix's order.
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

PATH = "/nix/store/00000000000000000000000000000001-hello"
SIG_A = Signature("cache-a:YWJj")
SIG_B = Signature("cache-b:ZGVm")

_SCHEMA = """
create table ValidPaths (
    id integer primary key autoincrement not null,
    path text unique not null,
    hash text not null,
    registrationTime integer not null,
    deriver text,
    narSize integer,
    ultimate integer,
    sigs text,
    ca text
);
"""


_OPENED: list[LocalStoreDB] = []


@pytest.fixture(autouse=True)
async def _close_opened() -> AsyncIterator[None]:
    """Close what `_db` opened: an open aiosqlite connection keeps a thread that stops the interpreter from exiting."""
    yield
    while _OPENED:
        await _OPENED.pop().close_db_pool()


def _db(tmp_path: Path, *, sigs: str | None = None) -> LocalStoreDB:
    """A store database with one valid path, which carries *sigs*."""
    with closing(sqlite3.connect(tmp_path / "db.sqlite")) as conn, conn:
        conn.executescript(_SCHEMA)
        conn.execute(
            "insert into ValidPaths (path, hash, registrationTime, narSize, sigs) values (?, 'sha256:abc', 1, 10, ?)",
            (PATH, sigs),
        )
    db = LocalStoreDB(db_path=tmp_path / "db.sqlite", store_path=None, read_only=False, reference_flush_interval=5.0)
    _OPENED.append(db)
    return db


async def _sigs(db: LocalStoreDB) -> str | None:
    row = await db.fetchone("select sigs from ValidPaths where path = ?", (PATH,))
    return None if row is None else row[0]


async def test_a_signature_is_written_to_a_path_that_has_none(tmp_path: Path) -> None:
    db = _db(tmp_path)
    assert await db.nix.add_signatures(PATH, [SIG_A]) is True
    assert await _sigs(db) == "cache-a:YWJj"


async def test_the_signatures_merge_in_nix_s_order(tmp_path: Path) -> None:
    db = _db(tmp_path, sigs="cache-b:ZGVm")
    assert await db.nix.add_signatures(PATH, [SIG_A]) is True
    assert await _sigs(db) == "cache-a:YWJj cache-b:ZGVm"


async def test_a_repeated_signature_changes_nothing(tmp_path: Path) -> None:
    db = _db(tmp_path, sigs="cache-a:YWJj")
    assert await db.nix.add_signatures(PATH, [SIG_A]) is True
    assert await _sigs(db) == "cache-a:YWJj"


async def test_an_invalid_path_writes_nothing_and_says_so(tmp_path: Path) -> None:
    db = _db(tmp_path)
    missing = "/nix/store/00000000000000000000000000000002-gone"
    assert await db.nix.add_signatures(missing, [SIG_A]) is False
    assert await _sigs(db) is None


async def _no_config() -> dict[str, Any]:
    raise AssertionError("addSignatures reads no Nix setting")


def _engine(tmp_path: Path, changed: list[str]) -> StoreEngine:
    return StoreEngine(
        StoreLayout.chroot(tmp_path),
        writes=True,
        read_config=_no_config,
        on_change=lambda path: changed.append(str(path)),
        name="test",
    )


async def test_the_engine_signs_a_path_and_forgets_its_cached_info(tmp_path: Path) -> None:
    db = _db(tmp_path)
    changed: list[str] = []
    engine = _engine(tmp_path, changed)
    engine.db = db
    await engine.add_signatures(PATH, [SIG_A, SIG_B])
    assert await _sigs(db) == "cache-a:YWJj cache-b:ZGVm"
    assert changed == [PATH]


async def test_the_engine_refuses_an_invalid_path(tmp_path: Path) -> None:
    changed: list[str] = []
    engine = _engine(tmp_path, changed)
    engine.db = _db(tmp_path)
    missing = "/nix/store/00000000000000000000000000000002-gone"
    with pytest.raises(register.RegistrationError) as refused:
        await engine.add_signatures(missing, [SIG_A])
    assert "not a valid store path" in str(refused.value)
    assert changed == []


async def test_the_engine_refuses_when_pynixd_writes_is_off(tmp_path: Path) -> None:
    engine = StoreEngine(
        StoreLayout.chroot(tmp_path), writes=False, read_config=_no_config, on_change=lambda _: None, name="test"
    )
    with pytest.raises(RuntimeError, match="pynixd_writes is off"):
        await engine.add_signatures(PATH, [SIG_A])
