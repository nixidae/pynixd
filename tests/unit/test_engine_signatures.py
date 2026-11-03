"""The engine merges signatures into a path as Nix does (`LocalStore::addSignatures`).

`addSignatures` (`local-store.cc:1583`) reads the path, inserts the new
signatures into its set, and writes the set back. `Signature::toStrings` of a
`std::set` orders the set, so one path signed twice holds each signature once,
in Nix's order.
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


_OPENED: list[aiosqlite.Connection] = []


@pytest.fixture(autouse=True)
async def _close_opened() -> AsyncIterator[None]:
    """Close what `_db` opened: an open aiosqlite connection keeps a thread that stops the interpreter from exiting."""
    yield
    while _OPENED:
        await _OPENED.pop().close()


async def _db(tmp_path: Path, *, sigs: str | None = None) -> aiosqlite.Connection:
    """A store database with one valid path, which carries *sigs*."""
    conn = await aiosqlite.connect(tmp_path / "db.sqlite")
    _OPENED.append(conn)
    await conn.executescript(_SCHEMA)
    await conn.execute(
        "insert into ValidPaths (path, hash, registrationTime, narSize, sigs) values (?, 'sha256:abc', 1, 10, ?)",
        (PATH, sigs),
    )
    await conn.commit()
    return conn


async def _sigs(conn: aiosqlite.Connection) -> str | None:
    async with conn.execute("select sigs from ValidPaths where path = ?", (PATH,)) as cursor:
        row = await cursor.fetchone()
    return None if row is None else row[0]


async def test_a_signature_is_written_to_a_path_that_has_none(tmp_path: Path) -> None:
    conn = await _db(tmp_path)
    assert await register.add_signatures(conn, PATH, [SIG_A]) is True
    assert await _sigs(conn) == "cache-a:YWJj"


async def test_the_signatures_merge_in_nix_s_order(tmp_path: Path) -> None:
    conn = await _db(tmp_path, sigs="cache-b:ZGVm")
    assert await register.add_signatures(conn, PATH, [SIG_A]) is True
    assert await _sigs(conn) == "cache-a:YWJj cache-b:ZGVm"


async def test_a_repeated_signature_changes_nothing(tmp_path: Path) -> None:
    conn = await _db(tmp_path, sigs="cache-a:YWJj")
    assert await register.add_signatures(conn, PATH, [SIG_A]) is True
    assert await _sigs(conn) == "cache-a:YWJj"


async def test_an_invalid_path_writes_nothing_and_says_so(tmp_path: Path) -> None:
    conn = await _db(tmp_path)
    missing = "/nix/store/00000000000000000000000000000002-gone"
    assert await register.add_signatures(conn, missing, [SIG_A]) is False
    assert await _sigs(conn) is None


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
    conn = await _db(tmp_path)
    changed: list[str] = []
    engine = _engine(tmp_path, changed)
    engine.db = _EngineDB(conn)  # type: ignore[assignment]  # a stand-in database
    await engine.add_signatures(PATH, [SIG_A, SIG_B])
    assert await _sigs(conn) == "cache-a:YWJj cache-b:ZGVm"
    assert changed == [PATH]


async def test_the_engine_refuses_an_invalid_path(tmp_path: Path) -> None:
    conn = await _db(tmp_path)
    changed: list[str] = []
    engine = _engine(tmp_path, changed)
    engine.db = _EngineDB(conn)  # type: ignore[assignment]  # a stand-in database
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
