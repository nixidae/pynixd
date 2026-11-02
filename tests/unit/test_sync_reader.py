"""The per-session read-only connection that answers `IsValidPath`.

A build sends one `IsValidPath` for every derivation of its closure, so the
query is the hot path of a build. Read through the pooled `aiosqlite`
connection, each one costs a thread hop; the synchronous reader removes it.
Measured on dynhetz: 59 us for the hop, 3.2 us for the query itself.

A closure query sends one `QueryPathInfo` per path instead, and it paid two
hops per path. The reader answers that too, and both readers land in one
response builder so the two paths cannot diverge.

The reader belongs to one client session, so a slow query of one session does
not wait behind the query of another. `DaemonProxy` creates it and closes it.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from cachetools import TTLCache

from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd.local_store_db import LocalStoreDB, SyncReader
from pynixd.serde import IsValidPathRequest, QueryPathInfoRequest, StorePath
from pynixd.store.engine import StoreEngine
from pynixd.store.local_db import LocalDBStore
from pynixd.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

HELLO = "/nix/store/00000000000000000000000000000001-hello"
LIBC = "/nix/store/00000000000000000000000000000002-libc"
GONE = "/nix/store/00000000000000000000000000000003-gone"


async def _no_settings() -> dict[str, Any]:
    raise AssertionError("a read answer reads no Nix setting")


def _store(db: LocalStoreDB, root: Path) -> LocalDBStore:
    """A store with what the read answers use, and no daemon: `__init__` would start one."""
    store = LocalDBStore.__new__(LocalDBStore)
    store.engine = StoreEngine(
        StoreLayout.chroot(root), writes=False, read_config=_no_settings, on_change=lambda _: None, name="test"
    )
    store.db = db
    store.path_info_cache = TTLCache[str, ValidPathInfo](maxsize=10000, ttl=300)
    return store


def _store_db(tmp_path: Path) -> Path:
    """A store database that holds `HELLO` and not `GONE`."""
    db_path = tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths ("
            "id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (1, ?, 0)", (HELLO,))
    return db_path


def _store_db_with_info(tmp_path: Path) -> Path:
    """A store database where `HELLO` carries path info and references `LIBC`."""
    db_path = tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths ("
            "id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, hash TEXT, registrationTime INTEGER, "
            "narSize INTEGER, ultimate INTEGER, sigs TEXT, ca TEXT)",
        )
        conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
        conn.execute(
            "INSERT INTO ValidPaths (id, path, deriver, hash, registrationTime, narSize, ultimate, sigs, ca)"
            " VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
            (HELLO, "", "sha256:deadbeef", 1700000000, 12345, 1, "cache.example-1:abc", ""),
        )
        conn.execute(
            "INSERT INTO ValidPaths (id, path, deriver, hash, registrationTime, narSize, ultimate, sigs, ca)"
            " VALUES (2, ?, ?, ?, ?, ?, ?, ?, ?)",
            (LIBC, "", "sha256:feedface", 1700000000, 999, 1, "", ""),
        )
        conn.execute("INSERT INTO Refs (referrer, reference) VALUES (1, 2)")
    return db_path


class TestTheReaderOverOneDatabase:
    def test_a_path_the_store_holds(self, tmp_path: Path) -> None:
        reader = SyncReader(_store_db(tmp_path))
        try:
            assert reader.is_valid_path(HELLO) is True
        finally:
            reader.close()

    def test_a_path_the_store_does_not_hold(self, tmp_path: Path) -> None:
        reader = SyncReader(_store_db(tmp_path))
        try:
            assert reader.is_valid_path(GONE) is False
        finally:
            reader.close()

    def test_a_database_that_is_not_there_reports_no_answer(self, tmp_path: Path) -> None:
        """`None` and not `False`. A store pynixd cannot read must fall back."""
        reader = SyncReader(tmp_path / "nowhere" / "db.sqlite")
        try:
            assert reader.is_valid_path(HELLO) is None
        finally:
            reader.close()

    def test_it_reads_again_after_a_close(self, tmp_path: Path) -> None:
        reader = SyncReader(_store_db(tmp_path))
        assert reader.is_valid_path(HELLO) is True
        reader.close()
        assert reader.is_valid_path(HELLO) is True
        reader.close()


@pytest.mark.anyio
class TestTheStoreUsesTheSessionReader:
    async def test_the_session_reader_answers_is_valid_path(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            store = _store(db, tmp_path)

            class Client:
                sync_reader = db.sync_reader()

            client = Client()
            try:
                held = await store.own_is_valid_path(IsValidPathRequest(path=StorePath(path=HELLO)), client=client)
                gone = await store.own_is_valid_path(IsValidPathRequest(path=StorePath(path=GONE)), client=client)
            finally:
                if client.sync_reader is not None:
                    client.sync_reader.close()

            assert held.valid is True
            assert gone.valid is False

    async def test_a_session_without_a_reader_still_answers(self, tmp_path: Path) -> None:
        """The pooled connection is the fallback, and it must stay correct."""
        _store_db(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            store = _store(db, tmp_path)

            held = await store.own_is_valid_path(IsValidPathRequest(path=StorePath(path=HELLO)), client=None)
            gone = await store.own_is_valid_path(IsValidPathRequest(path=StorePath(path=GONE)), client=None)

            assert held.valid is True
            assert gone.valid is False


@pytest.mark.anyio
class TestThePoolGivesOutAReader:
    async def test_an_active_database_gives_a_reader(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            reader = db.sync_reader()
            try:
                assert reader is not None
                assert reader.is_valid_path(HELLO) is True
            finally:
                assert reader is not None
                reader.close()

    async def test_an_inactive_database_gives_none(self, tmp_path: Path) -> None:
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            assert not db.active
            assert db.sync_reader() is None

    async def test_each_call_gives_its_own_connection(self, tmp_path: Path) -> None:
        """One reader for each client, so one session cannot block another."""
        _store_db(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            one = db.sync_reader()
            two = db.sync_reader()
            try:
                assert one is not None and two is not None
                assert one is not two
            finally:
                if one is not None:
                    one.close()
                if two is not None:
                    two.close()


class TestTheReaderQueryPathInfo:
    def test_a_path_info_row_with_its_references(self, tmp_path: Path) -> None:
        reader = SyncReader(_store_db_with_info(tmp_path))
        try:
            found = reader.query_path_info(HELLO)
            assert found is not None
            row, refs = found
            assert row is not None
            assert row[0] == HELLO
            assert refs == [LIBC]
        finally:
            reader.close()

    def test_a_path_the_store_does_not_hold(self, tmp_path: Path) -> None:
        """`(None, [])`: the read happened, so the caller answers invalid."""
        reader = SyncReader(_store_db_with_info(tmp_path))
        try:
            assert reader.query_path_info(GONE) == (None, [])
        finally:
            reader.close()

    def test_a_path_with_no_references_reads_one_row(self, tmp_path: Path) -> None:
        """The left join keeps the row and yields a null reference: no refs."""
        reader = SyncReader(_store_db_with_info(tmp_path))
        try:
            found = reader.query_path_info(LIBC)
            assert found is not None
            row, refs = found
            assert row is not None
            assert row[0] == LIBC
            assert refs == []
        finally:
            reader.close()

    def test_a_database_that_is_not_there_reports_no_answer(self, tmp_path: Path) -> None:
        """`None`: no read happened, so the caller falls back to the pool."""
        reader = SyncReader(tmp_path / "nowhere" / "db.sqlite")
        try:
            assert reader.query_path_info(HELLO) is None
        finally:
            reader.close()


@pytest.mark.anyio
class TestTheStoreUsesTheReaderForPathInfo:
    async def test_the_session_reader_answers_query_path_info(self, tmp_path: Path) -> None:
        _store_db_with_info(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            store = _store(db, tmp_path)

            class Client:
                sync_reader = db.sync_reader()

            client = Client()
            try:
                held = await store.own_query_path_info(QueryPathInfoRequest(path=StorePath(path=HELLO)), client=client)
                gone = await store.own_query_path_info(QueryPathInfoRequest(path=StorePath(path=GONE)), client=client)
            finally:
                if client.sync_reader is not None:
                    client.sync_reader.close()

            assert held.valid is True
            assert {str(r) for r in held.info.references} == {LIBC}
            assert str(held.info.nar_hash) != ""
            assert gone.valid is False

    async def test_the_reader_matches_the_pooled_connection(self, tmp_path: Path) -> None:
        """Both paths land in one builder, so their answers are identical."""
        _store_db_with_info(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            store = _store(db, tmp_path)

            class Client:
                sync_reader = db.sync_reader()

            client = Client()
            try:
                via_reader = await store.own_query_path_info(
                    QueryPathInfoRequest(path=StorePath(path=HELLO)), client=client
                )
            finally:
                if client.sync_reader is not None:
                    client.sync_reader.close()
            via_pool = await store.own_query_path_info(QueryPathInfoRequest(path=StorePath(path=HELLO)), client=None)

            assert via_reader.model_dump() == via_pool.model_dump()

    async def test_a_session_without_a_reader_still_answers(self, tmp_path: Path) -> None:
        """The pooled connection is the fallback, and it must stay correct."""
        _store_db_with_info(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            store = _store(db, tmp_path)

            held = await store.own_query_path_info(QueryPathInfoRequest(path=StorePath(path=HELLO)), client=None)
            gone = await store.own_query_path_info(QueryPathInfoRequest(path=StorePath(path=GONE)), client=None)

            assert held.valid is True
            assert {str(r) for r in held.info.references} == {LIBC}
            assert gone.valid is False
