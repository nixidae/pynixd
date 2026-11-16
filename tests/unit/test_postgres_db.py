"""The session API of the store database, on PostgreSQL (`pynixd_store/postgres_db.py`).

The statements here are this file's own, over one table of its own; `test_postgres_statements.py`
and the tests of each statement group run `sql.POSTGRES`.
"""

from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING

import psycopg
import pytest

from pynixd_store.database import Backend, DatabaseError
from pynixd_store.postgres_db import PostgresBackend, PostgresDatabase
from pynixd_store.sql import POSTGRES

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

HELLO = "/nix/store/00000000000000000000000000000000-hello"
LIBC = "/nix/store/11111111111111111111111111111111-libc"

IS_VALID = "SELECT 1 FROM ValidPaths WHERE path = %s"
INSERT = "INSERT INTO ValidPaths (path, narSize) VALUES (%s, %s) RETURNING id"


@pytest.fixture
def valid_paths(postgres_conninfo: str) -> str:
    """A database with one table of the store's, and its conninfo."""
    with psycopg.connect(postgres_conninfo, autocommit=True) as conn:
        conn.execute('CREATE TABLE ValidPaths (id BIGSERIAL PRIMARY KEY, path TEXT COLLATE "C" UNIQUE, narSize BIGINT)')
    return postgres_conninfo


@pytest.fixture
async def backend(valid_paths: str) -> AsyncIterator[PostgresBackend]:
    backend = PostgresBackend(valid_paths, POSTGRES)
    await backend.open()
    try:
        yield backend
    finally:
        await backend.close()


def _idle_in_transaction(conninfo: str) -> int:
    with psycopg.connect(conninfo, autocommit=True) as conn:
        row = conn.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
            " AND state IN ('idle in transaction', 'idle in transaction (aborted)')"
        ).fetchone()
    assert row is not None
    return int(row[0])


async def test_a_written_path_reads_back(backend: PostgresBackend) -> None:
    async with backend.connection() as session, session.transaction():
        row_id = await session.insert(INSERT, (HELLO, 1 << 40))
    async with backend.connection() as session:
        assert await session.fetchone(IS_VALID, (HELLO,)) == (1,)
        assert await session.fetchone(IS_VALID, (LIBC,)) is None
        assert await session.fetchall("SELECT id, narSize FROM ValidPaths") == [(row_id, 1 << 40)]


async def test_a_failed_block_rolls_back(backend: PostgresBackend) -> None:
    with pytest.raises(RuntimeError, match="stop"):
        async with backend.connection() as session, session.transaction():
            await session.insert(INSERT, (HELLO, 1))
            raise RuntimeError("stop")
    async with backend.connection() as session:
        assert await session.fetchone(IS_VALID, (HELLO,)) is None


async def test_a_refused_statement_is_a_database_error(backend: PostgresBackend) -> None:
    async with backend.connection() as session, session.transaction():
        await session.insert(INSERT, (HELLO, 1))
    with pytest.raises(DatabaseError, match="duplicate key"):
        async with backend.connection() as session, session.transaction():
            await session.insert(INSERT, (HELLO, 1))
    async with backend.connection() as session:
        assert await session.execute("UPDATE ValidPaths SET narSize = 2") == 1


async def test_a_read_leaves_no_transaction_open(backend: PostgresBackend, postgres_conninfo: str) -> None:
    async with backend.connection() as session:
        await session.fetchone(IS_VALID, (HELLO,))
        assert _idle_in_transaction(postgres_conninfo) == 0


async def test_a_snapshot_reads_one_state_and_holds_temporary_tables(
    backend: PostgresBackend, postgres_conninfo: str
) -> None:
    async with backend.database().snapshot() as snapshot:
        await snapshot.execute("CREATE TEMP TABLE seen (path TEXT)")
        assert await snapshot.fetchone(IS_VALID, (HELLO,)) is None
        with psycopg.connect(postgres_conninfo, autocommit=True) as other:
            other.execute(INSERT, (HELLO, 1))
        assert await snapshot.fetchone(IS_VALID, (HELLO,)) is None
    async with backend.connection() as session:
        assert await session.fetchone(IS_VALID, (HELLO,)) == (1,)


def test_a_thread_session_writes_only_when_asked(valid_paths: str) -> None:
    database = PostgresDatabase(valid_paths, POSTGRES)
    with pytest.raises(DatabaseError, match="read-only transaction"), database.sync_session() as session:
        session.execute(INSERT, (HELLO, 1))
    with database.sync_session(write=True) as session, session.transaction():
        session.executemany(INSERT, [(HELLO, 1), (LIBC, 2)])
    with database.sync_session() as session:
        assert session.fetchall("SELECT path FROM ValidPaths ORDER BY path") == [(HELLO,), (LIBC,)]


async def test_a_closed_pool_is_a_database_error(backend: PostgresBackend) -> None:
    await backend.close()
    with pytest.raises(DatabaseError):
        async with backend.connection() as session:
            await session.fetchone(IS_VALID, (HELLO,))


async def test_the_engine_answers_as_a_backend(postgres_conninfo: str) -> None:
    backend: Backend = PostgresBackend(postgres_conninfo, POSTGRES)
    try:
        await backend.probe()
        assert (await backend.migrate()).usable
        assert backend.sync_reader() is None
        assert await backend.checkpoint_wal() is False
        async with backend.connection() as session:
            assert await session.fetchone("SELECT count(*) FROM ValidPaths") == (0,)
    finally:
        await backend.close()


async def test_an_unreachable_server_fails_the_open() -> None:
    with tempfile.TemporaryDirectory(prefix="pynixd-pg-") as nowhere:
        backend = PostgresBackend(psycopg.conninfo.make_conninfo(host=nowhere, dbname="x"), POSTGRES, open_seconds=1.0)
        with pytest.raises(DatabaseError):
            await backend.open()
        await backend.close()
