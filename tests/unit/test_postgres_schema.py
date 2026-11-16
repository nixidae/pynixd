"""The PostgreSQL store schema has SQLite's tables, columns and indexes (`pynixd_store/postgres_schema.py`).

The SQLite side is a Nix 2.35 store with `ca-derivations` and `provenance`, plus pynixd's tables
migrated to the newest version. Names compare without case: PostgreSQL folds an unquoted name to
lower case.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import psycopg
import pytest

from pynixd_store.create_store import CA_SCHEMA, PROVENANCE_COLUMN, SCHEMA
from pynixd_store.db_migrations import TARGET_VERSION, apply_migrations
from pynixd_store.postgres_db import PostgresBackend
from pynixd_store.postgres_schema import BASE_VERSION, migrate
from pynixd_store.sql import POSTGRES

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

type Columns = dict[str, dict[str, bool]]
"""Each table's columns, and whether the column takes `NULL`."""

_SQLITE_ONLY_TABLES = {
    "sqlite_sequence": "SQLite's own counter of `AUTOINCREMENT`",
}


@pytest.fixture
async def backend(postgres_conninfo: str) -> AsyncIterator[PostgresBackend]:
    backend = PostgresBackend(postgres_conninfo, POSTGRES)
    await backend.open()
    try:
        yield backend
    finally:
        await backend.close()


async def _sqlite_store(tmp_path: Path) -> Path:
    db = tmp_path / "db.sqlite"
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.executescript(SCHEMA)
        conn.executescript(CA_SCHEMA)
        conn.execute(PROVENANCE_COLUMN)
    assert (await apply_migrations(db, read_only=False)).usable
    return db


def _sqlite_columns(db: Path) -> Columns:
    found: Columns = {}
    with closing(sqlite3.connect(db)) as conn:
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
        for table in tables:
            if table in _SQLITE_ONLY_TABLES:
                continue
            # A primary key takes no `NULL` in PostgreSQL; SQLite's `notnull` says only what the DDL spelled.
            found[table.lower()] = {
                str(name).lower(): not (notnull or pk)
                for _cid, name, _type, notnull, _default, pk in conn.execute(f"PRAGMA table_info('{table}')")
            }
    return found


def _sqlite_indexes(db: Path) -> set[str]:
    with closing(sqlite3.connect(db)) as conn:
        return {
            str(row[0]).lower()
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL")
        }


def _postgres_columns(conninfo: str) -> Columns:
    found: Columns = {}
    with psycopg.connect(conninfo) as conn:
        for table, column, nullable in conn.execute(
            "SELECT table_name, column_name, is_nullable FROM information_schema.columns"
            " WHERE table_schema = current_schema()"
        ):
            found.setdefault(table, {})[column] = nullable == "YES"
    return found


def _postgres_indexes(conninfo: str) -> set[str]:
    """The indexes a statement made, not the ones a primary key or `UNIQUE` made."""
    with psycopg.connect(conninfo) as conn:
        return {
            str(row[0])
            for row in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()"
                " AND indexname NOT IN (SELECT conname FROM pg_constraint)"
            )
        }


def _text_columns_not_bytewise(conninfo: str) -> set[str]:
    with psycopg.connect(conninfo) as conn:
        return {
            f"{table}.{column}"
            for table, column in conn.execute(
                "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = current_schema()"
                " AND data_type = 'text' AND collation_name IS DISTINCT FROM 'C'"
            )
        }


def test_the_postgres_tables_start_at_the_newest_sqlite_version() -> None:
    """A new SQLite migration needs its PostgreSQL twin in `postgres_schema.py`."""
    assert BASE_VERSION == TARGET_VERSION


async def test_both_engines_have_the_same_tables_and_columns(
    backend: PostgresBackend, postgres_conninfo: str, tmp_path: Path
) -> None:
    async with backend.connection() as session:
        assert (await migrate(session)).usable
    sqlite_db = await _sqlite_store(tmp_path)

    sqlite, postgres = _sqlite_columns(sqlite_db), _postgres_columns(postgres_conninfo)
    assert sqlite.keys() - postgres.keys() == set(), "a table PostgreSQL lacks"
    assert postgres.keys() - sqlite.keys() == set(), "a table SQLite lacks"
    for table, columns in sqlite.items():
        assert columns == postgres[table], table
    indexes = _sqlite_indexes(sqlite_db)
    assert {"indexreference", "idx_pynixd_live_epoch"} <= indexes, "the scan found the indexes"
    assert indexes == _postgres_indexes(postgres_conninfo)


async def test_every_text_column_compares_bytewise(backend: PostgresBackend, postgres_conninfo: str) -> None:
    async with backend.connection() as session:
        await migrate(session)
    assert _text_columns_not_bytewise(postgres_conninfo) == set()


async def test_a_second_migration_finds_the_tables(backend: PostgresBackend) -> None:
    async with backend.connection() as session:
        first = await migrate(session)
        second = await migrate(session)
    assert (first.usable, first.applied) == (True, ("base",))
    assert (second.usable, second.applied, second.version) == (True, (), BASE_VERSION)


async def test_another_version_is_unusable_and_untouched(backend: PostgresBackend) -> None:
    async with backend.connection() as session:
        await migrate(session)
        await session.execute("UPDATE PynixdSchema SET version = %s", (BASE_VERSION + 1,))
        state = await migrate(session)
        assert await session.fetchone("SELECT version FROM PynixdSchema") == (BASE_VERSION + 1,)
    assert not state.usable
    assert state.reason is not None
    assert str(BASE_VERSION + 1) in state.reason


async def test_a_path_that_names_itself_deletes(backend: PostgresBackend) -> None:
    """Nix's `DeleteSelfRefs`: without it, the restriction on `Refs.reference` refuses the delete."""
    async with backend.connection() as session:
        await migrate(session)
        async with session.transaction():
            path_id = await session.insert(
                "INSERT INTO ValidPaths (path, hash, registrationTime) VALUES (%s, %s, %s) RETURNING id",
                ("/nix/store/00000000000000000000000000000000-self", "sha256:0", 0),
            )
            await session.execute("INSERT INTO Refs (referrer, reference) VALUES (%s, %s)", (path_id, path_id))
        assert await session.execute("DELETE FROM ValidPaths WHERE id = %s", (path_id,)) == 1
        assert await session.fetchone("SELECT count(*) FROM Refs") == (0,)
