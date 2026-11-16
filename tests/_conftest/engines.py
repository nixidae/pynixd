"""One store database on each engine, and the PostgreSQL server's parse of a statement.

`store_db` runs a test once on SQLite and once on PostgreSQL. The SQLite file holds a Nix 2.35 store
with `ca-derivations` and `provenance`, the shape `postgres_schema` mirrors, and both engines hold
pynixd's tables at the newest version.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING

import psycopg
import pytest
from psycopg import pq

from pynixd_store.create_store import CA_SCHEMA, PROVENANCE_COLUMN, SCHEMA
from pynixd_store.db_migrations import PATH_ACCESS_TABLE
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.postgres_db import PostgresBackend
from pynixd_store.sql import POSTGRES

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


def sqlite_store(tmp_path: Path) -> LocalStoreDB:
    db_path = tmp_path / "db.sqlite"
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.executescript(SCHEMA)
        conn.executescript(CA_SCHEMA)
        conn.execute(PROVENANCE_COLUMN)
    return LocalStoreDB(db_path=db_path, store_path=None, read_only=False, reference_flush_interval=60.0)


def postgres_store(conninfo: str) -> LocalStoreDB:
    backend = PostgresBackend(conninfo, POSTGRES)
    return LocalStoreDB(None, None, read_only=False, reference_flush_interval=60.0, backend=backend)


@pytest.fixture(params=["sqlite", "postgres"])
async def store_db(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[LocalStoreDB]:
    """A store database with no path in it, opened and migrated: once on each engine."""
    if request.param == "sqlite":
        db = sqlite_store(tmp_path)
    else:
        db = postgres_store(request.getfixturevalue("postgres_conninfo"))
    try:
        await db.backend.probe()
        db.schema = await db.backend.migrate()
        assert db.schema.usable
        yield db
    finally:
        await db.close_db_pool()


async def access_times(db: LocalStoreDB) -> dict[str, int]:
    """Each `PynixdPathAccess` row as path and time: SQLite keys the table by path, PostgreSQL by `ValidPaths.id`."""
    if isinstance(db.backend, PostgresBackend):
        # `LEFT JOIN`: a row whose path is gone shows as an orphan, not as nothing.
        query = (
            f"SELECT COALESCE(v.path, 'orphan:' || a.pathId), a.lastReferencedAt FROM {PATH_ACCESS_TABLE} a"
            " LEFT JOIN ValidPaths v ON v.id = a.pathId"
        )
    else:
        query = f"SELECT path, lastReferencedAt FROM {PATH_ACCESS_TABLE}"
    return {str(path): int(stamp) for path, stamp in await db.fetchall(query)}


@dataclass(frozen=True, slots=True)
class Parameters:
    """What a statement binds: a count of positional parameters, or a set of names."""

    count: int = 0
    names: frozenset[str] = frozenset()


def postgres_parameters(conn: psycopg.Connection, name: str, statement: str) -> Parameters:
    """The server's parse of *statement*, or `psycopg.Error` with the server's message."""
    # psycopg's own conversion of `%s` to `$1`; no public call prepares a statement without its values.
    query, _formats, order, _parts = psycopg._queries._query2pg_nocache(statement.encode(), "utf-8")  # noqa: SLF001 -- see above
    result = conn.pgconn.prepare(name.encode(), query)
    if result.status != pq.ExecStatus.COMMAND_OK:
        raise psycopg.Error((result.error_message or b"").decode())
    count = conn.pgconn.describe_prepared(name.encode()).nparams
    return Parameters(names=frozenset(order)) if order else Parameters(count=count)


_SQLITE_PARAMETER = re.compile(r"\?(\d*)|(?<!:):([A-Za-z_]\w*)")


def sqlite_parameters(statement: str) -> Parameters:
    """What *statement* binds in SQLite: `?`, `?N` and `:name`."""
    found = _SQLITE_PARAMETER.findall(statement)
    names = frozenset(name for _number, name in found if name)
    if names:
        return Parameters(names=names)
    numbered = [int(number) for number, _name in found if number]
    return Parameters(count=max(numbered) if numbered else len(found))
