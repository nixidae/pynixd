"""What the store database offers its callers, whichever engine holds it.

`LocalStoreDB` answers with these types, so no caller names a driver: a second
engine (PostgreSQL, nixidae/nixidae#1209) implements `Session` and raises
`DatabaseError`, and nothing above it changes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from .db_migrations import SchemaState
    from .sql import Statements
    from .sqlite_db import SyncReader

__all__ = ["Backend", "Database", "DatabaseError", "Params", "Row", "Session", "SyncSession"]


class DatabaseError(Exception):
    """The database refused a statement or a connection. The driver's own error is the `__cause__`."""


type Row = tuple[Any, ...]  # Any: a column's type is the schema's, and each caller unpacks its own statement.
type Params = Sequence[object] | Mapping[str, object]


class Session(Protocol):
    """One connection, held for one block.

    A write belongs in `transaction`. A write outside one is not kept: the
    session is for reads, and for TEMP tables that die with it.
    """

    @property
    def sql(self) -> Statements:
        """The statements of this session's engine and database."""
        ...

    def transaction(self) -> AbstractAsyncContextManager[None]:
        """One write transaction on this connection: committed when the block ends, rolled back when it raises."""
        ...

    async def fetchone(self, sql: str, params: Params = ()) -> Row | None: ...

    async def fetchall(self, sql: str, params: Params = ()) -> list[Row]: ...

    async def execute(self, sql: str, params: Params = ()) -> int:
        """Run a statement that answers no rows; the number of rows it changed."""
        ...

    async def executemany(self, sql: str, rows: Iterable[Params]) -> None: ...

    async def insert(self, sql: str, params: Params = ()) -> int:
        """Run one `INSERT`; the id of the row it added."""
        ...


class SyncSession(Protocol):
    """A `Session` for a worker thread: one connection of its own, no pool, no event loop."""

    @property
    def sql(self) -> Statements: ...

    def transaction(self) -> AbstractContextManager[None]: ...

    def fetchone(self, sql: str, params: Params = ()) -> Row | None: ...

    def fetchall(self, sql: str, params: Params = ()) -> list[Row]: ...

    def execute(self, sql: str, params: Params = ()) -> int: ...

    def executemany(self, sql: str, rows: Iterable[Params]) -> None: ...


class Database(Protocol):
    """The sessions that do not come from the pool: a long read, and a thread's own connection."""

    @property
    def sql(self) -> Statements: ...

    def snapshot(self) -> AbstractAsyncContextManager[Session]:
        """One read transaction on a connection of its own, held for the block.

        Every statement reads the same snapshot, and only TEMP tables take writes.
        The collector's plan holds it for tens of seconds, which no pooled connection may.
        """
        ...

    def sync_session(self, *, write: bool = False) -> AbstractContextManager[SyncSession]:
        """A connection for a worker thread, closed when the block ends. A write needs *write*."""
        ...


class Backend(Protocol):
    """The engine half of `LocalStoreDB`: the connections of one database, and what only the engine knows."""

    synchronous: str | None
    """`pragma synchronous` for a writing SQLite connection. An engine without the pragma ignores it."""

    @property
    def sql(self) -> Statements:
        """The statements of this database. `probe` may replace them."""
        ...

    def connection(self) -> AbstractAsyncContextManager[Session]: ...

    def database(self) -> Database: ...

    def sync_reader(self) -> SyncReader | None:
        """A synchronous reader for one client session, or `None` when the engine gives none."""
        ...

    async def probe(self) -> None:
        """Reach the database once. A database that refuses raises `DatabaseError`."""
        ...

    async def migrate(self) -> SchemaState:
        """Bring pynixd's tables to this pynixd's version, where the database allows it."""
        ...

    async def checkpoint_wal(self) -> bool:
        """`True` when a checkpoint ran; `False` when the engine or the database has none to run."""
        ...

    async def close(self) -> None: ...
