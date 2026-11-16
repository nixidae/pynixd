"""What the store database offers its callers, whichever engine holds it.

`LocalStoreDB` answers with these types, so no caller names a driver: a second
engine (PostgreSQL, nixidae/nixidae#1209) implements `Session` and raises
`DatabaseError`, and nothing above it changes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from contextlib import AbstractAsyncContextManager

    from .sql import Statements

__all__ = ["DatabaseError", "Params", "Row", "Session"]


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
