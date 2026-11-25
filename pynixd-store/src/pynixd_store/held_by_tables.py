"""The paths a cold tier or a stable substituter holds, in the store database: `PynixdHeldBy`. nixidae/nixidae#1417.

A store is named by its key (`Store.held_key`), not its id: the id is local configuration, and the
key names the bucket or cache the answer came from.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Collection

    from .local_store_db import LocalStoreDB


class HeldByTables:
    def __init__(self, db: LocalStoreDB) -> None:
        self._db = db

    async def record(self, store: str, paths: Collection[str], at: int) -> None:
        if not paths:
            return
        async with self._db.transaction() as session:
            codec = session.sql.codec
            await session.execute(
                session.sql.held_by.record, {"store": store, "paths": codec.path_set(sorted(paths)), "at": at}
            )

    async def forget(self, store: str, paths: Collection[str]) -> None:
        if not paths:
            return
        async with self._db.transaction() as session:
            codec = session.sql.codec
            await session.execute(session.sql.held_by.forget, {"store": store, "paths": codec.path_set(sorted(paths))})

    async def held(self, stores: Collection[str], paths: Collection[str]) -> set[str]:
        """The paths of *paths* that any of *stores* holds."""
        if not stores or not paths:
            return set()
        async with self._db.transaction() as session:
            codec = session.sql.codec
            rows = await session.fetchall(
                session.sql.held_by.held,
                {"paths": codec.path_set(sorted(paths)), "stores": codec.path_set(sorted(stores))},
            )
        return {str(row[0]) for row in rows}

    async def uncovered(self, stores: Collection[str], after: int = 0) -> list[tuple[int, str]]:
        """`(id, path)` of each valid path above row *after* that none of *stores* holds, oldest first."""
        async with self._db.transaction() as session:
            rows = await session.fetchall(
                session.sql.held_by.uncovered,
                {"after": after, "stores": session.sql.codec.path_set(sorted(stores))},
            )
        return [(int(row[0]), str(row[1])) for row in rows]

    async def uncovered_of(self, stores: Collection[str], paths: Collection[str]) -> list[str]:
        """The valid paths of *paths* that none of *stores* holds, oldest first."""
        if not paths:
            return []
        async with self._db.transaction() as session:
            codec = session.sql.codec
            rows = await session.fetchall(
                session.sql.held_by.uncovered_of,
                {"paths": codec.path_set(sorted(paths)), "stores": codec.path_set(sorted(stores))},
            )
        return [str(row[0]) for row in rows]
