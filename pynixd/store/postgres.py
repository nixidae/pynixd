"""The store database on a PostgreSQL server (nixidae/nixidae#1209).

psycopg loads with this module, and `pynixd.store` loads it for the first store whose spec names `postgres`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio.to_thread

from pynixd_store import sql
from pynixd_store.database import DatabaseError
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.postgres_db import PostgresBackend
from pynixd_store.sqlite_db import holds_valid_paths

if TYPE_CHECKING:
    from pynixd_store.store_layout import StoreLayout

    from ..config import PostgresSpec


async def open_postgres_db(spec: PostgresSpec, layout: StoreLayout) -> LocalStoreDB:
    """Open the store database on the server that *spec* names, or raise `DatabaseError`.

    A server database with no valid path beside a `db.sqlite` that holds some is refused: a collection would
    find every path in the store dead and delete it.
    """
    backend = PostgresBackend(
        spec.conninfo.get_secret_value(),
        sql.POSTGRES,
        max_conns=spec.max_conns,
        open_seconds=spec.open_seconds,
    )
    db = await LocalStoreDB.open_backend(backend, layout)
    try:
        if await db.fetchone(db.sql.nix.any_valid_path, ()) is None and await anyio.to_thread.run_sync(
            holds_valid_paths, layout.db_path
        ):
            raise DatabaseError(
                f"{layout.db_path} holds valid paths and the PostgreSQL database holds none: pynixd does not "
                "move a store's paths to the server, and with an empty database it would collect them all"
            )
    except BaseException:
        await db.close()
        raise
    return db
