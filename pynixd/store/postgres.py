"""The store database on a PostgreSQL server (nixidae/nixidae#1209).

psycopg loads with this module, and `pynixd.store` loads it for the first store whose spec names `postgres`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pynixd_store import sql
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.postgres_db import PostgresBackend

if TYPE_CHECKING:
    from pynixd_store.store_layout import StoreLayout

    from ..config import PostgresSpec


async def open_postgres_db(spec: PostgresSpec, layout: StoreLayout) -> LocalStoreDB:
    """Open the store database on the server that *spec* names, or raise `DatabaseError`."""
    backend = PostgresBackend(
        spec.conninfo.get_secret_value(),
        sql.POSTGRES,
        max_conns=spec.max_conns,
        open_seconds=spec.open_seconds,
    )
    return await LocalStoreDB.open_backend(backend, layout)
