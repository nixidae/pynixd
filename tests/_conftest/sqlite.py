"""The SQLite engine under a `LocalStoreDB`, for a test that reads its pool."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pynixd_store.sqlite_db import SqliteBackend

if TYPE_CHECKING:
    from pynixd_store.local_store_db import LocalStoreDB


def sqlite_backend(db: LocalStoreDB) -> SqliteBackend:
    backend = db.backend
    assert isinstance(backend, SqliteBackend), backend
    return backend
