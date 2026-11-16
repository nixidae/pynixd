"""Only the store database's own modules import `sqlite3` or `aiosqlite`, and only its PostgreSQL engine imports psycopg.

The rest of pynixd reads the database through `LocalStoreDB` and `LocalStoreDB.nix`
(`pynixd_store/nix_tables.py`), holds a `Session` (`pynixd_store/database.py`), and
catches `DatabaseError`. A second database
behind the same methods (PostgreSQL, nixidae/nixidae#1209) then changes these
modules alone. nixidae/nixidae#1318.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
PACKAGES = (ROOT / "pynixd", ROOT / "pynixd-store" / "src" / "pynixd_store")
SQLITE = {"sqlite3", "aiosqlite"}
POSTGRES = {"psycopg", "psycopg_pool"}

ALLOWED = {
    "pynixd-store/src/pynixd_store/db_migrations.py": "the schema of pynixd's own tables",
    "pynixd-store/src/pynixd_store/create_store.py": "a new store's database, with Nix's schema",
    "pynixd-store/src/pynixd_store/sqlite_db.py": "the SQLite engine: its pool, its sessions and the session reader",
}

POSTGRES_ALLOWED = {
    "pynixd-store/src/pynixd_store/postgres_db.py": "the PostgreSQL engine: its pool and its sessions",
}


def imports_sqlite(source: str, drivers: set[str] = SQLITE) -> bool:
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import) and any(alias.name.split(".")[0] in drivers for alias in node.names):
            return True
        if isinstance(node, ast.ImportFrom) and node.level == 0 and (node.module or "").split(".")[0] in drivers:
            return True
    return False


def _found(drivers: set[str] = SQLITE) -> set[str]:
    return {
        str(path.relative_to(ROOT))
        for package in PACKAGES
        for path in package.rglob("*.py")
        if imports_sqlite(path.read_text(), drivers)
    }


def test_the_scanner_finds_each_form_of_import() -> None:
    assert imports_sqlite("import sqlite3")
    assert imports_sqlite("import aiosqlite as db")
    assert imports_sqlite("from aiosqlite import Connection")


def test_the_scanner_ignores_other_imports() -> None:
    assert not imports_sqlite("import sqlite_utils\nfrom .aiosqlite import x\n# import sqlite3")


def test_the_scanner_reads_the_packages() -> None:
    """The runner may work in a store copy; a scan that found nothing proves nothing."""
    assert all(len(list(package.rglob("*.py"))) > 10 for package in PACKAGES)


def test_only_the_database_modules_import_sqlite() -> None:
    found = _found()
    assert found - ALLOWED.keys() == set(), "read the database through LocalStoreDB, and catch DatabaseError"
    assert ALLOWED.keys() - found == set(), "a stale entry: the module imports no SQLite, so its permission goes too"


def test_the_scanner_finds_psycopg() -> None:
    assert imports_sqlite("from psycopg_pool import AsyncConnectionPool", POSTGRES)
    assert not imports_sqlite("import sqlite3", POSTGRES)


def test_only_the_postgres_engine_imports_psycopg() -> None:
    found = _found(POSTGRES)
    assert found - POSTGRES_ALLOWED.keys() == set(), "read the database through LocalStoreDB, and catch DatabaseError"
    assert POSTGRES_ALLOWED.keys() - found == set(), "a stale entry: the module imports no psycopg"
