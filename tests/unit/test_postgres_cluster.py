"""The PostgreSQL server of the store tests answers, and collates as a store's server can."""

from __future__ import annotations

import psycopg


def test_a_test_database_answers(postgres_conninfo: str) -> None:
    with psycopg.connect(postgres_conninfo) as conn:
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_the_cluster_does_not_collate_bytewise(postgres_conninfo: str) -> None:
    """A path column without `COLLATE "C"` then sorts unlike `ValidPaths` in SQLite, and a test can see it."""
    with psycopg.connect(postgres_conninfo) as conn:
        assert conn.execute("SELECT 'a' < 'B', 'a' COLLATE \"C\" < 'B'").fetchone() == (True, False)
