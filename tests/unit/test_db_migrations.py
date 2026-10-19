"""pynixd versions the tables it adds to a Nix store database.

Before `pynixd.db_migrations`, the answer to every schema question was `DROP
TABLE IF EXISTS DerivationStats`, on each start. It is a migration that always
works and always loses the data, and the data was the point: the scheduler
asks `get_build_stats_hint` how long a derivation took last time, so a
statistics table that empties on each start answers "no record" forever.

These tests state what the framework must do before a second table is worth
adding. Issue nixidae/nixidae#205 wants that second table.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import aiosqlite
import pytest

from pynixd import db_migrations
from pynixd.db_migrations import (
    DERIVATION_STATS_TABLE,
    SCHEMA_TABLE,
    TABLE_PREFIX,
    TARGET_VERSION,
    Migration,
    apply_migrations,
    expected_tables,
)
from pynixd.local_store_db import LocalStoreDB
from pynixd.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path


def _nix_database(tmp_path: Path) -> Path:
    """A store database with the one Nix table that `LocalStoreDB.open` probes."""
    db_path = tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute("CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL)")
    return db_path


async def _tables(db_path: Path) -> set[str]:
    async with (
        aiosqlite.connect(db_path) as conn,
        conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'") as cursor,
    ):
        return {str(row[0]) for row in await cursor.fetchall()}


async def _recorded_version(db_path: Path) -> int:
    async with aiosqlite.connect(db_path) as conn, conn.execute(f"SELECT version FROM {SCHEMA_TABLE}") as cursor:
        row = await cursor.fetchone()
    return int(row[0]) if row else 0


class TestTheShapeOfTheMigrationList:
    """Rules a new migration must keep. They cost nothing to check here."""

    def test_the_versions_count_from_one_with_no_gaps(self) -> None:
        assert db_migrations.migration_versions() == list(range(1, TARGET_VERSION + 1))

    def test_every_table_pynixd_creates_carries_the_prefix(self) -> None:
        """The prefix is what tells pynixd's tables from Nix's in one file."""
        for migration in db_migrations.MIGRATIONS:
            for table in migration.creates:
                assert table.startswith(TABLE_PREFIX), (
                    f"migration {migration.name} creates {table!r}, which Nix could also name"
                )

    def test_no_migration_writes_to_a_table_of_nix(self) -> None:
        """A migration owns pynixd's tables. Nix owns `ValidPaths` and the rest."""
        owned_by_nix = ("ValidPaths", "Refs", "DerivationOutputs", "Realisations", "RealisationsRefs")
        for migration in db_migrations.MIGRATIONS:
            for statement in migration.statements:
                for table in owned_by_nix:
                    assert f" {table}" not in statement, (
                        f"migration {migration.name} names {table}, which belongs to Nix"
                    )

    def test_expected_tables_folds_the_creates_and_the_drops(self) -> None:
        assert expected_tables(0) == frozenset({SCHEMA_TABLE})
        assert DERIVATION_STATS_TABLE in expected_tables(TARGET_VERSION)


@pytest.mark.anyio
class TestMigratingAStoreDatabase:
    async def test_a_database_with_no_pynixd_tables_reaches_the_current_version(self, tmp_path: Path) -> None:
        db_path = _nix_database(tmp_path)

        state = await apply_migrations(db_path, read_only=False)

        assert state.usable
        assert state.version == TARGET_VERSION
        assert state.applied == tuple(m.name for m in db_migrations.MIGRATIONS)
        assert expected_tables(TARGET_VERSION) <= await _tables(db_path)

    async def test_the_second_start_applies_nothing(self, tmp_path: Path) -> None:
        """The defect this replaces: the first version ran its DDL every time."""
        db_path = _nix_database(tmp_path)
        await apply_migrations(db_path, read_only=False)

        state = await apply_migrations(db_path, read_only=False)

        assert state.usable
        assert state.applied == ()

    async def test_the_tables_of_nix_are_left_alone(self, tmp_path: Path) -> None:
        db_path = _nix_database(tmp_path)
        await apply_migrations(db_path, read_only=False)
        assert "ValidPaths" in await _tables(db_path)

    async def test_a_table_the_old_pynixd_dropped_comes_back(self, tmp_path: Path) -> None:
        """Run an older pynixd against a migrated database and a table goes.

        That version drops and recreates its own table on each start, and it
        does not know the version column, so the file ends up claiming a table
        it no longer holds. The next start must notice and rebuild, or it
        queries a table that is not there.
        """
        db_path = _nix_database(tmp_path)
        await apply_migrations(db_path, read_only=False)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(f"DROP TABLE {DERIVATION_STATS_TABLE}")
            await conn.commit()

        state = await apply_migrations(db_path, read_only=False)

        assert state.usable
        assert state.applied != (), "the repair must run the migrations again"
        assert DERIVATION_STATS_TABLE in await _tables(db_path)

    async def test_the_unprefixed_table_of_the_first_version_is_removed(self, tmp_path: Path) -> None:
        db_path = _nix_database(tmp_path)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE DerivationStats (pname TEXT PRIMARY KEY)")
            await conn.commit()

        await apply_migrations(db_path, read_only=False)

        assert "DerivationStats" not in await _tables(db_path)

    async def test_a_version_from_a_newer_pynixd_is_refused(self, tmp_path: Path) -> None:
        """Migrating down is guessing. Report it and run without the tables."""
        db_path = _nix_database(tmp_path)
        await apply_migrations(db_path, read_only=False)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(f"UPDATE {SCHEMA_TABLE} SET version = ?", (TARGET_VERSION + 5,))
            await conn.commit()

        state = await apply_migrations(db_path, read_only=False)

        assert not state.usable
        assert state.version == TARGET_VERSION + 5
        assert "newer pynixd" in (state.reason or "")
        assert await _recorded_version(db_path) == TARGET_VERSION + 5

    async def test_a_database_that_is_not_there_is_not_fatal(self, tmp_path: Path) -> None:
        state = await apply_migrations(tmp_path / "absent.sqlite", read_only=False)

        assert not state.usable
        assert state.reason is not None


@pytest.mark.anyio
class TestAStoreDatabasePynixdMayNotWrite:
    async def test_a_current_read_only_database_is_usable(self, tmp_path: Path) -> None:
        db_path = _nix_database(tmp_path)
        await apply_migrations(db_path, read_only=False)

        state = await apply_migrations(db_path, read_only=True)

        assert state.usable
        assert state.version == TARGET_VERSION

    async def test_an_old_read_only_database_is_not(self, tmp_path: Path) -> None:
        """There is no way to migrate it, so the tables are off for this run."""
        db_path = _nix_database(tmp_path)

        state = await apply_migrations(db_path, read_only=True)

        assert not state.usable
        assert state.version == 0
        assert "read-only" in (state.reason or "")

    async def test_a_read_only_database_gains_no_table(self, tmp_path: Path) -> None:
        db_path = _nix_database(tmp_path)

        await apply_migrations(db_path, read_only=True)

        assert SCHEMA_TABLE not in await _tables(db_path)


@pytest.mark.anyio
class TestAMigrationThatFails:
    """One transaction holds every step, so a failure changes nothing."""

    @staticmethod
    def _with_a_broken_second_step(monkeypatch: pytest.MonkeyPatch) -> None:
        migrations = (
            *db_migrations.MIGRATIONS,
            Migration(
                version=TARGET_VERSION + 1,
                name="broken",
                statements=("CREATE TABLE PynixdGood (x INTEGER)", "THIS IS NOT SQL"),
                creates=("PynixdGood",),
            ),
        )
        monkeypatch.setattr(db_migrations, "MIGRATIONS", migrations)
        monkeypatch.setattr(db_migrations, "TARGET_VERSION", TARGET_VERSION + 1)

    async def test_the_recorded_version_does_not_move(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        db_path = _nix_database(tmp_path)
        await apply_migrations(db_path, read_only=False)
        self._with_a_broken_second_step(monkeypatch)

        state = await apply_migrations(db_path, read_only=False)

        assert not state.usable
        assert await _recorded_version(db_path) == TARGET_VERSION

    async def test_the_half_of_the_step_that_ran_is_rolled_back(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db_path = _nix_database(tmp_path)
        await apply_migrations(db_path, read_only=False)
        self._with_a_broken_second_step(monkeypatch)

        await apply_migrations(db_path, read_only=False)

        assert "PynixdGood" not in await _tables(db_path)


@pytest.mark.anyio
class TestBuildStatisticsSurviveARestart:
    """The reason the framework exists, stated at the caller.

    `LocalStoreDB.open` used to drop the statistics table here.
    """

    async def test_a_recorded_duration_is_still_there_after_a_reopen(self, tmp_path: Path) -> None:
        _nix_database(tmp_path)

        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            assert db.schema.usable, db.schema.reason
            await db.record_build_stats(
                pname="hello",
                platform="aarch64-linux",
                derivation_json="{}",
                cpu_user_us=None,
                cpu_system_us=None,
                duration_ms=1234,
            )

        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as reopened:
            assert await reopened.get_build_stats_hint("hello", "aarch64-linux") == 1234

    async def test_the_statistics_are_off_when_the_schema_is_not_usable(self, tmp_path: Path) -> None:
        """A store whose tables pynixd cannot bring up must not query them."""
        _nix_database(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.schema = db_migrations.SchemaState(version=0, usable=False, reason="test")
            assert await db.get_build_stats_hint("hello", "aarch64-linux") is None
