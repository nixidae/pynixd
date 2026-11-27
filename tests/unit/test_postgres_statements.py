"""The PostgreSQL server parses each twin in `pynixd_store/sql`, and each binds what its SQLite twin binds.

A twin that names a column or a table the schema lacks, or that takes another parameter, fails here and
not in a client's request. What a statement answers is the work of each group's own test on both
engines (`test_nix_statements.py` and the next ones).
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import psycopg
import pytest

from pynixd_store.gc import SPACE_TIME
from pynixd_store.postgres_db import PostgresBackend
from pynixd_store.postgres_schema import migrate
from pynixd_store.sql import (
    POSTGRES,
    SQLITE,
    gc,
    held_by,
    liveness,
    nix,
    observations,
    path_access,
    placement,
    provisioning,
)
from tests._conftest.engines import postgres_parameters, sqlite_parameters

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

_NO_REALISATIONS = "a PostgreSQL store keys realisations by path, so it has no `Realisations`"

GROUPS: dict[str, tuple[object, object]] = {
    "nix": (nix.POSTGRES, nix.sqlite(provenance=True)),
    "path_access": (path_access.POSTGRES, path_access.SQLITE),
    "observations": (observations.POSTGRES, observations.SQLITE),
    "placement": (placement.POSTGRES, placement.SQLITE),
    "provisioning": (provisioning.POSTGRES, provisioning.SQLITE),
    "gc": (gc.POSTGRES, gc.SQLITE),
    "held_by": (held_by.POSTGRES, held_by.SQLITE),
    "liveness": (liveness.POSTGRES, liveness.SQLITE),
}
"""Each group's PostgreSQL twins and the SQLite statements they mirror."""

REFUSED: dict[str, dict[str, str]] = {
    "nix": {
        "realisation_by_hash": _NO_REALISATIONS,
        "register_realisation_by_hash": _NO_REALISATIONS,
        "update_realisation_by_hash": _NO_REALISATIONS,
    },
}
"""The statements the server refuses on purpose, and why."""

SETUP: dict[str, tuple[str, ...]] = {
    "gc": gc.POSTGRES.schema,
    "liveness": (liveness.POSTGRES.create_live_now, liveness.POSTGRES.create_root_hits),
}
"""What a group's statements need on their connection first: the TEMP tables of the session."""


def _filled(group: object, statement: str) -> str:
    """*statement* with what a caller fills in: one of the collector's tables, the default delete order."""
    if isinstance(group, gc.GcSql):
        statement = statement.replace("{score}", group.score(SPACE_TIME))
    return statement.replace("{table}", "gc_live")


def _statements(group: object) -> dict[str, str]:
    """The group's statements, each filled as its caller fills it."""
    assert dataclasses.is_dataclass(group)
    return {f.name: _filled(group, getattr(group, f.name)) for f in dataclasses.fields(group) if f.type == "str"}


def test_the_postgres_set_holds_every_twin() -> None:
    """`sql.POSTGRES` holds each group's twins, and no statement of SQLite's."""
    for group, (postgres, _sqlite) in GROUPS.items():
        assert getattr(POSTGRES, group) is postgres, group
        assert getattr(POSTGRES, group) is not getattr(SQLITE, group), group


@pytest.fixture
async def schema(postgres_conninfo: str) -> AsyncIterator[str]:
    backend = PostgresBackend(postgres_conninfo, POSTGRES)
    await backend.open()
    try:
        async with backend.connection() as session:
            assert (await migrate(session)).usable
    finally:
        await backend.close()
    yield postgres_conninfo


def test_every_group_is_listed() -> None:
    """A group of `Statements` with no entry here would go unparsed."""
    # `codec` binds values; it holds no statement.
    assert GROUPS.keys() == {f.name for f in dataclasses.fields(SQLITE) if f.name != "codec"}
    assert REFUSED.keys() <= GROUPS.keys()
    assert SETUP.keys() <= GROUPS.keys()


@pytest.mark.parametrize("group", GROUPS)
async def test_the_server_parses_every_statement(schema: str, group: str) -> None:
    postgres, sqlite = (_statements(statements) for statements in GROUPS[group])
    assert len(postgres) >= 2, "the scan found the statements"
    assert postgres.keys() == sqlite.keys()
    expected = REFUSED.get(group, {})
    assert expected.keys() <= postgres.keys(), "a stale entry"

    refused: dict[str, str] = {}
    with psycopg.connect(schema, autocommit=True) as conn:
        for statement in SETUP.get(group, ()):
            conn.execute(statement)
        for name, statement in postgres.items():
            try:
                parameters = postgres_parameters(conn, name, statement)
            except psycopg.Error as exc:
                refused[name] = str(exc)
                continue
            assert parameters == sqlite_parameters(sqlite[name]), name
    assert refused.keys() == expected.keys(), refused
