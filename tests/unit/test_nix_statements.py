"""Nix's statements (`pynixd_store/sql/nix.py`) on both engines: one store, the same answers.

SQLite holds a Nix 2.35 store with `ca-derivations` and `provenance`, the shape `postgres_schema`
mirrors. The PostgreSQL server also parses every statement of its set, so a twin that names a
column or a table the schema lacks fails here, and not in a client's request.
"""

from __future__ import annotations

import dataclasses
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import psycopg
import pytest
from psycopg import pq

from nix_daemon_protocol.signature import Signature
from pynixd_store import register
from pynixd_store.create_store import CA_SCHEMA, PROVENANCE_COLUMN, SCHEMA
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.postgres_db import PostgresBackend
from pynixd_store.postgres_schema import migrate
from pynixd_store.sql import SQLITE, nix

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

LIBC = "/nix/store/00000000000000000000000000000001-libc"
HELLO = "/nix/store/00000000000000000000000000000002-hello"
DRV = "/nix/store/00000000000000000000000000000003-hello.drv"
ABSENT = "/nix/store/00000000000000000000000000000009-absent"
HASH = "sha256:" + "1" * 64
OTHER_HASH = "sha256:" + "2" * 64
SIG_A = Signature("cache-a:YWJj")
SIG_B = Signature("cache-b:ZGVm")

_ABSENT_TABLES = {
    "realisation_by_hash": "a PostgreSQL store keys realisations by path, so it has no `Realisations`",
    "register_realisation_by_hash": "a PostgreSQL store keys realisations by path, so it has no `Realisations`",
    "update_realisation_by_hash": "a PostgreSQL store keys realisations by path, so it has no `Realisations`",
}


def _sqlite(tmp_path: Path) -> LocalStoreDB:
    db_path = tmp_path / "db.sqlite"
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.executescript(SCHEMA)
        conn.executescript(CA_SCHEMA)
        conn.execute(PROVENANCE_COLUMN)
    return LocalStoreDB(db_path=db_path, store_path=None, read_only=False, reference_flush_interval=60.0)


def _postgres(conninfo: str) -> LocalStoreDB:
    backend = PostgresBackend(conninfo, dataclasses.replace(SQLITE, nix=nix.POSTGRES))
    return LocalStoreDB(None, None, read_only=False, reference_flush_interval=60.0, backend=backend)


@pytest.fixture(params=["sqlite", "postgres"])
async def db(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[LocalStoreDB]:
    db = _sqlite(tmp_path) if request.param == "sqlite" else _postgres(request.getfixturevalue("postgres_conninfo"))
    try:
        await db.backend.probe()
        assert (await db.backend.migrate()).usable
        yield db
    finally:
        await db.close_db_pool()


def _registration(
    path: str, references: frozenset[str] = frozenset(), outputs: dict[str, str] | None = None
) -> register.Registration:
    return register.Registration(
        path=path,
        nar_hash=HASH,
        registration_time=1_700_000_000,
        deriver=DRV if path == HELLO else None,
        nar_size=1 << 33,
        ultimate=1 if path == HELLO else None,
        sigs=SIG_B.to_str() if path == HELLO else None,
        ca=None,
        references=references,
        derivation_outputs=outputs or {},
        provenance='{"type":"test"}' if path == HELLO else None,
    )


async def _store(db: LocalStoreDB) -> None:
    await db.nix.register_valid_paths(
        [
            _registration(LIBC),
            _registration(HELLO, frozenset({LIBC, HELLO})),
            _registration(DRV, outputs={"out": HELLO}),
        ]
    )


async def test_the_reads_answer_what_the_registration_wrote(db: LocalStoreDB) -> None:
    await _store(db)
    assert db.sql.nix.provenance

    info = await db.nix.path_info(HELLO)
    assert info is not None
    assert {str(r) for r in info.info.references} == {LIBC, HELLO}
    assert (str(info.info.deriver), info.info.nar_size, info.info.ultimate) == (DRV, 1 << 33, True)
    assert info.info.provenance == '{"type":"test"}'
    assert await db.nix.path_info(ABSENT) is None

    assert await db.nix.is_valid_path(LIBC)
    assert not await db.nix.is_valid_path(ABSENT)
    assert {str(p) for p in await db.nix.all_valid_paths()} == {LIBC, HELLO, DRV}
    assert {str(p) for p in await db.nix.valid_paths([LIBC, ABSENT])} == {LIBC}
    assert {str(p) for p in await db.nix.referrers(LIBC)} == {HELLO}
    assert {str(p) for p in await db.nix.valid_derivers(HELLO)} == {DRV}
    assert {str(p) for p in await db.nix.closure([HELLO])} == {LIBC, HELLO}
    closure = await db.nix.closure_with_info([HELLO])
    assert [str(i.path) for i in closure] == [LIBC, HELLO]
    assert {str(r) for r in closure[1].info.references} == {LIBC, HELLO}
    infos = {str(i.path): i for i in await db.nix.path_infos([HELLO, LIBC, ABSENT])}
    assert infos.keys() == {LIBC, HELLO}
    assert {str(r) for r in infos[HELLO].info.references} == {LIBC, HELLO}

    outputs = await db.nix.derivation_outputs(DRV)
    assert outputs is not None
    assert {name: str(path) for name, path in outputs.items()} == {"out": HELLO}
    assert await db.nix.derivation_outputs(LIBC) == {}
    assert await db.nix.derivation_outputs(ABSENT) is None
    maps = await db.nix.derivation_output_maps([DRV, LIBC])
    assert {str(d): {n: str(p) for n, p in m.items()} for d, m in maps.items()} == {DRV: {"out": HELLO}}

    lower = "/nix/store/00000000000000000000000000000002"
    assert str(await db.nix.path_in_range(lower, lower + "\x7f")) == HELLO
    assert await db.nix.path_in_range(lower + "-z", lower + "\x7f") is None
    assert await db.nix.path_from_nar_hash(HASH) is not None
    assert await db.nix.path_from_nar_hash(OTHER_HASH) is None


async def test_the_writes_change_what_the_reads_answer(db: LocalStoreDB) -> None:
    await _store(db)
    await _store(db)
    assert len(await db.nix.all_valid_paths()) == 3

    assert await db.nix.add_signatures(HELLO, [SIG_A])
    assert not await db.nix.add_signatures(ABSENT, [SIG_A])
    info = await db.nix.path_info(HELLO)
    assert info is not None
    assert {sig.to_str() for sig in info.info.sigs} == {SIG_A.to_str(), SIG_B.to_str()}

    await db.nix.update_path_hashes(LIBC, nar_hash=OTHER_HASH, nar_size=7)
    libc = await db.nix.path_info(LIBC)
    assert libc is not None
    assert (libc.info.nar_hash.hash, libc.info.nar_size) == (OTHER_HASH.removeprefix("sha256:"), 7)

    trace = DRV.removeprefix("/nix/store/")
    await db.nix.register_drv_output(trace, "out", HELLO, [SIG_B], by_path=True)
    await db.nix.register_drv_output(trace, "out", HELLO, [SIG_A], by_path=True)
    found = await db.nix.realisation(trace, "out", by_path=True)
    assert found is not None
    assert (str(found[0]), found[1]) == (HELLO, [SIG_A.to_str(), SIG_B.to_str()])
    assert await db.nix.realisation(trace, "dev", by_path=True) is None

    await db.nix.invalidate_path(DRV)
    assert not await db.nix.is_valid_path(DRV)
    assert await db.nix.valid_derivers(HELLO) == set()


def _prepare(conn: psycopg.Connection, name: str, statement: str) -> int:
    """The server's parse of *statement*: its parameter count, or `psycopg.Error` with the server's message."""
    # psycopg's own conversion of `%s` to `$1`; no public call prepares a statement without its values.
    query, *_ = psycopg._queries._query2pg_nocache(statement.encode(), "utf-8")  # noqa: SLF001 -- see above
    result = conn.pgconn.prepare(name.encode(), query)
    if result.status != pq.ExecStatus.COMMAND_OK:
        raise psycopg.Error((result.error_message or b"").decode())
    return conn.pgconn.describe_prepared(name.encode()).nparams


def _sqlite_parameters(statement: str) -> int:
    """SQLite's parse of *statement*: its parameter count."""
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.executescript(SCHEMA)
        conn.executescript(CA_SCHEMA)
        conn.execute(PROVENANCE_COLUMN)
        try:
            conn.execute(statement, ())
        except sqlite3.ProgrammingError as exc:
            return int(str(exc).split("uses ")[1].split(",")[0])
    return 0


def _statements(sql: nix.NixSql) -> dict[str, str]:
    return {f.name: getattr(sql, f.name) for f in dataclasses.fields(sql) if f.type == "str"}


async def test_the_server_parses_every_statement(postgres_conninfo: str) -> None:
    backend = PostgresBackend(postgres_conninfo, SQLITE)
    await backend.open()
    try:
        async with backend.connection() as session:
            await migrate(session)
    finally:
        await backend.close()

    statements = _statements(nix.POSTGRES)
    assert len(statements) > 20, "the scan found the statements"
    assert _ABSENT_TABLES.keys() <= statements.keys(), "a stale exemption"
    sqlite = _statements(nix.sqlite(provenance=True))
    refused: dict[str, str] = {}
    with psycopg.connect(postgres_conninfo, autocommit=True) as conn:
        for name, statement in statements.items():
            try:
                count = _prepare(conn, name, statement)
            except psycopg.Error as exc:
                refused[name] = str(exc)
                continue
            assert count == _sqlite_parameters(sqlite[name]), name
    assert refused.keys() == _ABSENT_TABLES.keys(), refused
    assert all('relation "realisations" does not exist' in message for message in refused.values()), refused
