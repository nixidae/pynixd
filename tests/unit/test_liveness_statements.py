"""The statements of the liveness mirror and the roots report (`pynixd_store/sql/liveness.py`) on both engines."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pynixd_store import liveness, register
from pynixd_store.collect import GCSettings
from pynixd_store.postgres_db import PostgresBackend

if TYPE_CHECKING:
    from pynixd_store.local_store_db import LocalStoreDB

SRC = "/nix/store/00000000000000000000000000000001-src"
DRV = "/nix/store/00000000000000000000000000000002-hello.drv"
LIBC = "/nix/store/00000000000000000000000000000003-libc"
HELLO = "/nix/store/00000000000000000000000000000004-hello"
DEAD = "/nix/store/00000000000000000000000000000005-dead"
SIZE = 1 << 33


CA_DRV = "/nix/store/00000000000000000000000000000006-ca.drv"
CA_OUT = "/nix/store/00000000000000000000000000000007-ca"


def _registration(
    path: str,
    references: frozenset[str] = frozenset(),
    deriver: str | None = None,
    outputs: dict[str, str] | None = None,
) -> register.Registration:
    return register.Registration(
        path=path,
        nar_hash=bytes([0x11]) * 32,
        registration_time=1_700_000_000,
        deriver=deriver,
        nar_size=SIZE,
        ultimate=False,
        sigs=(),
        ca=None,
        references=references,
        derivation_outputs=outputs or {},
    )


async def _store(db: LocalStoreDB) -> None:
    """`hello` refers to `libc` and was built by `hello.drv`, which refers to `src`; `dead` stands alone."""
    await db.nix.register_valid_paths(
        [
            _registration(SRC),
            _registration(DRV, frozenset({SRC})),
            _registration(LIBC),
            _registration(HELLO, frozenset({LIBC}), deriver=DRV),
            _registration(DEAD),
        ]
    )


async def test_the_live_set_walks_references_and_derivers(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    database = store_db.backend.database()
    assert liveness.refresh_roots(database, {"/roots/hello": (HELLO, "gcroot")})
    assert not liveness.refresh_roots(database, {"/roots/hello": (HELLO, "gcroot")})
    assert liveness.refresh_roots(database, {"/roots/hello": (HELLO, "profile")})

    assert liveness.query_live_set(database, set()) == {HELLO, LIBC, DRV, SRC}
    assert liveness.query_live_set(database, {DEAD}) == {HELLO, LIBC, DRV, SRC, DEAD}
    count, newest = liveness.db_fingerprint(database)
    assert count == 5
    assert newest >= 5

    assert liveness.refresh_roots(database, {})
    assert liveness.query_live_set(database, set()) == set()


async def _built(db: LocalStoreDB) -> None:
    """`hello.drv` names `hello` in `DerivationOutputs`; `ca.drv` names `ca` only in its build trace."""
    await db.nix.register_valid_paths(
        [
            _registration(SRC),
            _registration(DRV, frozenset({SRC}), outputs={"out": HELLO}),
            _registration(LIBC),
            _registration(HELLO, frozenset({LIBC}), deriver=DRV),
            _registration(CA_DRV),
            _registration(CA_OUT, deriver=CA_DRV),
        ]
    )
    await db.nix.register_drv_output(CA_DRV.removeprefix("/nix/store/"), "out", CA_OUT, [], by_path=True)


async def test_keep_outputs_keeps_what_a_live_derivation_built(store_db: LocalStoreDB) -> None:
    """nixidae/nixidae#1639: Nix's `markAlive` follows a derivation to its outputs under `keep-outputs` (`misc.cc:57`)."""
    await _built(store_db)
    database = store_db.backend.database()
    liveness.refresh_roots(database, {"/roots/drv": (DRV, "gcroot"), "/roots/ca": (CA_DRV, "gcroot")})

    assert liveness.query_live_set(database, set()) == {DRV, SRC, CA_DRV}
    kept = liveness.query_live_set(database, set(), GCSettings(keep_outputs=True))
    assert kept == {DRV, SRC, HELLO, LIBC, CA_DRV, CA_OUT}


async def test_a_closure_follows_the_edges_it_is_given(store_db: LocalStoreDB) -> None:
    """`query_closure` is `live_set` of its seeds alone, the roots table aside."""
    await _built(store_db)
    liveness.refresh_roots(store_db.backend.database(), {"/roots/hello": (HELLO, "gcroot")})

    assert await store_db.query_closure({DRV}, keep_derivations=True, keep_outputs=False) == {DRV, SRC}
    kept = await store_db.query_closure({DRV, CA_DRV}, keep_derivations=True, keep_outputs=True)
    assert kept == {DRV, SRC, HELLO, LIBC, CA_DRV, CA_OUT}
    assert await store_db.query_closure({HELLO}, keep_derivations=False, keep_outputs=False) == {HELLO, LIBC}
    assert await store_db.query_closure(set(), keep_derivations=True, keep_outputs=True) == set()


async def test_keep_derivations_off_drops_the_deriver_edge(store_db: LocalStoreDB) -> None:
    await _built(store_db)
    database = store_db.backend.database()
    liveness.refresh_roots(database, {"/roots/hello": (HELLO, "gcroot")})

    assert liveness.query_live_set(database, set()) == {HELLO, LIBC, DRV, SRC}
    assert liveness.query_live_set(database, set(), GCSettings(keep_derivations=False)) == {HELLO, LIBC}


async def test_the_snapshot_keeps_each_path_s_first_epoch(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    database = store_db.backend.database()
    assert liveness.read_snapshot(database) is None

    liveness.write_snapshot(database, {HELLO, LIBC}, 7)
    assert liveness.read_snapshot(database) == ({HELLO, LIBC}, 7)
    liveness.write_snapshot(database, {HELLO, DEAD}, 9)
    assert liveness.read_snapshot(database) == ({HELLO, DEAD}, 9)
    liveness.write_snapshot(database, {HELLO}, 11)
    assert liveness.read_snapshot(database) == ({HELLO}, 7)


async def test_a_collected_path_leaves_the_snapshot(store_db: LocalStoreDB) -> None:
    """On PostgreSQL a live row names its path by id, and the foreign key drops it with the path."""
    if not isinstance(store_db.backend, PostgresBackend):
        pytest.skip("SQLite keys the live set by path and keeps the row until the next snapshot")
    await _store(store_db)
    database = store_db.backend.database()
    liveness.write_snapshot(database, {HELLO}, 7)
    liveness.write_snapshot(database, {HELLO, DEAD}, 9)
    await store_db.nix.invalidate_path(DEAD)
    assert liveness.read_snapshot(database) == ({HELLO}, 7), "no epoch of a collected path stays behind"


async def test_a_check_files_its_streak(store_db: LocalStoreDB) -> None:
    database = store_db.backend.database()
    assert liveness.read_streak(database) is None
    first = liveness.record_check(database, True, {HELLO})
    second = liveness.record_check(database, True, {HELLO, LIBC})
    third = liveness.record_check(database, False, {HELLO})
    assert first is not None
    assert second is not None
    assert third is not None
    assert (first.agreements, first.divergences, first.checks, first.live) == (1, 0, 1, 1)
    assert (second.agreements, second.divergences, second.checks, second.live) == (2, 0, 2, 2)
    assert (third.agreements, third.divergences, third.checks, third.live) == (0, 1, 3, 1)


async def test_the_report_counts_full_and_exclusive_storage(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    report = await store_db.query_roots_report([("a", [HELLO]), ("b", [HELLO]), ("c", [DEAD]), ("d", [LIBC])])
    assert report is not None
    found = {r.label: (r.full_paths, r.full_bytes, r.exclusive_paths, r.exclusive_bytes) for r in report}
    assert found == {
        "a": (4, 4 * SIZE, 0, 0),
        "b": (4, 4 * SIZE, 0, 0),
        "c": (1, SIZE, 1, SIZE),
        "d": (1, SIZE, 0, 0),
    }
    again = await store_db.query_roots_report([("c", [DEAD])])
    assert again is not None
    assert [(r.label, r.exclusive_paths) for r in again] == [("c", 1)]
