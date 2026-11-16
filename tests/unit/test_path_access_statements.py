"""The statements over `PynixdPathAccess` (`pynixd_store/sql/path_access.py`) on both engines."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from pynixd_store import register
from pynixd_store.db_migrations import PATH_ACCESS_TABLE
from tests._conftest.engines import access_times

if TYPE_CHECKING:
    from pynixd_store.local_store_db import LocalStoreDB

LIBC = "/nix/store/00000000000000000000000000000001-libc"
HELLO = "/nix/store/00000000000000000000000000000002-hello"
DRV = "/nix/store/00000000000000000000000000000003-hello.drv"
ABSENT = "/nix/store/00000000000000000000000000000009-absent"
REGISTERED = 1_700_000_000


def _registration(path: str, references: frozenset[str] = frozenset()) -> register.Registration:
    return register.Registration(
        path=path,
        nar_hash="sha256:" + "1" * 64,
        registration_time=REGISTERED,
        deriver=DRV if path == HELLO else None,
        nar_size=1 << 33,
        ultimate=False,
        sigs=(),
        ca=None,
        references=references,
        derivation_outputs={"out": HELLO} if path == DRV else {},
    )


async def _store(db: LocalStoreDB) -> None:
    """`libc` <- `hello` <- `hello.drv`, which builds `hello`."""
    await db.nix.register_valid_paths(
        [
            _registration(LIBC),
            _registration(HELLO, frozenset({LIBC})),
            _registration(DRV, frozenset({LIBC})),
        ]
    )


async def _touched(db: LocalStoreDB) -> set[str]:
    return set(await access_times(db))


async def test_a_runtime_seed_stops_at_a_derivation(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    store_db.mark_paths([HELLO, DRV])
    await store_db.flush_references()
    assert await _touched(store_db) == {LIBC, HELLO, DRV}

    async with store_db.transaction() as session:
        await session.execute(f"DELETE FROM {PATH_ACCESS_TABLE}")
    store_db.mark_paths([DRV])
    await store_db.flush_references()
    assert await _touched(store_db) == {DRV}


async def test_a_build_seed_walks_through_a_derivation(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    store_db.mark_paths([DRV], kind="build")
    store_db.mark_paths([ABSENT])
    await store_db.flush_references()
    assert await _touched(store_db) == {LIBC, DRV}


async def test_a_second_flush_moves_the_time_forward(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    store_db.mark_path(HELLO)
    await store_db.flush_references()
    async with store_db.transaction() as session:
        await session.execute(f"UPDATE {PATH_ACCESS_TABLE} SET lastReferencedAt = 1")
    store_db.mark_path(HELLO)
    await store_db.flush_references()
    times = await access_times(store_db)
    assert all(stamp >= int(time.time()) - 60 for stamp in times.values()), times


async def test_the_age_is_the_access_else_the_registration(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    store_db.mark_path(HELLO)
    await store_db.flush_references()
    stale = await store_db.query_paths_not_referenced_since(3600)
    assert stale is not None
    assert {str(path) for path in stale} == {DRV}

    weights = await store_db.query_weights([HELLO, DRV, ABSENT])
    assert weights is not None
    assert weights.keys() == {HELLO, DRV}
    assert weights[DRV] == (1 << 33, REGISTERED)
    seen = weights[HELLO][1]
    assert seen is not None
    assert seen >= int(time.time()) - 60


async def test_the_referrers_close_over_the_seeds(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    assert await store_db.query_referrer_closure([LIBC]) == {LIBC, HELLO, DRV}
    assert await store_db.query_referrer_closure([HELLO, ABSENT]) == {HELLO}


async def test_a_path_the_store_lost_loses_its_row(store_db: LocalStoreDB) -> None:
    """SQLite prunes the row; on PostgreSQL the foreign key drops it with the path, and nothing is left to prune."""
    await _store(store_db)
    store_db.mark_paths([HELLO, DRV])
    await store_db.flush_references()
    await store_db.nix.invalidate_path(DRV)
    assert await store_db.prune_path_access() == (0 if store_db.sql.path_access.prune is None else 1)
    assert await store_db.prune_path_access() == 0
    assert await _touched(store_db) == {LIBC, HELLO}
