"""The planner reads liveness from the mirror, tracing only as fallback.

A store whose layout pynixd can read never pays a Nix trace: the pass
refreshes the mirror just now -- filling it fully when it is behind, the
way Nix finds roots fresh on every call -- and plans from the snapshot.
A store without a mirror, or a refresh that fails, falls back to asking
Nix, which is the slow path that stays correct.
"""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from nix_daemon_protocol import (
    GCAction,
    QueryAllValidPathsRequest,
    QueryAllValidPathsResponse,
    QueryValidPathsRequest,
    QueryValidPathsResponse,
)
from nix_daemon_protocol.store_path import StorePath
from pynixd.db_migrations import LIVENESS_ROOT_TABLE, apply_migrations
from pynixd.exceptions import BackendError
from pynixd.gc import Collector
from pynixd.store_layout import StoreLayout

HASH_A = "00000000000000000000000000000000"
HASH_B = "11111111111111111111111111111111"


@dataclass
class FakeDB:
    """The two access queries, answering from a fixed stale set."""

    stale: set[str] = field(default_factory=set)
    referrers: dict[str, set[str]] = field(default_factory=dict)

    async def query_paths_not_referenced_since(self, _max_age: int) -> set[str]:
        return set(self.stale)

    async def query_referrer_closure(self, seeds: set[str]) -> set[str]:
        if not self.referrers:
            return set(seeds)
        closed = set(seeds)
        queue = list(seeds)
        while queue:
            for ref in self.referrers.get(queue.pop(), ()):
                if ref not in closed:
                    closed.add(ref)
                    queue.append(ref)
        return closed


@dataclass
class FakeLocal:
    """A local store over a lab database, with a layout and an open permit."""

    layout: Any = None
    db: Any = None
    gc_max_age: int | None = 86400
    gc_allow_execute: bool = True
    calls: list[str] = field(default_factory=list)

    def forget_path_info(self, path: str) -> None:
        return None

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        if isinstance(request, QueryAllValidPathsRequest):
            return QueryAllValidPathsResponse(
                paths={
                    StorePath(f"{self.layout.store_dir}/{HASH_A}-a"),
                    StorePath(f"{self.layout.store_dir}/{HASH_B}-b"),
                }
            )
        raise AssertionError(f"unexpected request {type(request).__name__}")

    async def call(self, request: Any, **_kwargs: Any) -> Any:
        self.calls.append(str(request.action))
        raise AssertionError("the mirror answers; Nix is never asked")


@dataclass
class FakeContext:
    local: FakeLocal

    @property
    def local_store(self) -> Any:
        return self.local

    @property
    def stores(self) -> dict[str, Any]:
        return {"local": self.local}


def _lab(tmp_path: Path) -> tuple[Path, Path, str, str]:
    """A state dir, a store dir, and the two paths of the lab store.

    `a` is rooted by a gcroots link; `b` is dead: nothing names it. The
    store paths name `/nix/store` without existing there: the walk records
    absolute links whatever they point at, and `StorePath` only accepts
    the real prefix.
    """
    store = Path("/nix/store")
    state = tmp_path / "state"
    (state / "gcroots" / "auto").mkdir(parents=True)
    (state / "db").mkdir(parents=True)
    a = f"{store}/{HASH_A}-a"
    b = f"{store}/{HASH_B}-b"
    (state / "gcroots" / "auto" / "root").symlink_to(a)
    return state, store, a, b


async def _database(state: Path, a: str, b: str) -> Path:
    """Nix's two tables for two paths, plus pynixd's own, migrated."""
    db = state / "db" / "db.sqlite"
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE, deriver TEXT)")
        conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
        conn.execute("INSERT INTO ValidPaths VALUES (1, ?, NULL)", (a,))
        conn.execute("INSERT INTO ValidPaths VALUES (2, ?, NULL)", (b,))
    assert (await apply_migrations(db, read_only=False)).usable
    return db


def _collector(state: Path, store: Path, db: FakeDB) -> tuple[Collector, FakeLocal]:
    layout = StoreLayout.relocated_store(store_dir=store, state_dir=state)
    local = FakeLocal(layout=layout, db=db)
    return Collector(FakeContext(local=local)), local  # type: ignore[arg-type] -- fakes


@pytest.mark.anyio
async def test_plan_reads_the_mirror_and_never_asks_nix(tmp_path: Path) -> None:
    """`a` is rooted, `b` is dead and stale: the plan names `b` alone.

    The fake's `call` raises on any action, so a plan that reaches Nix
    fails the test instead of answering slowly.
    """
    state, store, a, b = _lab(tmp_path)
    await _database(state, a, b)
    collector, local = _collector(state, store, FakeDB(stale={b}))

    assert {str(path) for path in await collector.plan()} == {b}
    assert local.calls == []


@pytest.mark.anyio
async def test_a_stale_roots_table_is_repaired_before_planning(tmp_path: Path) -> None:
    """A table from before the link existed still plans from the link.

    The refresh rediffs the walked links against the table on every pass,
    so a mirror left behind fills fully before anything plans from it --
    the completeness Nix gets by finding roots fresh on every call.
    """
    state, store, a, b = _lab(tmp_path)
    db_path = await _database(state, a, b)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            f"INSERT INTO {LIVENESS_ROOT_TABLE} (link, target, kind) VALUES (?, ?, ?)", ("gone", "nowhere", "gcroot")
        )
    collector, _local = _collector(state, store, FakeDB(stale={b}))

    assert {str(path) for path in await collector.plan()} == {b}

    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
        rows = conn.execute(f"SELECT link, target FROM {LIVENESS_ROOT_TABLE}").fetchall()
    assert rows == [(str(state / "gcroots" / "auto" / "root"), a)]


@pytest.mark.anyio
async def test_no_layout_falls_back_to_the_nix_trace(tmp_path: Path) -> None:
    """A store pynixd cannot lay out plans the old way: by asking Nix."""
    a = f"/nix/store/{HASH_A}-a"
    b = f"/nix/store/{HASH_B}-b"

    @dataclass
    class _Live:
        paths_deleted: set[Any]

    @dataclass
    class TracingLocal(FakeLocal):
        async def execute(self, request: Any, **_kwargs: Any) -> Any:
            if isinstance(request, QueryAllValidPathsRequest):
                return QueryAllValidPathsResponse(paths={StorePath(a), StorePath(b)})
            raise AssertionError(f"unexpected request {type(request).__name__}")

        async def call(self, request: Any, **_kwargs: Any) -> Any:
            self.calls.append(str(request.action))
            if request.action == GCAction.RETURN_LIVE:
                return _Live({StorePath(a)})
            raise AssertionError(f"unexpected call {request.action}")

    _state, _store, _a, _b = _lab(tmp_path)
    local = TracingLocal(layout=None, db=FakeDB(stale={b}))
    collector = Collector(FakeContext(local=local))  # type: ignore[arg-type] -- fakes

    assert {str(path) for path in await collector.plan()} == {b}
    assert str(GCAction.RETURN_LIVE) in local.calls


@pytest.mark.anyio
async def test_a_broken_mirror_falls_back_to_the_nix_trace(tmp_path: Path) -> None:
    """A database file that is not a database plans the old way, too."""
    state, store, _a, b = _lab(tmp_path)
    (state / "db" / "db.sqlite").write_bytes(b"not a database")  # noqa: ASYNC240 -- the fault under test

    @dataclass
    class _Live:
        paths_deleted: set[Any]

    @dataclass
    class TracingLocal(FakeLocal):
        async def call(self, request: Any, **_kwargs: Any) -> Any:
            self.calls.append(str(request.action))
            if request.action == GCAction.RETURN_LIVE:
                return _Live({StorePath(f"{store}/{HASH_A}-a")})
            raise AssertionError(f"unexpected call {request.action}")

    local = TracingLocal(
        layout=StoreLayout.relocated_store(store_dir=store, state_dir=state),
        db=FakeDB(stale={b}),
    )
    collector = Collector(FakeContext(local=local))  # type: ignore[arg-type] -- fakes

    assert {str(path) for path in await collector.plan()} == {b}
    assert str(GCAction.RETURN_LIVE) in local.calls


@pytest.mark.anyio
async def test_close_batch_completes_the_slice(tmp_path: Path) -> None:
    """A sliced batch pulls its planned referrers back in.

    The weight order puts the 8 GB image first and its small spec below the
    slice; gc.cc refuses the image without the spec in the same request, so
    the slice rejoins it. Issue #69.
    """
    image = f"/nix/store/{HASH_A}-image"
    spec = f"/nix/store/{HASH_B}-spec"
    state, store, _a, _b = _lab(tmp_path)
    local = FakeLocal(
        layout=StoreLayout.relocated_store(store_dir=store, state_dir=state),
        db=FakeDB(referrers={image: {spec}}),
    )
    collector = Collector(FakeContext(local=local))  # type: ignore[arg-type] -- fakes

    assert await collector._close_batch(local, [image], {image, spec}) == [image, spec]  # type: ignore[arg-type] -- fakes


@pytest.mark.anyio
async def test_close_batch_drops_a_live_anchored_seed(tmp_path: Path) -> None:
    """A seed whose referrer escaped the plan drops instead of poisoning."""
    image = f"/nix/store/{HASH_A}-image"
    spec = f"/nix/store/{HASH_B}-spec"
    state, store, _a, _b = _lab(tmp_path)
    local = FakeLocal(
        layout=StoreLayout.relocated_store(store_dir=store, state_dir=state),
        db=FakeDB(referrers={image: {spec}}),
    )
    collector = Collector(FakeContext(local=local))  # type: ignore[arg-type] -- fakes

    assert await collector._close_batch(local, [image], {image}) == []  # type: ignore[arg-type] -- fakes


@pytest.mark.anyio
async def test_delete_refuses_a_live_path_without_stopping_the_rest(tmp_path: Path) -> None:
    """A live path is isolated while the rest of its group is deleted."""
    first = f"/nix/store/{HASH_A}-first"
    live = f"/nix/store/{HASH_A}-live"
    second = f"/nix/store/{HASH_B}-second"

    @dataclass
    class _Deleted:
        paths_deleted: set[Any]
        bytes_freed: int

    @dataclass
    class HeldConn:
        local: DeletingLocal

        async def call(self, request: Any, **_kwargs: Any) -> Any:
            if isinstance(request, QueryValidPathsRequest):
                return QueryValidPathsResponse(paths=request.paths)
            asked = {str(path) for path in request.paths_to_delete}
            self.local.attempts.append(frozenset(asked))
            if live in asked:
                raise BackendError(f"Cannot delete path '{live}' since it is still alive")
            return _Deleted({StorePath(path) for path in asked}, 0)

    @dataclass
    class DeletingLocal(FakeLocal):
        attempts: list[frozenset[str]] = field(default_factory=list)
        acquired: int = 0

        async def retire_idle_connections(self) -> int:
            return 0

        async def probe(self) -> None:
            return None

        def transfer_conn(self, _options: Any = None) -> Any:
            return self._held()

        @asynccontextmanager
        async def _held(self) -> AsyncIterator[HeldConn]:
            self.acquired += 1
            yield HeldConn(self)

    state, store, _a, _b = _lab(tmp_path)
    local = DeletingLocal(
        layout=StoreLayout.relocated_store(store_dir=store, state_dir=state),
        db=FakeDB(),
    )
    collector = Collector(FakeContext(local=local))  # type: ignore[arg-type] -- fakes

    lines: list[Any] = []
    resp = await collector._delete([first, live, second], {}, lines, None)

    assert {str(path) for path in resp.store_paths} == {first, second}
    assert any("refused" in line.text for line in lines)
    assert local.attempts == [
        frozenset({first, live, second}),
        frozenset({first}),
        frozenset({live, second}),
        frozenset({live}),
        frozenset({second}),
        frozenset({live}),
    ]
    assert local.acquired == 2
