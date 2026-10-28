"""The pass vetoes fresh living roots, bounds itself per call, and narrates.

`run` re-walks the volatile roots just before planning and vetoes them
and their closure: a process that started after the last check must not
lose its libraries to this pass. `limit` and `target_usage` narrow one
pass without touching the store's rules, and every pass buffers its wire
lines into the response while streaming them to a client that rides
along. The fakes answer from two paths and a temproots file; guest
`/proc` mappings are real store paths but disjoint from the fake ones,
so they veto nothing here.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
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
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.wire_time import Time
from pynixd.daemon_extensions import (
    PynixdGCAction,
    QueryClosureRequest,
    QueryClosureResponse,
    QueryPathInfosRequest,
    QueryPathInfosResponse,
)
from pynixd.gc import Collector
from pynixd.serde import ValidPathInfo
from tests.unit.temp_root_owner import live_temp_root

A = "/nix/store/00000000000000000000000000000000-a"
B = "/nix/store/11111111111111111111111111111111-b"


def _weighed(path: str, nar_size: int) -> ValidPathInfo:
    """A path info carrying only what the collector weighs: size."""
    return ValidPathInfo(
        path=StorePath(path),
        info=UnkeyedValidPathInfo(
            deriver=None,
            nar_hash=NARHash(hash="abc123"),
            references=set(),
            registration_time=Time(ts=0),
            nar_size=nar_size,
            ultimate=False,
            sigs=set(),
            ca=ContentAddress(value=""),
        ),
    )


@dataclass
class FakeLayout:
    """The three directories of a lab store, all under `tmp_path`."""

    state_dir: Path
    store_dir: str = "/nix/store"
    real_store_dir: Path | None = None

    def __post_init__(self) -> None:
        if self.real_store_dir is None:
            self.real_store_dir = self.state_dir


@dataclass
class FakeDeleteConn:
    """One held upstream connection: the pass deletes over it alone."""

    local: FakeLocal

    async def call(self, request: Any, **_kwargs: Any) -> Any:
        self.local.deleted.extend(str(path) for path in request.paths_to_delete)
        return _Deleted(
            set(request.paths_to_delete), bytes_freed=sum(self.local.sizes[str(p)] for p in request.paths_to_delete)
        )


@dataclass
class FakeLocal:
    """A local store with two droppable paths and an open permit."""

    layout: FakeLayout | None = None
    gc_allow_execute: bool = True
    gc_defer: bool = False
    gc_target_usage: float | None = None
    answers_closure: bool = True
    live: set[str] = field(default_factory=set)
    sizes: dict[str, int] = field(default_factory=lambda: {A: 100, B: 200})
    calls: list[str] = field(default_factory=list)
    marks: list[tuple[str, bool]] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)

    def forget_path_info(self, path: str) -> None:
        return None

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        self.calls.append(type(request).__name__)
        self.marks.append((type(request).__name__, bool(_kwargs.get("mark", True))))
        if isinstance(request, QueryAllValidPathsRequest):
            return QueryAllValidPathsResponse(paths={StorePath(A), StorePath(B)})
        if isinstance(request, QueryClosureRequest):
            if not self.answers_closure:
                return QueryClosureResponse(paths=set())
            return QueryClosureResponse(paths={StorePath(str(path)) for path in request.paths})
        if isinstance(request, QueryPathInfosRequest):
            return QueryPathInfosResponse(infos=[_weighed(str(path), self.sizes[str(path)]) for path in request.paths])
        raise AssertionError(f"unexpected request {type(request).__name__}")

    @property
    def db(self) -> Any:
        return self

    async def query_referrer_closure(self, seeds: set[str]) -> set[str]:
        """The seeds themselves: nothing here refers to anything."""
        return set(seeds)

    async def query_access_times(self, paths: set[str]) -> dict[str, float]:
        """`B` is old and big, `A` is fresh and small: `B` leads at any pressure.

        Equal ages tie-break by path string, and a fresh tmpfs reports
        usage zero, so uniform timestamps would order by name instead of
        weight. The split timestamps keep the weight order total.
        """
        now = time.time()
        return {path: (now if path == A else 0.0) for path in paths}

    async def retire_idle_connections(self) -> int:
        return 0

    async def probe(self) -> None:
        return None

    def transfer_conn(self, _options: Any = None) -> Any:
        return self._held()

    @asynccontextmanager
    async def _held(self) -> AsyncIterator[FakeDeleteConn]:
        yield FakeDeleteConn(self)

    async def call(self, request: Any, **_kwargs: Any) -> Any:
        self.calls.append(f"call:{type(request).__name__}")
        if request.action == GCAction.RETURN_LIVE:
            return _Deleted({StorePath(path) for path in self.live})
        self.deleted.extend(str(path) for path in request.paths_to_delete)
        return _Deleted(
            set(request.paths_to_delete), bytes_freed=sum(self.sizes[str(p)] for p in request.paths_to_delete)
        )


@dataclass
class _Deleted:
    paths_deleted: set[Any]
    bytes_freed: int = 0


@dataclass
class FakeCache:
    """A substituter holding every path, so the plan drops the whole store."""

    gc_defer: bool = True
    store_id: str = "upstream"

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        if not isinstance(request, QueryValidPathsRequest):
            raise AssertionError(f"unexpected request {type(request).__name__}")
        return QueryValidPathsResponse(paths=set(request.paths))


@dataclass
class FakeContext:
    local: FakeLocal

    @property
    def local_store(self) -> Any:
        return self.local

    @property
    def stores(self) -> dict[str, Any]:
        return {"local": self.local, "upstream": FakeCache()}


class FakeClient:
    """A downstream client collecting the streamed lines."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, msg: Any) -> None:
        self.sent.append(msg.text)


def _collector(tmp_path: Path, **kwargs: Any) -> tuple[Collector, FakeLocal, Path]:
    state_dir = tmp_path / "state"
    (state_dir / "temproots").mkdir(parents=True)
    local = FakeLocal(layout=FakeLayout(state_dir=state_dir), **kwargs)
    return Collector(FakeContext(local=local)), local, state_dir  # type: ignore[arg-type]  # fakes


def _texts(resp: Any) -> list[str]:
    return [str(msg.text) for msg in resp.logs.messages]


@pytest.mark.anyio
async def test_a_fresh_temproot_vetoes_its_path(tmp_path: Path) -> None:
    """A living root read just now spares its path, and says so on the wire."""
    collector, local, state_dir = _collector(tmp_path)
    # NUL-terminated, the way Nix writes temp files (`gc.cc:163`), and held
    # by a living owner: an unlocked file is stale (`gc.cc:193`), and the
    # pass under test reaps those instead of vetoing them.
    with live_temp_root(state_dir, "99", f"{A}\x00".encode()):
        resp = await collector.run(PynixdGCAction.DRY_RUN)

    assert {str(path) for path in resp.store_paths} == {B}
    assert local.deleted == []
    assert any("volatile veto" in text and "spared 1" in text for text in _texts(resp))


@pytest.mark.anyio
async def test_an_unanswerable_closure_falls_back_to_the_seeds(tmp_path: Path) -> None:
    """No closure feature still spares the roots themselves, never nothing."""
    collector, _local, state_dir = _collector(tmp_path, answers_closure=False)
    # Held by a living owner, as above: the veto must see the root alive.
    with live_temp_root(state_dir, "99", f"{A}\x00".encode()):
        resp = await collector.run(PynixdGCAction.DRY_RUN)

    assert {str(path) for path in resp.store_paths} == {B}


@pytest.mark.anyio
async def test_gc_bookkeeping_queries_do_not_mark_the_judged(tmp_path: Path) -> None:
    """Weighing and keep-closure name dead candidates; marking them would freshen the judged.

    A dry-run must not move what it measures: the paths a pass weighs and
    closes over are under judgment, and recording them as referenced would
    keep every candidate fresh for ever. The veto keeps its marks --
    volatile seeds are live roots, and recording last-seen-live is what
    the access table is for.
    """
    collector, local, state_dir = _collector(tmp_path)
    with live_temp_root(state_dir, "99", f"{A}\x00".encode()):
        await collector.run(PynixdGCAction.DRY_RUN)

    assert ("QueryPathInfosRequest", False) in local.marks
    closure_marks = sorted(mark for name, mark in local.marks if name == "QueryClosureRequest")
    assert closure_marks == [False, True]


@pytest.mark.anyio
async def test_limit_takes_the_weight_order_head(tmp_path: Path) -> None:
    """One path when asked for one: the heavier of the two, first."""
    collector, local, _state_dir = _collector(tmp_path)
    client = FakeClient()

    resp = await collector.run(PynixdGCAction.EXECUTE, client=client, limit=1)  # type: ignore[arg-type]  # fake client

    assert {str(path) for path in resp.store_paths} == {B}
    assert local.deleted == [B]
    assert client.sent[0].startswith("volatile veto")
    assert f"deleting '{B}'" in client.sent
    assert f"deleting '{A}'" not in client.sent
    assert _texts(resp) == client.sent


@pytest.mark.anyio
async def test_target_usage_override_wins_over_the_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The store says fuller-than-full (nothing), the call says half (one).

    Disk usage is mocked: a fresh tmpfs reports zero used, which already
    meets every positive target and would make the override unobservable.
    Thirty bytes a path against eighty of a hundred needs exactly one.

    The override narrows execute passes only: a dry-run answers the whole
    plan under either bound, so an under-target disk does not read as
    clean (#73).
    """
    monkeypatch.setattr(Collector, "_disk_usage", staticmethod(lambda local: (80, 100)))
    collector, local, _state_dir = _collector(tmp_path, gc_target_usage=0.9, sizes={A: 30, B: 30})

    assert {str(path) for path in (await collector.run(PynixdGCAction.DRY_RUN)).store_paths} == {A, B}

    assert (await collector.run(PynixdGCAction.EXECUTE)).store_paths == set()

    resp = await collector.run(PynixdGCAction.EXECUTE, target_usage=0.5)

    assert {str(path) for path in resp.store_paths} == {B}
    assert local.deleted == [B]


@pytest.mark.anyio
async def test_bounds_below_zero_are_refused(tmp_path: Path) -> None:
    collector, _local, _state_dir = _collector(tmp_path)

    with pytest.raises(ValueError, match="not a count"):
        await collector.run(PynixdGCAction.DRY_RUN, limit=-1)
    with pytest.raises(ValueError, match="not one"):
        await collector.run(PynixdGCAction.DRY_RUN, target_usage=0.0)
