"""A dry-run answers the plan, not the pressure. Issue #73.

On a disk under `gc_target_usage` the pressure bound empties every
execute batch by construction -- and the dry-run rode the same bound,
so it reported "no paths eligible" with tens of thousands droppable.
The bound now narrows execute passes only; the dry-run answers the
whole plan, and the execute test below locks the bound in place.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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
from pynixd.daemon_extensions import (
    PynixdGCAction,
    QueryClosureRequest,
    QueryClosureResponse,
    QueryPathInfosRequest,
    QueryPathInfosResponse,
)
from pynixd.gc import Collector
from tests.unit.test_gc_execute_permit import _weighed

A = "/nix/store/00000000000000000000000000000000-a"
B = "/nix/store/11111111111111111111111111111111-b"


@dataclass
class FakeLocal:
    """A local store with one droppable path and a target above usage."""

    gc_allow_execute: bool = False
    gc_defer: bool = False
    gc_target_usage: float | None = 0.7
    live: set[str] = field(default_factory=set)
    deleted: list[str] = field(default_factory=list)

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        if isinstance(request, QueryAllValidPathsRequest):
            return QueryAllValidPathsResponse(paths={StorePath(A), StorePath(B)})
        if isinstance(request, QueryClosureRequest):
            return QueryClosureResponse(paths={StorePath(str(path)) for path in request.paths})
        if isinstance(request, QueryPathInfosRequest):
            return QueryPathInfosResponse(infos=[_weighed(A, 100)])
        raise AssertionError(f"unexpected request {type(request).__name__}")

    @property
    def db(self) -> Any:
        return self

    async def query_weights(self, paths: set[str]) -> dict[str, tuple[int, int | None]]:
        return {A: (100, 0)} if A in paths else {}

    async def retire_idle_connections(self) -> int:
        return 0

    async def call(self, request: Any, **_kwargs: Any) -> Any:
        if request.action == GCAction.RETURN_LIVE:
            return _Deleted({StorePath(path) for path in self.live})
        self.deleted.extend(str(path) for path in request.paths_to_delete)
        return _Deleted(set(request.paths_to_delete))


@dataclass
class _Deleted:
    paths_deleted: set[Any]
    bytes_freed: int = 1


@dataclass
class FakeCache:
    """A substituter holding `A`, so the plan drops exactly it."""

    gc_defer: bool = True
    store_id: str = "upstream"

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        if not isinstance(request, QueryValidPathsRequest):
            raise AssertionError(f"unexpected request {type(request).__name__}")
        return QueryValidPathsResponse(paths={path for path in request.paths if str(path) == A})


@dataclass
class FakeContext:
    local: FakeLocal

    @property
    def local_store(self) -> Any:
        return self.local

    @property
    def stores(self) -> dict[str, Any]:
        return {"local": self.local, "upstream": FakeCache()}


def _collector(**kwargs: Any) -> tuple[Collector, FakeLocal]:
    local = FakeLocal(**kwargs)
    return Collector(FakeContext(local=local)), local  # type: ignore[arg-type]  # a fake context


@pytest.mark.anyio
async def test_dry_run_reports_droppable_under_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """Usage 0.61 under a 0.7 target: the dry-run still names the plan."""
    monkeypatch.setattr(Collector, "_disk_usage", staticmethod(lambda local: (61, 100)))
    collector, local = _collector()

    resp = await collector.run(PynixdGCAction.DRY_RUN)

    assert {str(path) for path in resp.store_paths} == {A}
    assert local.deleted == []


@pytest.mark.anyio
async def test_execute_still_bounds_under_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """The same pressure on an execute pass deletes nothing: the bound stays."""
    monkeypatch.setattr(Collector, "_disk_usage", staticmethod(lambda local: (61, 100)))
    collector, local = _collector(gc_allow_execute=True)

    resp = await collector.run(PynixdGCAction.EXECUTE)

    assert resp.store_paths == set()
    assert local.deleted == []
