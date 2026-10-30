"""EXECUTE stays refused until the operator permits it.

Planning is free: a dry-run deletes nothing, so it never needs the
permit. EXECUTE without `gc_allow_execute` raises `GCNotPermittedError`
before any store traffic, which is the fail-closed posture until the
liveness mirror shows sustained zero-divergence. The functional suites
prove what a permitted pass deletes; this file proves the gate in front
of it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
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
from pynixd.exceptions import GCNotPermittedError
from pynixd.gc import Collector
from pynixd.serde import ValidPathInfo

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
class FakeDeleteConn:
    """One held upstream connection: the pass deletes over it alone."""

    local: FakeLocal
    calls: int = 0

    async def call(self, request: Any, **_kwargs: Any) -> Any:
        self.calls += 1
        self.local.calls.append(f"held:{type(request).__name__}")
        self.local.deleted.extend(str(path) for path in request.paths_to_delete)
        return _Deleted(set(request.paths_to_delete))


@dataclass
class FakeLocal:
    """A local store with one droppable path and an unset permit."""

    gc_allow_execute: bool = False
    gc_defer: bool = False
    live: set[str] = field(default_factory=set)
    calls: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    acquired: int = 0

    def forget_path_info(self, path: str) -> None:
        return None

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        self.calls.append(type(request).__name__)
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

    async def probe(self) -> None:
        return None

    def transfer_conn(self, _options: Any = None) -> Any:
        return self._held()

    @asynccontextmanager
    async def _held(self) -> AsyncIterator[FakeDeleteConn]:
        self.acquired += 1
        yield FakeDeleteConn(self)

    async def call(self, request: Any, **_kwargs: Any) -> Any:
        self.calls.append(f"call:{type(request).__name__}")
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


@pytest.mark.anyio
async def test_execute_without_permit_raises_before_any_store_traffic() -> None:
    local = FakeLocal()
    collector = Collector(FakeContext(local=local))  # type: ignore[arg-type]  # a fake context

    with pytest.raises(GCNotPermittedError):
        await collector.run(PynixdGCAction.EXECUTE)

    assert local.calls == []
    assert local.deleted == []


@pytest.mark.anyio
async def test_dry_run_without_permit_still_plans() -> None:
    local = FakeLocal()
    collector = Collector(FakeContext(local=local))  # type: ignore[arg-type]  # a fake context

    resp = await collector.run(PynixdGCAction.DRY_RUN)

    assert {str(path) for path in resp.store_paths} == {A}
    assert local.deleted == []


@pytest.mark.anyio
async def test_execute_with_permit_deletes() -> None:
    """The permit opens the real path: plan, weigh, delete, report."""
    local = FakeLocal(gc_allow_execute=True)
    collector = Collector(FakeContext(local=local))  # type: ignore[arg-type]  # a fake context

    resp = await collector.run(PynixdGCAction.EXECUTE)

    assert {str(path) for path in resp.store_paths} == {A}
    assert local.deleted == [A]
    assert local.acquired == 1


@pytest.mark.anyio
async def test_execute_without_the_attribute_is_denied() -> None:
    """A store predating the knob reads as off: `getattr` defaults closed."""

    @dataclass
    class LegacyLocal:
        """The shape of a local store from before the permit existed."""

        calls: list[str] = field(default_factory=list)

        async def execute(self, request: Any, **_kwargs: Any) -> Any:
            self.calls.append(type(request).__name__)
            raise AssertionError(f"unexpected request {type(request).__name__}")

    @dataclass
    class LegacyContext:
        local: LegacyLocal

        @property
        def local_store(self) -> Any:
            return self.local

        @property
        def stores(self) -> dict[str, Any]:
            return {"local": self.local}

    local = LegacyLocal()
    collector = Collector(LegacyContext(local=local))  # type: ignore[arg-type]  # a fake context

    with pytest.raises(GCNotPermittedError):
        await collector.run(PynixdGCAction.EXECUTE)

    assert local.calls == []
