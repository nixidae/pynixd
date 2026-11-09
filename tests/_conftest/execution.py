"""Fakes of what execution reads, held to the Protocols of `pynixd/execution.py`.

A test passes them with no cast, so the type checker fails a fake that lacks
a member execution reads. nixidae/nixidae#1246.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd.config import StoreRankingSettings
from pynixd.daemon_extensions.query_closure_with_info import QueryClosureWithInfoResponse
from pynixd.execution import Executor
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode
from pynixd_store.store_path import StorePath
from tests._conftest.placement import FakeStore

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    import anyio

    from nix_daemon_protocol.ids import StoreId
    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd.build_queue import BuildQueue, QueuedBuild
    from pynixd.connection import ClientConn
    from pynixd.execution import Answers, Builder, RootHolder, Streams
    from pynixd.serde import SetOptionsRequest


class FakeLocal:
    """The local store: it answers a closure query with *infos*."""

    def __init__(self, infos: Iterable[ValidPathInfo] = ()) -> None:
        self.store_id = LOCAL_STORE_ID
        self.infos = list(infos)

    async def execute(self, request: WireRequest, client: ClientConn | None = None) -> Any:
        del request, client
        return QueryClosureWithInfoResponse(infos=self.infos)


class Streamed:
    """Records each transfer and copies nothing."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.calls: list[tuple[StoreId, set[StorePath], bool]] = []
        self.events = events

    async def __call__(
        self,
        src: FakeLocal,
        dst: FakeStore,
        paths: Iterable[StorePath],
        cancel_event: anyio.Event | None = None,
        *,
        peer_store_id: str | None = None,
        direction: Literal["in", "out"] | None = None,
        substitute: bool = False,
    ) -> int:
        del src, cancel_event, peer_store_id, direction
        self.calls.append((dst.store_id, set(paths), substitute))
        if self.events is not None:
            self.events.append("copy")
        return 0


class FakeConn:
    """A build connection: it records each request it carries."""

    def __init__(self, features: frozenset[str] = frozenset(), events: list[str] | None = None) -> None:
        self.standard_features = features
        self.holds_roots = False
        self.calls: list[WireRequest] = []
        self.events = events

    async def call(self, request: WireRequest, client: ClientConn | None = None, *, raise_on_error: bool) -> Any:
        del client
        assert raise_on_error, "a root that did not hold must fail the build"
        self.calls.append(request)
        if self.events is not None:
            self.events.append("hold")


def _conforms(
    store: FakeStore, local: FakeLocal, conn: FakeConn, streamed: Streamed
) -> tuple[Builder, Answers, RootHolder, Streams[FakeStore, FakeLocal]]:
    """Never called. A fake that lacks a member fails `checks.types` here, and the error names the member."""
    return store, local, conn, streamed


def request(name: str, platform: str = "x86_64-linux", input_srcs: Iterable[str] = ()) -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path=f"/nix/store/{name:0>32}-{name}.drv"),
        derivation=BasicDerivation(
            platform=platform, builder="/bin/sh", input_srcs={StorePath(path=p) for p in input_srcs}
        ),
        build_mode=BuildMode.NORMAL,
    )


async def queued(
    queue: BuildQueue,
    name: str = "x",
    *,
    options: SetOptionsRequest | None = None,
    builders: frozenset[StoreId] | None = None,
    input_srcs: Iterable[str] = (),
) -> QueuedBuild:
    """A build in *queue*, as a client's `BuildDerivation` enqueues it."""
    build_id, _ = await queue.enqueue(request(name, input_srcs=input_srcs), options=options, builders=builders)
    return queue.by_id[build_id]


class Triggers:
    """Counts the scheduling passes execution asks for."""

    def __init__(self) -> None:
        self.count = 0

    def __call__(self) -> None:
        self.count += 1


def fake_executor(
    queue: BuildQueue,
    stores: Mapping[StoreId, FakeStore],
    *,
    local: FakeLocal | None = None,
    settings: StoreRankingSettings | None = None,
    trigger: Triggers | None = None,
    stream: Streamed | None = None,
) -> Executor[FakeStore, FakeLocal]:
    """An executor over fake stores, with the local store's id `local`."""
    return Executor(
        queue=queue,
        local=local or FakeLocal(),
        stores=lambda: stores,
        settings=settings or StoreRankingSettings(),
        trigger=trigger or Triggers(),
        strip_handled_features=lambda _build: None,
        stream=stream or Streamed(),
    )
