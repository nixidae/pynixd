"""Fakes of what execution reads, held to the Protocols of `pynixd/execution.py`.

A test passes them with no cast, so the type checker fails a fake that lacks
a member execution reads. nixidae/nixidae#1246.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd.config import PlacementTraceSettings, StoreRankingSettings
from pynixd.daemon_extensions.query_closure_with_info import QueryClosureWithInfoResponse
from pynixd.execution import Executor
from pynixd.placement_trace import PlacementTrace
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    DerivationOutput,
    DrvOutput,
    Realisation,
)
from pynixd_store.builder.settings import BuildSettings
from pynixd_store.store_path import StorePath
from tests._conftest.placement import FakeConn as FakeConn, FakeStore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping

    import anyio

    from nix_daemon_protocol.ids import StoreId
    from nix_daemon_protocol.logs import LogMessage
    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd.build_queue import BuildQueue, QueuedBuild
    from pynixd.connection import ClientConn
    from pynixd.execution import Answers, BuildConn, Builder, Sandbox, Streams
    from pynixd.serde import SetOptionsRequest
    from pynixd_store.build_stats import BuildObservation, DerivationIdentity


class FakeLocal:
    """The local store: it answers a closure query with *infos*."""

    def __init__(self, infos: Iterable[ValidPathInfo] = ()) -> None:
        self.store_id = LOCAL_STORE_ID
        self.infos = list(infos)

    async def execute(self, request: WireRequest, client: ClientConn | None = None) -> Any:
        del request, client
        return QueryClosureWithInfoResponse(infos=self.infos)


class FakeRoots:
    """Temporary roots that hold every path, or none when *holds* is false."""

    def __init__(self, *, holds: bool = True) -> None:
        self.holds = holds
        self.paths: list[str] = []
        self.closed = False

    async def add(self, path: str | StorePath) -> bool:
        self.paths.append(str(path))
        return self.holds

    async def close(self) -> None:
        self.closed = True


class FakeEngine:
    def __init__(self, roots: FakeRoots) -> None:
        self.roots = roots

    def temp_roots(self) -> FakeRoots:
        return self.roots


class FakeRecords:
    """Keeps each observation a build records."""

    def __init__(self) -> None:
        self.observations: list[BuildObservation] = []

    async def record_build_observation(self, identity: DerivationIdentity, observation: BuildObservation) -> int | None:
        del identity
        self.observations.append(observation)
        return len(self.observations)


@dataclass
class FakeSandbox:
    """A local store pynixd's builder builds into; *build* answers each build."""

    store_id: StoreId = LOCAL_STORE_ID
    build: Callable[[], Awaitable[BuildDerivationResponse]] | None = None
    settings: BuildSettings | None = None
    hook_failure: str | None = None
    db: FakeRecords = field(default_factory=FakeRecords)
    roots: FakeRoots = field(default_factory=FakeRoots)
    hooks: list[set[StorePath]] = field(default_factory=list)

    @property
    def engine(self) -> FakeEngine:
        return FakeEngine(self.roots)

    async def execute(self, request: WireRequest, client: ClientConn | None = None) -> Any:
        del request, client
        return QueryClosureWithInfoResponse(infos=[])

    async def local_build_settings(
        self, request: BuildDerivationRequest, options: SetOptionsRequest | None
    ) -> BuildSettings | None:
        del request, options
        return self.settings

    async def build_here(
        self,
        request: BuildDerivationRequest,
        settings: BuildSettings,
        on_log: Callable[[LogMessage], Awaitable[None]],
        verbosity: int,
    ) -> BuildDerivationResponse:
        del request, settings, on_log, verbosity
        if self.build is None:
            raise AssertionError("the sandbox was not asked to build")
        return await self.build()

    async def post_build_hook_after(
        self,
        request: BuildDerivationRequest,
        options: SetOptionsRequest | None,
        out_paths: Iterable[StorePath],
        on_log: Callable[[LogMessage], Awaitable[None]],
    ) -> str | None:
        del request, options, on_log
        self.hooks.append(set(out_paths))
        return self.hook_failure


type End = FakeStore | FakeLocal | FakeSandbox


@dataclass(frozen=True)
class Transfer:
    src: End
    dst: End
    paths: frozenset[StorePath]
    substitute: bool


class Streamed:
    """Records each transfer and copies nothing; the first *fail_first* raise as a dropped stream."""

    def __init__(self, events: list[str] | None = None, *, fail_first: int = 0, error: Exception | None = None) -> None:
        self.calls: list[Transfer] = []
        self.events = events
        self.fail_first = fail_first
        self.error = error or EOFError("0 bytes read on a total of 1668838416 expected bytes")
        self.before: Callable[[], None] | None = None
        """Runs as each copy starts, to look at what holds then."""

    async def __call__(
        self,
        src: End,
        dst: End,
        paths: Iterable[StorePath],
        cancel_event: anyio.Event | None = None,
        *,
        peer_store_id: str | None = None,
        direction: Literal["in", "out"] | None = None,
        substitute: bool = False,
    ) -> int:
        del cancel_event, peer_store_id, direction
        self.calls.append(Transfer(src, dst, frozenset(paths), substitute))
        if self.before is not None:
            self.before()
        if self.events is not None:
            self.events.append("copy")
        if len(self.calls) <= self.fail_first:
            raise self.error
        return 0


class Logs:
    """The build logs `keep_log` writes, by derivation name."""

    def __init__(self, *, fails: bool = False) -> None:
        self.written: list[tuple[str, bytes]] = []
        self.fails = fails

    async def __call__(self, name: str, text: bytes) -> None:
        if self.fails:
            raise OSError("no room for the log")
        self.written.append((name, text))


def _conforms(
    store: FakeStore, local: FakeLocal, conn: FakeConn, streamed: Streamed, sandbox: FakeSandbox
) -> tuple[Builder, Answers, BuildConn, Streams[End], Sandbox]:
    """Never called. A fake that lacks a member fails `checks.types` here, and the error names the member."""
    return store, local, conn, streamed, sandbox


def log_bytes(build: QueuedBuild) -> bytes:
    """Every log message *build* fanned out, as its subscribers receive them."""
    return build._log_writer.get_bytes()  # noqa: SLF001 -- the log a late subscriber replays


def sandbox_settings() -> BuildSettings:
    """What pynixd's builder reads from an empty `nix.conf` on this system."""
    return BuildSettings.from_nix_config({}, {"system": "x86_64-linux"})


def request(
    name: str, platform: str = "x86_64-linux", input_srcs: Iterable[str] = (), outputs: Mapping[str, str] | None = None
) -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path=f"/nix/store/{name:0>32}-{name}.drv"),
        derivation=BasicDerivation(
            platform=platform,
            builder="/bin/sh",
            input_srcs={StorePath(path=p) for p in input_srcs},
            outputs={k: DerivationOutput(path=v) for k, v in (outputs or {}).items()},
        ),
        build_mode=BuildMode.NORMAL,
    )


async def queued(
    queue: BuildQueue,
    name: str = "x",
    *,
    platform: str = "x86_64-linux",
    options: SetOptionsRequest | None = None,
    builders: frozenset[StoreId] | None = None,
    input_srcs: Iterable[str] = (),
    outputs: Mapping[str, str] | None = None,
) -> QueuedBuild:
    """A build in *queue*, as a client's `BuildDerivation` enqueues it."""
    build_id, _ = await queue.enqueue(
        request(name, platform, input_srcs=input_srcs, outputs=outputs), options=options, builders=builders
    )
    return queue.by_id[build_id]


class Triggers:
    """Counts the scheduling passes execution asks for."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.count = 0
        self.events = events

    def __call__(self) -> None:
        self.count += 1
        if self.events is not None:
            self.events.append("trigger")


def fake_executor(
    queue: BuildQueue,
    stores: Mapping[StoreId, FakeStore],
    *,
    local: FakeLocal | None = None,
    here: FakeSandbox | None = None,
    sandboxes: Mapping[StoreId, FakeSandbox] | None = None,
    settings: StoreRankingSettings | None = None,
    trigger: Triggers | None = None,
    stream: Streamed | None = None,
    logs: Logs | None = None,
    output_locations: dict[str, StoreId] | None = None,
) -> Executor[FakeStore, FakeLocal, FakeSandbox]:
    """An executor over fake stores. The local builder is `stores["local"]`, or a fresh one."""
    return Executor(
        queue=queue,
        local=local or FakeLocal(),
        local_builder=stores.get(LOCAL_STORE_ID) or FakeStore(LOCAL_STORE_ID),
        here=here,
        sandbox_at=lambda store_id: (sandboxes or {}).get(store_id),
        current=lambda store_id: stores.get(store_id),
        stores=lambda: stores,
        settings=settings or StoreRankingSettings(),
        trigger=trigger or Triggers(),
        strip_handled_features=lambda _build: None,
        stream=stream or Streamed(),  # type: ignore[arg-type]  # zuban-bug: a union type argument fails a callable Protocol; mypy accepts the call
        marks=lambda: None,
        add_build_log=logs or Logs(),
        output_locations={} if output_locations is None else output_locations,
        trace=PlacementTrace(None, PlacementTraceSettings()),
        pull_backoff=0.0,
    )


def answering(status: BuildResultStatus, *, realised: str | None = None) -> Callable[[WireRequest], Awaitable[Any]]:
    """A builder's answer to `BuildDerivation`: *status*, with *realised* as the output a CA build produced."""
    built = (
        {}
        if realised is None
        else {
            f"sha256:{'0' * 64}!out": Realisation(id=DrvOutput(f"sha256:{'0' * 64}!out"), out_path=StorePath(realised))
        }
    )

    async def answer(request: WireRequest) -> BuildDerivationResponse:
        del request
        return BuildDerivationResponse(result=BuildResult(status=int(status), built_outputs=built))

    return answer
