"""Running a placed build on its store, typed over the stores it reads.

`Scheduler.schedule` places a build through `Placer`, and the placer's spawn
callback runs it here. Each dependency is a Protocol or a real object, so a
test's fake that lacks a member fails `checks.types`, not the guest's unit
phase. nixidae/nixidae#1246.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal, Protocol

import structlog

from pynixd_store.store_path import StorePath

from .allocator import outside_the_session
from .daemon_extensions.query_closure_with_info import QueryClosureWithInfoRequest
from .exceptions import BackendError, error_text
from .goals.results import result_succeeded
from .placement import client_forbids_local
from .serde import AddTempRootRequest, AddTempRootsRequest, BuildResultStatus, LogNext, QueryValidPathsRequest
from .system_features import effective_required_features
from .wire import FEATURE_ADD_TEMP_ROOTS

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping

    import anyio

    from nix_daemon_protocol.aliases import StorePathSet
    from nix_daemon_protocol.ids import StoreId
    from nix_daemon_protocol.wire_ops import WireRequest

    from .build_queue import BuildQueue, QueuedBuild
    from .config import StoreRankingSettings
    from .connection import ClientConn
    from .serde import BuildDerivationResponse, BuildResult

log = structlog.get_logger(__name__)

_MACHINE_FAULTS = (
    "No space left on device",
    "Disk quota exceeded",
    "Cannot allocate memory",
    "Too many open files",
    "Read-only file system",
    "Input/output error",
    "getting status of",
    "is not valid",
    "Killed",
)
"""Words in a failed build's message that blame the builder's machine, not the derivation."""


def machine_fault(result: BuildResult) -> str | None:
    """Why a failed result is the machine's fault, or `None` for the derivation's own failure.

    Nix marks a build that failed with a full disk, or any unsandboxed build,
    `TransientFailure` (`derivation-builder.cc:242-250`), and an error outside
    the builder `MiscFailure` (`derivation-building-goal.cc:805`). The text
    catches a disk that filled and emptied again before Nix looked.
    """
    if result_succeeded(result):
        return None
    status = BuildResultStatus(result.status)
    if status in (BuildResultStatus.TRANSIENT_FAILURE, BuildResultStatus.MISC_FAILURE):
        return status.name.lower()
    message = str(result.error_msg)
    return next((words for words in _MACHINE_FAULTS if words in message), None)


class Answers(Protocol):
    """A store that answers an operation."""

    @property
    def store_id(self) -> StoreId: ...
    async def execute(self, request: WireRequest, client: ClientConn | None = None) -> Any: ...


class Builder(Answers, Protocol):
    """What execution reads of a store that builds."""

    @property
    def session_only(self) -> bool: ...

    disk_full_until: float

    def supports_derivation(self, system: str, features: set[str], /) -> bool: ...


class RootHolder(Protocol):
    """A connection to a builder's daemon, whose temporary roots live as long as it does."""

    @property
    def standard_features(self) -> frozenset[str]: ...

    holds_roots: bool

    async def call(self, request: WireRequest, client: ClientConn | None = None, *, raise_on_error: bool) -> Any: ...


class Streams[S, L](Protocol):
    """`stream_paths_store_to_store`, from the local store to a builder."""

    def __call__(
        self,
        src: L,
        dst: S,
        paths: Iterable[StorePath],
        cancel_event: anyio.Event | None = None,
        *,
        peer_store_id: str | None = None,
        direction: Literal["in", "out"] | None = None,
        substitute: bool = False,
    ) -> Awaitable[int]: ...


async def add_temp_roots(conn: RootHolder, paths: StorePathSet) -> None:
    """Root *paths* on *conn*'s daemon for the connection's life. The pool closes *conn* when it comes back."""
    if not paths:
        return
    conn.holds_roots = True
    if FEATURE_ADD_TEMP_ROOTS in conn.standard_features:
        await conn.call(AddTempRootsRequest(paths=paths), raise_on_error=True)
        return
    for path in sorted(paths, key=str):
        await conn.call(AddTempRootRequest(path=path), raise_on_error=True)


class Executor[S: Builder, L: Answers]:
    """Runs a build on the store the placer chose, and moves it on when the machine fails."""

    def __init__(
        self,
        *,
        queue: BuildQueue,
        local: L,
        stores: Callable[[], Mapping[StoreId, S]],
        settings: StoreRankingSettings,
        trigger: Callable[[], None],
        strip_handled_features: Callable[[QueuedBuild], None],
        stream: Streams[S, L],
    ) -> None:
        self.queue = queue
        self.local = local
        self.local_id = local.store_id
        self._stores = stores
        self.settings = settings
        self.trigger = trigger
        self.strip_handled_features = strip_handled_features
        self.stream = stream

    def note_disk_full(self, store: S, message: str) -> None:
        """Hold *store* back from builds when it ran out of disk. nixidae/nixidae#1013."""
        if "No space left on device" in message:
            store.disk_full_until = time.monotonic() + self.settings.disk_full_hold_seconds
            log.warning("store_held_back_for_disk", store_id=store.store_id)

    def retryable_elsewhere(self, build: QueuedBuild, store: S, resp: BuildDerivationResponse) -> str | None:
        """The machine fault behind a failed result, when another store has not tried yet.

        The local store is a store like the others here: a remote's fault
        may fall back to it, and its own fault may move to a remote. It stays
        out when the client forbids local builds (`--max-jobs 0`).
        """
        fault = machine_fault(resp.result)
        if fault is None:
            return None
        platform = build.request.derivation.platform
        features = effective_required_features(build.request.derivation)
        local_forbidden = client_forbids_local(self.queue, build)
        untried = [
            other
            for other in self._stores().values()
            if other.store_id != store.store_id
            and (other.store_id == self.local_id or outside_the_session(other, build) is None)
            and other.store_id not in build.store_failures
            and not (other.store_id == self.local_id and local_forbidden)
            and other.supports_derivation(platform, features)
        ]
        return fault if untried else None

    async def retry_elsewhere(self, build: QueuedBuild, store: S, resp: BuildDerivationResponse, fault: str) -> None:
        """The builder's machine failed, not the derivation: try another store.

        The local one is included, as a crashed connection already does.
        nixidae/nixidae#1015.
        """
        log.warning("build_failed_on_the_machine", build_id=build.build_id, store_id=store.store_id, fault=fault)
        self.note_disk_full(store, str(resp.result.error_msg))
        await build.post_log_and_fanout(
            LogNext(text=f"pynixd: build failed on {store.store_id} ({fault}), retrying elsewhere\n")
        )
        build.reset_for_retry(store.store_id, str(resp.result.error_msg) or fault)
        self.trigger()

    async def prepare_build(self, build: QueuedBuild, store: S, conn: RootHolder) -> int:
        """Hold the inputs on the builder and send what it lacks; answer the NAR bytes sent.

        All operations that must happen before the build request is sent
        to the backend daemon.
        """
        self.strip_handled_features(build)

        # The builder substitutes first, and pynixd pushes only what no
        # substituter of the builder holds: a builder next to a cache pulls
        # from it, not over the link from this host. A failed substitution is
        # only a warning on the builder (`store-api.cc:758`), and the push
        # covers it. nixidae/nixidae#1074.
        input_srcs = build.request.derivation.input_srcs
        substitute = True
        if input_srcs:
            paths_to_check = {StorePath(path=str(p)) for p in input_srcs}
            await self.hold_inputs_on(conn, paths_to_check)
            try:
                check = await store.execute(QueryValidPathsRequest(paths=paths_to_check, substitute=1))
                valid_paths = [str(cp) for cp in check.paths]
                known = [str(p) for p in input_srcs if str(p) in valid_paths]
            except (BackendError, OSError, ConnectionError) as e:
                # A builder that fell silent while it substituted is asked
                # again by the push, without the download.
                log.warning("builder_substitution_failed", store_id=store.store_id, error=error_text(e))
                known = []
                substitute = False
            missing_paths = {StorePath(str(p)) for p in input_srcs if str(p) not in known}
        else:
            missing_paths = set()
        if not missing_paths:
            return 0
        log.debug("build_sending_inputs", build_id=build.build_id, store_id=store.store_id, count=len(missing_paths))
        return await self.stream(
            self.local, store, missing_paths, peer_store_id=store.store_id, direction="out", substitute=substitute
        )

    async def hold_inputs_on(self, conn: RootHolder, inputs: set[StorePath]) -> None:
        """Root the closure of *inputs* on the builder, for the build connection's life.

        Before the validity check, as `copyPaths` in `src/libstore/store-api.cc`
        does: a path the builder holds and its collector deletes between the
        check and the copy is neither sent nor present, and the copy fails on
        it. The whole closure and not the inputs alone, because a root keeps
        alive only the closure of a path that is valid, and an input the
        builder lacks protects none of its references there. Issue #887.
        """
        closure = await self.local.execute(QueryClosureWithInfoRequest(paths=inputs), client=None)
        await add_temp_roots(conn, inputs | {info.path for info in closure.infos})
