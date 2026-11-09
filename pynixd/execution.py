"""Running a placed build on its store, typed over the stores it reads.

`Scheduler.schedule` places a build through `Placer`, and the placer's spawn
callback runs it here. Each dependency is a Protocol or a real object, so a
test's fake that lacks a member fails `checks.types`, not the guest's unit
phase. nixidae/nixidae#1246.
"""

from __future__ import annotations

import contextlib
import functools
import json
import re
import time
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from nix_daemon_protocol.logs import LogResult
from nix_daemon_protocol.protocol import ResultType, Verbosity
from pynixd_store.build_stats import BuildObservation, identity_of
from pynixd_store.store_path import StorePath

from . import metrics
from ._lazy import ssh_errors
from .allocator import outside_the_session
from .daemon_extensions.query_closure_with_info import QueryClosureWithInfoRequest
from .exceptions import BackendError, BuilderGone, InfrastructureError, error_text, leaves
from .goals.results import failure, result_succeeded
from .placement import client_forbids_local, ms_since
from .serde import (
    AddTempRootRequest,
    AddTempRootsRequest,
    BuildDerivationResponse,
    BuildResultStatus,
    LogNext,
    QueryValidPathsRequest,
)
from .store.base import get_current_system
from .system_features import effective_required_features
from .trust import warning_text
from .wire import FEATURE_ADD_TEMP_ROOTS

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping, MutableMapping
    from contextlib import AbstractAsyncContextManager

    from nix_daemon_protocol.aliases import StorePathSet
    from nix_daemon_protocol.ids import StoreId
    from nix_daemon_protocol.logs import LogMessage
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd_store.build_stats import DerivationIdentity
    from pynixd_store.builder.settings import BuildSettings
    from pynixd_store.local_store_db import MarkKind

    from .build_queue import BuildQueue, QueuedBuild
    from .config import StoreRankingSettings
    from .connection import ClientConn
    from .placement_trace import PlacementTrace
    from .serde import BuildDerivationRequest, BuildResult, SetOptionsRequest

log = structlog.get_logger(__name__)

_PULL_ATTEMPTS = 3
PULL_BACKOFF_SECONDS = 2.0
"""The first wait before a pull again; it doubles. A test passes 0, not a
patched `anyio.sleep`: nixidae/nixidae#1019."""
SILENT_BUILD_PROBE = 600.0
"""How long a build on a builder is silent before pynixd asks its daemon for a fresh handshake."""
HANDSHAKE_TIMEOUT = 60.0
"""How long that handshake may take before pynixd moves the build to another store."""
_ROOTS_CLOSE_SECONDS = 10.0
"""The bound on releasing a build's roots on a store with no daemon, under a shield."""

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


class BuildConn(Protocol):
    """A connection to a builder's daemon. Its temporary roots live as long as it does."""

    @property
    def standard_features(self) -> frozenset[str]: ...

    holds_roots: bool

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        options: SetOptionsRequest | None = None,
        keep_logs: bool | None = None,
    ) -> Any: ...


class Builder(Answers, Protocol):
    """What execution reads of a store that builds."""

    @property
    def session_only(self) -> bool: ...
    @property
    def runs_daemon(self) -> bool: ...

    disk_full_until: float

    def supports_derivation(self, system: str, features: set[str], /) -> bool: ...
    def build_conn(self, options: SetOptionsRequest | None = None, /) -> AbstractAsyncContextManager[BuildConn]: ...
    def transfer_conn(self) -> AbstractAsyncContextManager[BuildConn]: ...
    async def answer_handshake(self) -> None: ...


class Marks(Protocol):
    """The local store's database, as a build decision marks the paths it uses."""

    def mark_paths(self, paths: Iterable[StorePath | str], kind: MarkKind = "runtime") -> None: ...


class Records(Protocol):
    """Where a build's cost goes, for the next build of the same identity."""

    async def record_build_observation(
        self, identity: DerivationIdentity, observation: BuildObservation
    ) -> int | None: ...


class Roots(Protocol):
    """Temporary roots in a store pynixd builds into itself."""

    async def add(self, path: str | StorePath) -> bool: ...
    async def close(self) -> None: ...


class Engine(Protocol):
    def temp_roots(self) -> Roots: ...


class Sandbox(Answers, Protocol):
    """A local store that pynixd's own builder builds into (`docs/design/builder.md`)."""

    @property
    def db(self) -> Records: ...
    @property
    def engine(self) -> Engine: ...

    async def local_build_settings(
        self, request: BuildDerivationRequest, options: SetOptionsRequest | None, /
    ) -> BuildSettings | None: ...
    async def build_here(
        self,
        request: BuildDerivationRequest,
        settings: BuildSettings,
        on_log: Callable[[LogMessage], Awaitable[None]],
        verbosity: int,
        /,
    ) -> BuildDerivationResponse: ...
    async def post_build_hook_after(
        self,
        request: BuildDerivationRequest,
        options: SetOptionsRequest | None,
        out_paths: Iterable[StorePath],
        on_log: Callable[[LogMessage], Awaitable[None]],
        /,
    ) -> str | None: ...


class Streams[E](Protocol):
    """`stream_paths_store_to_store`, between the local store and a store that builds."""

    def __call__(
        self,
        src: E,
        dst: E,
        paths: Iterable[StorePath],
        cancel_event: anyio.Event | None = None,
        *,
        peer_store_id: str | None = None,
        direction: Literal["in", "out"] | None = None,
        substitute: bool = False,
    ) -> Awaitable[int]: ...


class _OutputClock:
    """The log sink of a running build: it notes when each message arrives."""

    options = None

    def __init__(self, build: QueuedBuild) -> None:
        self._build = build

    async def send(self, msg: LogMessage) -> None:
        del msg
        self._build.last_output_at = time.monotonic()


def output_paths(build: QueuedBuild, resp: BuildDerivationResponse) -> StorePathSet:
    """The output paths of a successful build: the static ones and the realised ones."""
    realised = resp.result.realised_outputs() or {}
    ca_paths = {StorePath(str(r.out_path)).with_store_prefix() for r in realised.values() if r.out_path}
    static_paths = {p for p in build.request.derivation.output_paths().values() if p != StorePath("")}
    return static_paths | ca_paths


def builder_log(build: QueuedBuild, resp: BuildDerivationResponse) -> bytes | None:
    """The log a build on another store leaves in the local store, or None under `keep-build-log = false`.

    `buildWithHook` (`derivation-building-goal.cc:619`) opens a local `LogFile` for a build on a
    builder too. From an `ssh-ng://` builder it writes each `BUILD_LOG_LINE` result with a
    newline, and each `SET_PHASE` result as the `@nix` line stdenv prints
    (`derivation-building-goal.cc:671`). `build-remote.sh:74` reads the log back with `nix log`.
    """
    overrides = build.options.overrides if build.options is not None else {}
    if overrides.get("keep-build-log") == "false":
        return None
    lines: list[str] = []
    for message in resp.logs.messages:
        if not isinstance(message, LogResult):
            continue
        first = message.fields[0].valstr if message.fields else None
        if message.result_type == ResultType.BUILD_LOG_LINE:
            lines.append(f"{first or ''}\n")
        elif message.result_type == ResultType.SET_PHASE and first is not None:
            phase = json.dumps({"action": "setPhase", "phase": first}, separators=(",", ":"), ensure_ascii=False)
            lines.append(f"@nix {phase}\n")
    return "".join(lines).encode(errors="surrogateescape")


def kept_on_the_builder(build: QueuedBuild) -> str | None:
    """The hook's warning for a failed build under `--keep-failed` (`build-remote.cc:328`), or None.

    `build-remote.sh:113` asserts it.
    """
    if build.options is None or not build.options.keep_failed:
        return None
    hint = (
        " You can re-run the command with `--builders ''` to disable remote building for this invocation."
        if build.request.derivation.platform == get_current_system()
        else ""
    )
    return warning_text(f"The failed build directory was kept on the remote builder due to `--keep-failed`.{hint}")


async def _hold(roots: Roots, paths: Iterable[StorePath], store_id: StoreId) -> None:
    """Hold *paths* against the collector of a store pynixd builds into itself. No daemon worker holds them."""
    for path in paths:
        if path == StorePath(""):
            continue
        if not await roots.add(path):
            raise BackendError(f"pynixd: store {store_id} cannot hold '{path}' against its collector")


def _cannot_build_there(store_id: StoreId) -> BackendError:
    """A build pynixd's builder leaves to a daemon, on a store that runs none. The log names the reason."""
    return BackendError(f"pynixd: store {store_id} runs no Nix daemon, and pynixd cannot build there")


def with_build_times(resp: BuildDerivationResponse, start: int, stop: int) -> BuildDerivationResponse:
    """*resp* with the times pynixd measured where a builder left Nix's zero.

    NIX-DEFECT (#23): `Store::buildDerivation` (`entry-points.cc:85`) runs a `DerivationTrampolineGoal`,
    whose `doneSuccess` (`derivation-trampoline-goal.cc:227`) answers with a new `BuildResult` that holds the
    status and the outputs alone. A build that ran answers `timesBuilt`, `startTime` and `stopTime` of 0, so
    a client cannot tell how long it took. pynixd fills each field that still holds 0, and only for `Built`:
    a builder that fills one keeps its own value, so pynixd follows Nix again once Nix fixes it.
    Lillecarl/nix#283 merges the stats in the trampoline goal. nixidae/nixidae#1217.
    """
    result = resp.result
    if result.status != BuildResultStatus.BUILT:
        return resp
    update: dict[str, int] = {}
    if not result.times_built:
        update["times_built"] = 1
    if not result.start_time:
        update["start_time"] = start
    if not result.stop_time:
        update["stop_time"] = stop
    if not update:
        return resp
    return resp.model_copy(update={"result": result.model_copy(update=update)})


_ERROR_PREFIX = re.compile(r"\x1b\[31;1merror(?: \[[^\]]*\])?:\x1b\[0m ")
"""What `showErrorInfo` writes before an error's message (`error.cc:223`, `:262`)."""


def without_error_prefix(resp: BuildDerivationResponse) -> BuildDerivationResponse:
    """*resp* with the `error: ` prefix taken off the front of its `errorMsg`.

    NIX-DEFECT (#23): `Store::buildDerivation` (`entry-points.cc:95`) answers an `Error` that escapes the goal
    with `.msg = e.msg()`, the whole formatted text, prefix and all, where `message()` (`error.hh:164`) is the
    message alone. The client adds its own prefix, so it prints `error: error: ...`; Nix's own remote builds
    embed the doubled text the same way. pynixd removes the leading prefix from a daemon's answer and from its
    own builder's, which copies `e.msg()`, so the client prints one, as the daemon alone prints it. A client that
    sends `BuildDerivation` to pynixd gets the message without the prefix too. Lillecarl/nix#409.
    nixidae/nixidae#1204.
    """
    message = resp.result.error_msg
    prefix = _ERROR_PREFIX.match(message)
    if prefix is None:
        return resp
    return resp.model_copy(update={"result": resp.result.model_copy(update={"error_msg": message[prefix.end() :]})})


def builder_options(
    options: SetOptionsRequest | None, store: Answers, local_builder: Answers | None
) -> SetOptionsRequest | None:
    """The client's options, with `max-jobs = 1` for a builder, as the hook sends them (`build-remote.cc:97`).

    The client's `--max-jobs 0` means "not here"; forwarded, it makes the builder refuse the build.
    """
    if options is None or store is local_builder:
        return options
    return options.model_copy(update={"max_build_jobs": 1})


def build_decision_seeds(drv_path: str, input_srcs: Iterable[str]) -> set[str]:
    """The seed set a build decision records, as build-kind.

    The derivation plus the inputs its request names: no derivation file
    is read, because the wire request already carries every input path.
    Deeper layers resolve through the store's own references at flush --
    if they needed building, their own decisions record them in turn.
    Issue #65.
    """
    return {drv_path, *input_srcs}


async def say_where_it_builds(build: QueuedBuild, store: Answers) -> None:
    """Name the backend, but only when the backend is not the local one.

    Nix writes `building '<drv>'...` for a local build, and `building
    '<drv>' on '<machine>'...` for a remote one. The location is news only
    in the second case, and the backend daemon writes the first line
    itself, which pynixd forwards to the client.

    pynixd wrote `pynixd: starting build on local at <timestamp>` for every
    build, and two faults came from that. The extra line broke
    `main:cli-characterisation`, which compares the output of a command
    against a recorded `.exp` file. The timestamp also made the output
    different on each run, so no two recordings of one build could agree.
    """
    if store.store_id == LOCAL_STORE_ID:
        return
    await build.post_log_and_fanout(LogNext(text=f"pynixd: building on {store.store_id}\n"))


async def add_temp_roots(conn: BuildConn, paths: StorePathSet) -> None:
    """Root *paths* on *conn*'s daemon for the connection's life. The pool closes *conn* when it comes back."""
    if not paths:
        return
    conn.holds_roots = True
    if FEATURE_ADD_TEMP_ROOTS in conn.standard_features:
        await conn.call(AddTempRootsRequest(paths=paths), raise_on_error=True)
        return
    for path in sorted(paths, key=str):
        await conn.call(AddTempRootRequest(path=path), raise_on_error=True)


class Executor[S: Builder, L: Answers, H: Sandbox]:
    """Runs a build on the store the placer chose, and moves it on when the machine fails.

    *S* is a store that builds, *L* the local store, and *H* a local store
    that pynixd's own builder builds into. *here* is the local store as such
    a store, or `None` when a daemon builds for it.
    """

    def __init__(
        self,
        *,
        queue: BuildQueue,
        local: L,
        local_builder: S,
        here: H | None,
        sandbox_at: Callable[[StoreId], H | None],
        current: Callable[[StoreId], S | None],
        stores: Callable[[], Mapping[StoreId, S]],
        settings: StoreRankingSettings,
        trigger: Callable[[], None],
        strip_handled_features: Callable[[QueuedBuild], None],
        stream: Streams[S | L | H],
        marks: Callable[[], Marks | None],
        add_build_log: Callable[[str, bytes], Awaitable[object]],
        output_locations: MutableMapping[str, StoreId],
        trace: PlacementTrace,
        pull_backoff: float = PULL_BACKOFF_SECONDS,
        silent_build_probe: float = SILENT_BUILD_PROBE,
        handshake_timeout: float = HANDSHAKE_TIMEOUT,
    ) -> None:
        self.queue = queue
        self.local = local
        self.local_id = local.store_id
        self.local_builder = local_builder
        """The daemon client that builds for the local store; `local` answers its reads."""
        self.here = here
        self._sandbox_at = sandbox_at
        self._current = current
        self._stores = stores
        self.settings = settings
        self.trigger = trigger
        self.strip_handled_features = strip_handled_features
        self.stream = stream
        self._marks = marks
        self._add_build_log = add_build_log
        self.output_locations = output_locations
        self.trace = trace
        self.pull_backoff = pull_backoff
        self.silent_build_probe = silent_build_probe
        self.handshake_timeout = handshake_timeout

    async def run(self, build: QueuedBuild, store: S) -> None:
        """Run *build* on *store*: its inputs there, the build, and its outputs back.

        Phases:
        1. `prepare_build`: strip features, hold and send the inputs
        2. `_execute`: call the builder over its build connection
        3. `collect_outputs`: pull the outputs, register realisations, record stats
        """
        build.assigned_store_id = store.store_id
        # The decision is the use-event for the closure: queue the
        # derivation plus the inputs its request names as build seeds.
        # Synchronous queueing only -- the flush expands and writes on
        # its own schedule -- so recording never slows the build start.
        # Issue #65.
        marks = self._marks()
        if marks is not None:
            marks.mark_paths(
                build_decision_seeds(
                    str(build.request.drv_path),
                    (str(p) for p in build.request.derivation.input_srcs),
                ),
                kind="build",
            )
        build_resp: BuildDerivationResponse | None = None
        completed = False
        try:
            here = self.here
            if store is self.local_builder and here is not None:
                settings = await here.local_build_settings(build.request, build.options)
                if settings is not None:
                    # `derivation-building-goal.cc:44`, `derivation-goal.cc:67`: the derivation and its outputs.
                    # The collector matches hash parts, so the `.lock` and `.drv.chroot` beside them stay too.
                    roots = here.engine.temp_roots()
                    try:
                        await _hold(
                            roots,
                            [build.request.drv_path, *build.request.derivation.output_paths().values()],
                            here.store_id,
                        )
                        await self.build_in_sandbox(build, store, here, settings)
                    finally:
                        with anyio.CancelScope(shield=True), anyio.move_on_after(_ROOTS_CLOSE_SECONDS):
                            await roots.close()
                    return
                if not store.runs_daemon:
                    raise _cannot_build_there(store.store_id)
            # A local store with no daemon has none to delegate to: pynixd builds into it itself.
            other = self._sandbox_at(store.store_id)
            if other is not None and not store.runs_daemon:
                settings = await other.local_build_settings(build.request, build.options)
                if settings is None:
                    raise _cannot_build_there(store.store_id)
                roots = other.engine.temp_roots()
                try:
                    await self.send_inputs(build, other, roots)
                    await self.build_in_sandbox(build, store, other, settings, roots)
                finally:
                    with anyio.CancelScope(shield=True), anyio.move_on_after(_ROOTS_CLOSE_SECONDS):
                        await roots.close()
                return
            async with contextlib.AsyncExitStack() as held:
                async with store.build_conn(builder_options(build.options, store, self.local_builder)) as conn:
                    observation = build.observation = BuildObservation(
                        store_id=str(store.store_id),
                        status=-1,
                        queued_ms=ms_since(build.enqueued_at),
                    )
                    inputs_started = time.monotonic()
                    observation.inputs_bytes = await self.prepare_build(build, store, conn)
                    observation.inputs_ms = ms_since(inputs_started)
                    await say_where_it_builds(build, store)
                    build_resp = await self._execute(build, store, conn)
                    if build.started_at is not None:
                        observation.build_ms = ms_since(build.started_at)
                    # Every success, not `BUILT` alone. A builder that already
                    # holds the outputs answers `ALREADY_VALID`, and the local
                    # store still lacks them. A dynamic derivation's produced
                    # `.drv` then failed with "derivation not found" on each run
                    # until the builder collected it. nixidae/nixidae#998.
                    if result_succeeded(build_resp.result):
                        await self._hold_outputs(held, build, store, build_resp)
                    elif (fault := self.retryable_elsewhere(build, store, build_resp)) is not None:
                        await self.retry_elsewhere(build, store, build_resp, fault)
                        build_resp = None
                    if build_resp is not None and store is not self.local_builder:
                        await self.keep_log(build, build_resp)
                        if not result_succeeded(build_resp.result) and (warning := kept_on_the_builder(build)):
                            await build.post_log_and_fanout(LogNext(text=warning))
                # The build connection goes back before the pull, so the slot
                # takes the next build while this one pulls. A pull took 92 s
                # against a build of 40 s for t3code, with the slot idle for
                # all of it. nixidae/nixidae#1075.
                if build_resp is not None and result_succeeded(build_resp.result):
                    # The outputs first, and the completion after. It ran the
                    # other way round and logged a collection failure on the
                    # server, so a client held a successful BuildResult and
                    # then asked the local store for a path that is not there.
                    # The error it got named the missing path, never the copy
                    # that failed. Issue #12.
                    try:
                        await self.collect_outputs(build, store, build_resp)
                    except Exception as e:
                        log.exception("output_pull_failed", build_id=build.build_id, store_id=store.store_id)
                        reason = f"built on {store.store_id}, but pynixd could not collect the outputs: {e}"
                        await build.post_log_and_fanout(LogNext(text=f"pynixd: {reason}\n"))
                        await self.queue.fail(build.build_id, reason)
                        self.trigger()
                        return

                    build_resp = await self.post_build_hook_here(build, store, build_resp)
                    await self.queue.complete(build.build_id, build_resp)
                    completed = True
                    self.trigger()

        except Exception as e:
            # Decided by the leaves: a task group on the way wraps a
            # BackendError in a group, which no `except BackendError` matches.
            # A dropped connection is the builder's fault as well: a keepalive
            # that closes a silent peer raises here. nixidae/nixidae#1063.
            reason = error_text(e)
            retryable = (BackendError, InfrastructureError, EOFError, OSError, *ssh_errors())
            if all(isinstance(leaf, retryable) for leaf in leaves(e)):
                log.warning("build_failed_retryable", build_id=build.build_id, error=reason)
                self.note_disk_full(store, reason)
                await build.post_log_and_fanout(
                    LogNext(text=f"pynixd: build failed on {store.store_id}, retrying: {reason}\n")
                )
                gone = any(isinstance(leaf, BuilderGone) for leaf in leaves(e))
                build.reset_for_retry(store.store_id, reason, elsewhere=gone)
            else:
                log.exception("build_crashed", build_id=build.build_id)
                await build.post_log_and_fanout(LogNext(text=f"pynixd: internal error, failing build: {reason}\n"))
                await self.queue.fail(build.build_id, f"pynixd internal error: {reason}")
            self.trigger()

        if build_resp is not None and not completed:
            await self.queue.complete(build.build_id, build_resp)
            self.trigger()

    async def build_in_sandbox(
        self,
        build: QueuedBuild,
        store: S,
        local: H,
        settings: BuildSettings,
        roots: Roots | None = None,
    ) -> None:
        """pynixd's own sandbox builds it into *local* (`docs/design/builder.md`).

        Into the local store, nothing is pulled and no worker of the daemon
        holds a root. Into another local store, which has no daemon, *roots*
        hold the outputs there, and they are pulled as from a builder. The
        log is fanned out after the build, as `_execute` does.
        """
        observation = build.observation = BuildObservation(
            store_id=str(store.store_id), status=-1, queued_ms=ms_since(build.enqueued_at)
        )
        await say_where_it_builds(build, store)
        build.started_at = build.last_output_at = time.monotonic()
        if build.wait_time is not None:
            metrics.QUEUE_WAIT_DURATION.observe(build.wait_time)
        messages: list[LogMessage] = []

        async def keep(message: LogMessage) -> None:
            build.last_output_at = time.monotonic()
            messages.append(message)

        verbosity = build.options.verbosity if build.options is not None else Verbosity.INFO
        try:
            resp = without_error_prefix(await local.build_here(build.request, settings, keep, verbosity))
        except Exception as e:
            # pynixd's own builder raised: a bug, or this machine (a full disk). Either way not a
            # lost connection, so the same store gets no second try; another store may.
            log.exception("local_build_crashed", build_id=build.build_id)
            resp = BuildDerivationResponse(result=failure(f"pynixd's sandbox failed: {error_text(e)}"))
        observation.build_ms = ms_since(build.started_at)
        for message in messages:
            await build.post_log_and_fanout(message)
        if result_succeeded(resp.result) and roots is not None:
            try:
                # Until the pull ends, as `_hold_outputs` holds them on a builder.
                await _hold(roots, output_paths(build, resp), local.store_id)
                await self.stream(
                    local, self.local, output_paths(build, resp), peer_store_id=store.store_id, direction="in"
                )
            except Exception as e:
                log.exception("output_pull_failed", build_id=build.build_id, store_id=store.store_id)
                reason = f"built on {store.store_id}, but pynixd could not collect the outputs: {e}"
                await build.post_log_and_fanout(LogNext(text=f"pynixd: {reason}\n"))
                await self.queue.fail(build.build_id, reason)
                self.trigger()
                return
        if result_succeeded(resp.result):
            await self._record_observation(build, resp)
            for path in output_paths(build, resp):
                self.output_locations[str(path)] = store.store_id
        elif (fault := self.retryable_elsewhere(build, store, resp)) is not None:
            await self.retry_elsewhere(build, store, resp, fault)
            return
        await self.queue.complete(build.build_id, resp)
        self.trigger()

    async def send_inputs(self, build: QueuedBuild, to: H, roots: Roots) -> None:
        """Copy the input closure that *to* lacks, before pynixd builds into it.

        *roots* hold the whole closure on *to* first, as `hold_inputs_on` does
        on a builder. No substitution, unlike a push to a builder
        (nixidae/nixidae#1074): the local store holds every input, and both
        stores share this machine's disk.
        """
        inputs = {StorePath(path=str(p)) for p in build.request.derivation.input_srcs}
        closure = await self.local.execute(QueryClosureWithInfoRequest(paths=inputs), client=None)
        await _hold(roots, inputs | {info.path for info in closure.infos}, to.store_id)
        sent = await self.stream(self.local, to, inputs, peer_store_id=to.store_id, direction="out")
        log.debug("build_inputs_sent", build_id=build.build_id, store_id=to.store_id, bytes=sent)

    async def _execute(self, build: QueuedBuild, store: S, conn: BuildConn) -> BuildDerivationResponse:
        """Call the backend daemon and return the response.

        Stderr is buffered in the response, then fanned out to the build's
        subscribers. `_OutputClock` only notes when each message arrives:
        a fan-out from inside the read would let a client that stops reading
        stall the upstream build.
        """
        log.debug("build_executing", build_id=build.build_id, store_id=store.store_id)
        build.started_at = build.last_output_at = time.monotonic()
        if build.wait_time is not None:
            metrics.QUEUE_WAIT_DURATION.observe(build.wait_time)

        start = int(time.time())
        call = functools.partial(
            conn.call,
            build.request,
            client=cast("ClientConn", _OutputClock(build)),
            options=builder_options(build.options, store, self.local_builder),
            keep_logs=True,
        )
        resp: BuildDerivationResponse = await self._while_the_builder_answers(build, store, call)
        resp = without_error_prefix(with_build_times(resp, start, int(time.time())))
        if resp.logs.messages:
            # The count, so a run can tell one fan-out of many messages from
            # many fan-outs of one message.
            log.debug("build_logs_fanned_out", build_id=build.build_id, count=len(resp.logs.messages))
            for msg in resp.logs.messages:
                await build.post_log_and_fanout(msg)
        # **A failure of the build itself gets no log line here.** The message
        # travels in `BuildResult.error_msg`, and the goal that asked for the
        # build decides what a reader sees. `Goal::amDone` at `goal.cc:214`
        # takes the same decision: it logs the failure of a goal that another
        # goal waits for, and it stays quiet for a goal at the top, because
        # the caller reports that one.
        #
        # pynixd wrote one `pynixd: <line>` for each line of the message. A
        # `BuildPathsWithResults` request then carried a log that `nix-daemon`
        # does not send, and the client printed the same text twice. Issue
        # nixidae/nixidae#227.
        log.debug("build_executed", build_id=build.build_id, status=resp.result.status)
        return resp

    async def _while_the_builder_answers[T](self, build: QueuedBuild, store: S, call: Callable[[], Awaitable[T]]) -> T:
        """*call*, or `BuilderGone` once *build* is silent and *store* does not answer a fresh handshake.

        NIX-DEVIATION (#27): `Worker::waitForInput` at `worker.cc:499-501`
        ends a silent build only at `max-silent-time`, and then fails it. A
        builder whose SSH answers keepalives while its daemon hangs held 48
        builds for 55 min (nixidae/nixidae#1063). Silence alone is no proof:
        a busy builder compiles for minutes with no output, and its health
        stream stalls too. So pynixd asks the daemon for a new session, and
        moves the build to another store only when that fails. To reverse
        it, measure a build moved off a builder that would have finished it.
        """
        gone: list[str] = []
        result: list[T] = []
        try:
            async with anyio.create_task_group() as tg:

                async def run() -> None:
                    result.append(await call())
                    tg.cancel_scope.cancel()

                tg.start_soon(self._watch_silence, build, store, tg.cancel_scope, gone)
                tg.start_soon(run)
        except BaseExceptionGroup as group:
            # The watcher never raises: the one error is the call's, and a caller matches it by its class.
            if len(group.exceptions) == 1:
                raise group.exceptions[0] from None
            raise
        if gone:
            log.warning("builder_gone", build_id=build.build_id, store_id=store.store_id, error=gone[0])
            raise BuilderGone(
                f"{store.store_id} sent nothing for {self.silent_build_probe:g} s"
                f" and did not answer a new session: {gone[0]}"
            )
        return result[0]

    async def _watch_silence(self, build: QueuedBuild, store: S, scope: anyio.CancelScope, gone: list[str]) -> None:
        """Ask *store* for a handshake each time *build* is silent for `silent_build_probe`; cancel *scope* when it fails."""
        answered_at = time.monotonic()
        while True:
            heard = max(answered_at, build.last_output_at or answered_at)
            if (left := heard + self.silent_build_probe - time.monotonic()) > 0:
                await anyio.sleep(left)
                continue
            try:
                with anyio.fail_after(self.handshake_timeout):
                    await store.answer_handshake()
            except Exception as e:
                gone.append(error_text(e))
                scope.cancel()
                return
            answered_at = time.monotonic()

    async def post_build_hook_here(
        self, build: QueuedBuild, store: S, resp: BuildDerivationResponse
    ) -> BuildDerivationResponse:
        """*resp*, failed when the local post-build hook fails after a build on a builder.

        `buildWithHook` runs the hook locally once the outputs are valid (`derivation-building-goal.cc:765`),
        and the builder runs its own as well. `build-remote-input-addressed.sh:35` counts both. The local
        goal never sees the builder's status, so an `ALREADY_VALID` answer runs the hook too.
        """
        if store is self.local_builder or self.here is None or not result_succeeded(resp.result):
            return resp
        failed = await self.here.post_build_hook_after(
            build.request, build.options, output_paths(build, resp), build.post_log_and_fanout
        )
        if failed is None:
            return resp
        return resp.model_copy(update={"result": failure(failed)})

    async def keep_log(self, build: QueuedBuild, resp: BuildDerivationResponse) -> None:
        """Write the log of a build on a builder to the local store, as Nix's build hook does."""
        text = builder_log(build, resp)
        if text is None:
            return
        try:
            # The base name, as `AddBuildLog` carries it (`daemon.cc:1013`).
            await self._add_build_log(StorePath(build.request.drv_path).name, text)
        except Exception:
            # `LogFile` failing ends Nix's goal; pynixd keeps the build, which has its outputs.
            log.warning("build_log_not_kept", build_id=build.build_id, exc_info=True)

    async def _hold_outputs(
        self, held: contextlib.AsyncExitStack, build: QueuedBuild, store: S, resp: BuildDerivationResponse
    ) -> None:
        """Root the outputs on the builder for the pull, past the build connection.

        The daemon worker of the build connection holds a root on each output,
        and it ends with that connection. A transfer connection taken here, in
        *held*, holds the roots instead. It is taken inside the build
        connection, so it is re-entrant, and every acquire of the pull under
        it skips the pool limit as before: a pull holds the builder and waits
        for local, a push the reverse, and full pools would deadlock the two.
        """
        conn = await held.enter_async_context(store.transfer_conn())
        await add_temp_roots(conn, output_paths(build, resp))

    async def collect_outputs(self, build: QueuedBuild, store: S, resp: BuildDerivationResponse) -> None:
        """Pull outputs back to local store, register realisations, record stats.

        Runs after a successful build execution.
        """
        if not result_succeeded(resp.result):
            return

        # `realised_outputs` reads whichever of the two wire shapes the answer
        # carried. Issue #14.
        realised = resp.result.realised_outputs()
        if realised:
            build.ca_realisations = list(realised.values())
        all_output_paths = output_paths(build, resp)
        log.info("pulling_paths", store_id=store.store_id, count=len(all_output_paths))

        # The record after the pull, so it holds the pull's cost, and in
        # `finally`, so a failed pull keeps the rest. Issue #12.
        pull_started = time.monotonic()
        try:
            pulled = await self._pull_with_retries(build, store, all_output_paths)
            if build.observation is not None:
                build.observation.pull_ms = ms_since(pull_started)
                build.observation.pull_bytes = pulled
        finally:
            await self._record_observation(build, resp)
        # After the pull, and not before it: `_ensure_opaque` answers success
        # for a path named here, and a failed pull left nothing local to serve.
        for path in all_output_paths:
            self.output_locations[str(path)] = store.store_id

    async def _pull_with_retries(self, build: QueuedBuild, store: S, paths: StorePathSet) -> int:
        """The pull, tried again when it fails: the build succeeded, and one dropped stream is no reason to fail it.

        A 1.6 GB output once failed with "0 bytes read", and the same pull
        worked minutes later. nixidae/nixidae#942. A pull again skips what
        already arrived: it asks the local store which paths it holds.

        Each attempt asks for the store by id. That "0 bytes read" was sg1-n1's
        pynixd restarting: a reverse builder comes back as a new store object
        under the same id, 3.25 s later, and the old object has no connection
        left. The backoff (2 s, then 4 s) outlasts that.
        """
        attempt = 1
        while True:
            try:
                return await self._pull_outputs(store, paths)
            except Exception as e:
                if attempt >= _PULL_ATTEMPTS:
                    raise
                log.warning(
                    "output_pull_retrying",
                    build_id=build.build_id,
                    store_id=store.store_id,
                    attempt=attempt,
                    error=error_text(e),
                )
                await anyio.sleep(self.pull_backoff * 2 ** (attempt - 1))
                attempt += 1
                current = self._current(store.store_id)
                if current is not None:
                    store = current

    async def _record_observation(self, build: QueuedBuild, resp: BuildDerivationResponse) -> None:
        """Write what this build cost, for the next build of the same identity.

        Every success, `ALREADY_VALID` included: a build that built nothing
        measures what delegating it cost. The estimator reads `BUILT` alone.
        nixidae/nixidae#1001.
        """
        observation = build.observation
        here = self.here
        if here is None or observation is None:
            return
        observation.status = int(resp.result.status)
        observation.cpu_user_us = resp.result.cpu_user.value if resp.result.cpu_user else None
        observation.cpu_system_us = resp.result.cpu_system.value if resp.result.cpu_system else None
        if (used := resp.result.resources).measured:
            observation.resource_source = used.source
            observation.peak_memory_bytes = used.peak_memory_bytes
            observation.io_read_bytes = used.io_read_bytes
            observation.io_write_bytes = used.io_write_bytes
            observation.cpu_stall_us = used.cpu_stall.value
            observation.memory_stall_us = used.memory_stall.value
            observation.io_stall_us = used.io_stall.value
            observation.samples = tuple(used.samples)
            if used.host_source:
                observation.host_resource_source = used.host_source
                observation.host_cpu_user_us = used.host_cpu_user_us
                observation.host_cpu_system_us = used.host_cpu_system_us
                observation.host_peak_memory_bytes = used.host_peak_memory_bytes
        identity = identity_of(build.request.derivation)
        observation_id = await here.db.record_build_observation(identity, observation)
        if build.placement is not None and observation_id is not None:
            self.trace.observed(build.placement, observation_id)
        log.info(
            "build_observed",
            name=identity.name,
            store_id=observation.store_id,
            status=observation.status,
            expected_ms=build.expected_duration,
            build_ms=observation.build_ms,
            inputs_ms=observation.inputs_ms,
            pull_ms=observation.pull_ms,
        )

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
        build.reset_for_retry(store.store_id, str(resp.result.error_msg) or fault, elsewhere=True)
        self.trigger()

    async def prepare_build(self, build: QueuedBuild, store: S, conn: BuildConn) -> int:
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

    async def hold_inputs_on(self, conn: BuildConn, inputs: set[StorePath]) -> None:
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

    async def _pull_outputs(self, store: S, paths: StorePathSet) -> int:
        """Bring the outputs of a backend build into the local store, answering the NAR bytes moved.

        The local store has to hold the paths, and a record of where they came
        from is not enough. A client on a Unix socket never asks pynixd for a
        NAR: `UDSRemoteStore::narFromPath` calls `Store::narFromPath`, which
        reads the store directory through `LocalFSStore::getFSAccessor`. So
        `nix copy --from unix://...` of a backend-built path reported "path
        ... does not exist" and named the local store directory -- issue nixidae/nixidae#199.

        `ctx.output_locations` still holds, and it still answers for the
        clients that do use the wire, such as `ssh-ng://` and the HTTP cache.
        This is the transfer that path mapping cannot replace.

        One route, over the wire, for every backend. A second route copied the
        store directory and then the SQLite rows of the builder database
        straight into the local one, when both ends were a `LocalDBStore`.
        Issue nixidae/nixidae#197 removed it, and it was wrong in four ways:

        - No NAR was written and no hash was checked, so a truncated copy
          registered as valid.
        - The `Refs` copy carried only rows whose referrer was a new output,
          and inserted with `INSERT OR IGNORE ... SELECT`. A reference whose
          path had no row in the destination yet was dropped in silence.
        - It wrote into a running daemon's database behind that daemon's back.
        - It copied `ultimate` verbatim, which marked the local store as the
          builder of a path it never built. Nix sets `ultimate` in
          `unix/build/derivation-builder.cc` alone -- the local builder -- and
          clears it on every copy. A distributed build leaves `ultimate =
          false` in the store that receives the output, and the shortcut said
          `true`.

        `stream_paths_store_to_store` has none of those. It asks for the
        closure, so a reference cannot go missing, and it hands each path to
        the destination daemon, which hashes it and registers it itself.
        """
        if not paths:
            return 0
        log.debug(
            "pull_outputs_streaming",
            store_id=store.store_id,
            count=len(paths),
        )
        return await self.stream(store, self.local, paths, peer_store_id=store.store_id, direction="in")
