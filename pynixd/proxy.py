"""
Nix daemon protocol session handler.

Accepts a client connection, performs the handshake, decodes operations,
and dispatches them to request type handle() classmethods.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any, cast

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.wire_ops import WIRE_REGISTRY, WireResponse

from . import metrics, wire
from ._lazy import ssh_connection_lost
from .config import ScheduleMode
from .connection import ClientConn
from .derived_path import DerivedPath
from .exceptions import ClosingError, OpNotImplementedError, error_text
from .goals import GoalEngine
from .handlers._base import HANDLER_REGISTRY
from .protocol import get_extension_features
from .serde import (
    AddTempRootRequest,
    BuildMode,
    BuildPathsRequest,
    BuildPathsWithResultsRequest,
    IsValidPathRequest,
    IsValidPathResponse,
    LogError,
    QueryMissingRequest,
    QueryPathInfoRequest,
    QueryValidPathsRequest,
    QueryValidPathsResponse,
)
from .serde.auth import Role
from .serde.context import ReadContext, RequestContext as RequestContext, WriteContext
from .serde.protocol import OptTrusted, Verbosity
from .store_layout import StoreLayout
from .temp_roots import TempRoots

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from nix_daemon_protocol.wire_ops import WireRequest

    from .build_queue import BuildQueue
    from .context import PynixdContext
    from .scheduler import Scheduler
    from .store import LocalStore, Store
    from .wire import NixReader, NixWriter

from .store import DaemonStore as DaemonStore

log = structlog.get_logger(__name__)


NIX_VERSION: str = "pynixd-0.1.0"
"""What pynixd answers when it has no upstream daemon to name.

**The field is a version a client compares, and not a name.** `store-info.sh`
of the Nix functional suite reads it with `nix store info` and greps for the
version of the daemon, and `isDaemonNewer` in `common/functions.sh:138-142`
feeds it to `builtins.compareVersions`. `pynixd-0.1.0` compared against
`2.7.0pre20220126` answers nothing useful, and no client can tell that it
failed to. So `_version_for_the_client` below answers the upstream version
where there is one, and this only where there is not.

pynixd says what it is in `client_handshake_complete` and in the log it
writes for every build. It does not need this field to do it.
"""


class DaemonProxy:
    """Per-client session: handshake, op dispatch, response encoding.

    Each client connection gets its own DaemonProxy. Connection instances
    are acquired from the local_store pool per-operation and returned
    immediately, keeping pool usage minimal.
    """

    def __init__(
        self,
        client_r: NixReader,
        client_w: NixWriter,
        ctx: PynixdContext,
        role: Role = Role.USER,
        username: str = "unknown",
        schedule_mode: ScheduleMode = ScheduleMode.auto,
        transport: str = "unknown",
        hangup: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.r = client_r
        self._hangup = hangup
        """Returns when the client hangs up. `None` watches nothing."""
        self.connected_at = time.monotonic()
        self.current_op: tuple[str, float] | None = None
        """The operation this session runs now, and when it began. `pynixd
        state` reads it to say what each client waits on. Issue #92."""
        self.w = client_w
        self.client = ClientConn(w=self.w)
        self.ctx = ctx
        self.version: int = wire.PROTOCOL_VERSION
        self.standard_features: frozenset[str] = frozenset()
        self.role: Role = role
        self.username: str = username
        self.schedule_mode: ScheduleMode = schedule_mode
        self.transport = transport
        """Which listener accepted this session. A metric label, so it is one
        of a fixed few words and never an address."""
        self._op_timing: dict[int, tuple[int, float]] = {}
        self._encode_timing: dict[int, tuple[int, float]] = {}
        self._temp_roots: TempRoots | None = None
        self._sync_reader_ready = False
        self._metrics_enabled = ctx.settings.metrics_enabled
        # The Prometheus children for (op, result), looked up once per series.
        self._op_metrics: dict[tuple[str, str], metrics.OpSeries] = {}

    @property
    def local_store(self) -> LocalStore:
        """The local Nix store for direct store operations."""
        return self.ctx.local_store

    @property
    def metrics_enabled(self) -> bool:
        """Whether this session updates Prometheus metrics.

        Read from the settings once per session: the switch restarts the
        service, so it cannot change under a session.
        """
        return self._metrics_enabled

    def _version_for_the_client(self) -> str:
        """The version of the daemon that answers behind pynixd.

        `store-info.sh:69` of the Nix functional suite asserts that `nix store
        info` reports the version of the daemon it is talking to. pynixd
        answered `pynixd-0.1.0` and failed that assertion -- the one test the
        suite failed against pynixd and not against `nix-daemon`.

        The field decides what a client believes the daemon can do, so the
        upstream version is the true answer: pynixd forwards to that daemon
        and cannot do more than it can. A name in this field is not merely
        cosmetic, because `compareVersions` takes it and answers rubbish
        quietly.
        """
        return self.local_store.nix_version or NIX_VERSION

    @property
    def scheduler(self) -> Scheduler | None:
        """Build scheduler instance, or None if no remote stores."""
        return self.ctx.scheduler

    @property
    def build_queue(self) -> BuildQueue | None:
        """Build queue from the scheduler, or None if unconfigured."""
        return self.scheduler.queue if self.scheduler else None

    @property
    def goal_engine(self) -> GoalEngine:
        """Goal engine for client-facing build operations."""
        return GoalEngine(self.ctx)

    @property
    def scheduler_trigger(self) -> Callable[[], None] | None:
        """Callback to wake the scheduler, or None if unconfigured."""
        return self.scheduler.trigger if self.scheduler else None

    @property
    def stores(self) -> Mapping[StoreId, Store]:
        """All configured stores by store ID."""
        return self.ctx.stores

    @property
    def use_scheduler_for_builds(self) -> bool:
        """Whether builds should go through the scheduler queue.

        In 'auto' mode, uses scheduler when builder stores are configured.
        In 'scheduler' mode, always uses scheduler (builds queue even with
        no builders). In 'proxy' mode, never uses scheduler.
        """
        if self.schedule_mode == ScheduleMode.proxy:
            return False
        if self.schedule_mode == ScheduleMode.scheduler:
            return True
        # auto: scheduler if there are builder stores
        return self.scheduler is not None and bool(self.scheduler.stores)

    async def run(self) -> None:
        """Run the full session lifecycle."""
        metrics.DAEMON_SESSIONS_TOTAL.labels(transport=self.transport).inc()
        metrics.DAEMON_SESSIONS.labels(transport=self.transport).inc()
        # Register before the handshake: from here on this session exists,
        # and shutdown must answer it. Issue #64.
        self.ctx.sessions.track(self, asyncio.current_task())
        try:
            with anyio.CancelScope() as scope:
                watcher = None if self._hangup is None else asyncio.create_task(self._cancel_on_hangup(scope))
                try:
                    await self.handshake()
                    self._open_sync_reader()
                    await self.op_loop()
                finally:
                    if watcher is not None:
                        watcher.cancel()
                        with contextlib.suppress(Exception, asyncio.CancelledError):
                            await watcher
        except (EOFError, BrokenPipeError, ConnectionError, OSError) + ssh_connection_lost():
            log.debug("client_disconnected")
        except Exception:
            log.exception("session_error")
        finally:
            self.ctx.sessions.untrack(self)
            metrics.DAEMON_SESSIONS.labels(transport=self.transport).dec()
            if self._temp_roots is not None:
                await self._temp_roots.close()
            self._close_sync_reader()
            if self._op_timing:
                total_time = sum(t for _, t in self._op_timing.values())
                total_ops = sum(n for n, _ in self._op_timing.values())
                total_encode = sum(t for _, t in self._encode_timing.values())
                breakdown = {}
                for op_num, (count, acc_time) in sorted(self._op_timing.items()):
                    req_cls = WIRE_REGISTRY.get(op_num)
                    name = req_cls.name if req_cls else f"op_{op_num}"
                    breakdown[name] = f"x{count} {acc_time:.3f}s"
                encode = {}
                for op_num, (count, acc_time) in sorted(self._encode_timing.items()):
                    req_cls = WIRE_REGISTRY.get(op_num)
                    name = req_cls.name if req_cls else f"op_{op_num}"
                    encode[name] = f"x{count} {acc_time:.3f}s"
                log.info(
                    "client_op_timing",
                    total_ops=total_ops,
                    total_time=f"{total_time:.3f}s",
                    total_encode=f"{total_encode:.3f}s",
                    breakdown=breakdown,
                    encode_breakdown=encode,
                )

    async def _cancel_on_hangup(self, scope: anyio.CancelScope) -> None:
        """End the session when the client hangs up, as `MonitorFdHup` does.

        `daemon.cc:1016` interrupts whatever operation runs. A request that
        waits for a build then gives its reference back, and the queue ends a
        build that nobody else wants. Not a task group: that wraps the
        session's own `EOFError` in an `ExceptionGroup`. Issue #91.
        """
        if self._hangup is None:
            return
        await self._hangup()
        op = self.current_op
        if op is None:
            log.debug("client_hung_up")
        else:
            log.info("client_hung_up", op=op[0], op_seconds=round(time.monotonic() - op[1], 3))
        scope.cancel()

    def _note_encode_time(self, op_num: int, started: float) -> None:
        """Record the time to encode and write the response above `dispatch`.

        `_op_timing` measures `dispatch` alone, and a response is encoded and
        written after it returns. For a response stream of 16 000 small ops,
        that half is worth measuring separately: it is the cost of the shape
        pynixd writes, and the client pays it again to decode.
        """
        elapsed = time.monotonic() - started
        count, acc = self._encode_timing.get(op_num, (0, 0.0))
        self._encode_timing[op_num] = (count + 1, acc + elapsed)

    # ── Handshake ────────────────────────────────────────────────────

    def honourable_features(self) -> frozenset[str]:
        """The standard features that pynixd may name to a client.

        **A proxy honours a feature by speaking its shape on both sides.**
        `SUPPORTED_STANDARD_FEATURES` says which shapes this code can write
        at all, and that is only half of the question. The other half is
        whether the backend reads them. This answers what **any** store that
        a build can go to offers: one backend with the feature is enough for
        pynixd to name it. Issue #84.

        The union is sound because every hop negotiates apart. The client and
        pynixd agree one set, and pynixd and each backend agree their own on
        every pooled connection. A build request carries no realisation
        shape at all -- a `BasicDerivation` only -- so any store can run any
        build; the result crosses each hop in the shape that hop agreed to,
        via `for_the_wire` on the way out and the per-connection codec on
        the way in. The two requests that do carry one shape alone, op 42
        and op 43, never reach the scheduler: `register_drv_output` and
        `query_realisation` of `store/daemon.py` fill the missing shape, or
        refuse loudly, before the verbatim forward. Nothing is ever sent a
        shape it never agreed to.

        **A substituter is left out.** `no_schedule` marks a store that the
        scheduler never sends a build to, and a binary cache is not a peer of
        the worker protocol at all, so its empty feature set would answer
        "nothing" for every configuration that holds one. The loop below that
        collects the feature matrix leaves them out for the same reason.

        A store that has never connected contributes nothing, and that is
        the honest answer and not a veto: it reports an empty set until its
        first handshake, and an offline builder no longer takes a feature
        away from builds that would never touch it.
        """
        honourable: set[str] = set()
        for store in self.stores.values():
            if store.no_schedule:
                continue
            honourable |= set(store.features)
        return frozenset(honourable & set(wire.SUPPORTED_STANDARD_FEATURES))

    async def handshake(self) -> None:
        """Server-side daemon protocol handshake."""
        magic = await self.r.read_uint64()
        if magic != wire.WORKER_MAGIC_1:
            raise ValueError(f"Bad client magic: {magic:#x}")

        # Present pynixd's supported protocol version to the client.
        # This enables feature negotiation (1.38+) even if the local store is older.
        server_version = wire.PROTOCOL_VERSION
        self.w.write_uint64(wire.WORKER_MAGIC_2)
        self.w.write_uint64(server_version)
        await self.w.drain()

        client_version = await self.r.read_uint64()
        if client_version < wire.MINIMUM_CLIENT_PROTOCOL:
            raise ValueError("the Nix client version is too old")
        self.version = min(server_version, client_version)
        log.info(
            "client_protocol_negotiated",
            client_version=wire.proto_str(client_version),
            local_store_version=wire.proto_str(server_version),
            negotiated_version=wire.proto_str(self.version),
        )

        # Feature negotiation (1.38+) — before CPU/reserveSpace
        if self.version >= wire.FEATURE_EXCHANGE_PROTOCOL:
            client_features = await self.r.read_string_set()
            # The intersection, and not what the client named. A client that
            # names `realisation-with-path-not-hash` gets the new codec only
            # when pynixd names it back, which is `intersectFeatures` at
            # `worker-protocol-connection.cc:148`. Issue #14.
            honourable = self.honourable_features()
            self.standard_features = wire.negotiate_features(client_features, honourable)
            # `ClientConn.send` writes a log message straight to the client,
            # so it needs the set of that client and not of a backend.
            self.client.standard_features = self.standard_features
            log.debug(
                "client_features",
                client_features=client_features,
                honourable_features=sorted(honourable),
                standard_features=sorted(self.standard_features),
            )

            our_features = get_extension_features() | set(honourable)
            # Features the proxy honours itself join here rather than in
            # `honourable_features`: that answers what any buildable store
            # reads, and op 49 never reaches one -- it writes session roots
            # the way op 11 does. An old backend must not unname it, because
            # the gate has no fallback: a client that misses the name
            # silently holds no batch roots at all. Issue #66.
            our_features.add(wire.FEATURE_ADD_TEMP_ROOTS)

            # Only build-capable stores contribute scheduling capabilities.
            # Substituters are deliberately non-scheduleable; advertising
            # their host features would incorrectly tell clients that this
            # server can build for those platforms.
            for store in self.stores.values():
                if store.no_schedule:
                    continue
                fm = store._feature_matrix
                if fm:
                    for system, features in fm.items():
                        our_features.add(f"feature_matrix:{system}")
                        for feat in features:
                            our_features.add(f"feature_matrix:{system}:{feat}")

            self.w.write_string_set(our_features)  # our features
            await self.w.drain()

        if await self.r.read_uint64():  # sendCpu
            await self.r.read_uint64()  # cpuAffinity (ignored)
        await self.r.read_uint64()  # reserveSpace (ignored)

        # Server conditions these on clientVersion
        if client_version >= wire.proto(1, 33):
            self.w.write_string(self._version_for_the_client())
            if client_version >= wire.proto(1, 35):
                self.w.write_uint64(OptTrusted.TRUSTED if self.role == Role.ADMIN else OptTrusted.NOT_TRUSTED)
        self.w.write_uint64(wire.STDERR_LAST)
        await self.w.drain()

        log.info("client_handshake_complete")

    # ── Op loop ──────────────────────────────────────────────────────

    async def op_loop(self) -> None:
        """Read ops, dispatch, write responses.

        **An operation that no registry knows ends the connection.** Nothing
        read the arguments of that operation, so the next `read_uint64` would
        read the first argument as the next operation number. Every operation
        after it is then nonsense, and the client learns nothing about it.

        `performOp` of Nix throws `invalid operation` at `daemon.cc:1107`,
        before `logger->startWork()`. `canSendStderr` is therefore false, so
        `errorAllowed` at `daemon.cc:1218` is false and the handler re-throws.
        The outer catch at `daemon.cc:1232` writes the error, flushes and
        returns, which closes the connection. Issue nixidae/nixidae#232.
        """
        while True:
            try:
                op_num = await self.r.read_uint64()
            except (EOFError,) + ssh_connection_lost():
                break

            req_cls = WIRE_REGISTRY.get(op_num)
            handler_cls = HANDLER_REGISTRY.get(op_num)
            if req_cls is None and handler_cls is None:
                log.warning("unknown_op", op_num=op_num)
                await self.send_error(f"Unsupported operation: {op_num}")
                break

            op_name = req_cls.name if req_cls else handler_cls.__name__ if handler_cls else f"op_{op_num}"

            t0 = time.monotonic()
            self.current_op = (op_name, t0)
            result = "ok"
            try:
                response = await self.dispatch(op_num)

                if response is not None:
                    await self.client.flush()
                    t_encoded = time.monotonic()
                    await response.to_writer(WriteContext.from_proxy(self))
                    await self.w.drain()
                    self._note_encode_time(op_num, t_encoded)
                # else: already handled (streaming, error, etc.)

            except ClosingError as ex:
                result = "error"
                log.info("handle_op_closing", name=op_name, reason=str(ex))
                await self.client.flush()
                await self.send_error(str(ex))
                break
            except Exception as ex:
                result = "error"
                log.exception("handle_op_error", name=op_name)
                await self.client.flush()
                await self.send_error(error_text(ex))
            finally:
                self.current_op = None
                elapsed = time.monotonic() - t0
                count, acc = self._op_timing.get(op_num, (0, 0.0))
                self._op_timing[op_num] = (count + 1, acc + elapsed)
                if self._metrics_enabled:
                    key = (op_name, result)
                    series = self._op_metrics.get(key)
                    if series is None:
                        series = metrics.op_series(op_name, result)
                        self._op_metrics[key] = series
                    series.observe(elapsed)

    # ── Dispatch ─────────────────────────────────────────────────────

    async def execute(  # type: ignore[no-overload-impl]
        self,
        request: WireRequest,
    ) -> Any:
        """Execute an operation, falling back to other stores for extensions."""
        if (
            isinstance(request, BuildPathsRequest | BuildPathsWithResultsRequest)
            and request.build_mode == BuildMode.REPAIR
            and self.role < Role.ADMIN
        ):
            # `daemon.cc:554,572`, before `startWork`, so Nix closes the connection.
            raise ClosingError("repairing is not allowed because you are not in 'trusted-users'")
        if isinstance(request, BuildPathsRequest | BuildPathsWithResultsRequest) and request.build_mode not in (
            BuildMode.NORMAL,
            BuildMode.REPAIR,
            BuildMode.CHECK,
        ):
            # `WorkerProto::Serialise<BuildMode>::read` throws while the
            # request is read, before `startWork` (`worker-protocol.cc:70`).
            raise ClosingError("Invalid build mode")
        if isinstance(request, BuildPathsRequest | BuildPathsWithResultsRequest | QueryMissingRequest):
            # Nix parses each derived path while it reads the request, before
            # `startWork` (`derived-path.cc:117`, `path.cc:12`), so a bad
            # output name closes the session. nixidae/nixidae#1005.
            for wire_path in request.derived_paths:
                try:
                    DerivedPath(wire_path.value)
                except ValueError as exc:
                    raise ClosingError(str(exc)) from exc
        if isinstance(request, BuildPathsWithResultsRequest):
            return await self.goal_engine.build_paths_with_results(request, client=self.client, hold=self.add_temp_root)
        if isinstance(request, BuildPathsRequest):
            return await self.goal_engine.build_paths(request, client=self.client, hold=self.add_temp_root)
        if isinstance(request, QueryMissingRequest):
            return await self.goal_engine.query_missing(request, client=self.client)

        if isinstance(request, QueryValidPathsRequest):
            self._force_substitute_on_destination(request)

        local_resp: WireResponse | None = None
        try:
            local_resp = await self.local_store.execute(request, client=self.client)
            mapped_resp = await self._mapped_output_response(request, local_resp)
            if mapped_resp is not None:
                return mapped_resp
            if local_resp is not None and not (request.is_extension and local_resp.is_not_found):
                return local_resp
        except OpNotImplementedError:
            if not request.is_extension:
                raise

        # Extension not supported by local store or returned not found — try other stores
        for store in self.stores.values():
            if store.store_id == LOCAL_STORE_ID:
                continue  # already tried above
            if request.name not in store.features:
                # Decided from the handshake cache, in the established idiom,
                # not from a send-and-refuse: a store that never advertised
                # the op is skipped before a packet names it. Issue #82.
                log.debug(
                    "extension_op_not_advertised",
                    op=request.name,
                    store_id=store.store_id,
                )
                continue
            try:
                # We don't forward client logs to remote stores for simple queries
                # unless they are builds.
                resp = await store.execute(request, client=self.client)
                if not resp.is_not_found:
                    return resp
            except OpNotImplementedError:
                continue

        # If we got here, none of the backends had a "found" result.
        # Return the local_store result (even if empty) as the final word,
        # unless it wasn't even implemented.
        if local_resp is not None:
            return local_resp

        raise OpNotImplementedError(
            f"Extension operation {type(request).__name__} (op={request.op}) not supported by any configured store",
        )

    async def _mapped_output_response(
        self, request: WireRequest, local_resp: WireResponse | None
    ) -> WireResponse | None:
        if isinstance(request, QueryPathInfoRequest):
            if local_resp is not None and getattr(local_resp, "valid", False):
                return None
            store = self.store_for_output_path(str(request.path))
            if store is None:
                return None
            return cast("WireResponse", await store.execute(request, client=self.client))

        if isinstance(request, IsValidPathRequest):
            if local_resp is not None and getattr(local_resp, "valid", False):
                return None
            if self.store_for_output_path(str(request.path)) is None:
                return None
            # A constant of the declared type: nothing to coerce or refuse.
            return IsValidPathResponse.fast(valid=True)

        if isinstance(request, QueryValidPathsRequest):
            paths = set(getattr(local_resp, "paths", set())) if local_resp is not None else set()
            for path in request.paths:
                if self.store_for_output_path(str(path)) is not None:
                    paths.add(path)
            if local_resp is not None and paths == getattr(local_resp, "paths", set()):
                return None
            return QueryValidPathsResponse(paths=paths)

        return None

    def store_for_output_path(self, path: str) -> DaemonStore | None:
        """Look up the DaemonStore that produced a given output path."""
        return self.ctx.store_for_output_path(path)

    def _force_substitute_on_destination(self, request: QueryValidPathsRequest) -> None:
        """Rewrite substitute to 1 when the deployment forces it. Issue #54.

        The request is the decoded per-connection object, so rewriting it
        reaches every store the operation then touches, in the serialized
        form each one reads. Internal callers never pass through here: the
        scheduler and the transfer helper call `store.execute` directly with
        `substitute=0`, and that stays. `None` is an old client whose
        protocol predates the flag, and forcing covers it too: the upstream
        reads the field exactly when its version carries it.
        """
        if request.substitute:
            return
        if not self.ctx.settings.force_substitute_on_destination:
            return
        request.substitute = 1
        log.debug(
            "force_substitute_on_destination",
            paths=len(request.paths),
        )

    async def dispatch(self, op_num: int) -> WireResponse | None:
        """Route an operation to its request type's handle method."""
        # NEW: try new handler registry first
        if handler_cls := HANDLER_REGISTRY.get(op_num):
            response = await handler_cls().handle(
                RequestContext(
                    proxy=self,
                    role=self.role,
                    version=self.version,
                    username=self.username,
                )
            )
            return cast(
                "WireResponse | None",
                response,
            )

        # NEW: try serde wire registry for handler-less ops
        if wire_cls := WIRE_REGISTRY.get(op_num):
            req = await wire_cls.from_reader(
                ReadContext(reader=self.r, version=self.version, features=self.standard_features),
            )
            return await self.execute(req)

        log.warning("unhandled_op", op_num=op_num)
        await self.send_error(f"Unhandled operation: {op_num}")
        return None

    # ── Store database reads ─────────────────────────────────────────

    def _open_sync_reader(self) -> None:
        """Give this session its own read-only store-DB connection.

        One connection for each client, so a slow query of one session cannot
        wait behind the query of another, and the read then runs on the event
        loop without a thread hop. `SyncReader` has the measurement. A store
        with no readable database gives `None`, and every read uses the pooled
        connection.
        """
        if self._sync_reader_ready:
            return
        self._sync_reader_ready = True
        db = getattr(self.local_store, "db", None)
        if db is None:
            return
        self.client.sync_reader = db.sync_reader()

    def _close_sync_reader(self) -> None:
        reader = self.client.sync_reader
        if reader is not None:
            reader.close()
            self.client.sync_reader = None

    # ── Temporary roots ──────────────────────────────────────────────

    async def add_temp_root(self, path: str | StorePath) -> None:
        """Hold `path` against the collector until this client goes away.

        pynixd writes the root itself, in the `temproots` directory of the
        store. It used to forward the operation to the upstream daemon, and
        the root then belonged to a pooled connection rather than to the
        client that asked for it. Issue #20, and `temp_roots.py` for how the
        file works.
        """
        if self._temp_roots is None:
            # The state directory of the store that pynixd serves, which is
            # `<root>/nix/var/nix` for a chroot store and `NIX_STATE_DIR` for
            # a relocated one. `StoreLayout` answers both. Issue nixidae/nixidae#215.
            layout = getattr(self.local_store, "layout", None) or StoreLayout.chroot(None)
            self._temp_roots = TempRoots(layout.state_dir)
        if await self._temp_roots.add(path):
            return
        # NIX-DEVIATION (#27): Nix fails `AddTempRoot` when it cannot write
        # its roots file (`gc.cc:84-143`). An unprivileged pynixd always
        # would, and `nix build` asks for a root on every derivation, so
        # pynixd gives the root to the daemon instead. That root lives as
        # long as the pooled connection, not the session. Reverse it if
        # pynixd ever runs only where it can write the state directory.
        # nixidae/nixidae#1007.
        await self.local_store.execute(AddTempRootRequest(path=StorePath(str(path))))

    # ── Helpers ───────────────────────────────────────────────────────

    async def send_error(self, msg: str) -> None:
        """Send a STDERR_ERROR to the client."""
        await self.client.send(
            LogError(
                type="Error",
                level=Verbosity.ERROR,
                name="Error",
                msg=msg,
                have_pos=0,
                traces=[],
            )
        )
        await self.client.flush()
