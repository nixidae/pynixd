"""Core pynixd instance orchestration."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import parse_qs, urlsplit

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from nix_daemon_protocol.store_dir import set_real_store_dir, set_store_dir

from . import _optional, metrics, wire
from .config import ExternalUnixStoreSpec, HTTPBinaryCacheSpec, LocalSocketStoreSpec, PynixdSettings
from .context import PynixdContext
from .exceptions import GCNotPermittedError
from .gc import Collector, LivenessWatch, _pressure_usage
from .health import HealthReport, LoopLagMonitor, StallWatchdog
from .liveness_watch import DirtyFlag, StableRootsWatch
from .scheduler import Scheduler
from .serde.protocol import PynixdGCAction
from .state import upstream_in_flight
from .store import DaemonStore, ExternalUnixStore, LocalDBStore, LocalStore, Store, is_http_binary_cache
from .store_layout import DEFAULT_STORE_DIR
from .unix_server import start_unix_server

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

    import asyncssh
    from aiohttp import web

    from .daemon_extensions.pynixd_collect_garbage import PynixdCollectGarbageResponse

log = structlog.get_logger(__name__)


def _trigger_due(now: float, last_trigger: float, cooldown: float) -> bool:
    """Whether an automatic trigger may fire: the cooldown since the last one passed.

    Watermark drives and scheduled full passes share the window, so a
    hovering disk triggers at most once per cooldown no matter which rule
    fires. Zero disables it.
    """
    return now - last_trigger >= cooldown


# The size of `sun_path` in `struct sockaddr_un`, per platform. The kernel
# copies the path into that array, so a longer one cannot be bound at all.
# `sys.platform` and not a `uname` call, because the value is a property of
# the C library this process was built against.
SILENT_UPSTREAM_WARN = 600.0
"""Seconds of silence after which an upstream operation is logged once."""
SILENT_UPSTREAM_CHECK = 60.0

_LOOPBACK_HOSTS: Final = frozenset({"127.0.0.1", "::1", "localhost"})

_SUN_PATH_LIMIT = {"linux": 108, "darwin": 104}
_SUN_PATH_LIMIT_DEFAULT = 104
"""The smaller of the two, for a platform this table does not name. A path
that a stricter limit accepts is bindable everywhere."""


def _adopt_store_dir(local_store: Store) -> None:
    """Take the two store directories from the store that pynixd serves.

    **They are not always the same directory, and which one moves depends on
    the shape of the store.** `--store <root>` puts the files at
    `<root>/nix/store` and leaves `builtins.storeDir` at `/nix/store`.
    Measured against Nix 2.34.8: `nix --store $T eval --expr
    builtins.storeDir` answers `"/nix/store"`, and the layout appears under
    `$T/nix/store`. `NIX_STORE_DIR` moves both, because a relocated store
    keeps its files at the directory that its paths name.

    `StoreLayout` answers both, so this reads the layout of the store rather
    than a root. It used to set the real directory alone, from the root, and
    a relocated store then got `/nix/store` for the logical one and answered
    a store path that no client could read. Issue nixidae/nixidae#215.

    A store at `/` changes neither, and it is the default.

    Issue nixidae/nixidae#212 holds what one constant did instead: it put `/nix/store/` in
    front of a path that already named another store, and reported nothing.
    """
    layout = getattr(local_store, "layout", None)
    if layout is None:
        return
    if layout.real_store_dir != DEFAULT_STORE_DIR:
        set_real_store_dir(layout.real_store_dir)
    if layout.store_dir != DEFAULT_STORE_DIR:
        set_store_dir(layout.store_dir)


def _configured_substituters(local_store: Store) -> Sequence[str] | None:
    """The substituters the local store's `nix_config` names, or `None` without one.

    `None` defers to what the daemon's own nix.conf says, which `Server.start`
    reads. In a given `nix_config`, an unset `substituters` is Nix's default.
    """
    nix_config = getattr(local_store, "nix_config", None)
    if nix_config is None:
        return None
    configured = getattr(nix_config, "substituters", None)
    return ["https://cache.nixos.org/"] if configured is None else configured


def _default_http_substituter_urls(configured: Sequence[str]) -> list[str]:
    """The HTTP substituters of `configured`, each once.

    A `substituters` set to nothing means no substituter, as it does to Nix.
    Adding cache.nixos.org to a list that leaves it out costs a node without
    a network a DNS timeout on every miss.
    """
    urls = [url for url in configured if url.startswith(("http://", "https://"))]

    seen: set[str] = set()
    result: list[str] = []
    for url in urls:
        key = url.rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        result.append(url)
    return result


def _default_unix_substituters(configured: Sequence[str]) -> list[tuple[Path, Path]]:
    urls: list[tuple[Path, Path]] = []
    for url in configured:
        split = urlsplit(url)
        if split.scheme != "unix":
            continue
        socket_path = Path(split.path)
        roots = parse_qs(split.query).get("root")
        store_path = Path(roots[0]) if roots else Path("/")
        urls.append((socket_path, store_path))

    seen: set[tuple[Path, Path]] = set()
    result: list[tuple[Path, Path]] = []
    for item in urls:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _is_own_cache(url: str, settings: PynixdSettings) -> bool:
    """Whether *url* is this pynixd's own HTTP cache.

    A node's nix.conf can list it, for Nix. As a substituter of pynixd, a
    narinfo miss there asks pynixd itself, joins the race it is part of, and
    waits out `substitution_query_timeout`: 2 s on every new local build on
    dynhetz, 675 timeouts in 30 minutes (nixidae/nixidae#1125).
    """
    split = urlsplit(url)
    port = settings.http_port if split.scheme == "http" else settings.https_port
    if port is None or split.port != port:
        return False
    host = split.hostname or ""
    return host in _LOOPBACK_HOSTS or (bool(settings.http_host) and host == settings.http_host)


def _http_store_id(url: str) -> str:
    split = urlsplit(url)
    host = split.netloc or split.path
    return f"http-{host.rstrip('/').replace(':', '-')}"


def _unix_store_id(socket_path: Path, store_path: Path) -> str:
    payload = f"{socket_path}:{store_path}"
    safe = payload.strip("/").replace("/", "-").replace(":", "-")
    return f"unix-{safe}"[:120]


def _add_substituter_stores(
    stores: dict[StoreId, Store], configured: Sequence[str], settings: PynixdSettings
) -> list[Store]:
    """A store in `stores` for each HTTP and unix substituter of `configured` it lacks; returns the new ones.

    This pynixd's own HTTP cache is left out: `_is_own_cache`.
    """
    added: list[Store] = []
    existing_http_urls = {store.url.rstrip("/") for store in stores.values() if is_http_binary_cache(store)}
    for url in _default_http_substituter_urls(configured):
        if url.rstrip("/") in existing_http_urls:
            continue
        if _is_own_cache(url, settings):
            log.info("substituter_is_own_cache", url=url)
            continue
        store_id = StoreId(_http_store_id(url))
        cache_spec = HTTPBinaryCacheSpec(
            store_id=store_id,
            url=url,
        )
        stores[store_id] = cache_spec.to_store(str(store_id))
        added.append(stores[store_id])
        existing_http_urls.add(url.rstrip("/"))

    existing_unix = {
        (store.socket_path, store.store_path) for store in stores.values() if isinstance(store, ExternalUnixStore)
    }
    for socket_path, store_path in _default_unix_substituters(configured):
        if (socket_path, store_path) in existing_unix:
            continue
        store_id = StoreId(_unix_store_id(socket_path, store_path))
        unix_spec = ExternalUnixStoreSpec(
            store_id=store_id,
            socket_path=socket_path,
            store_path=store_path,
        )
        stores[store_id] = unix_spec.to_store(str(store_id))
        added.append(stores[store_id])
        existing_unix.add((socket_path, store_path))
    return added


class Server:
    """Programmatic pynixd server instance.

    Accepts either a ``PynixdSettings`` object (from config file + env vars)
    or individual kwargs for programmatic/test use.
    """

    def __init__(
        self,
        stores: dict[StoreId, Store] | None = None,
        settings: PynixdSettings | None = None,
        **kwargs: Any,
    ) -> None:
        if settings is None:
            # ``PynixdSettings`` describes the daemon CLI, whose default Unix
            # socket lives under /run.  A programmatic Server is commonly an
            # ephemeral SSH or HTTP endpoint, however, and must not require
            # permission to create a system runtime directory unless its
            # caller explicitly requested a Unix socket.
            kwargs.setdefault("unix_path", None)
            settings = PynixdSettings(**kwargs)

        if stores is None:
            spec = LocalSocketStoreSpec(store_id=LOCAL_STORE_ID)
            stores = {LOCAL_STORE_ID: spec.to_store(str(LOCAL_STORE_ID))}
        elif LOCAL_STORE_ID not in stores:
            stores = dict(stores)
            spec = LocalSocketStoreSpec(store_id=LOCAL_STORE_ID)
            stores[LOCAL_STORE_ID] = spec.to_store(str(LOCAL_STORE_ID))

        local_store = stores[LOCAL_STORE_ID]
        _adopt_store_dir(local_store)

        configured = _configured_substituters(local_store)
        if configured is not None:
            _add_substituter_stores(stores, configured, settings)

        self.ctx = PynixdContext(
            settings=settings,
            _stores=stores,
        )

        self.ctx.scheduler = Scheduler(self.ctx)
        sessions = self.ctx.sessions
        metrics.watch_client_ops(lambda: [p.current_op[1] for p in sessions.proxies() if p.current_op is not None])

        self.background_tasks: list[asyncio.Task[Any]] = []
        self.ssh_server: asyncssh.SSHAcceptor | None = None
        self.unix_server: asyncio.Server | None = None
        self.reverse_acceptor: asyncssh.SSHAcceptor | None = None
        self.http_server: web.AppRunner | None = None
        self.stall_watchdog = (
            StallWatchdog(
                self.settings.stall_traceback_seconds,
                path=self.settings.stall_traceback_path,
                max_bytes=self.settings.stall_traceback_max_bytes,
            )
            if self.settings.stall_traceback_seconds > 0
            else None
        )
        self.loop_lag = LoopLagMonitor(
            window=self.settings.health_loop_lag_window,
            watchdog=self.stall_watchdog,
        )
        self.http_bound_port: int | None = None
        self.https_server: web.AppRunner | None = None
        self.https_bound_port: int | None = None
        self._started = False
        self._done_event = anyio.Event()

    @staticmethod
    def _ensure_unix_socket_parent(socket_path: Path) -> None:
        Server._check_unix_socket_length(socket_path)
        parent = socket_path.parent
        try:
            parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise RuntimeError(f"Cannot create unix socket directory {parent}") from exc
        if not parent.is_dir():
            raise RuntimeError(f"Unix socket parent is not a directory: {parent}")
        if not os.access(parent, os.W_OK):
            raise RuntimeError(f"Unix socket directory is not writable: {parent}")

    @staticmethod
    def _check_unix_socket_length(socket_path: Path) -> None:
        """Refuse a socket path the operating system cannot bind.

        `sun_path` is `char[108]` on Linux and `char[104]` on darwin, and a
        path over the limit fails at `bind`. The failure arrives late and from
        the wrong place: the directory is made, the server starts, and uvloop
        raises `OSError: AF_UNIX path too long` with no path in the message.

        The check is here, beside the directory check, so the error names the
        path and the limit before anything is created.
        """
        limit = _SUN_PATH_LIMIT.get(sys.platform, _SUN_PATH_LIMIT_DEFAULT)
        encoded = len(os.fsencode(socket_path))
        if encoded > limit:
            raise RuntimeError(
                f"Unix socket path is {encoded} bytes, and {sys.platform} allows {limit}: {socket_path}. "
                f"Shorten `unix_path`."
            )

    @property
    def local_store(self) -> LocalStore:
        """The primary local Nix daemon store."""
        return self.ctx.local_store

    @property
    def stores(self) -> Mapping[StoreId, Store]:
        """Read-only view of all connected stores (including local)."""
        return self.ctx.stores

    @property
    def settings(self) -> PynixdSettings:
        """The server's configuration settings."""
        return self.ctx.settings

    @property
    def scheduler(self) -> Scheduler | None:
        """The build scheduler, or ``None`` if scheduling is disabled."""
        return self.ctx.scheduler

    async def add_store(self, store: Store, dynamic: bool = False) -> None:
        """Add a store to the server.

        If dynamic=True, the store's feature_matrix is also registered in
        the scheduler's dynamic_feature_matrix, so builds for that platform
        continue to queue even after the store is removed.
        """
        # Wire reconnect callback before start so the loop is ready
        captured_dynamic = dynamic

        async def _on_store_reconnect() -> None:
            if self.scheduler and isinstance(store, DaemonStore) and not store.no_schedule:
                self.scheduler.on_store_added(store, dynamic=captured_dynamic)

        if isinstance(store, DaemonStore) and not store.no_schedule:
            store._on_reconnect = _on_store_reconnect

        try:
            await store.start()
        except Exception:
            log.warning(
                "store_start_failed",
                store_id=store.store_id,
                exc_info=True,
            )
            # Store is already in ctx._stores from construction.
            # Don't register with scheduler — it's unavailable.
            return

        # Server is the primary owner and mutator of the stores collection
        self.ctx._stores[store.store_id] = store

        if self.scheduler and isinstance(store, DaemonStore) and not store.no_schedule:
            self.scheduler.on_store_added(store, dynamic=dynamic)

    async def remove_store(self, store_id: StoreId, drain_timeout: float = 300.0) -> None:
        """Remove a remote store, cleaning DB records and closing connections.

        NOTE: Used by external projects — do not remove.
        """
        if store_id == LOCAL_STORE_ID:
            raise RuntimeError("Cannot remove local store")
        if self.scheduler:
            # First, drain the store in the scheduler to cancel/requeue jobs
            await self.scheduler.drain_store(store_id, drain_timeout=drain_timeout)

        # Then remove from the context
        store = self.ctx._stores.pop(store_id, None)
        if store is None:
            log.warning("remove_store_not_found", store_id=store_id)
            return
        if self.scheduler:
            self.scheduler.allocator.discard_store(store_id)

        local_store = self.local_store
        if isinstance(local_store, LocalDBStore):
            # DB cleanup is handled at the DB layer when paths are flushed.
            pass

        # Finally, close the store connection
        await store.close()

    async def _gc_tick(self) -> None:
        """Watch the disk on a short cadence, collect on pressure or schedule.

        The high watermark starts a drive. The store's target ends it.
        Each drive runs bounded passes while collection makes progress.
        Triggers share a cooldown window; scheduled passes also collect below
        the watermark at `gc_interval`.
        """
        settings = self.ctx.settings
        log.info(
            "gc_loop_started",
            interval=settings.gc_interval,
            poll=settings.gc_poll_interval,
            watermark=settings.gc_high_watermark,
        )
        last_full = time.monotonic()
        last_trigger = 0.0
        # The store opens its database in `start`, after this loop begins, so
        # the first checkpoint retries each poll until it finds the database.
        checkpoint_due = True
        while True:
            await anyio.sleep(settings.gc_poll_interval)
            if checkpoint_due:
                checkpoint_due = not await self._checkpoint_wal()
            now = time.monotonic()
            if self._over_watermark():
                if not _trigger_due(now, last_trigger, settings.gc_cooldown):
                    metrics.GC_TRIGGER_SKIPPED.labels(reason="watermark").inc()
                    log.debug("gc_trigger_cooldown", reason="watermark")
                    continue
                await self._drive_to_target()
                last_trigger = time.monotonic()
                continue
            if now - last_full >= settings.gc_interval:
                last_full = now
                if not _trigger_due(now, last_trigger, settings.gc_cooldown):
                    metrics.GC_TRIGGER_SKIPPED.labels(reason="scheduled").inc()
                    log.debug("gc_trigger_cooldown", reason="scheduled")
                    continue
                await self._gc_pass("scheduled")
                last_trigger = time.monotonic()

    async def _drive_to_target(self) -> None:
        """Bounded passes back to back until pressure drops or progress stops.

        One watermark pass relieves a little; a flood needs many, and a
        poll interval between each turns minutes of deleting into hours of
        waiting. So the drive re-checks pressure right after each pass and
        runs the next while still over the store's target: up to
        `gc_watermark_max_passes` passes, stopping early when a pass frees
        nothing -- refused everything or planned nothing -- because more
        passes change nothing until the world does. Issue #71.
        """
        cap = self.ctx.settings.gc_watermark_max_passes
        for _ in range(cap):
            if not self._over_gc_target():
                return
            resp = await self._gc_pass("watermark")
            if resp is None or not resp.store_paths:
                return
        log.info("gc_drive_capped", cap=cap)

    def _over_gc_target(self) -> bool:
        """Continue an active drive until the local store reaches its target."""
        local = self.ctx.local_store
        target = getattr(local, "gc_target_usage", None)
        if target is None:
            return self._over_watermark()
        usage = Collector._disk_usage(local)
        if usage is None:
            return False
        used, total = usage
        return total > 0 and used / total > target

    def _over_watermark(self) -> bool:
        """Whether disk pressure currently warrants a bounded pass."""
        watermark = self.ctx.settings.gc_high_watermark
        if watermark is None:
            return False
        try:
            directory = self.ctx.local_store.layout.real_store_dir
            used, total = _pressure_usage(directory)
        except Exception:
            log.warning("gc_watch_usage_failed", exc_info=True)
            return False
        return used / total > watermark

    async def _checkpoint_wal(self) -> bool:
        """Shrink the store database's WAL, which Nix never does. Whether a checkpoint ran."""
        checkpoint = getattr(getattr(self.ctx.local_store, "db", None), "checkpoint_wal", None)
        if checkpoint is None:
            return False
        ran: bool = await checkpoint()
        return ran

    async def _gc_pass(self, reason: str) -> PynixdCollectGarbageResponse | None:
        """One EXECUTE pass, with the metrics the loop always reports.

        Answers what the pass deleted, or `None` when it deleted nothing:
        refused, errored, or planned nothing. The drive uses the answer to
        stop when passes stop freeing.
        """
        started = time.monotonic()
        try:
            resp = await Collector(self.ctx).run(PynixdGCAction.EXECUTE)
            # A pass that deleted wrote its removals through the WAL.
            await self._checkpoint_wal()
        except anyio.get_cancelled_exc_class():
            raise
        except GCNotPermittedError:
            # Not a failure: the loop is on and the permit is off, which is
            # the posture until the liveness mirror is proven. Info, not
            # error, and no cycle metric either way.
            log.info("gc_execute_not_permitted", reason=reason)
            return None
        except Exception:
            metrics.GC_CYCLES.labels(result="error").inc()
            log.exception("gc_pass_failed", reason=reason)
            return None
        else:
            metrics.GC_CYCLES.labels(result="ok").inc()
            metrics.GC_PATHS_DELETED.inc(len(resp.store_paths))
            metrics.GC_BYTES_FREED.inc(resp.bytes)
            # A wall clock, because this is compared against `time()` in an
            # alert. `time.monotonic` below measures a duration, which is
            # the other question.
            metrics.GC_LAST_SUCCESS.set(time.time())
            log.info("gc_loop_pass", reason=reason, deleted=len(resp.store_paths), bytes=resp.bytes)
            return resp
        finally:
            metrics.GC_CYCLE_DURATION.observe(time.monotonic() - started)

    async def _warn_silent_upstreams(self) -> None:
        """Log each upstream operation once when its store goes quiet too long.

        Nothing is cut off: a GC or a build may be silent for an hour and be
        right. The warning and `pynixd state --section upstream` are how a
        stalled one gets seen (#93).
        """
        warned: set[tuple[str, str]] = set()
        while True:
            await anyio.sleep(SILENT_UPSTREAM_CHECK)
            live: set[tuple[str, str]] = set()
            for row in upstream_in_flight(self.ctx):
                key = (row.conn, row.op)
                if row.silent_s < SILENT_UPSTREAM_WARN:
                    continue
                live.add(key)
                if key not in warned:
                    log.warning("upstream_silent", **row.model_dump())
            warned = live

    async def _liveness_tick(self, watch: LivenessWatch, interval: float, dirty: DirtyFlag) -> None:
        """Refresh Python liveness after root changes and at each interval."""
        log.info("gc_liveness_watch_started", interval=interval)
        while True:
            with anyio.move_on_after(interval):
                await dirty.wait()
            dirty.clear()
            try:
                await watch.refresh()
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                log.exception("gc_liveness_check_failed")

    @property
    def host(self) -> str:
        """SSH bind host for remote connections."""
        return self.settings.ssh_host

    @property
    def port(self) -> int:
        """Bound SSH port, or the configured port if the server is not yet listening."""
        if self.ssh_server and self.ssh_server.sockets:
            port: int = self.ssh_server.sockets[0].getsockname()[1]
            return port
        return self.settings.ssh_port or 0

    @property
    def username(self) -> str:
        """Current OS user for ssh-ng URI generation."""
        return os.environ.get("USER", "root")

    def uri(self) -> str:
        """ssh-ng:// URI for --store."""
        return f"ssh-ng://{self.username}@{self.host}:{self.port}"

    def health(self) -> HealthReport:
        """Is the loop running, and is every configured interface serving?

        Both halves are invisible from outside. A TCP probe on the data port
        passes against a wedged process, because the kernel completes
        `connect()` from the listen backlog with no help from this code, and it
        says nothing at all about the other interfaces.

        An interface that was never configured is not reported. Only one that
        was asked for and is not there counts as a fault.
        """
        s = self.settings
        checks: dict[str, str] = {}

        def listener(name: str, configured: bool, obj: object | None) -> None:
            if not configured:
                return
            checks[name] = "serving" if obj is not None else "configured but not serving"

        listener("ssh", s.ssh_port is not None, self.ssh_server)
        listener("reverse_acceptor", s.reverse_acceptor.enabled, self.reverse_acceptor)
        listener("unix", bool(s.unix_path), self.unix_server)
        listener("http", s.http_port is not None, self.http_server)
        listener("https", s.https_port is not None, self.https_server)

        limit = s.health_loop_lag_max
        lag = self.loop_lag.window_max
        lag_ok = lag < limit
        checks["event_loop"] = f"lag {lag:.3f}s" if lag_ok else f"stalled, lag {lag:.3f}s over {limit}s"

        ok = lag_ok and all(state == "serving" for name, state in checks.items() if name != "event_loop")
        return HealthReport(ok=ok, checks=checks)

    async def __aenter__(self) -> Server:
        await self.start()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def start(self) -> None:
        """Start the server listeners and background tasks."""
        try:
            if self._started:
                raise RuntimeError("Server already started")
            self._started = True
            await self._start_inner()
        except BaseException:
            # A start that raises leaves started state behind: stores hold
            # pool connections and database handles, and their threads keep
            # the interpreter alive after the traceback. Tear down before
            # the error leaves, or the process never exits. Issue #61.
            with contextlib.suppress(Exception):
                await self.close()
            raise

    async def _start_inner(self) -> None:
        """Open stores, start tasks, and bind every listener."""
        local_store = self.ctx.local_store

        await local_store.start()

        if local_store.version < wire.proto(1, 35):
            raise RuntimeError(
                f"Local store {local_store.store_id} uses protocol {wire.proto_str(local_store.version)}, "
                "but pynixd requires at least 1.35 for the local store. "
                "Please upgrade your Nix daemon.",
            )

        self.ctx.trust = await local_store.trust_policy()
        log.info(
            "trust_policy",
            trusted_users=self.ctx.trust.trusted_users,
            allowed_users=self.ctx.trust.allowed_users,
            build_users_group=self.ctx.trust.build_users_group,
        )
        # Without a `nix_config`, the substituters are the ones the daemon
        # reads from its nix.conf. Nix's default here would put
        # cache.nixos.org in place of the caches the node is configured for.
        if _configured_substituters(local_store) is None:
            added = _add_substituter_stores(self.ctx._stores, self.ctx.trust.substituters, self.ctx.settings)
            log.info("substituters_from_nix_conf", stores=[str(store.store_id) for store in added])
        if self.ctx.scheduler:
            self.ctx.scheduler.substitution_queue.set_import_slots(self.ctx.trust.max_substitution_jobs)

        if isinstance(local_store, LocalDBStore):
            self.ctx.db = local_store.db
            log.info(
                "local_store_db_connected",
                db_path=str(self.ctx.db.db_path),
            )
        else:
            self.ctx.db = None
            # Say which store it is and why. The line carried no field at all,
            # so a reader could see that the SQLite fast path was off and not
            # what had turned it off. Issue nixidae/nixidae#202 holds the
            # decision.
            #
            # **Reaching here means a caller named the class.** Both ways of
            # building the local store -- `PynixdSettings.to_stores` and
            # `Server.__init__` -- go through `spec.to_store()`, which reads
            # `use_db`, and `use_db` defaults to true. A caller that writes
            # `LocalStore(spec)` or the legacy alias `LocalSocketStore(spec)`
            # chooses the store without the fast paths at the call site, and
            # the `use_db` beside it is then read by nothing.
            #
            # nixkube's three entry points did exactly that, through the
            # legacy name, so every pynixd pod it deploys logged this line and
            # answered every query over the socket. The store path is not the
            # tell: this fires on `/`, on a chroot store and on a PVC alike.
            log.warning(
                "local_store_db_disabled",
                store_id=str(local_store.store_id),
                store_type=type(local_store).__name__,
                reason="the local store is not a LocalDBStore, so no SQLite fast path is available",
            )

        if self.ctx.db:
            self.ctx.db.start()

        # Start non-local stores concurrently — they're already in _stores.
        async with anyio.create_task_group() as tg:
            for store_id, store in list(self.ctx._stores.items()):
                if store_id != LOCAL_STORE_ID:
                    tg.start_soon(self.add_store, store, False)

        if self.ctx.scheduler:
            self.background_tasks.append(
                asyncio.create_task(self.ctx.scheduler.start()),
            )

        if self.ctx.db and self.ctx.settings.gc_enabled:
            self.background_tasks.append(asyncio.create_task(self._gc_tick()))
            liveness_interval = self.ctx.settings.gc_liveness_interval
            if liveness_interval is not None:
                watch = LivenessWatch.from_local(self.ctx.local_store)
                if watch is None:
                    log.warning("gc_liveness_watch_unavailable")
                else:
                    dirty = DirtyFlag()
                    roots = StableRootsWatch(watch.tracker.state_dir)
                    self.background_tasks.append(asyncio.create_task(roots.run(dirty)))
                    self.background_tasks.append(
                        asyncio.create_task(self._liveness_tick(watch, liveness_interval, dirty))
                    )

        # Before the listeners, so a stall during their startup is recorded.
        if self.stall_watchdog is not None:
            self.stall_watchdog.beat()
            self.stall_watchdog.start(asyncio.get_running_loop())
        self.background_tasks.append(asyncio.create_task(self.loop_lag.run()))
        self.background_tasks.append(asyncio.create_task(self._warn_silent_upstreams()))

        s = self.settings
        if s.ssh_port is not None:
            self.ssh_server = await _optional.ssh_server.start_ssh_server(
                ctx=self.ctx,
                host=s.ssh_host,
                port=s.ssh_port,
                host_key_path=s.ssh_host_key,
                schedule_mode=s.schedule_mode,
                user_keys=s.ssh_user_keys,
                admin_keys=s.ssh_admin_keys,
                admin_users=s.admin_users,
            )

        # **The `enabled` check belongs here as well as in the function.**
        # `start_reverse_acceptor` answers `None` for a set that is off, and
        # reaching it at all imports `asyncssh`. Issue #30.
        if s.reverse_acceptor.enabled:
            self.reverse_acceptor = await _optional.reverse_server.start_reverse_acceptor(
                server=self,
                settings=s.reverse_acceptor,
            )

        if s.reverse_initiator.enabled:
            initiator = _optional.reverse_client.ReverseInitiator(self.ctx, s.reverse_initiator)
            self.background_tasks.append(asyncio.create_task(initiator.run()))

        if s.unix_path:
            self._ensure_unix_socket_parent(s.unix_path)
            self.unix_server = await start_unix_server(
                ctx=self.ctx,
                socket_path=s.unix_path,
                schedule_mode=s.schedule_mode,
            )

        if s.http_port is not None or s.https_port is not None:
            scheduler = self.ctx.scheduler
            cache = _optional.http_server.PynixdHttpServer(
                local_store,
                enable_cache=s.http_enable_cache,
                enable_metrics=s.http_enable_metrics,
                metrics_no_auth=s.http_metrics_no_auth,
                username=s.http_user,
                password=s.http_pass,
                htpasswd_path=s.http_htpasswd,
                priority=s.http_priority,
                upload_dir=s.http_upload_dir,
                health_check=self.health,
                substitution_queue=scheduler.substitution_queue if scheduler is not None else None,
                upstream_race=s.http_upstream_race,
                prefer_local_nar=s.http_prefer_local_nar,
            )
            if s.http_port is not None:
                runner, port = await cache.start(
                    host=s.http_host,
                    port=s.http_port,
                )
                self.http_server = runner
                self.http_bound_port = port
            if s.https_port is not None:
                runner, port = await cache.start(
                    host=s.http_host,
                    port=s.https_port,
                    ssl_cert=str(s.https_cert) if s.https_cert else None,
                    ssl_key=str(s.https_key) if s.https_key else None,
                )
                self.https_server = runner
                self.https_bound_port = port

        if not (self.reverse_acceptor or self.ssh_server or self.unix_server or self.http_server or self.https_server):
            log.warning("no_servers_started")

    async def wait_finished(self) -> None:
        """Wait for the server to shut down.

        This returns when :meth:`close` has been called and all listeners
        and background tasks have been cleaned up.  Useful for library
        integrations that need to block until pynixd is fully stopped.
        """
        await self._done_event.wait()

    async def close(self) -> None:
        """Gracefully shut down the server."""
        if not self._started:
            return
        self._started = False
        log.info("server_shutting_down")

        if self.stall_watchdog is not None:
            self.stall_watchdog.stop()

        if self.http_server:
            await self.http_server.cleanup()
            self.http_server = None
        if self.https_server:
            await self.https_server.cleanup()
            self.https_server = None

        if self.reverse_acceptor:
            self.reverse_acceptor.close()
            await self.reverse_acceptor.wait_closed()
            self.reverse_acceptor = None
        if self.ssh_server:
            self.ssh_server.close()
            await self.ssh_server.wait_closed()
            self.ssh_server = None
        if self.unix_server:
            self.unix_server.close()
            await self.unix_server.wait_closed()
            self.unix_server = None

        # Answer every established client before tearing down the state
        # their operations read. The listeners are gone, so no new client
        # can connect, and each old one gets an explicit error instead of
        # a dead socket. Issue #64.
        await self.ctx.sessions.shutdown()

        if self.ctx.db:
            await self.ctx.db.close()
            self.ctx.db = None

        if self.ctx.scheduler:
            await self.ctx.scheduler.close()

        for task in self.background_tasks:
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
        self.background_tasks.clear()

        for store in self.ctx._stores.values():
            await store.close()
        self.ctx._stores.clear()
        self._done_event.set()
