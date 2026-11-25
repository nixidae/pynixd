"""Core pynixd instance orchestration."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import os
import socket
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import anyio
import anyio.lowlevel
import anyio.to_thread
import structlog

from nix_daemon_protocol.add_multiple_to_store import UNVERSIONED_PATH_INFO
from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from nix_daemon_protocol.store_dir import set_real_store_dir, set_store_dir
from pynixd_store.background import Background
from pynixd_store.builder.cgroup import default_cores
from pynixd_store.db_migrations import TARGET_VERSION
from pynixd_store.gc import LivenessWatch, pressure_usage
from pynixd_store.local_store_db import DatabaseError
from pynixd_store.store_layout import DEFAULT_STORE_DIR

from . import _optional, metrics, wire
from .cold_tier import ColdTier, ColdUploader
from .config import (
    LocalSocketStoreSpec,
    PynixdSettings,
)
from .context import PynixdContext
from .exceptions import GCNotPermittedError
from .execution import output_paths
from .gc import Collector
from .handover import HandoverControl, ListenerName, local_store_key, raw_socket
from .health import HealthReport, LoopLagMonitor, StallWatchdog
from .liveness_watch import DirtyFlag, StableRootsWatch
from .provisioning.ec2 import Ec2Provider
from .provisioning.gha import GhaProvider
from .provisioning.kubernetes import KubernetesProvider
from .provisioning.nomac import NomacProvider
from .provisioning.provisioner import Provisioner
from .provisioning.vivarium import VivariumProvider
from .recursive import restricted_server
from .scheduler import Scheduler
from .serde.protocol import PynixdGCAction
from .session_builders import SessionBuilders
from .state import upstream_in_flight
from .store import LocalDBStore, LocalStore, Store, preload as preload_stores
from .store_url import reference_key, substituter_spec
from .tcp_listen import adopt_tcp
from .tracing import NO_TRACING
from .unix_server import UnixServer, bind_unix_server, serve_unix_clients

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
    from types import TracebackType

    import asyncssh
    from aiohttp import web
    from anyio.abc import TaskStatus

    from .build_queue import QueuedBuild
    from .daemon_extensions.pynixd_collect_garbage import PynixdCollectGarbageResponse
    from .handover import Inherited, Offer
    from .http_server import PynixdHttpServer
    from .provisioning.provider import Provider
    from .serde import BuildDerivationResponse
    from .tracing import Tracing

log = structlog.get_logger(__name__)


@dataclasses.dataclass(frozen=True, slots=True)
class _StoreHost:
    """The task that holds one store open: cancel *scope*, then await *done*."""

    scope: anyio.CancelScope = dataclasses.field(default_factory=anyio.CancelScope)
    done: anyio.Event = dataclasses.field(default_factory=anyio.Event)


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
DRAIN_POLL = 1.0

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


def _add_substituter_stores(
    stores: dict[StoreId, Store], configured: Sequence[str], settings: PynixdSettings
) -> list[Store]:
    """A store in `stores` for each substituter of `configured` the registry opens and `stores` lacks; returns the new ones.

    `store_url.substituter_spec` leaves out this pynixd's own HTTP cache, and
    a `file://` directory pynixd cannot read, which the daemon may: it stays
    in `SubstitutionQueue.uncovered`, so the daemon answers for it, and its
    paths do not read as missing.
    """
    added: list[Store] = []
    keys = {store.reference_key for store in stores.values()} - {None}
    for url in configured:
        key = reference_key(url)
        if key is None or key in keys:
            continue
        spec = substituter_spec(url, settings)
        if spec is None:
            continue
        store_id = StoreId(str(spec.store_id))
        stores[store_id] = spec.to_store(str(store_id))
        added.append(stores[store_id])
        keys.add(key)
    return added


_START_PAIRS: Final = ((wire.PROTOCOL_VERSION, frozenset[str]()), (UNVERSIONED_PATH_INFO, frozenset[str]()))
"""The codec pairs no handshake names: build logs and stderr at pynixd's own version, and the path infos of an
unversioned `AddMultipleToStore` stream."""


def preload() -> None:
    """Load what a server needs later, so its first use does not load it on the event loop. It blocks.

    Every module a configuration may load (an import reads files), the codecs
    of `_START_PAIRS` (a codec compiles at its first message), and a build's
    default cores (they come from cgroup files).
    """
    preload_stores()
    _optional.preload()
    default_cores()
    for version, features in _START_PAIRS:
        wire.compile_codecs(version, features)


class Server:
    """Programmatic pynixd server instance.

    Accepts either a ``PynixdSettings`` object (from config file + env vars)
    or individual kwargs for programmatic/test use.
    """

    def __init__(
        self,
        stores: dict[StoreId, Store] | None = None,
        settings: PynixdSettings | None = None,
        *,
        inherited: Inherited | None = None,
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
        self.ctx.session_builders = SessionBuilders(
            settings,
            self.ctx.local_store.nix_bin,
            self.add_store,
            self.remove_store,
            lambda: [
                value
                for proxy in sessions.proxies()
                if proxy.client.options is not None
                and (value := proxy.client.options.overrides.get("builders")) is not None
            ],
            self._pending_builders,
        )
        metrics.watch_client_ops(lambda: [p.current_op[1] for p in sessions.proxies() if p.current_op is not None])

        self.ssh_server: asyncssh.SSHAcceptor | None = None
        self.unix_server: UnixServer | None = None
        self.reverse_acceptor: asyncssh.SSHAcceptor | None = None
        self.provisioner: Provisioner | None = None
        self.http_server: web.AppRunner | None = None
        self.stall_watchdog = (
            StallWatchdog(
                self.settings.stall_traceback_threshold,
                path=self.settings.stall_traceback_path,
                max_bytes=self.settings.stall_traceback_max_bytes,
                gil_dump=self.settings.stall_traceback_gil_dump,
            )
            if self.settings.stall_traceback_threshold > 0
            else None
        )
        self.loop_lag = LoopLagMonitor(
            window=self.settings.health_loop_lag_window,
            watchdog=self.stall_watchdog,
        )
        self.http_bound_port: int | None = None
        self.https_server: web.AppRunner | None = None
        self.https_bound_port: int | None = None
        self._http_cache: PynixdHttpServer | None = None
        self._tracing: Tracing = NO_TRACING
        self._started = False
        self._done_event = anyio.Event()
        self._background = Background("Server")
        self._running: contextlib.AbstractAsyncContextManager[Server] | None = None
        self._store_hosts: dict[StoreId, _StoreHost] = {}
        self._inherited = inherited
        self.handover: HandoverControl | None = None
        self.draining = False
        self.drained = anyio.Event()
        """Set when a drain ends: no session and no build left, or the drain limit passed."""
        self._singletons: list[anyio.CancelScope] = []
        """Loops one process runs for the whole machine. A drain stops them; the successor runs its own."""
        self._passes = 0
        self.cold_uploader: ColdUploader | None = None
        self._tcp: dict[ListenerName, list[socket.socket]] = {}
        """The TCP listening sockets, by listener, for a hand-over."""
        self._more_acceptors: list[asyncssh.SSHAcceptor] = []
        """SSH acceptors past the first of `ssh_server` and `reverse_acceptor`: one per address of the host."""
        """Singleton passes running now: a GC pass, a cold-tier round, a liveness refresh."""

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
    def local_store(self) -> LocalStore | LocalDBStore:
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

    async def add_store(
        self,
        store: Store,
        dynamic: bool = False,
        admit: Callable[[Store], Awaitable[bool]] | None = None,
    ) -> bool:
        """Start *store* and serve from it until `remove_store` or shutdown.

        If dynamic=True, the store's feature_matrix is also registered in
        the scheduler's dynamic_feature_matrix, so builds for that platform
        continue to queue even after the store is removed.

        *admit* runs on the started store, before any build can reach it; a
        store it refuses closes. Answers whether the store was added.
        """
        # Wire reconnect callback before start so the loop is ready
        captured_dynamic = dynamic
        client = store.daemon_client

        if client is not None and not client.no_schedule:
            scheduled = client

            async def _on_store_reconnect() -> None:
                if self.scheduler:
                    self.scheduler.on_store_added(scheduled, dynamic=captured_dynamic)

            scheduled._on_reconnect = _on_store_reconnect

        try:
            host = await self._background.start(self._host_store, store, dynamic, admit, False)
        except Exception:
            log.warning(
                "store_start_failed",
                store_id=store.store_id,
                exc_info=True,
            )
            # Store is already in ctx._stores from construction.
            # Don't register with scheduler — it's unavailable.
            return False
        return host is not None

    async def _host_store(
        self,
        store: Store,
        dynamic: bool,
        admit: Callable[[Store], Awaitable[bool]] | None,
        local: bool,
        *,
        task_status: TaskStatus[_StoreHost | None],
    ) -> None:
        """Hold *store* open in the server's background task group.

        A store loop that raises closes that store alone, and the daemon
        serves on. The *local* store is the exception: without it pynixd
        serves nothing. It is never registered, because its `store_id` need
        not be the key it already has.
        """
        host = _StoreHost()
        started = False
        try:
            with host.scope:
                async with store:
                    if admit is not None and not await admit(store):
                        started = True
                        task_status.started(None)
                        return
                    if not local:
                        self._register_store(store, dynamic)
                        self._store_hosts[store.store_id] = host
                    started = True
                    task_status.started(host)
                    await anyio.sleep_forever()
        except Exception:
            if not started or local:
                raise
            log.exception("store_background_failed", store_id=store.store_id)
            with anyio.CancelScope(shield=True):
                await self._forget_store(store.store_id, drain_timeout=0)
        finally:
            if self._store_hosts.get(store.store_id) is host:
                del self._store_hosts[store.store_id]
            host.done.set()

    def _register_store(self, store: Store, dynamic: bool) -> None:
        # Server is the primary owner and mutator of the stores collection
        self.ctx._stores[store.store_id] = store

        client = store.daemon_client
        if self.scheduler and client is not None and not client.no_schedule:
            self.scheduler.on_store_added(client, dynamic=dynamic)

    async def _forget_store(self, store_id: StoreId, drain_timeout: float) -> Store | None:
        if self.scheduler:
            # First, drain the store in the scheduler to cancel/requeue jobs
            await self.scheduler.drain_store(store_id, drain_timeout=drain_timeout)

        store = self.ctx._stores.pop(store_id, None)
        if store is not None and self.scheduler:
            self.scheduler.allocator.discard_store(store_id)
        return store

    async def remove_store(self, store_id: StoreId, drain_timeout: float = 300.0, *, missing_ok: bool = False) -> None:
        """Remove a remote store, cleaning DB records and closing connections.

        A store already gone is a warning, unless *missing_ok*: a caller that races another remover.

        NOTE: Used by external projects — do not remove.
        """
        if store_id == LOCAL_STORE_ID:
            raise RuntimeError("Cannot remove local store")
        if await self._forget_store(store_id, drain_timeout) is None and not missing_ok:
            log.warning("remove_store_not_found", store_id=store_id)
        host = self._store_hosts.pop(store_id, None)
        if host is not None:
            host.scope.cancel()
            await host.done.wait()

    async def _pending_builders(self) -> list[frozenset[StoreId] | None]:
        scheduler = self.ctx.scheduler
        return [] if scheduler is None else [build.builders for build in await scheduler.queue.get_pending()]

    def _provisioner(self, controller_key: str) -> Provisioner | None:
        """The provisioner `provisioning.pools` asks for, or None for no pools."""
        settings = self.ctx.settings.provisioning
        if not settings.pools:
            return None
        if self.ctx.db is None or self.ctx.scheduler is None:
            raise RuntimeError("provisioning pools need the store database and the scheduler: the rows outlive pynixd")
        providers: list[Provider] = []
        for name in sorted(settings.providers):
            match name:
                case "vivarium":
                    providers.append(VivariumProvider(str(settings.vivarium_socket)))
                case "ec2" if settings.ec2 is not None:
                    providers.append(Ec2Provider(settings.ec2))
                case "gha" if settings.gha is not None:
                    providers.append(GhaProvider(settings.gha))
                case "nomac" if settings.nomac is not None:
                    providers.append(NomacProvider(settings.nomac))
                case "kubernetes" if settings.kubernetes is not None:
                    providers.append(KubernetesProvider(settings.kubernetes))
                case other:
                    raise RuntimeError(f"provider {other!r} has no settings")
        return Provisioner(
            settings,
            providers,
            self.ctx.db.provisioned,
            self.ctx.scheduler,
            self.remove_store,
            controller_key,
        )

    def _spawn(self, fn: Callable[..., Awaitable[object]], *args: object) -> None:
        """Run *fn* for the daemon's lifetime, in the server's background task group.

        A loop that raises takes the server down with it: the error leaves
        `async with Server(...)`, and systemd restarts the daemon.
        """
        self._background.spawn(fn, *args)

    def _spawn_singleton(self, fn: Callable[..., Awaitable[object]], *args: object) -> None:
        """`_spawn`, for a loop that a drain stops: GC, the cold tier, liveness."""
        self._singletons.append(self._background.spawn(fn, *args))

    @contextlib.asynccontextmanager
    async def _singleton_pass(self) -> AsyncIterator[None]:
        """One pass of a singleton loop. A drain stops the loops between passes, never inside one.

        A pass cut short leaves what it started: a GC pass its deletes half recorded, an upload its NAR without
        a narinfo. The successor's first pass may overlap the last one here; the daemon serialises collectors,
        and the bucket keys a NAR by its hash. A drain counts a running pass as work, and starts no new one.
        """
        if self.draining:
            self._stop_singletons()
            await anyio.lowlevel.checkpoint()
        self._passes += 1
        try:
            yield
        finally:
            self._passes -= 1
            if self.draining and not self._passes:
                self._stop_singletons()

    def _stop_singletons(self) -> None:
        for scope in self._singletons:
            scope.cancel()

    async def handover_refusal(self, offer: Offer) -> str | None:
        """Why this process cannot overlap with a successor, or `None`. docs/design/upgrade-handover.md."""
        if self.draining:
            return "it is draining already"
        # A moved socket or local store comes up with units the switch starts after the reload: the successor
        # found no daemon (tests.switch, beside to replace). A restart orders after them.
        unix_path = str(self.settings.unix_path) if self.settings.unix_path else ""
        if offer.unix_path is not None and offer.unix_path != unix_path:
            return f"the daemon socket moves from {unix_path or 'none'} to {offer.unix_path or 'none'}"
        if offer.local_store is not None and offer.local_store != local_store_key(self.settings):
            return "the local store's settings change"
        if self.ctx.local_store.managed:
            # The successor would remove the socket and kill the daemon this process builds on.
            return "pynixd manages the local store's daemon"
        if self.settings.reverse_initiator.enabled:
            return "a reverse initiator would register this builder twice"
        if offer.schema_version != TARGET_VERSION:
            return f"the database schema moves from version {TARGET_VERSION} to {offer.schema_version}"
        if self.provisioner is not None:
            # The successor's `reconcile` ends every live machine; none passes over yet. With none live, the
            # old process launches none until the hand-over ends.
            self.provisioner.launches_held = True
            live = await self.provisioner.live_count()
            if live:
                self.provisioner.launches_held = False
                return f"{live} provisioned builders are live, and none passes to a successor yet"
        return None

    def handover_abandoned(self) -> None:
        if self.provisioner is not None:
            self.provisioner.launches_held = False

    def _tcp_sockets(self, name: ListenerName, host: str, port: int) -> list[socket.socket]:
        handed = self._inherited.take(name) if self._inherited is not None else []
        sockets = self._tcp[name] = adopt_tcp(handed, host, port)
        return sockets

    def listener_sockets(self) -> dict[ListenerName, list[socket.socket]]:
        sockets: dict[ListenerName, list[socket.socket]] = dict(self._tcp)
        if self.unix_server is not None:
            sockets[ListenerName.UNIX] = [raw_socket(self.unix_server.listener)]
        if self.handover is not None:
            sockets[ListenerName.HANDOVER] = [raw_socket(self.handover.listener)]
        return sockets

    def unix_owns_path(self) -> bool:
        return self.unix_server is not None and self.unix_server.owns_path

    async def drain(self) -> None:
        """Stop accepting, stop the singleton loops, and set `drained` once no session and no build is left.

        The successor holds the same listening sockets, so every path stays.
        """
        if self.draining:
            return
        self.draining = True
        log.info("server_draining", limit=self.settings.drain_limit)
        if self.unix_server is not None:
            self.unix_server.owns_path = False
            await self.unix_server.aclose()
            self.unix_server = None
        if self.handover is not None:
            await self.handover.aclose(unlink=False)
            self.handover = None
        # `close`, not `wait_closed`: established connections stay, and a reverse builder's carry its builds.
        for acceptor in (self.ssh_server, self.reverse_acceptor, *self._more_acceptors):
            if acceptor is not None:
                acceptor.close()
        self.ssh_server = self.reverse_acceptor = None
        self._more_acceptors.clear()
        # A site's `stop` closes its listener; the runner keeps the transfers until `close`. Each response
        # from now closes its connection, so a keep-alive scraper reconnects to the successor.
        if self._http_cache is not None:
            self._http_cache.draining = True
        for runner in (self.http_server, self.https_server):
            if runner is not None:
                for site in list(runner.sites):
                    await site.stop()
        self._tcp.clear()
        if not self._passes:
            self._stop_singletons()
        self._spawn(self._drain_watch)

    def _holds_work(self) -> bool:
        if len(self.ctx.sessions) or self._passes:
            return True
        for runner in (self.http_server, self.https_server):
            # An idle keep-alive connection counts too, until aiohttp's keep-alive timeout closes it.
            if runner is not None and runner.server is not None and runner.server.connections:
                return True
        scheduler = self.ctx.scheduler
        if scheduler is None:
            return False
        counts = scheduler.queue.counts()
        return counts.pending + counts.building > 0

    async def _release_reverse_builders(self) -> None:
        """Close each reverse builder, so it registers with the successor, once no build here waits or runs.

        Not per builder by its open connections: between a build's end and the copy of its outputs a builder has
        none, and a release there left the client waiting for ever (tests.upgrade `remote`). A queued build could
        also lose the one builder of its system.
        """
        scheduler = self.ctx.scheduler
        if scheduler is not None:
            counts = scheduler.queue.counts()
            if counts.pending or counts.building:
                return
        for store_id, store in list(self.ctx.stores.items()):
            if store.reconnects_on_close and not store.in_flight:
                log.info("drain_releases_reverse_builder", store_id=store_id)
                await self.remove_store(store_id, drain_timeout=0, missing_ok=True)

    async def _drain_watch(self) -> None:
        started = time.monotonic()
        with anyio.move_on_after(self.settings.drain_limit) as limit:
            # Polled: sessions and builds end in many places, and none of them signals a waiter.
            while self._holds_work():
                await self._release_reverse_builders()
                await anyio.sleep(DRAIN_POLL)
        if limit.cancelled_caught:
            log.warning(
                "server_drain_limit", sessions=len(self.ctx.sessions), seconds=round(time.monotonic() - started)
            )
        else:
            log.info("server_drained", seconds=round(time.monotonic() - started, 3))
        self.drained.set()

    async def _open_stores(self, opened: anyio.Event) -> None:
        """Open every store but the local one, at once, then set *opened*."""
        started = time.monotonic()
        try:
            async with anyio.create_task_group() as tg:
                for store_id, store in list(self.ctx._stores.items()):
                    if store_id != LOCAL_STORE_ID:
                        tg.start_soon(self.add_store, store, False, None)
        finally:
            opened.set()
        log.info("stores_opened", stores=len(self.ctx._stores) - 1, seconds=round(time.monotonic() - started, 3))

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
            if await anyio.to_thread.run_sync(self._over_watermark):
                if not _trigger_due(now, last_trigger, settings.gc_cooldown):
                    metrics.GC_TRIGGER_SKIPPED.labels(reason="watermark").inc()
                    log.debug("gc_trigger_cooldown", reason="watermark")
                    continue
                async with self._singleton_pass():
                    await self._drive_to_target()
                last_trigger = time.monotonic()
                continue
            if now - last_full >= settings.gc_interval:
                last_full = now
                if not _trigger_due(now, last_trigger, settings.gc_cooldown):
                    metrics.GC_TRIGGER_SKIPPED.labels(reason="scheduled").inc()
                    log.debug("gc_trigger_cooldown", reason="scheduled")
                    continue
                async with self._singleton_pass():
                    await self._gc_pass("scheduled")
                last_trigger = time.monotonic()

    async def _stores_opened(self) -> None:
        """Wait for the stores that open in the background: the bucket among them."""
        if self.ctx.scheduler is not None and self.ctx.scheduler.stores_opened is not None:
            await self.ctx.scheduler.stores_opened.wait()

    async def _cold_upload(self, uploader: ColdUploader) -> None:
        """Run the cold tier's uploader. A drain cancels it anywhere: a cut NAR has no narinfo, so no client sees it."""
        await self._stores_opened()
        await uploader.run()

    async def _cold_tick(self, tier: ColdTier, uploader: ColdUploader) -> None:
        """Put the paths a pressed pass would delete first at the head of the upload queue.

        A busy database skips one round.
        """
        settings = self.ctx.settings
        local = self.ctx.local_store
        target = local.gc_target_usage
        watermark = settings.gc_high_watermark
        ahead = settings.gc_cold_ahead
        if ahead is None:
            ahead = watermark - target if watermark is not None and target is not None else 0.05
        log.info("gc_cold_loop_started", store_id=tier.cold.store_id, interval=settings.gc_cold_interval, ahead=ahead)
        await self._stores_opened()
        while True:
            usage = await anyio.to_thread.run_sync(Collector._disk_usage, local)
            if usage is not None and usage[1] > 0:
                used, total = usage
                pressure = max(used / total, watermark or 0.0)
                try:
                    async with self._singleton_pass():
                        await uploader.urge(await Collector(self.ctx).cold_window(tier, int(total * ahead), pressure))
                except DatabaseError:
                    log.warning("gc_cold_round_failed", store_id=tier.cold.store_id, exc_info=True)
            # A round reads the whole plan. The uploader works through what it urged in the meantime.
            await anyio.sleep(settings.gc_cold_interval)

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
            if not await anyio.to_thread.run_sync(self._over_gc_target):
                return
            resp = await self._gc_pass("watermark")
            if resp is None or not resp.store_paths:
                return
        log.info("gc_drive_capped", cap=cap)

    def _over_gc_target(self) -> bool:
        """Continue an active drive until the local store reaches its target. A `statvfs`: call it in a thread."""
        local = self.ctx.local_store
        target = local.gc_target_usage
        if target is None:
            return self._over_watermark()
        usage = Collector._disk_usage(local)
        if usage is None:
            return False
        used, total = usage
        return total > 0 and used / total > target

    def _over_watermark(self) -> bool:
        """Whether disk pressure currently warrants a bounded pass. A `statvfs`: call it in a thread."""
        watermark = self.ctx.settings.gc_high_watermark
        if watermark is None:
            return False
        try:
            directory = self.ctx.local_store.layout.real_store_dir
            used, total = pressure_usage(directory)
        except Exception:
            log.warning("gc_watch_usage_failed", exc_info=True)
            return False
        return used / total > watermark

    async def _checkpoint_wal(self) -> bool:
        """Shrink the store database's WAL, which Nix never does. Whether a checkpoint ran."""
        db = self.ctx.local_store.db
        if db is None:
            return False
        return await db.checkpoint_wal()

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
                async with self._singleton_pass():
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
        running = self._run()
        server = await running.__aenter__()
        self._running = running
        return server

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        running, self._running = self._running, None
        if running is None:
            raise RuntimeError("Server.__aexit__ without __aenter__")
        return await running.__aexit__(exc_type, exc_val, exc_tb)

    @contextlib.asynccontextmanager
    async def _run(self) -> AsyncIterator[Server]:
        """The server's lifetime: its background loops live in this task group."""
        async with self._background.running():
            try:
                await self.start()
                yield self
            finally:
                # Shielded: a loop that failed has cancelled this scope, and
                # the clients still get their shutdown error.
                with anyio.CancelScope(shield=True):
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

    def _hand_on_built_outputs(self, build: QueuedBuild, response: BuildDerivationResponse) -> None:
        """Give the outputs of a build that succeeded to each store that keeps them: the GitHub Actions cache."""
        paths = output_paths(build, response)
        for store in self.ctx.stores.values():
            store.take_built_outputs(self.ctx.local_store, paths)

    async def _start_inner(self) -> None:
        """Open stores, start tasks, and bind every listener."""
        local_store = self.ctx.local_store
        s = self.settings

        # Bind before the store work, as nix-daemon does; accept after it. A
        # client that connects early waits in the backlog. Nix's functional
        # suite waits 6 s for the socket (`common/functions.sh:82`), and the
        # store work took 0.3-0.4 s of a 1.5 s start in its guest (#1302).
        inherited = self._inherited
        if s.unix_path:
            self._ensure_unix_socket_parent(s.unix_path)
            self.unix_server = await bind_unix_server(
                s.unix_path,
                inherited.take(ListenerName.UNIX) if inherited else [],
                inherited_owns_path=inherited is not None and inherited.unix_owns_path,
            )
        if s.handover_path:
            self._ensure_unix_socket_parent(s.handover_path)
            self.handover = await HandoverControl.bind(
                s.handover_path, inherited.take(ListenerName.HANDOVER) if inherited else []
            )

        await anyio.to_thread.run_sync(preload)
        if s.tracing.endpoint is not None:
            self._tracing = await anyio.to_thread.run_sync(lambda: _optional.tracing_otel.OtelTracing.start(s.tracing))
            self.ctx.tracing = self._tracing
            if self.ctx.scheduler:
                self.ctx.scheduler.queue.tracing = self._tracing

        await self._background.start(self._host_store, local_store, False, None, True)

        self.ctx.trust = await local_store.trust_policy()
        self.ctx.daemon_header = await local_store.daemon_header()
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
            self.ctx.scheduler.queue.on_built.append(self._hand_on_built_outputs)
            queue = self.ctx.scheduler.substitution_queue
            queue.set_import_slots(self.ctx.trust.max_substitution_jobs)
            queue.note_daemon_substituters(self.ctx.trust.substituters)
            if queue.uncovered:
                log.info("substituters_only_the_daemon_reaches", substituters=queue.uncovered)

        if isinstance(local_store, LocalDBStore):
            if self.ctx.scheduler:
                local_store.substitution_queue = self.ctx.scheduler.substitution_queue
            local_store.recursive_server = restricted_server(self.ctx)
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

        # The listeners do not wait for the other stores. A substituter that
        # cannot be reached held every client for its whole open: 10.5 s on a
        # DNS timeout (nixidae/nixidae#1268). What reads the store set waits.
        opened = anyio.Event()
        if self.ctx.scheduler:
            self.ctx.scheduler.stores_opened = opened
            self.ctx.scheduler.substitution_queue.stores_opened = opened
        self._spawn(self._open_stores, opened)

        if self.ctx.scheduler:
            await self._background.start(self.ctx.scheduler.substitution_queue.serve)
            self._spawn(self.ctx.scheduler.start)

        if self.ctx.db and self.ctx.settings.gc_enabled:
            self._spawn_singleton(self._gc_tick)
            settings = self.ctx.settings
            if (tier := ColdTier.of(self.ctx)) is not None and settings.gc_cold_workers:
                uploader = ColdUploader(tier, workers=settings.gc_cold_workers, poll=settings.gc_poll_interval)
                self.cold_uploader = uploader
                self._spawn_singleton(self._cold_upload, uploader)
                self._spawn_singleton(self._cold_tick, tier, uploader)
            liveness_interval = self.ctx.settings.gc_liveness_interval
            if liveness_interval is not None:
                watch = LivenessWatch.of(self.ctx.local_store.layout, self.ctx.db)
                if watch is None:
                    log.warning("gc_liveness_watch_unavailable")
                else:
                    dirty = DirtyFlag()
                    roots = StableRootsWatch(watch.tracker.state_dir)
                    self._spawn_singleton(roots.run, dirty)
                    self._spawn_singleton(self._liveness_tick, watch, liveness_interval, dirty)

        # Before the listeners, so a stall during their startup is recorded.
        if self.stall_watchdog is not None:
            self.stall_watchdog.beat()
            self.stall_watchdog.start(asyncio.get_running_loop())
        self._spawn(self.loop_lag.run)
        self._spawn(self._warn_silent_upstreams)
        if self.ctx.session_builders is not None:
            self._spawn(self.ctx.session_builders.run)

        if s.ssh_port is not None:
            ssh_key = _optional.ssh_server.load_host_key(s.ssh_host_key)
            acceptors = [
                await _optional.ssh_server.start_ssh_server(
                    ctx=self.ctx,
                    host=s.ssh_host,
                    port=s.ssh_port,
                    schedule_mode=s.schedule_mode,
                    user_keys=s.ssh_user_keys,
                    admin_keys=s.ssh_admin_keys,
                    admin_users=s.admin_users,
                    sock=sock,
                    host_key=ssh_key,
                )
                for sock in self._tcp_sockets(ListenerName.SSH, s.ssh_host, s.ssh_port)
            ]
            self.ssh_server, *more = acceptors
            self._more_acceptors.extend(more)

        # **The `enabled` check belongs here as well as in the function.**
        # `start_reverse_acceptor` answers `None` for a set that is off, and
        # reaching it at all imports `asyncssh`. Issue #30.
        if s.reverse_acceptor.enabled:
            host_key = _optional.reverse_server.client_key(s.reverse_acceptor)
            self.provisioner = self.ctx.provisioner = self._provisioner(host_key.export_public_key().decode())
            reverse = [
                await _optional.reverse_server.start_reverse_acceptor(
                    server=self,
                    settings=s.reverse_acceptor,
                    spawn=self._spawn,
                    joiner=self.provisioner,
                    host_key=host_key,
                    sock=sock,
                )
                for sock in self._tcp_sockets(ListenerName.REVERSE, s.reverse_acceptor.host, s.reverse_acceptor.port)
            ]
            self.reverse_acceptor, *more_reverse = reverse
            self._more_acceptors.extend(acceptor for acceptor in more_reverse if acceptor is not None)
            if self.provisioner is not None:
                self._spawn_singleton(self.provisioner.run)
        elif s.provisioning.pools:
            raise RuntimeError("provisioning pools need the reverse acceptor: launched builders register through it")

        if s.reverse_initiator.enabled:
            initiator = _optional.reverse_client.ReverseInitiator(self.ctx, s.reverse_initiator)
            self._spawn(initiator.run)

        if self.unix_server is not None:
            serve_unix_clients(self.unix_server, self.ctx, self._spawn, s.schedule_mode)
        if self.handover is not None:
            self.handover.serve(self._spawn, self)

        if s.http_port is not None or s.https_port is not None:
            scheduler = self.ctx.scheduler
            cache = self._http_cache = _optional.http_server.PynixdHttpServer(
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
            )
            if s.http_port is not None:
                runner, port = await cache.start(
                    host=s.http_host,
                    port=s.http_port,
                    sockets=self._tcp_sockets(ListenerName.HTTP, s.http_host, s.http_port),
                )
                self.http_server = runner
                self.http_bound_port = port
            if s.https_port is not None:
                runner, port = await cache.start(
                    host=s.http_host,
                    port=s.https_port,
                    ssl_cert=str(s.https_cert) if s.https_cert else None,
                    ssl_key=str(s.https_key) if s.https_key else None,
                    sockets=self._tcp_sockets(ListenerName.HTTPS, s.http_host, s.https_port),
                )
                self.https_server = runner
                self.https_bound_port = port

        if inherited is not None:
            inherited.close_rest()

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
        for acceptor in self._more_acceptors:
            acceptor.close()
            await acceptor.wait_closed()
        self._more_acceptors.clear()
        if self.unix_server:
            await self.unix_server.aclose()
            self.unix_server = None
        if self.handover:
            await self.handover.aclose(unlink=True)
            self.handover = None

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

        # The stores' hosts are background tasks: this closes the stores too.
        await self._background.aclose()
        self.ctx._stores.clear()
        # After the scheduler: the builds it cancelled end their spans first. `close` bounds its own wait.
        await anyio.to_thread.run_sync(self._tracing.close)
        self._done_event.set()
