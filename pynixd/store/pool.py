"""
Connection pooling and concurrency limiting for Nix daemon connections.
"""

from __future__ import annotations

import time
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from contextvars import ContextVar
from typing import TYPE_CHECKING

import anyio
import structlog

from .. import metrics

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from ..background import Background
    from ..connection import Connection
    from ..serde import SetOptionsRequest

log = structlog.get_logger(__name__)


# Per-store connection holder tracking via ContextVar
# Tracks nested connection count per store_id
_nested_conns: ContextVar[dict[str, int]] = ContextVar("_nested_conns")


class ConnectionPool:
    """Manages a pool of reusable daemon connections with concurrency limiting and idle TTL.

    Tracks connection nesting per-task via a ContextVar to detect re-entrant
    acquisitions and allocate fresh connections to avoid deadlock.
    """

    def __init__(
        self,
        store_id: str,
        factory: Callable[[], Awaitable[Connection]],
        background: Background,
        idle_ttl: float = 10.0,
        max_lifetime: float = 300.0,
        max_connections: int = 64,
        on_connection_created: Callable[[Connection], None] | None = None,
        on_pool_empty: Callable[[], Awaitable[None]] | None = None,
        lane: str = "transfer",
        reuse: bool = True,
    ) -> None:
        """Configure pool with connection factory and concurrency limits."""

        self.store_id = store_id
        self.factory = factory
        self.idle_ttl = idle_ttl
        self.max_lifetime = max_lifetime
        """How long a connection may serve before the pool retires it.

        `idle_ttl` retires a connection that nobody uses. This retires one
        that everybody uses, and the reason is the temporary roots of the
        daemon. A worker adds a root for each path that it builds or
        substitutes, and it releases those roots when it exits. A pooled
        connection keeps the worker alive, so without this rule a connection
        in steady use holds every root it ever made, and the collector can
        free nothing that passed through it.

        This is a bound on how long that lasts, and not a promise that a root
        goes away at the end of the client that made it. Issue #20.
        """
        self.lane = lane
        """Which of a store's pools this is: `build` or `transfer`. Issue #97."""
        self.reuse = reuse
        """Whether a returned connection waits for the next caller, or closes.

        A build pool does not reuse. The daemon's worker keeps a temporary
        root on each output a build checked or made, for as long as it
        lives, so an idle build connection kept those roots past the build.
        `multiple-outputs.sh:80` of Nix's suite deletes such an output
        around pynixd and failed. Nix's worker serves one client and exits
        with it; a build connection here serves one build and closes."""
        self.max_connections = max_connections
        self._slots = anyio.Semaphore(max_connections)
        self.slot_waiters: list[float] = []
        """When each acquire that waits for a slot now began to wait."""
        self.on_connection_created = on_connection_created
        self.on_pool_empty = on_pool_empty
        """Called once the pool holds no connection at all, active or idle.

        A store that owns a transport under the pool releases it here. An SSH
        store holds one `asyncssh` connection for every channel it opens, and
        nothing above the pool knows when the last one goes away.
        """

        self.active_connections = 0
        self.idle_conns: list[tuple[Connection, float]] = []
        self.all_conns: list[Connection] = []
        self._background = background
        """The owning store's loops. The idle sweep runs there, so it ends when the store closes."""
        self._sweep: anyio.CancelScope | None = None
        # The collector holds this weakly and reads the three counts above on
        # each scrape, so nothing here updates a gauge per acquire.
        metrics.STORE_POOLS.register(self)
        metrics.STORE_TRAFFIC.register(self)
        self.wire_bytes_read = 0
        self.wire_bytes_written = 0
        """Wire bytes of connections this pool has retired.

        Live connections carry their own totals on their readers and
        writers; `traffic_totals` adds both. A store that goes away takes
        its pool -- and these numbers -- with it; the traffic collector
        keeps the highest total per store across that.
        """

    def _note_conn_closed(self, conn: Connection) -> None:
        """Fold one dead connection's wire totals into the pool's."""
        self.wire_bytes_read += conn.r.bytes_read
        self.wire_bytes_written += conn.w.bytes_written

    def traffic_totals(self) -> tuple[int, int]:
        """(bytes read, bytes written) over every connection this pool held.

        Retired connections read from the accumulators above, live ones
        from their readers and writers.
        """
        read = self.wire_bytes_read
        written = self.wire_bytes_written
        for conn in self.all_conns:
            read += conn.r.bytes_read
            written += conn.w.bytes_written
        return read, written

    @property
    def in_flight(self) -> int:
        """Number of currently active connections."""
        return self.active_connections

    @property
    def stats(self) -> str:
        """Human-readable pool statistics."""
        return f"active={self.active_connections} idle={len(self.idle_conns)} total={len(self.all_conns)}"

    def waiting_on_upstream(self) -> list[Connection]:
        """Connections waiting on an operation now. Issue #93."""
        return [conn for conn in self.all_conns if conn.current_op is not None]

    def start_sweep(self) -> None:
        """Start the idle sweep if it is not running."""
        if self._sweep is None:
            self._sweep = self._background.spawn(self.sweep_idle)

    async def sweep_idle(self) -> None:
        """Periodically close idle connections that have expired."""
        # `close` clears `_sweep` while this unwinds, and a newer sweep may take its place.
        mine = self._sweep
        try:
            await self._sweep_until_empty()
        finally:
            if self._sweep is mine:
                self._sweep = None

    async def _sweep_until_empty(self) -> None:
        while self.idle_conns:
            await anyio.sleep(self.idle_ttl / 2)
            now = time.monotonic()

            expired: list[tuple[Connection, float]] = []
            for item in self.idle_conns:
                conn, returned_at = item
                if now - returned_at >= self.idle_ttl or self._too_old(conn, now):
                    expired.append(item)

            # Remove from tracking lists synchronously to prevent race conditions
            # with connections returned during the 'await conn.close()' yields below.
            for item in expired:
                if item in self.idle_conns:
                    self.idle_conns.remove(item)
                conn = item[0]
                if conn in self.all_conns:
                    self.all_conns.remove(conn)

            # Perform the async closures safely
            for conn, _ in expired:
                metrics.POOL_IDLE_EXPIRED.labels(store_id=self.store_id).inc()
                log.debug(
                    "pool_closing_expired_idle",
                    store_id=self.store_id,
                    conn_id=conn.id,
                )
                with suppress(Exception):
                    await conn.close()

            await self._notify_if_empty()

    async def _notify_if_empty(self) -> None:
        """Tell the owner when the last connection has gone.

        Checked after the sweep closes what expired, which is the only place
        the pool can reach zero without someone about to use it again.
        """
        if self.active_connections or self.idle_conns or self.all_conns:
            return
        # Counted before the callback guard: the pool reached zero whether or
        # not an owner asked to hear about it.
        metrics.POOL_EMPTIED.labels(store_id=self.store_id).inc()
        log.debug("pool_empty", store_id=self.store_id)
        if self.on_pool_empty is None:
            return
        try:
            await self.on_pool_empty()
        except Exception:
            # A sweep that raises closes the whole store, and a failed
            # teardown of the transport is no reason for that.
            log.exception("pool_empty_callback_failed", store_id=self.store_id)

    def _too_old(self, conn: Connection, now: float) -> bool:
        """True when this connection has served long enough to retire."""
        return self.max_lifetime > 0 and now - conn.opened_at >= self.max_lifetime

    async def get_or_create_conn(self, options: SetOptionsRequest | None = None) -> Connection:
        """Return a reusable idle connection, or create a new one.

        **A connection carries the options of one client for its whole life.**
        The daemon protocol has `SetOptions` and no operation that takes an
        option away, so a second `SetOptions` on one connection adds to the
        first set rather than replacing it. A client that built with
        `--auto-optimise-store` therefore left that setting on, and the next
        client got a store that optimises when it asked for none.
        `main:optimise-store` reads exactly that. So an idle connection with
        another set is of no use here, and this discards it. Issue nixidae/nixidae#231.
        """

        """Pop an idle connection or create a new one."""
        now = time.monotonic()
        while self.idle_conns:
            candidate, returned_at = self.idle_conns.pop()
            if now - returned_at >= self.idle_ttl or self._too_old(candidate, now):
                metrics.POOL_IDLE_EXPIRED.labels(store_id=self.store_id).inc()
                log.debug(
                    "pool_discarding_expired",
                    store_id=self.store_id,
                    conn_id=candidate.id,
                    age=f"{now - candidate.opened_at:.1f}s",
                )
                if candidate in self.all_conns:
                    self.all_conns.remove(candidate)
                with suppress(Exception):
                    await candidate.close()
                continue

            if candidate.applied_options is not None and candidate.applied_options != options:
                log.debug(
                    "pool_discarding_other_options",
                    store_id=self.store_id,
                    conn_id=candidate.id,
                )
                if candidate in self.all_conns:
                    self.all_conns.remove(candidate)
                with suppress(Exception):
                    await candidate.close()
                continue

            if candidate.r.peer_closed():
                # The daemon closed it while it sat idle, as a restart does
                # to every pooled link. Handing it out failed the next
                # client with an IncompleteReadError (#93).
                log.info("pool_discarding_closed_by_peer", store_id=self.store_id, conn_id=candidate.id)
                if candidate in self.all_conns:
                    self.all_conns.remove(candidate)
                with suppress(Exception):
                    await candidate.close()
                continue

            if candidate.dirty or await candidate.r.is_dirty():
                log.warning(
                    "pool_discarding_dirty_conn",
                    store_id=self.store_id,
                    conn_id=candidate.id,
                    op_log=" -> ".join(candidate.op_log[-10:]) or "(empty)",
                )
                if candidate in self.all_conns:
                    self.all_conns.remove(candidate)
                with suppress(Exception):
                    await candidate.close()
                continue

            log.debug(
                "pool_reusing_conn",
                store_id=self.store_id,
                conn_id=candidate.id,
            )
            return candidate

        conn = await self.factory()
        conn.on_close = self._note_conn_closed
        self.all_conns.append(conn)
        if self.on_connection_created:
            self.on_connection_created(conn)

        metrics.POOL_CONNECTIONS_CREATED.labels(store_id=self.store_id).inc()
        log.debug(
            "pool_created_connection",
            store_id=self.store_id,
            conn_id=conn.id,
            pool_stats=self.stats,
        )
        return conn

    @asynccontextmanager
    async def acquire(
        self,
        kind: str | None = None,
        options: SetOptionsRequest | None = None,
    ) -> AsyncIterator[Connection]:
        """Acquire a connection from the shared pool.

        Wait for memory pressure to subside before allocating a new connection.
        If the same task that is already holding a connection tries to
        acquire again (re-entry), it skips the limit to avoid deadlock: an
        idle connection when there is one, else a new one.
        """
        # Use a copy to avoid mutating a shared default or parent context dict
        counts = _nested_conns.get({}).copy()
        in_use = counts.get(self.store_id, 0)
        re_entrant = in_use > 0

        if re_entrant:
            # Every push and pull of a build runs here, inside its build
            # connection. A new connection for each one cost a handshake each:
            # 1117 in one apply. nixidae/nixidae#1075.
            log.debug(
                "store_reentrant_acquire",
                store_id=self.store_id,
                kind=kind,
                nesting_level=in_use,
            )
            shared: Connection = await self.get_or_create_conn(options)

            # Increment nesting count in a NEW dict to ensure task isolation
            counts[self.store_id] = in_use + 1
            _nested_conns.set(counts)

            try:
                async with shared:
                    yield shared
            finally:
                # Decrement nesting count
                counts = _nested_conns.get({}).copy()
                counts[self.store_id] = counts.get(self.store_id, 0) - 1
                _nested_conns.set(counts)
                await self._give_back(shared)
            return

        await self._take_slot()
        try:
            self.active_connections += 1
            conn: Connection | None = None
            try:
                conn = await self.get_or_create_conn(options)

                # Increment nesting count
                counts = _nested_conns.get({}).copy()
                counts[self.store_id] = counts.get(self.store_id, 0) + 1
                _nested_conns.set(counts)

                try:
                    async with conn:
                        yield conn
                finally:
                    # Decrement nesting count
                    counts = _nested_conns.get({}).copy()
                    counts[self.store_id] = counts.get(self.store_id, 0) - 1
                    _nested_conns.set(counts)

                    if conn is not None:
                        await self._give_back(conn)
            finally:
                self.active_connections -= 1
        finally:
            self._slots.release()

    async def _give_back(self, conn: Connection) -> None:
        """Keep *conn* idle for the next caller, or close it when it may not serve again."""
        now = time.monotonic()
        retired = self._too_old(conn, now)
        # A closed store runs no sweep, so an idle connection would never expire.
        if conn.dirty or retired or not self.reuse or not self._background.open:
            if conn.dirty:
                log.warning(
                    "store_discarding_dirty_connection",
                    store_id=self.store_id,
                    conn_id=conn.id,
                    op_log=" -> ".join(conn.op_log[-10:]) or "(empty)",
                )
            else:
                log.debug(
                    "pool_retiring_old_connection" if retired else "pool_closing_used_connection",
                    store_id=self.store_id,
                    conn_id=conn.id,
                    lane=self.lane,
                    age=f"{now - conn.opened_at:.1f}s",
                )
            if conn in self.all_conns:
                self.all_conns.remove(conn)
            with suppress(Exception):
                await conn.close()
        else:
            self.idle_conns.append((conn, time.monotonic()))
            self.start_sweep()

    async def _take_slot(self) -> None:
        since = time.monotonic()
        self.slot_waiters.append(since)
        try:
            await self._slots.acquire()
        finally:
            self.slot_waiters.remove(since)
            metrics.POOL_SLOT_WAIT.labels(store_id=self.store_id, lane=self.lane).inc(time.monotonic() - since)

    def build_conn(self) -> AbstractAsyncContextManager[Connection]:
        """Acquire a connection for build operations."""
        return self.acquire("build")

    def transfer_conn(self) -> AbstractAsyncContextManager[Connection]:
        """Acquire a connection for transfer operations."""
        return self.acquire("transfer")

    async def retire_idle(self) -> int:
        """Close each connection that nobody uses now, and say how many.

        `idle_ttl` does this after some seconds, and a garbage collection
        cannot wait: a worker of the daemon holds a temporary root for each
        path that it took, and it releases those roots when it exits. An idle
        connection keeps a worker alive, so the collector reads a root that no
        client asked for and frees nothing.

        **This narrows the window, and it does not close it.** A connection
        that is in flight stays, so its roots stay. Nothing stops another
        client from taking a connection and adding a root between this call
        and the moment the collector reads the file. And a client that reaches
        the store directly, with no pynixd in the path, never calls this at
        all: `multiple-outputs.sh:80` of Nix runs
        `env -u NIX_REMOTE nix store delete --ignore-liveness`, and it fails
        for that reason.

        The lifetime of the connection is the mechanism, so the answer is the
        one that issue #20 states: give each client session its own upstream
        connection, and close it with the session. Then a root lives as long
        as the client that made it, as in `nix-daemon`, and no window is left.
        """
        idle = self.idle_conns
        self.idle_conns = []
        for conn, _ in idle:
            if conn in self.all_conns:
                self.all_conns.remove(conn)
            log.debug("pool_retiring_idle", store_id=self.store_id, conn_id=conn.id)
            with suppress(Exception):
                await conn.close()
        return len(idle)

    async def close(self) -> None:
        """Close all connections and cancel the idle sweep."""
        if self._sweep is not None:
            self._sweep.cancel()
            self._sweep = None

        for conn in self.all_conns:
            with suppress(Exception):
                await conn.close()
        self.all_conns.clear()
        self.idle_conns.clear()
