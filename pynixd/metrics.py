"""Prometheus metrics for pynixd, served from the `/metrics` route.

**Every label here is bounded.** A store path, a client address or a
derivation name would give one series per value, and a cache that serves a
cluster sees millions of them. `op` comes from `WIRE_REGISTRY`, `store_id`
from the configuration, and `result`, `route` and `transport` are written out
in this file.

`prometheus_client` registers `ProcessCollector`, `PlatformCollector` and
`GCCollector` on the default registry by itself, so `process_cpu_seconds_total`,
`process_resident_memory_bytes`, `process_open_fds` and `python_gc_*` are
already served. Do not add a gauge for any of them.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Iterable, Mapping
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, get_args
from weakref import WeakSet

import structlog
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

from nix_daemon_protocol.store_dir import real_store_dir

from .state_models import WaitReason

if TYPE_CHECKING:
    from prometheus_client.metrics_core import Metric

    from .build_queue import BuildQueue
    from .store.pool import ConnectionPool

log = structlog.get_logger(__name__)

# --- Queue Metrics ---

QUEUE_SIZE = Gauge(
    "pynixd_build_queue_size",
    "Number of builds currently in the queue",
    ["status"],  # pending, building, done
)


UPSTREAM_READ_TIMEOUTS = Counter(
    "pynixd_upstream_read_timeouts_total",
    "Database reads a store left unanswered past read_timeout (#93)",
    ["store_id"],
)

POOL_SLOT_WAIT = Counter(
    "pynixd_store_pool_slot_wait_seconds_total",
    "Time operations spent waiting for a free connection slot, per pool lane (#97)",
    ["store_id", "lane"],
)

BUILDS_WAITING = Gauge(
    "pynixd_builds_waiting",
    "Pending builds by what they wait for: local_slot, unwanted or scheduling (#92)",
    ["reason"],
)

CLIENT_OPS = Gauge(
    "pynixd_client_ops_in_progress",
    "Client operations pynixd is answering now",
)

CLIENT_OP_SECONDS_MAX = Gauge(
    "pynixd_client_op_seconds_max",
    "How long the oldest client operation in progress has run",
)


def watch_waiting(reasons: Callable[[], Mapping[WaitReason, int]]) -> None:
    """Answer `pynixd_builds_waiting` from *reasons* at each scrape."""
    for reason in get_args(WaitReason):
        BUILDS_WAITING.labels(reason=reason).set_function(lambda reason=reason: reasons().get(reason, 0))


def watch_client_ops(ops: Callable[[], list[float]]) -> None:
    """Answer the client operation gauges from *ops*, the start of each one in progress."""
    CLIENT_OPS.set_function(lambda: len(ops()))
    CLIENT_OP_SECONDS_MAX.set_function(lambda: max((time.monotonic() - t for t in ops()), default=0.0))


def watch_queue(queue: BuildQueue) -> None:
    """Answer the queue gauge from *queue* itself at each scrape.

    A walk of a few hundred builds per scrape, and it cannot drift the way
    the increments did (#89). The last queue bound wins, which is the
    scheduler's own: there is one per server.
    """
    QUEUE_SIZE.labels(status="pending").set_function(lambda: queue.counts().pending)
    QUEUE_SIZE.labels(status="building").set_function(lambda: queue.counts().building)
    QUEUE_SIZE.labels(status="done").set_function(lambda: queue.counts().done)


BUILD_DURATION = Histogram(
    "pynixd_build_duration_seconds",
    "Time spent actively building (excluding queue wait time)",
    buckets=(1, 5, 15, 30, 60, 120, 300, 600, 1200, 3600),
)

QUEUE_WAIT_DURATION = Histogram(
    "pynixd_build_queue_wait_duration_seconds",
    "Time spent in the queue before a build task starts",
    buckets=(1, 5, 10, 30, 60, 120, 300),
)

BUILDS_COMPLETED = Counter(
    "pynixd_builds_completed_total",
    "Total number of builds completed",
    ["status"],  # success, failure
)

# --- Store Metrics ---

STORE_CPU_UTILIZATION = Gauge(
    "pynixd_store_cpu_utilization_percent",
    "Reported CPU utilization of a backend store",
    ["store_id"],
)


STORE_HEALTHY = Gauge(
    "pynixd_store_healthy",
    "Health status of a backend store (1 = healthy, 0 = unhealthy)",
    ["store_id"],
)

# --- Event loop metrics ---

# How long the loop goes between running ready callbacks. Everything pynixd
# serves is answered by the loop, so this is the one number that says whether
# it can answer at all -- a transfer that does not yield stalls the loop, and a
# stalled loop accepts no connection and runs no probe handler.
#
# A TCP probe cannot see this. `connect()` is completed by the kernel from the
# listen backlog whether or not the application ever accepts, so it passes
# against a wedged process and fails only on a timeout. That is nixidae/nixidae#487.
EVENT_LOOP_LAG = Gauge(
    "pynixd_event_loop_lag_seconds",
    "Seconds the event loop went without running a ready callback, over the last window",
)

EVENT_LOOP_LAG_MAX = Gauge(
    "pynixd_event_loop_lag_max_seconds",
    "Largest event loop stall seen since the process started",
)

# **A maximum carries no time, so it cannot be tied to an event.** A cluster
# reported a lifetime maximum of 7.912 s against a 0.802 s window, and nothing
# in the scrape said whether that stall landed during a push, during a pull or
# at rest. These two answer that without a scrape at the right moment.
#
# The histogram is the one to graph: `rate(..._bucket[5m])` shows when the
# stalls happened, and its `_count` divided by the scrape interval says the
# monitor is still sampling, which a gauge stuck at its maximum does not.
EVENT_LOOP_LAG_SAMPLES = Histogram(
    "pynixd_event_loop_lag_sample_seconds",
    "Every event loop lag sample, so a stall can be located in time",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)

# Wall clock, not monotonic: the point is to line the peak up against a pod
# event or a push in somebody else's log.
EVENT_LOOP_LAG_MAX_AT = Gauge(
    "pynixd_event_loop_lag_max_timestamp_seconds",
    "Unix time at which the largest event loop stall was observed",
)

# --- Daemon protocol ---
#
# `DaemonProxy.op_loop` already times every operation and prints the total when
# the session ends. A number that exists only in a closing log line answers
# nothing while the transfer is running, which is when somebody looks.

DAEMON_OPS = Counter(
    "pynixd_daemon_ops_total",
    "Daemon protocol operations this process has finished",
    ["op", "result"],  # result: ok, error
)

# The top bucket is 600 s on purpose. A `BuildPaths` waits for the build, so
# this histogram carries a request that runs for minutes beside one that
# answers from SQLite in microseconds.
DAEMON_OP_DURATION = Histogram(
    "pynixd_daemon_op_duration_seconds",
    "Time one daemon protocol operation took, from reading its number to its response",
    ["op"],
    buckets=(0.001, 0.005, 0.025, 0.1, 0.5, 2.5, 10, 60, 300, 600),
)


class OpSeries(NamedTuple):
    """The Prometheus children for one (operation, result) series.

    `.labels()` takes a lock and a lookup per call, and an operation pays
    two of them. The series are bounded (one op name each, two results), so
    a session resolves each series once and holds the children.
    """

    ops: Counter
    duration: Histogram

    def observe(self, elapsed: float) -> None:
        self.ops.inc()
        self.duration.observe(elapsed)


def op_series(op: str, result: str) -> OpSeries:
    """Resolve the children for (op, result)."""
    return OpSeries(DAEMON_OPS.labels(op=op, result=result), DAEMON_OP_DURATION.labels(op=op))


class TransferMeter:
    """Count the bytes, paths and duration of one NAR transfer.

    Off when the session disabled metrics: `on_bytes` is then None, so the
    transfer loop pays no call per chunk, and `finish` records nothing.
    """

    def __init__(self, *, enabled: bool, byte_counter: Counter, path_counter: Counter, duration: Histogram) -> None:
        self._enabled = enabled
        self._byte_counter = byte_counter
        self._path_counter = path_counter
        self._duration = duration
        self._started = time.monotonic()

    @property
    def on_bytes(self) -> Callable[[int], None] | None:
        """The chunk callback for the transfer loop, or None when disabled."""
        return self._byte_counter.inc if self._enabled else None

    def finish(self) -> None:
        """Count the path and its duration, unless disabled."""
        if self._enabled:
            self._path_counter.inc()
            self._duration.observe(time.monotonic() - self._started)


DAEMON_SESSIONS = Gauge(
    "pynixd_daemon_sessions",
    "Client sessions being served now",
    ["transport"],  # ssh, unix, reverse
)

# Not `pynixd_daemon_sessions_total`: `prometheus_client` strips the `_total`
# to get a counter's base name, and that base would collide with the gauge
# above. The registry rejects the collision at import, so this is a build
# failure and not a silent one.
DAEMON_SESSIONS_TOTAL = Counter(
    "pynixd_daemon_sessions_accepted_total",
    "Client sessions this process has accepted",
    ["transport"],
)

# --- NAR transfer ---
#
# **Three families, because a NAR takes one of three loops and they fail
# differently.** A dashboard that reads only one of them sees a pod that moved
# gigabytes as idle.
#
#     bytes received = pynixd_nar_forward_bytes_total   (op 44)
#                    + pynixd_nar_add_bytes_total       (op 7, op 39)
#     bytes served   = pynixd_nar_serve_bytes_total     (op 38)
#
# Which op a client sends, `remote-store.cc`: `nix copy` at protocol 1.32 and
# above is AddMultipleToStore, line 508. A single-path add -- `nix store
# add-path`, `nix-store --import`, any client below 1.32 -- is AddToStoreNar,
# line 451. A pull is NarFromPath.
#
# **So the split is also the discriminator.** Given a push whose op nobody
# recorded, whichever of the two receive counters moved names the loop it
# took. Keep them separate for that reason, and do not fold them into one
# series with a label: an alert on the whole would fire per label.

# The server side of `AddMultipleToStore`: what a `nix copy` into pynixd
# spends its time and its memory on. This is the path that peaked at 0.979 of
# the payload in RAM and 3.074 s of CPU per 128 MiB before the backpressure and
# the SSH read-ahead landed, so a dashboard needs to see it directly.

NAR_FORWARD_BYTES = Counter(
    "pynixd_nar_forward_bytes_total",
    "NAR bytes received from a client for AddMultipleToStore (op 44)",
)

NAR_FORWARD_PATHS = Counter(
    "pynixd_nar_forward_paths_total",
    "Store paths received from a client for AddMultipleToStore (op 44)",
)

NAR_FORWARD_DURATION = Histogram(
    "pynixd_nar_forward_duration_seconds",
    "Time one AddMultipleToStore payload took to forward",
    buckets=(0.1, 0.5, 1, 5, 15, 60, 300, 600, 1800),
)

# A transfer that finished but whose source never reached EOF. The client is
# still holding the connection, and the handler gave up waiting after 10 s.
NAR_FORWARD_EOF_TIMEOUTS = Counter(
    "pynixd_nar_forward_source_eof_timeouts_total",
    "AddMultipleToStore payloads whose source did not reach EOF in time",
)

# The other receiving direction: one path per request, through
# `wire.forward_framed`. It held the whole payload in the transport and never
# suspended until 2026-09-21, so a push of a large closure was resident in
# full: measured 206.9 MiB of a 256 MiB path.

NAR_ADD_BYTES = Counter(
    "pynixd_nar_add_bytes_total",
    "NAR bytes received from a client for AddToStore or AddToStoreNar (op 7, op 39)",
)

NAR_ADD_PATHS = Counter(
    "pynixd_nar_add_paths_total",
    "Store paths received one at a time (op 7, op 39)",
)

NAR_ADD_DURATION = Histogram(
    "pynixd_nar_add_duration_seconds",
    "Time one AddToStore or AddToStoreNar payload took to forward",
    buckets=(0.1, 0.5, 1, 5, 15, 60, 300, 600, 1800),
)

# The serving direction, which nothing measured before. A node reads its
# closure this way when a pod starts, so `nar_serve_duration_seconds` is the
# series that says a pod is waiting on this process rather than on the
# scheduler.

NAR_SERVE_BYTES = Counter(
    "pynixd_nar_serve_bytes_total",
    "NAR bytes served to a client for NarFromPath (op 38)",
)

NAR_SERVE_PATHS = Counter(
    "pynixd_nar_serve_paths_total",
    "Store paths served to a client for NarFromPath (op 38)",
)

NAR_SERVE_DURATION = Histogram(
    "pynixd_nar_serve_duration_seconds",
    "Time one NarFromPath response took to stream",
    buckets=(0.1, 0.5, 1, 5, 15, 60, 300, 600, 1800),
)

# --- Store-to-store NAR volume ---
#
# The client edge above meters wire bytes per chunk behind
# `metrics_enabled`. These two series answer a different question: how
# much data each backend store moved, and in which direction. They
# answer it from the NAR sizes of the infos the transfer already
# walks: one `.inc()` per path, never per chunk, recorded
# unconditionally like `SUBSTITUTED_BYTES`. A direction that surprises
# is the point, so in and out stay separate: "in" is into the local
# store from the peer, "out" is out of it to the peer.

STORE_TRANSFER_NAR_BYTES = Counter(
    "pynixd_store_transfer_nar_bytes_total",
    "NAR bytes moved store to store, summed from path infos (not wire bytes)",
    ["store_id", "direction"],  # direction: in, out — relative to the local store
)

STORE_TRANSFER_PATHS = Counter(
    "pynixd_store_transfer_paths_total",
    "Store paths moved store to store",
    ["store_id", "direction"],
)


_STORE_TRANSFER_SERIES: dict[tuple[str, str], tuple[Counter, Counter]] = {}
"""Resolved children per (store id, direction), after `OpSeries`.

`.labels()` takes a lock and a lookup per call, so a transfer resolves
each pair once and holds the children. Stores come from the
configuration plus registered builders, and directions are two, so the
dict stays small.
"""


def _store_transfer_series(store_id: str, direction: str) -> tuple[Counter, Counter]:
    """The byte and path children for one (store, direction) pair."""
    key = (store_id, direction)
    hit = _STORE_TRANSFER_SERIES.get(key)
    if hit is None:
        hit = (
            STORE_TRANSFER_NAR_BYTES.labels(store_id=store_id, direction=direction),
            STORE_TRANSFER_PATHS.labels(store_id=store_id, direction=direction),
        )
        _STORE_TRANSFER_SERIES[key] = hit
    return hit


def record_store_transfer(
    store_id: str,
    direction: Literal["in", "out"],
    nar_bytes: int,
    paths: int = 1,
) -> None:
    """Count one store-to-store movement, unconditionally.

    Callers pass the NAR sizes of the infos they already hold; nothing
    here touches a byte loop, so recording stays off the hot path. The
    existing global meters keep their `metrics_enabled` switch.
    """
    byte_child, path_child = _store_transfer_series(store_id, direction)
    byte_child.inc(nar_bytes)
    path_child.inc(paths)


# --- Connection pools ---

POOL_CONNECTIONS_CREATED = Counter(
    "pynixd_store_pool_connections_created_total",
    "Daemon connections a store pool has opened",
    ["store_id"],
)

POOL_IDLE_EXPIRED = Counter(
    "pynixd_store_pool_idle_expired_total",
    "Pooled connections closed because they went idle or reached their lifetime",
    ["store_id"],
)

POOL_EMPTIED = Counter(
    "pynixd_store_pool_emptied_total",
    "Times a store pool dropped to no connection at all",
    ["store_id"],
)

# --- Substitution ---
#
# What a build waits on before it decides to build anything. A substituter
# that answers slowly and a substituter that answers "no" cost a client the
# same wall clock and are a different fault, and nothing here said which.

SUBSTITUTER_QUERIES = Counter(
    "pynixd_substituter_queries_total",
    "QueryPathInfo calls made to a substituter while choosing one",
    ["store_id", "result"],  # hit, miss, timeout, error, unsupported
)

SUBSTITUTER_QUERY_DURATION = Histogram(
    "pynixd_substituter_query_duration_seconds",
    "Time one substituter took to answer whether it has a path",
    ["store_id"],
    buckets=(0.01, 0.05, 0.25, 1, 5, 15, 60),
)

SUBSTITUTIONS = Counter(
    "pynixd_substitutions_total",
    "Paths pynixd tried to fetch from a substituter",
    ["result"],  # ok, error, no_candidate
)

SUBSTITUTION_DURATION = Histogram(
    "pynixd_substitution_duration_seconds",
    "Time spent importing one substituted path, the query for a candidate included",
    buckets=(0.05, 0.25, 1, 5, 15, 60, 300, 900),
)

SUBSTITUTED_BYTES = Counter(
    "pynixd_substituted_bytes_total",
    "NAR bytes imported from substituters",
)

# The queue's own health record, and not `STORE_HEALTHY`. That one is about a
# build store the scheduler dispatches to; this is about a substituter the
# queue decides whether to wait for, and a store can be both.
SUBSTITUTER_WAITED_FOR = Gauge(
    "pynixd_substituter_waited_for",
    "Whether the queue still blocks a selection on this substituter (1 = yes)",
    ["store_id"],
)

# --- HTTP binary cache clients ---

BINARY_CACHE_NARINFO = Counter(
    "pynixd_binary_cache_narinfo_total",
    "`.narinfo` requests pynixd made to an upstream HTTP binary cache",
    ["store_id", "result"],  # hit, miss, error
)

HTTP_CACHE_UPSTREAM = Counter(
    "pynixd_http_cache_upstream_total",
    "Local HTTP cache misses answered from the upstream race",
    ["result"],  # stream
)

# --- Garbage collection ---
#
# Same shape as nixkube's GC series, so one dashboard reads both.

GC_CYCLES = Counter(
    "pynixd_gc_cycles_total",
    "Garbage collection passes this process has finished",
    ["result"],  # ok, error
)

GC_CYCLE_DURATION = Histogram(
    "pynixd_gc_cycle_duration_seconds",
    "Time one garbage collection pass took, planning included",
    buckets=(1, 5, 15, 30, 60, 120, 300, 600),
)

GC_PATHS_DELETED = Counter(
    "pynixd_gc_paths_deleted_total",
    "Store paths the collector has deleted",
)

GC_BYTES_FREED = Counter(
    "pynixd_gc_bytes_freed_total",
    "Bytes the collector reports it freed",
)

GC_TRIGGER_SKIPPED = Counter(
    "pynixd_gc_trigger_skipped_total",
    "Automatic GC triggers stood down by the cooldown",
    ["reason"],  # watermark, scheduled
)

# Zero until the first pass finishes, so an alert reads it as
# `== 0 or time() - it > N`. A timestamp that stops advancing is what says the
# collector is stuck; no size gauge says that on its own.
GC_LAST_SUCCESS = Gauge(
    "pynixd_gc_last_success_timestamp_seconds",
    "When a garbage collection pass last finished, in unix seconds",
)

# Consecutive liveness agreements the mirror filed. A divergence resets it
# to zero, so the cutover gate reads one number: how many hourly checks in
# a row agreed with Nix.
GC_LIVENESS_STREAK = Gauge(
    "pynixd_gc_liveness_streak_agreements",
    "Consecutive liveness checks where the mirror agreed with Nix",
)

# Stale temporary-roots files the mirror unlinked. Nix removes these on
# its own traces (`gc.cc:193`); the mirror does the same on every wake, so
# a dead owner's file stops seeding within minutes instead of lingering.
GC_TEMPROOTS_REAPED = Counter(
    "pynixd_gc_temproots_reaped_total",
    "Stale temporary roots files the mirror has unlinked",
)

# --- HTTP binary cache ---

# `route` is the pattern aiohttp matched, not the path the client asked for.
# `/{hash}.narinfo` is one series; the path itself would be one series per
# store path.
HTTP_REQUESTS = Counter(
    "pynixd_http_requests_total",
    "Requests served on the HTTP interface",
    ["route", "method", "status"],
)

HTTP_REQUEST_DURATION = Histogram(
    "pynixd_http_request_duration_seconds",
    "Time one HTTP request took, including streaming its body",
    ["route", "method"],
    buckets=(0.001, 0.01, 0.1, 0.5, 2.5, 10, 60, 300),
)

HTTP_NAR_BYTES_SENT = Counter(
    "pynixd_http_nar_bytes_sent_total",
    "NAR bytes written to binary cache clients",
)

HTTP_NAR_BYTES_RECEIVED = Counter(
    "pynixd_http_nar_bytes_received_total",
    "NAR bytes accepted from binary cache uploads",
)

# --- Build info ---

BUILD_INFO = Gauge(
    "pynixd_build_info",
    "Always 1. The version rides on the label, which is how a dashboard joins on it",
    ["version"],
)

try:
    BUILD_INFO.labels(version=version("pynixd")).set(1)
except PackageNotFoundError:
    # Running from a source tree with no installed distribution. The series is
    # absent, which is a truthful answer, and not a reason to fail an import.
    log.debug("build_info_version_unavailable")


class StorePoolCollector:
    """Pool depth per store, read when a scrape asks for it.

    A collector and not three gauges, because the pool changes these counts on
    every acquire and release. A gauge kept in step costs an update per
    operation for a number nobody reads between scrapes, and it goes wrong the
    first time a path returns a connection by another route.

    The set holds weak references. A pool belongs to its store, and a store
    that goes away must not be held alive by the metrics registry.
    """

    def __init__(self) -> None:
        self._pools: WeakSet[ConnectionPool] = WeakSet()

    def register(self, pool: ConnectionPool) -> None:
        self._pools.add(pool)

    def collect(self) -> Iterable[Metric]:
        """Yield in-flight, idle and total connections for every live pool."""
        in_flight = GaugeMetricFamily(
            "pynixd_store_pool_in_flight_connections",
            "Pooled connections handed out and not yet returned",
            labels=["store_id"],
        )
        idle = GaugeMetricFamily(
            "pynixd_store_pool_idle_connections",
            "Pooled connections open and available",
            labels=["store_id"],
        )
        total = GaugeMetricFamily(
            "pynixd_store_pool_connections",
            "Connections the pool holds, in flight and idle",
            labels=["store_id"],
        )
        # Summed per `store_id`, and not one sample per pool. A store that is
        # removed and added again leaves the old pool in this set until the
        # collector runs, and two samples carrying the same labels make
        # Prometheus reject the **whole scrape** -- every series of this
        # process goes dark, not only these three.
        waiting = GaugeMetricFamily(
            "pynixd_upstream_ops_waiting",
            "Operations sent to the store and not yet answered",
            labels=["store_id"],
        )
        silence = GaugeMetricFamily(
            "pynixd_upstream_silence_seconds_max",
            "Longest time a waiting operation has heard nothing from the store (#93)",
            labels=["store_id"],
        )
        slots = GaugeMetricFamily(
            "pynixd_store_pool_slots",
            "Connection slots of each pool lane (#97)",
            labels=["store_id", "lane"],
        )
        slot_waiters = GaugeMetricFamily(
            "pynixd_store_pool_slot_waiters",
            "Operations waiting for a free slot of a pool lane now (#97)",
            labels=["store_id", "lane"],
        )
        slot_wait_max = GaugeMetricFamily(
            "pynixd_store_pool_slot_wait_seconds_max",
            "Longest current wait for a slot of a pool lane (#97)",
            labels=["store_id", "lane"],
        )
        counts: dict[str, tuple[int, int, int]] = {}
        quiet: dict[str, tuple[int, float]] = {}
        lanes: dict[tuple[str, str], tuple[int, int, float]] = {}
        now = time.monotonic()
        for pool in self._pools:
            size, waiters, longest_wait = lanes.get((pool.store_id, pool.lane), (0, 0, 0.0))
            lanes[pool.store_id, pool.lane] = (
                # A removed store's pool may linger here; its size is no extra slot.
                max(size, pool.max_connections),
                waiters + len(pool.slot_waiters),
                max([longest_wait, *(now - since for since in pool.slot_waiters)]),
            )
            was = counts.get(pool.store_id, (0, 0, 0))
            counts[pool.store_id] = (
                was[0] + pool.active_connections,
                was[1] + len(pool.idle_conns),
                was[2] + len(pool.all_conns),
            )
            ops, longest = quiet.get(pool.store_id, (0, 0.0))
            for conn in pool.waiting_on_upstream():
                op = conn.current_op
                if op is None:
                    continue
                ops += 1
                longest = max(longest, now - max(op[1], conn.r.last_read_at))
            quiet[pool.store_id] = (ops, longest)
        for store_id, (busy, free, held) in counts.items():
            in_flight.add_metric([store_id], busy)
            idle.add_metric([store_id], free)
            total.add_metric([store_id], held)
        for store_id, (ops, longest) in quiet.items():
            waiting.add_metric([store_id], ops)
            silence.add_metric([store_id], longest)
        for (store_id, lane), (size, waiters, longest_wait) in lanes.items():
            slots.add_metric([store_id, lane], size)
            slot_waiters.add_metric([store_id, lane], waiters)
            slot_wait_max.add_metric([store_id, lane], longest_wait)
        yield slots
        yield slot_waiters
        yield slot_wait_max
        yield in_flight
        yield idle
        yield total
        yield waiting
        yield silence


STORE_POOLS = StorePoolCollector()
REGISTRY.register(STORE_POOLS)

BUILD_LIMIT_CHANGES = Counter(
    "pynixd_store_build_limit_changes_total",
    "Changes of a store's build limit, by direction and reason (#98)",
    ["store_id", "direction", "reason"],
)


class StorePressureCollector:
    """Each store's build limit and the host sample behind it. Issue #98.

    Reads `store.build_limit` and `store.monitor.health` at scrape, by
    attribute, because importing the store classes here is a cycle.
    """

    def __init__(self) -> None:
        self._stores: WeakSet[Any] = WeakSet()

    def register(self, store: object) -> None:
        self._stores.add(store)

    def collect(self) -> Iterable[Metric]:
        """Yield the limit, its reason, and the sample of each live store."""
        limit = GaugeMetricFamily("pynixd_store_build_limit", "Builds the store may run now (#98)", labels=["store_id"])
        reason = GaugeMetricFamily(
            "pynixd_store_build_limit_reason",
            "1 for the reason of the current build limit",
            labels=["store_id", "reason"],
        )
        pressure = GaugeMetricFamily(
            "pynixd_store_pressure_percent",
            "PSI avg10 of the store's host, machine-wide",
            labels=["store_id", "resource", "kind"],
        )
        available = GaugeMetricFamily(
            "pynixd_store_memory_available_bytes", "MemAvailable of the store's host", labels=["store_id"]
        )
        load = GaugeMetricFamily("pynixd_store_load1", "1-minute load average of the store's host", labels=["store_id"])
        age = GaugeMetricFamily(
            "pynixd_store_sample_age_seconds", "Seconds since the last host sample of the store", labels=["store_id"]
        )
        # One sample per store_id: a removed and re-added store can linger
        # here, and duplicate labels make Prometheus reject the whole scrape.
        latest: dict[str, Any] = {str(store.store_id): store for store in self._stores}
        now = time.monotonic()
        for store_id, store in latest.items():
            build_limit = store.build_limit
            limit.add_metric([store_id], build_limit.limit)
            reason.add_metric([store_id, str(build_limit.reason)], 1)
            if build_limit.sampled_at is None or store.monitor is None:
                continue
            age.add_metric([store_id], now - build_limit.sampled_at)
            health = store.monitor.health
            if health.psi is not None:
                for resource in ("cpu", "memory", "io"):
                    metric = getattr(health.psi, resource)
                    pressure.add_metric([store_id, resource, "some"], metric.some_avg10)
                    pressure.add_metric([store_id, resource, "full"], metric.full_avg10)
            if health.meminfo is not None and health.meminfo.mem_total:
                available.add_metric([store_id], health.meminfo.mem_available * 1024)
            if health.load1 is not None:
                load.add_metric([store_id], health.load1)
        yield limit
        yield reason
        yield pressure
        yield available
        yield load
        yield age


STORE_PRESSURE = StorePressureCollector()
REGISTRY.register(STORE_PRESSURE)


class StoreTrafficCollector:
    """Wire bytes per store, read when a scrape asks for it.

    A collector and not a counter, for the same reason as
    `StorePoolCollector`: nothing here updates a series per operation.
    The readers and writers count their own transport bytes with one
    int-add per transport read and flush, each pool folds its retired
    connections into two accumulators, and this sums live plus retired
    per `store_id` when asked. Directions match the NAR volume series:
    "in" is read from the peer, "out" is written to it.

    The third layer is the sticky below: the highest total reported per
    store, kept across pool replacement. A builder that re-registers is
    a new store object with a new pool whose counters start at zero;
    without this its lifetime numbers would reset on every reconnect,
    and no removal hook has to exist for that. Only stores with a live
    pool are emitted, so a store that is gone reads absent rather than
    frozen. The keys are store ids ever seen, which stays bounded like
    every other label in this file.
    """

    def __init__(self) -> None:
        self._pools: WeakSet[ConnectionPool] = WeakSet()
        self._sticky: dict[str, list[int]] = {}

    def register(self, pool: ConnectionPool) -> None:
        self._pools.add(pool)

    def collect(self) -> Iterable[Metric]:
        """Yield read and written wire bytes for every live pool's store."""
        # Summed per `store_id`, and not one sample per pool: the comment
        # on `StorePoolCollector.collect` says why duplicates are fatal.
        totals: dict[str, list[int]] = {}
        for pool in self._pools:
            read, written = pool.traffic_totals()
            was = totals.get(pool.store_id, [0, 0])
            totals[pool.store_id] = [was[0] + read, was[1] + written]
        family = CounterMetricFamily(
            "pynixd_store_wire_bytes",
            "Wire bytes moved with backend stores, all traffic on the connection",
            labels=["store_id", "direction"],
        )
        for store_id, (read, written) in totals.items():
            was = self._sticky.get(store_id, [0, 0])
            held = [max(was[0], read), max(was[1], written)]
            self._sticky[store_id] = held
            family.add_metric([store_id, "in"], held[0])
            family.add_metric([store_id, "out"], held[1])
        yield family


STORE_TRAFFIC = StoreTrafficCollector()
REGISTRY.register(STORE_TRAFFIC)


class StoreSpaceCollector:
    """How much room the store has, read when a scrape asks for it.

    A collector, and not a `Gauge` that something keeps up to date. The answer
    costs one `statvfs` and nothing else in pynixd needs it, so reading it on
    the scrape is both cheaper and fresher than a periodic write.

    **This is the file system that holds the store, and not the size of the
    store.** The two are not the same number and the difference is large:
    measured on this machine, `sum(narSize)` over `ValidPaths` answers 952 GB
    for a file system of 268 GB. NAR sizes add up without the deduplication
    and the hard links that the store on disk has, so that sum answers a
    question nobody asked.

    It is also too slow to serve. `select count(*), sum(narSize)` took 584 ms
    over 141,749 paths, against 0.010 ms for the `statvfs` here. A scrape
    every 15 s cannot pay half a second.
    """

    def collect(self) -> Iterable[Metric]:
        """Yield the two numbers, or nothing when the store is unreachable."""
        path = real_store_dir()
        try:
            stat = os.statvfs(path)
        except OSError:
            # A store directory that is gone or unreadable is not a reason to
            # fail a scrape: every other metric in the registry is still an
            # answer. Absent series read as absent on a dashboard, which is
            # what this is.
            log.warning("store_space_unreadable", path=path, exc_info=True)
            return

        yield GaugeMetricFamily(
            "pynixd_store_filesystem_size_bytes",
            "Total size of the file system that holds the store directory",
            value=stat.f_blocks * stat.f_frsize,
        )
        # `f_bavail` and not `f_bfree`: the reserved blocks are not available
        # to pynixd, and a dashboard that reads `f_bfree` says there is room
        # where a build would fail.
        yield GaugeMetricFamily(
            "pynixd_store_filesystem_available_bytes",
            "Space on that file system available to this user",
            value=stat.f_bavail * stat.f_frsize,
        )


REGISTRY.register(StoreSpaceCollector())


# --- appstarter ---

APPSTARTER_STATE = Path("var/appstarter/state.json")
"""Where `appstarter init` records what it put in the store, relative to the
store root. See `AppstarterCollector`."""


class AppstarterCollector:
    """Whether this pod runs what its deployment asks for, or the image's copy.

    `appstarter init` fetches the environment the deployment names, and when
    that fetch fails it seeds the store from the copy baked into the image
    instead. That is deliberate -- a pod that starts behind beats a pod that
    does not start -- and it is invisible from outside: the pod is Running and
    every probe passes either way. The two store paths in `state.json` are the
    only thing that says which happened.

    **Read from the file and on the scrape, not from the environment at
    import.** `appstarter run` puts both paths in the environment of the
    container it execs, and only some of these containers are started that way
    -- the builder runs its program directly. The store they share answers for
    all of them.

    **Absent rather than zero when the file cannot be read.** A process that
    cannot tell must not report "not degraded", which is the one answer that
    would hide exactly what this exists to show. Alert on `== 1`, and on
    `absent()` separately if the silence itself matters.

    **1 is a state, not an event: it cannot clear while the pod lives.**
    `appstarter init` is an `initContainer`, and a container restart does not
    re-run one -- a pod killed by its liveness probe comes back onto the same
    store and reports the same bit. Only a new pod, or a sandbox the kubelet
    recreates after a node restart, decides it again. So an alert on `== 1`
    needs no `for:` beyond a scrape or two, will not flap, and stays firing
    until somebody replaces the pod.

    The paths are deliberately not labels: this module's rule against store
    paths holds, the bit is what an alert needs, and the paths are in the
    pod's log and in `APPSTARTER_RUNNING_STORE_PATH`.

    The store root is the parent of the *real* store directory, so a chroot
    store finds its own state file rather than the host's.
    """

    def collect(self) -> Iterable[Metric]:
        """Yield the one bit, or nothing when no state was recorded."""
        path = Path(real_store_dir()).parent / APPSTARTER_STATE
        try:
            recorded = json.loads(path.read_text())
            wanted, running = recorded["wanted"], recorded["running"]
        except (OSError, ValueError, KeyError, TypeError):
            # Deliberately silent. `REGISTRY.register` calls `collect()` once
            # to check for a duplicate metric name, so anything written here
            # lands on stdout during import -- which broke the JSON a probe in
            # this suite reads out of a bare interpreter. A scrape every 15
            # seconds would then repeat it for ever. The absent series is the
            # signal.
            return
        yield GaugeMetricFamily(
            "pynixd_appstarter_degraded",
            "1 when this pod runs the environment baked into its image instead of the one the deployment asks for",
            value=float(wanted != running),
        )


REGISTRY.register(AppstarterCollector())


def get_metrics_response() -> tuple[bytes, str]:
    """Generate a Prometheus-formatted metrics response."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


# --- Public readers ---
#
# Cumulative counters exist only here: no other structure remembers lifetime
# totals, so these functions are the one way to read them in-process (the
# `pynixd state` collector uses them). Point-in-time state (queue depth,
# sessions, store health) reads its own sources instead: it costs no
# recording, cannot drift, and stays answered when `metrics_enabled` is
# false. These readers follow that switch -- a deployment that records
# nothing reports zeros here.


def _sample(name: str, labels: dict[str, str] | None = None) -> float:
    """One sample from the default registry, or 0 when never recorded."""
    value = REGISTRY.get_sample_value(name, labels or {})
    return value if value is not None else 0.0


def nar_bytes_received_total() -> int:
    """NAR bytes taken from clients, both multi-path and single-path loops."""
    return int(
        _sample("pynixd_nar_forward_bytes_total") + _sample("pynixd_nar_add_bytes_total"),
    )


def nar_paths_received_total() -> int:
    """Store paths taken from clients, both multi-path and single-path loops."""
    return int(
        _sample("pynixd_nar_forward_paths_total") + _sample("pynixd_nar_add_paths_total"),
    )


def nar_bytes_served_total() -> int:
    """NAR bytes served to clients from `NarFromPath`."""
    return int(_sample("pynixd_nar_serve_bytes_total"))


def nar_paths_served_total() -> int:
    """Store paths served to clients from `NarFromPath`."""
    return int(_sample("pynixd_nar_serve_paths_total"))


def builds_completed_by_status() -> dict[str, int]:
    """Finished builds by outcome. Keys come from the metric's labels."""
    return {
        status: int(_sample("pynixd_builds_completed_total", {"status": status})) for status in ("success", "failure")
    }


def sessions_accepted_by_transport() -> dict[str, int]:
    """Accepted client sessions by transport. Keys come from the metric's labels."""
    return {
        transport: int(_sample("pynixd_daemon_sessions_accepted_total", {"transport": transport}))
        for transport in ("ssh", "unix", "reverse")
    }


def store_wire_bytes(store_ids: Iterable[str]) -> dict[str, dict[str, int]]:
    """Per-store wire bytes by direction, for the stores named.

    A store with no traffic reads zeros, like every other reader here.
    """
    wired = {}
    for store_id in store_ids:
        wired[store_id] = {
            "wire_bytes_in": int(_sample("pynixd_store_wire_bytes_total", {"store_id": store_id, "direction": "in"})),
            "wire_bytes_out": int(_sample("pynixd_store_wire_bytes_total", {"store_id": store_id, "direction": "out"})),
        }
    return wired


def store_transfers(store_ids: Iterable[str]) -> dict[str, dict[str, int]]:
    """Per-store NAR movement by direction, for the stores named.

    A store with no traffic reads zeros, like every other reader here.
    """
    transfers = {}
    for store_id in store_ids:
        transfers[store_id] = {
            "bytes_in": int(
                _sample("pynixd_store_transfer_nar_bytes_total", {"store_id": store_id, "direction": "in"})
            ),
            "bytes_out": int(
                _sample("pynixd_store_transfer_nar_bytes_total", {"store_id": store_id, "direction": "out"})
            ),
            "paths_in": int(_sample("pynixd_store_transfer_paths_total", {"store_id": store_id, "direction": "in"})),
            "paths_out": int(_sample("pynixd_store_transfer_paths_total", {"store_id": store_id, "direction": "out"})),
        }
    return transfers
