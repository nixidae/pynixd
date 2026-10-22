"""DaemonStore — talks to a Nix daemon over the wire protocol."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import math
import time
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Final

import anyio
import structlog

from nix_daemon_protocol import (
    IsValidPathRequest,
    QueryDerivationOutputMapRequest,
    QueryPathFromHashPartRequest,
    QueryPathInfoRequest,
    QueryReferrersRequest,
    QueryValidPathsRequest,
)
from nix_daemon_protocol.build_result import DUMMY_DRV_HASH
from nix_daemon_protocol.protocol import ResultType
from nix_daemon_protocol.store_dir import store_prefix
from nix_daemon_protocol.unkeyed_realisation import UnkeyedRealisation
from nix_daemon_protocol.wire_ops import WireRequest

from .. import metrics, wire
from .._lazy import ssh_errors
from ..config import PressureSettings
from ..constants import FEATURE_REALISATION_WITH_PATH
from ..drv_hash import output_hashes
from ..exceptions import BackendError
from ..monitor import ResourceMonitor
from ..pressure import BuildLimit
from ..serde import (
    AddToStoreRequest,
    BasicDerivation,
    BuildDerivationRequest,
    BuildMode,
    BuildResultStatus,
    ContentAddress,
    DerivationOutput,
    DrvOutput,
    QueryRealisationResponse,
    Realisation,
    StorePath,
)
from ..serde.context import WriteContext
from ..store_layout import StoreLayout
from ..system_features import BUILTIN_SYSTEM, KNOWN_FEATURES, PROBE_SYSTEMS
from ..utils import random_nix32_hash
from .base import Store
from .pool import ConnectionPool

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Set as AbstractSet
    from contextlib import AbstractAsyncContextManager

    from ..config import StoreSpecBase
    from ..connection import Connection
    from ..drv_parser import Derivation
    from ..psi import CpuUtil, MemInfo, SystemHealth
    from ..serde import SetOptionsRequest
    from ..store_path import StorePath

log = structlog.get_logger(__name__)

_BOUNDED_READS = (
    IsValidPathRequest,
    QueryPathInfoRequest,
    QueryValidPathsRequest,
    QueryReferrersRequest,
    QueryPathFromHashPartRequest,
    QueryDerivationOutputMapRequest,
)
"""Reads of the store database that take no lock, so `read_timeout` bounds
them. A GC, a build and anything behind the GC lock stay unbounded: the
daemon can be silent for a long time and still be right (#93)."""

_CB_THRESHOLD: int = 3
_CB_MAX_COOLDOWN: float = 300.0

SSHD_DEFAULT_MAX_SESSIONS: Final[int] = 10
"""`MaxSessions` in `sshd_config(5)`, which a client cannot read.

It counts "open shell, login or subsystem sessions permitted per network
connection", and the same page says setting it to 0 "will prevent all shell,
login and subsystem sessions **while still permitting forwarding**". So a
forwarded channel does not count against it and a session channel does. That
is why `SSHSubprocessStore` and `SSHSocketStore` take different bounds."""

PROBE_CONCURRENCY: Final[int] = 5
"""Probes in flight at once, over every system and every feature.

Each probe is one build, and over SSH each build takes a channel. `sshd`
permits ten sessions by default, and `nix` assumes the same ten for its own
channels, so half of that leaves room for a build running beside a probe.

Measured on issue #15: eighteen probes at once -- two systems by nine
features -- failed eight of them with `OPEN_REQUEST_SESSION_FAILED`, which
is eighteen minus the ten `sshd` allows. The store then sat in a 300 second
cooldown, so the failure was not transient from the caller's side. A client
cannot read `MaxSessions`, so this is a constant and not a negotiation."""

# **The asyncssh half is read at the moment of the `except`, and not here.**
# A `try: import asyncssh` at module level loads the library for every store,
# and a Unix-socket daemon opens no SSH connection. `ssh_errors` answers with
# an empty tuple until something imports asyncssh, which is exact: no error of
# that library can be in flight before the library is there. Issue #30.
_TRANSPORT_ERRORS: tuple[type[BaseException], ...] = (
    ConnectionError,
    EOFError,
    OSError,
    TimeoutError,
)


_BUILDER_OUTPUT_LINES: Final[int] = 6
"""How many lines of a refused probe's build log travel with its reason.

Enough to carry what the builder said and not enough to fill a log line. The
build log of a probe is six lines of `nix` chatter at most, because the
derivation is one `echo`."""


PROBE_SYSTEM_SCRIPT: Final[str] = (
    'echo {system} > "$out"; test -e "$out" && echo "probe wrote $out" >&2 || echo "probe wrote no $out" >&2'
)
"""The shell a system probe runs, and the one line it says about the result.

**A silent builder tells a refusal nothing.** `echo <system> > $out` alone
exits 0 and prints nothing, so a store that answered `failed to produce output
path` carried no evidence of where the builder had written. The report names
the path the builder saw, which a reader compares against the path Nix names
in that message. Issue #47."""


_BUILD_LOG_RESULTS: Final[frozenset[int]] = frozenset(
    {ResultType.BUILD_LOG_LINE, ResultType.POST_BUILD_LOG_LINE},
)


def _builder_lines(logs: Any) -> list[str]:
    """Every line the builder wrote, taken from the log stream of a response.

    **Build output is an activity result, not a log line.** The daemon sends
    it as `STDERR_RESULT` with type `BUILD_LOG_LINE`, and the text sits in
    `fields[0].valstr`. `LogResult` carries no `text` attribute at all, so
    reading `message.text` collected Nix's own progress chatter and dropped
    every line the builder wrote. Measured against nix 2.34.8: a builder that
    printed one line to stderr and produced no output path answered with
    exactly one `BUILD_LOG_LINE` result holding that line. Issue #47.
    """
    lines: list[str] = []
    for message in getattr(logs, "messages", []):
        if getattr(message, "result_type", None) not in _BUILD_LOG_RESULTS:
            continue
        lines.extend(field.valstr.rstrip("\n") for field in message.fields if field.valstr and field.valstr.strip())
    return lines


def _refusal_with_builder_output(resp: Any) -> str:
    """Why a store refused a probe, with what its builder said.

    **`BuildResult.error_msg` alone does not say why a build produced no
    output.** On a GitHub runner the x86_64-linux probe answered `builder for
    '...-probe-system-x86_64-linux.drv' failed to produce output path for
    output 'out'`, which states the symptom and nothing else: the builder ran,
    so it is not a missing `/bin/sh` and not a permission refusal, and there
    the trail ended. pynixd issue #47.

    The daemon already sent the builder's output, and the response buffers it.
    Nix's progress lines are the fallback, because a store that refuses before
    it starts a builder says why there and only there.
    """
    error_msg = resp.result.error_msg or f"status {resp.result.status}"
    lines = _builder_lines(resp.logs) or [
        text.rstrip("\n")
        for message in getattr(resp.logs, "messages", [])
        if (text := getattr(message, "text", "")).strip()
    ]
    if not lines:
        return error_msg
    tail = " | ".join(lines[-_BUILDER_OUTPUT_LINES:])
    return f"{error_msg} [builder said: {tail}]"


class ProbeState(IntEnum):
    """Tracks the protocol probing state of a DaemonStore connection."""

    NOT_PROBED = 0
    PROBING = 1
    PROBED = 2


def answer_in_both_shapes(response: QueryRealisationResponse, request: Any) -> QueryRealisationResponse:
    """Fill whichever shape of a QueryRealisation answer the store left empty."""
    if response.realisations and response.realisation is None:
        first = response.realisations[0]
        return response.model_copy(
            update={
                "present": 1,
                "realisation": UnkeyedRealisation(
                    out_path=StorePath(str(first.out_path)).with_store_prefix(), signatures=set(first.signatures)
                ),
            },
        )
    if response.realisation is not None and not response.realisations:
        drv_output = getattr(request, "drv_output", None)
        keyed = getattr(request, "keyed_drv_output", None)
        if not drv_output and keyed is not None:
            drv_output = DrvOutput(drv_hash=DUMMY_DRV_HASH, output_name=keyed.output_name)
        if drv_output:
            return response.model_copy(
                update={
                    "realisations": [
                        Realisation(
                            id=DrvOutput(str(drv_output)),
                            out_path=StorePath(str(response.realisation.out_path).removeprefix(store_prefix())),
                            signatures=sorted(response.realisation.signatures),
                        )
                    ],
                },
            )
    return response


class DaemonStore(Store):
    """Store that communicates with a Nix daemon via the wire protocol.

    Adds connection pooling, protocol probing, feature discovery,
    circuit breaking, and reconnect logic on top of the Store ABC.
    """

    MAX_CONNECTIONS: int = 64
    """How many connections the pool may hold at once.

    A connection to a Unix-socket daemon costs a process on this machine and
    nothing on a far one, so 64 is a bound on memory rather than a limit
    somebody else imposes. A subclass whose transport has its own ceiling
    lowers this; `SSHSubprocessStore` does. Issue #43."""

    TRANSFER_CONNECTIONS: int | None = None
    """How many of `MAX_CONNECTIONS` the transfer pool takes; the build pool
    takes the rest. `None` is a quarter, at least 2.

    Two pools, because a build holds its connection for the whole build: on
    an SSH builder with 8 slots, 8 running builds held every read to it until
    `read_timeout`. Issue #97."""

    def _pool_sizes(self, spec: StoreSpecBase) -> tuple[int, int]:
        """`(build, transfer)` slots: the spec's numbers, or this class's."""
        total = max(2, spec.max_connections or self.MAX_CONNECTIONS)
        transfer = spec.transfer_connections or self.TRANSFER_CONNECTIONS or max(2, total // 4)
        transfer = max(1, min(transfer, total - 1))
        return total - transfer, transfer

    def __init__(self, spec: StoreSpecBase) -> None:
        """Initialize daemon store with pool, probing state, circuit breaker, and reconnect loop."""
        super().__init__(spec)
        self.store_path = spec.store_path
        self.layout: StoreLayout = spec.layout()
        """The three directories of this store. Issue nixidae/nixidae#215.

        A store that is not local has no layout of its own, so it takes the
        chroot layout of its root. Nothing reads the state directory of such
        a store: the temporary roots and the SQLite fast paths belong to the
        local store that pynixd serves.
        """
        self.scheduleable = spec.scheduleable
        self.priority = spec.priority
        self.score_penalty = spec.score_penalty
        self.gc_enabled = spec.gc_enabled
        self.gc_max_age = spec.gc_max_age
        self.gc_target_usage = spec.gc_target_usage
        self.gc_allow_execute = spec.gc_allow_execute
        self.no_schedule = spec.no_schedule
        self.idle_ttl = spec.idle_ttl
        self.max_lifetime = spec.max_lifetime
        self.read_timeout = spec.read_timeout
        self.version: int = wire.PROTOCOL_VERSION
        self.nix_version: str = ""
        self.conn_counter = 0

        fm = spec._effective_feature_matrix()
        self._feature_matrix: dict[str, set[str]] | None = fm
        if spec.probe is not None:
            self._probe = spec.probe
        else:
            self._probe = fm is None

        build_slots, transfer_slots = self._pool_sizes(spec)
        self.build_pool, self.transfer_pool = (
            ConnectionPool(
                store_id=self.store_id,
                factory=self._create_conn_with_counter,
                idle_ttl=self.idle_ttl,
                max_lifetime=self.max_lifetime,
                max_connections=slots,
                on_connection_created=self._on_connection_created,
                on_pool_empty=self._on_a_pool_empty,
                lane=lane,
                reuse=lane != "build",
            )
            for lane, slots in (("build", build_slots), ("transfer", transfer_slots))
        )

        self.pressure_settings: PressureSettings = spec.pressure or (
            spec.settings.pressure if spec.settings else PressureSettings()
        )
        self.build_limit = BuildLimit(str(self.store_id), build_slots, self.pressure_settings)
        self.on_capacity: Callable[[], None] | None = None
        """Called when the build limit rises; the scheduler sets it to its trigger."""
        metrics.STORE_PRESSURE.register(self)
        self.monitor: ResourceMonitor | None = None
        self.consecutive_failures: int = 0
        self.cooldown_until: float = 0.0
        self.disk_full_until: float = 0.0
        """Until when a build failed here for want of disk; read when no disk sample is known."""
        self.sample_listeners: set[Callable[[SystemHealth], None]] = set()
        """Each hears every sample of this store's host: a `PynixdHostHealthStream` session. nixidae/nixidae#1016."""
        self._features: set[str] = set()
        self.probe_state: ProbeState = ProbeState.NOT_PROBED
        self._probe_event: anyio.Event = anyio.Event()
        self.draining: bool = False
        self.reconnect_enabled = spec.reconnect
        self.reconnect_min_delay = spec.reconnect_min_delay
        self.reconnect_max_delay = spec.reconnect_max_delay
        self._reconnect_task: asyncio.Task[None] | None = None
        self._reconnect_trigger = anyio.Event()
        self._reconnect_delay = self.reconnect_min_delay
        self._on_reconnect: Callable[[], Awaitable[None]] | None = None

    # ── Daemon metadata ─────────────────────────────────────────────

    @property
    def features(self) -> AbstractSet[str]:
        """Set of daemon-advertised protocol features."""
        return self._features

    @property
    def feature_matrix(self) -> dict[str, set[str]]:
        """Mapping of system → feature-set probed from the daemon."""
        if self._feature_matrix is not None:
            return self._feature_matrix
        return {}

    @feature_matrix.setter
    def feature_matrix(self, value: dict[str, set[str]]) -> None:
        """Override the probed feature matrix."""
        self._feature_matrix = value

    @property
    def systems(self) -> set[str] | None:
        """Set of systems this store can build for, or None if unrestricted."""
        fm = self._feature_matrix
        if fm is not None:
            return set(fm.keys()) if fm else None
        return None

    @systems.setter
    def systems(self, value: set[str] | None) -> None:
        """Override the set of systems this store can build for."""
        if self._feature_matrix is not None:
            if value is None:
                self._feature_matrix = {}
                return
            old = self._feature_matrix
            self._feature_matrix = {s: old.get(s, set()) for s in value}
        elif value is not None:
            self._feature_matrix = {s: set() for s in value}

    def supports_system(self, system: str) -> bool:
        """Check if this store can build for the given system."""
        fm = self._feature_matrix
        if fm is None:
            return True
        return system in fm

    def supports_derivation(self, system: str, features: set[str] | None = None) -> bool:
        fm = self._feature_matrix
        if fm is not None:
            sys_features = fm.get(system)
            if sys_features is None and system == BUILTIN_SYSTEM and fm:
                # Every machine takes `builtin`: `Machine::systemSupported`
                # at `machines.cc:42`, and `derivation-building-goal.cc:421`
                # for the local one. The daemon itself runs the builder.
                sys_features = set().union(*fm.values())
            if sys_features is None:
                return False
            if not features:
                return True
            return features.issubset(sys_features)
        if not features:
            return True
        _platform_specific = frozenset({"kvm", "apple-virt"})
        return features.isdisjoint(_platform_specific)

    # ── Resource metrics ────────────────────────────────────────────

    def on_pressure_sample(self, health: SystemHealth) -> None:
        """Move the build limit for one sample of this store's host, and pass the sample on."""
        if self.build_limit.observe(health, self.build_pool.active_connections, time.monotonic()):
            if self.on_capacity is not None:
                self.on_capacity()
        for listener in tuple(self.sample_listeners):
            listener(health)

    @property
    def pressure(self) -> float | None:
        return None

    @property
    def meminfo(self) -> MemInfo | None:
        return None

    @property
    def cpu_util(self) -> CpuUtil | None:
        return None

    # ── Connection pool ─────────────────────────────────────────────

    @property
    def pools(self) -> tuple[ConnectionPool, ConnectionPool]:
        return self.build_pool, self.transfer_pool

    @property
    def in_flight(self) -> int:
        """Number of currently active connections."""
        return sum(pool.active_connections for pool in self.pools)

    @property
    def pool_stats(self) -> str:
        """Human-readable pool statistics string."""
        return " ".join(f"{pool.lane}: {pool.stats}" for pool in self.pools)

    def build_conn(self, options: SetOptionsRequest | None = None) -> AbstractAsyncContextManager[Connection]:
        """Acquire a connection for build operations."""
        return self.build_pool.acquire("build", options)

    def transfer_conn(self, options: SetOptionsRequest | None = None) -> AbstractAsyncContextManager[Connection]:
        """Acquire a connection for transfer operations."""
        return self.transfer_pool.acquire("transfer", options)

    async def retire_idle_connections(self) -> int:
        """Close each pooled connection that nobody uses, and release its roots."""
        return sum([await pool.retire_idle() for pool in self.pools])

    async def _on_a_pool_empty(self) -> None:
        """`_on_pool_empty`, once both pools are empty: they share one transport."""
        if all(not (pool.active_connections or pool.idle_conns or pool.all_conns) for pool in self.pools):
            await self._on_pool_empty()

    async def add_text_to_store(self, name: str, text: str, references: AbstractSet[str]) -> str:
        """Put a text file in the store of the daemon, and answer the path it took.

        `AddToStore` with `text:sha256` is what `Store::addTextToStore` of Nix
        sends. The ingestion method is flat, so the framed body is the file
        itself and not a NAR, at `daemon.cc:436`.

        The daemon computes the path. pynixd could compute it as well, and
        then two implementations of one formula would have to agree; the
        answer of the daemon is the path that the daemon will read.
        """
        request = AddToStoreRequest(
            path_name=name,
            cam=ContentAddress("text:sha256"),
            references={StorePath(path=str(ref)) for ref in references},
            repair=0,
        )
        await self.probe()
        async with self.transfer_conn() as conn:
            response = await conn.call_with_payload(request, text.encode())
        info = response.info
        self.add_path_info(info)
        return str(info.path)

    async def _create_conn_with_counter(self) -> Connection:
        self.conn_counter += 1
        return await self.create_conn()

    # ── Circuit breaker ─────────────────────────────────────────────

    @property
    def is_healthy(self) -> bool:
        """Whether the store is not in cooldown."""
        return time.monotonic() >= self.cooldown_until

    def record_success(self) -> None:
        """Reset circuit breaker after a successful operation."""
        if self.consecutive_failures > 0:
            log.info("store_recovered", store_id=self.store_id, consecutive_failures=self.consecutive_failures)
        self.consecutive_failures = 0
        self.cooldown_until = 0.0

    def record_failure(self) -> None:
        """Record a failure and optionally enter circuit-breaker cooldown."""
        self.consecutive_failures += 1
        if self.consecutive_failures >= _CB_THRESHOLD:
            cooldown = min(30 * 2 ** (self.consecutive_failures - _CB_THRESHOLD), _CB_MAX_COOLDOWN)
            self.cooldown_until = time.monotonic() + cooldown
            log.warning(
                "store_cooldown",
                store_id=self.store_id,
                consecutive_failures=self.consecutive_failures,
                cooldown=cooldown,
            )
        self._schedule_reconnect()

    # ── Reconnect loop ──────────────────────────────────────────────

    def _schedule_reconnect(self) -> None:
        if self._reconnect_task is not None and not self._reconnect_task.done():
            self._reconnect_trigger.set()

    def _start_reconnect_loop(self) -> None:
        if not self.reconnect_enabled:
            return
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _stop_reconnect_loop(self) -> None:
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            with contextlib.suppress(anyio.get_cancelled_exc_class()):
                await self._reconnect_task
            self._reconnect_task = None

    async def _do_reconnect(self) -> None:
        for pool in self.pools:
            await pool.close()
        self.probe_state = ProbeState.NOT_PROBED
        self._probe_event = anyio.Event()
        await self.probe()

    async def _reconnect_loop(self) -> None:
        while True:
            await self._reconnect_trigger.wait()
            self._reconnect_trigger = anyio.Event()

            while True:
                delay = self._reconnect_delay
                await anyio.sleep(delay)

                try:
                    await self._do_reconnect()
                except _TRANSPORT_ERRORS + ssh_errors():
                    self._reconnect_delay = min(self._reconnect_delay * 2, self.reconnect_max_delay)
                    log.warning("store_reconnect_failed", store_id=self.store_id, next_retry=self._reconnect_delay)
                    continue
                except anyio.get_cancelled_exc_class():
                    raise
                except Exception:
                    log.exception("store_reconnect_unexpected_error", store_id=self.store_id)
                    self._reconnect_delay = min(self._reconnect_delay * 2, self.reconnect_max_delay)
                    continue
                else:
                    self._reconnect_delay = self.reconnect_min_delay
                    self.record_success()
                    log.info("store_reconnected", store_id=self.store_id)
                    if self._on_reconnect:
                        await self._on_reconnect()
                    break

    # ── Lifecycle ───────────────────────────────────────────────────

    async def start(self, sync_paths: bool = True) -> None:
        if self._started:
            return
        await self.probe()
        if self.monitor:
            self.monitor.start()
        self._started = True
        self._start_reconnect_loop()

    async def close(self) -> None:
        await self._stop_reconnect_loop()
        if self.monitor is not None:
            # A monitor that outlives its store asks a closed store for ever.
            await self.monitor.stop()
            self.monitor = None
        for pool in self.pools:
            await pool.close()

    # ── Execution ───────────────────────────────────────────────────

    async def call(self, request, client=None, suppress_last=False, raise_on_error=False, skip_probe=False):
        """Send a wire request over a pooled connection."""
        if not skip_probe:
            await self.probe()

        is_build = not request.forward if isinstance(request, WireRequest) else request.is_build
        pool = self.build_conn if is_build else self.transfer_conn
        options = client.options if client is not None else None
        bound = self.read_timeout if isinstance(request, _BOUNDED_READS) else math.inf

        try:
            with anyio.fail_after(bound):
                async with pool(options) as conn:
                    return await conn.call(
                        request, client=client, suppress_last=suppress_last, raise_on_error=raise_on_error
                    )
        except TimeoutError:
            # The connection left through an exception, so the pool drops it.
            metrics.UPSTREAM_READ_TIMEOUTS.labels(store_id=self.store_id).inc()
            raise BackendError(
                f"pynixd: store {self.store_id} did not answer {type(request).__name__} within {bound:g} s"
            ) from None
        except _TRANSPORT_ERRORS + ssh_errors():
            self.record_failure()
            raise

    async def execute(self, request, client=None, suppress_last=False, skip_probe=False, mark=True):
        """Execute an operation, dispatching to a registered executor or falling back to call().

        `mark` is accepted and ignored here: only `LocalDBStore` records
        reference observations, so planning queries pass `mark=False`
        uniformly instead of branching on the store class. Issue #65.
        """
        if not skip_probe:
            await self.probe()

        if method_name := self._executors.get(request.op):
            fn = getattr(self, method_name)
            if result := await fn(request, client=client, suppress_last=suppress_last):
                return result

        return await self.call(request, client=client, suppress_last=suppress_last)

    # ── Probing ─────────────────────────────────────────────────────

    async def _on_pool_empty(self) -> None:
        """Release whatever this store holds under the pool. Nothing, by default.

        A store whose connections ride on a transport of its own overrides
        this. `SSHStore` does: its `asyncssh` connection outlives every
        channel, and holding it open keeps a builder that starts on demand
        awake for as long as pynixd runs.
        """
        return None

    def _on_connection_created(self, conn: Connection) -> None:
        self.version = conn.version
        self.nix_version = conn.nix_version
        self._features = conn.features

        if self._feature_matrix is None and any(f.startswith("feature_matrix:") for f in conn.features):
            fm: dict[str, set[str]] = {}
            for f in conn.features:
                if not f.startswith("feature_matrix:"):
                    continue
                parts = f.split(":")
                if len(parts) == 2:
                    system = parts[1]
                    if system not in fm:
                        fm[system] = set()
                elif len(parts) == 3:
                    system, feat = parts[1], parts[2]
                    if system not in fm:
                        fm[system] = set()
                    fm[system].add(feat)

            if fm:
                self._feature_matrix = fm
                self.probe_state = ProbeState.PROBED
                self._probe_event.set()
                log.info(
                    "store_probed_via_handshake",
                    store_id=self.store_id,
                    systems=sorted(fm.keys()),
                    feature_matrix={k: sorted(v) for k, v in fm.items()},
                )

    async def probe(self) -> None:
        """Ensure the store has been probed, performing probing if needed."""
        if self.probe_state == ProbeState.PROBED:
            return
        if self.probe_state == ProbeState.PROBING:
            await self._probe_event.wait()
            return

        self.probe_state = ProbeState.PROBING

        async with self.transfer_pool.acquire("probe"):
            pass

        if self.probe_state == ProbeState.PROBED:
            log.debug("probe_skipped_probed_via_handshake", store_id=self.store_id)
            return

        if not self._probe:
            self.probe_state = ProbeState.PROBED
            self._probe_event.set()
            return

        existing_systems = set(self._feature_matrix.keys()) if self._feature_matrix else set()
        existing_features: set[str] = set()
        if self._feature_matrix:
            for feats in self._feature_matrix.values():
                existing_features.update(feats)

        candidate_systems = existing_systems or set(PROBE_SYSTEMS)
        candidate_features = existing_features or set(KNOWN_FEATURES)
        # Before the builds: a probe that waits for build slots is silent
        # until `systems_probed` otherwise (#58).
        log.info(
            "store_probing",
            store_id=self.store_id,
            systems=sorted(candidate_systems),
            features=sorted(candidate_features),
        )

        # One limiter for both halves, because they run one after the other.
        # Built here and not in `__init__`: `anyio.CapacityLimiter` reads the
        # running backend with sniffio at construction, and `__init__` is sync.
        limiter = anyio.CapacityLimiter(PROBE_CONCURRENCY)

        systems = await self._probe_systems(candidate_systems, limiter)
        self._feature_matrix = await self._probe_features(systems, candidate_features, limiter)

        log.info(
            "store_probed",
            store_id=self.store_id,
            systems=sorted(self._feature_matrix.keys()) if self._feature_matrix else [],
            feature_matrix={k: sorted(v) for k, v in (self._feature_matrix or {}).items()},
        )

        self.probe_state = ProbeState.PROBED
        self._probe_event.set()

    async def _probe_systems(self, candidates: set[str], limiter: anyio.CapacityLimiter) -> set[str]:
        # An anyio task group hands back no task object, so each child records
        # its answer at its own index. `candidates` becomes a list first,
        # because the zip below needs the same order twice.
        ordered = sorted(candidates)
        supported: list[bool] = [False] * len(ordered)

        refusals: list[str] = [""] * len(ordered)

        async def probe_system(index: int, system: str) -> None:
            async with limiter:
                _, ok, reason = await self._send_probe(
                    f"probe-system-{system}",
                    system,
                    "",
                    ["-c", PROBE_SYSTEM_SCRIPT.format(system=system)],
                )
            supported[index] = ok
            refusals[index] = reason

        async with anyio.create_task_group() as tg:
            for index, system in enumerate(ordered):
                tg.start_soon(probe_system, index, system)

        systems = {system for system, ok in zip(ordered, supported, strict=True) if ok}
        log.info("systems_probed", store_id=self.store_id, systems=sorted(systems))
        if not systems:
            # **A warning, and the reasons with it.** A store with no system
            # builds nothing, and the scheduler then answers every request
            # with `No compatible store for <system>`, which names the system
            # the client wanted and not the store that could not take it.
            # The reasons were at debug level, so a CI run reported 35 of that
            # message and nothing about its cause. Issue #47.
            log.warning(
                "store_probed_no_system",
                store_id=self.store_id,
                candidates=ordered,
                refusals={system: reason for system, reason in zip(ordered, refusals, strict=True) if reason},
            )
        return systems

    async def _probe_features(
        self,
        systems: set[str],
        system_features: set[str],
        limiter: anyio.CapacityLimiter,
    ) -> dict[str, set[str]]:
        to_probe = (system_features or set()) | KNOWN_FEATURES
        probes: list[Callable[[], Awaitable[tuple[str, bool, str]]]] = []
        probe_keys: list[tuple[str, str]] = []
        for system in systems:
            for feature in to_probe:
                if feature == "kvm":
                    args = [
                        "-c",
                        "test -w /dev/kvm && echo kvm > $out || { echo 'kvm: /dev/kvm not writable' >&2; exit 1; }",
                    ]
                else:
                    args = ["-c", f"echo {feature} > $out"]

                extra_env: dict[str, str] = {
                    "requiredSystemFeatures": feature,
                    "NIXBUILDNET_MIN_CPU": "1",
                    "NIXBUILDNET_MAX_CPU": "1",
                    "NIXBUILDNET_MIN_MEM": "128",
                    "NIXBUILDNET_MAX_MEM": "128",
                }
                probe_keys.append((system, feature))
                # A partial, and not a coroutine object: `start_soon` takes a
                # callable and its arguments, and it calls that callable in the
                # child task.
                probes.append(
                    functools.partial(
                        self._send_probe,
                        f"probe-feature-{feature}",
                        system,
                        feature,
                        args,
                        extra_env,
                    ),
                )

        supported: list[bool] = [False] * len(probes)

        async def run_probe(index: int, probe: Callable[[], Awaitable[tuple[str, bool, str]]]) -> None:
            async with limiter:
                _, ok, _reason = await probe()
            supported[index] = ok

        async with anyio.create_task_group() as tg:
            for index, probe in enumerate(probes):
                tg.start_soon(run_probe, index, probe)

        feature_matrix: dict[str, set[str]] = {system: set() for system in systems}
        for (system, feature), ok in zip(probe_keys, supported, strict=True):
            if ok:
                feature_matrix[system].add(feature)

        self._feature_matrix = feature_matrix
        log.info(
            "features_probed",
            store_id=self.store_id,
            feature_matrix={key: sorted(value) for key, value in feature_matrix.items()},
        )
        return feature_matrix

    async def _send_probe(
        self,
        name: str,
        system: str,
        required_features: str,
        args: list[str],
        extra_env: dict[str, str] | None = None,
    ) -> tuple[str, bool, str]:
        """The probe's name, whether the store took it, and why it did not.

        The third part is empty when the store took it. It carries the
        daemon's own message otherwise, so that a caller can say why a store
        probed to nothing without a reader turning on debug logging.
        """
        drv_hash = random_nix32_hash()
        out_path = f"{store_prefix()}{drv_hash}-{name}"
        drv_path = StorePath(path=f"{store_prefix()}{drv_hash}-{name}.drv")

        env: dict[str, str] = {
            "builder": "/bin/sh",
            "name": name,
            "out": out_path,
            "system": system,
            "hash": drv_hash,
        }
        if required_features:
            env["requiredSystemFeatures"] = required_features
        if extra_env:
            env.update(extra_env)

        request = BuildDerivationRequest(
            drv_path=drv_path,
            derivation=BasicDerivation(
                outputs={"out": DerivationOutput(path=out_path, method="", hash_digest="")},
                input_srcs=set(),
                platform=system,
                builder="/bin/sh",
                args=args,
                env=env,
            ),
            build_mode=BuildMode.NORMAL,
        )
        try:
            resp = await self.call(request, skip_probe=True)
            accepted = resp.result.status in (
                BuildResultStatus.BUILT,
                BuildResultStatus.SUBSTITUTED,
                BuildResultStatus.ALREADY_VALID,
                BuildResultStatus.RESOLVES_TO_ALREADY_VALID,
            )
            if accepted:
                log.debug("probe_accepted", store_id=self.store_id, probe=name)
                reason = ""
            else:
                reason = _refusal_with_builder_output(resp)
                log.debug(
                    "probe_denied",
                    store_id=self.store_id,
                    probe=name,
                    status=resp.result.status,
                    error_msg=resp.result.error_msg,
                )
        except (BackendError, OSError, ConnectionError) as e:
            log.debug("probe_exception", store_id=self.store_id, probe=name, error=str(e))
            return name, False, str(e)
        else:
            return name, accepted, reason

    # ── Standard operations ──────────────────────────────────────────

    async def is_valid_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """IsValidPath (op 1) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def query_referrers(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryReferrers (op 6) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def add_to_store(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """AddToStore (op 7) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def build_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """BuildPaths (op 9) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def ensure_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """EnsurePath (op 10) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def add_temp_root(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """AddTempRoot (op 11) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def add_indirect_root(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """AddIndirectRoot (op 12) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def find_roots(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """FindRoots (op 14) — delegate to daemon.

        The idle connections go first, for the reason that
        `retire_idle_connections` gives: each one keeps a worker of the daemon
        alive, and that worker holds a temporary root for every path that it
        took. `nix-store -q --roots` reads those roots and prints `{temp:NNN}`
        beside the root that the client really made. `gc.sh:16` of the
        functional tests of Nix compares that output with one line.
        """
        await self.retire_idle_connections()
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def set_options(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """SetOptions (op 19) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def collect_garbage(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """CollectGarbage (op 20) — delegate to daemon, and forget what it deleted.

        A collected path stays in `path_info_cache` for 300 s, and
        `QueryPathInfo` answers `valid=True` from it. Deletion is the only
        daemon-mediated write that nothing invalidated: `nix store delete`
        is this operation with `DELETE_SPECIFIC`, and `nix-collect-garbage`
        is it with `DELETE_DEAD`. The response names every deleted path, so
        each one is forgotten individually rather than clearing the cache
        that the live paths still use.
        """
        response = await self.call(request, client=client, suppress_last=suppress_last)
        for path in response.paths_deleted:
            self.forget_path_info(path)
        return response

    async def query_all_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryAllValidPaths (op 23) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def query_path_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathInfo (op 26) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def query_path_from_hash_part(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathFromHashPart (op 29) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def query_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryValidPaths (op 31) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def query_substitutable_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QuerySubstitutablePaths (op 32) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def query_valid_derivers(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryValidDerivers (op 33) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def optimise_store(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """OptimiseStore (op 34) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def verify_store(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """VerifyStore (op 35) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def build_derivation(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """BuildDerivation (op 36) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def add_signatures(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """AddSignatures (op 37) — delegate to daemon, and forget what it changed.

        The cached `ValidPathInfo` of this path holds the signatures from
        before this call, and `QueryPathInfo` answers from that cache for
        300 s. `forget_path_info` says why that is a divergence.
        """
        response = await self.call(request, client=client, suppress_last=suppress_last)
        self.forget_path_info(request.path)
        return response

    async def nar_from_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """NarFromPath (op 38) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def add_to_store_nar(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """AddToStoreNar (op 39) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def query_missing(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryMissing (op 40) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def query_derivation_output_map(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryDerivationOutputMap (op 41) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def register_drv_output(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """RegisterDrvOutput (op 42) — carry the shape this store reads, then delegate."""
        return await self.call(
            await self._request_in_the_shape_this_store_reads(request),
            client=client,
            suppress_last=suppress_last,
        )

    async def query_realisation(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryRealisation (op 43) — carry the shape this store reads, then answer in both.

        The answer goes to a client that may have agreed on the other shape,
        and the codec writes only the one that client reads. A 2.34 store's
        list read as "no realisation" to a client with the feature.
        nixidae/nixidae#1006.
        """
        response = await self.call(
            await self._request_in_the_shape_this_store_reads(request),
            client=client,
            suppress_last=suppress_last,
        )
        if not isinstance(response, QueryRealisationResponse):
            return response
        return answer_in_both_shapes(response, request)

    async def _request_in_the_shape_this_store_reads(self, request: Any) -> Any:
        """Fill the missing shape of op 42/43, or refuse.

        **Every hop negotiates apart, and a verbatim forward crosses one.**
        The client decoded this request under its own set, and the pooled
        connection will encode it under this store's. The two shapes of a
        realisation reference share no byte: the old one carries the hash of
        the derivation and the new one the path. A request that holds only
        the shape the store did not agree to would reach the wire as an
        empty string -- a `None` scalar writes `""` -- or as a model the
        writer cannot fill. Never as the value the client sent. Issue #84.

        pynixd's own queries carry both shapes and pass through untouched:
        `realisations_of` of `goals/realisations.py` and
        `_register_realisations` of `goals/ensure.py` fill each field, and
        the codec drops the one the store did not agree to. Only a request
        from a client can hold one shape alone.

        New to old fills the hash from the derivation, which this store
        holds. Old to new has no answer anywhere: the old shape carries the
        hash and not the path, and no index maps one to the other. That
        direction refuses loudly instead of writing an empty path.
        """
        store_reads_new = FEATURE_REALISATION_WITH_PATH in self.features
        keyed = request.keyed_drv_output
        old = getattr(request, "drv_output", None) or getattr(request, "realisation", None)
        if store_reads_new and old is not None and keyed is None:
            raise BackendError(
                f"store {self.store_id} offers 'realisation-with-path-not-hash' but the request carries "
                "only the old shape; the derivation path cannot be recovered from the output hash",
            )
        if not store_reads_new and keyed is not None and old is None:
            update: dict[str, object] = {"drv_output": await self._old_drv_output_id(keyed)}
            if getattr(request, "unkeyed_realisation", None) is not None:
                update["realisation"] = await self._old_realisation(keyed, request.unkeyed_realisation)
            elif hasattr(request, "unkeyed_realisation"):
                raise BackendError(
                    f"store {self.store_id} does not offer 'realisation-with-path-not-hash' and the request "
                    "carries half of the new shape: the key without the body",
                )
            return request.model_copy(update=update)
        return request

    async def _old_drv_output_id(self, keyed: Any) -> DrvOutput:
        """The old-shape id of a new-shape derivation output.

        The hash comes from the derivation this store holds, read the way
        `realisations_of` of `goals/realisations.py` reads it. A derivation
        the store does not hold, or whose closure gives no hash, refuses:
        the all-zero id that Nix writes when it has no hash would query a
        realisation that was never registered.
        """
        drv_path = str(keyed.drv_path)
        output_name = keyed.output_name
        parsed = await self.read_derivation(drv_path)
        hashes = await output_hashes(parsed, self.read_derivation) if parsed is not None else None
        digest = hashes.get(output_name) if hashes else None
        if digest is None:
            raise BackendError(
                f"store {self.store_id} does not offer 'realisation-with-path-not-hash' and the hash of "
                f"{drv_path}!{output_name} is not computable",
            )
        return DrvOutput(f"sha256:{digest}!{output_name}")

    async def _old_realisation(self, keyed: Any, unkeyed: Any) -> Realisation:
        """The old-shape whole of a new-shape key and body.

        The JSON of `Realisation` carries the bare `<hash>-<name>`, while
        `UnkeyedRealisation` carries the whole path; `_bare_path` of
        `nix_daemon_protocol.build_result` is that translation.
        """
        bare = str(unkeyed.out_path).removeprefix(store_prefix())
        return Realisation(
            id=await self._old_drv_output_id(keyed),
            out_path=StorePath(bare),
            signatures=sorted(unkeyed.signatures),
        )

    async def add_multiple_to_store(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """AddMultipleToStore (op 44) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def add_build_log(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """AddBuildLog (op 45) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def build_paths_with_results(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """BuildPathsWithResults (op 46) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def add_perm_root(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """AddPermRoot (op 47) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    # ── Extension operations ─────────────────────────────────────────

    async def pynixd_collect_garbage(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """PynixdCollectGarbage (op 101) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def query_path_infos(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathInfos (op 103) — batch path info query, falls back to per-path calls."""

        from nix_daemon_protocol.valid_path_info import ValidPathInfo

        from ..serde import QueryPathInfoRequest, QueryPathInfosResponse

        if not request.paths:
            return QueryPathInfosResponse(infos=[])

        if "QueryPathInfos" in self.features:
            return await self.call(request, client=client, suppress_last=suppress_last)

        infos: list[ValidPathInfo] = []
        for path in request.paths:
            resp = await self.query_path_info(
                QueryPathInfoRequest(path=path), client=client, suppress_last=suppress_last
            )
            if resp.valid and resp.info is not None:
                infos.append(ValidPathInfo(path=path, info=resp.info))
        return QueryPathInfosResponse(infos=infos)

    async def query_closure(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryClosure (op 104) — closure of store paths, falls back to empty if unsupported."""

        if "QueryClosure" in self.features:
            return await self.call(request, client=client, suppress_last=suppress_last)
        from ..serde import QueryClosureResponse

        return QueryClosureResponse(paths=set())

    async def query_closure_with_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryClosureWithInfo (op 105) — closure with full path info, falls back to iterative query."""

        from ..serde import QueryClosureWithInfoResponse, QueryPathInfosRequest

        if not request.paths:
            return QueryClosureWithInfoResponse(infos=[])

        if "QueryClosureWithInfo" in self.features:
            return await self.call(request, client=client, suppress_last=suppress_last)

        pending: set[StorePath] = set(request.paths)
        all_infos: dict[StorePath, Any] = {}
        while pending:
            to_fetch = {p for p in pending if p not in all_infos}
            if not to_fetch:
                break
            infos_resp = await self.query_path_infos(
                QueryPathInfosRequest(paths=to_fetch),
                client=client,
                suppress_last=suppress_last,
            )
            new_infos = {info.path: info for info in infos_resp.infos}
            for p in to_fetch:
                if p not in new_infos:
                    raise ValueError(f"Path {p} not found in store closure")
            all_infos.update(new_infos)
            next_pending: set[StorePath] = set()
            for info in new_infos.values():
                for ref in info.info.references:
                    if ref not in all_infos:
                        next_pending.add(ref)
            pending = next_pending

        sorted_infos: list = []
        visited: set[StorePath] = set()
        visiting: set[StorePath] = set()

        def visit(p: StorePath) -> None:
            if p in visited or p in visiting:
                return
            visiting.add(p)
            info = all_infos[p]
            for ref in info.info.references:
                if ref != p:
                    visit(ref)
            visiting.remove(p)
            visited.add(p)
            sorted_infos.append(info)

        for p in sorted(all_infos.keys(), key=str):
            visit(p)
        return QueryClosureWithInfoResponse(infos=sorted_infos)

    async def query_derivation_output_map_batch(
        self, request: Any, client: Any = None, suppress_last: bool = False
    ) -> Any:
        """QueryDerivationOutputMapBatch (op 106) — batch output map query, falls back per-drv."""

        from pynixd.daemon_extensions.query_derivation_output_map_batch import DerivationOutputMapBatchResponse

        from ..serde import StorePath

        if not request.drv_paths:
            return DerivationOutputMapBatchResponse(outputs={})

        if "QueryDerivationOutputMapBatch" in self.features:
            return await self.call(request, client=client, suppress_last=suppress_last)

        outputs: dict[StorePath, dict[str, StorePath]] = {}
        for drv_path in request.drv_paths:
            try:
                parsed = await self.read_derivation(drv_path)
                if parsed is not None:
                    sp = StorePath(path=str(drv_path))
                    outs: dict[str, StorePath | None] = dict(parsed.output_paths().items())  # type: ignore[dict-item]
                    clean: dict[str, StorePath] = {k: v for k, v in outs.items() if v is not None}
                    outputs[sp] = clean
            except FileNotFoundError:
                pass
        return DerivationOutputMapBatchResponse(outputs=outputs)

    async def sign_path_info(
        self,
        request: Any,
        client: Any = None,
        suppress_last: bool = False,
        conn: Connection | None = None,
    ) -> Any:
        """SignPathInfo (op 107) — sign with local keys and relay to daemon.

        `conn` is a connection the caller already holds, idle and carrying
        the client's options. Signing over it saves a pool acquire for a
        connection that does the same work. `AddToStore` passes its transfer
        connection, idle once the NAR and the response crossed it.
        """
        # The held connection or a pooled one: one call either way.
        call = conn.call if conn is not None else self.call
        if "SignPathInfo" in self.features:
            response = await call(request, client=client, suppress_last=suppress_last)
            self.forget_path_info(request.info.path)
            return response

        # Decompose: sign locally with pynixd keys, then call AddSignatures on daemon
        from nix_daemon_protocol.add_signatures import AddSignaturesRequest
        from nix_daemon_protocol.signature import Signature
        from pynixd.daemon_extensions.sign_path_info import SignPathInfoResponse

        from ..signing import fingerprint

        info = request.info
        refs = {str(r) for r in info.info.references}
        # `fingerprint` renders the NAR hash the way `path-info.cc:48` of Nix
        # does, so this passes the value of the wire and converts nothing.
        # This call used to pass it with no name of an algorithm and
        # `sign_path_info` used to put `sha256:` in front of the base-16
        # digest, so one path signed two ways gave two signatures and a
        # verifier of Nix read both as false.
        fp = fingerprint(
            store_path=str(info.path),
            nar_hash=info.info.nar_hash,
            nar_size=info.info.nar_size,
            references=refs,
        )
        for key in self._signing_keys.values():
            sig_str = key.sign_fingerprint(fp)
            name, _, sig_val = sig_str.partition(":")
            info.info.sigs.add(Signature(name=name, signature=sig_val))

        # `self.call` and not `self.add_signatures`, so this path does not get
        # the invalidation of that method and states it here instead.
        await call(
            AddSignaturesRequest(path=info.path, sigs=info.info.sigs),
            client=client,
            suppress_last=suppress_last,
        )
        self.forget_path_info(info.path)
        return SignPathInfoResponse(info=info)

    async def probe_systems(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """ProbeSystems (op 108) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    async def probe_features(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """ProbeFeatures (op 109) — delegate to daemon."""
        return await self.call(request, client=client, suppress_last=suppress_last)

    # ── Derivation reading ──────────────────────────────────────────

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        """Fetch and parse a .drv file from the daemon via NAR stream."""

        from ..drv_parser import parse_drv
        from ..nar import NarRegular, parse_nar
        from ..serde import IsValidPathRequest, NarFromPathRequest, QueryPathInfoRequest, StorePath

        sp = StorePath(path=str(drv_store_path))

        valid_resp = await self.execute(IsValidPathRequest(path=sp))
        if not valid_resp.valid:
            log.warning("drv_not_found", drv_path=str(drv_store_path), reason="not_valid")
            return None

        info_resp = await self.execute(QueryPathInfoRequest(path=sp))
        nar_size = info_resp.info.nar_size if info_resp.valid and info_resp.info is not None else 0

        async with self.transfer_conn() as conn:
            await NarFromPathRequest(path=sp).to_writer(WriteContext.from_conn(conn))
            await conn.w.drain()
            await conn.r.drain_stderr()
            if nar_size > 0:
                nar_data = await conn.r.readexactly(nar_size)
            else:
                collector = wire.BytesWriter("drv-nar")
                await wire.stream_parse_nar(conn.r, collector, capture=False)
                nar_data = collector.get_bytes()

        if not nar_data:
            log.warning("drv_not_found", drv_path=str(drv_store_path), reason="nar_empty")
            return None

        node = parse_nar(nar_data)
        if not isinstance(node, NarRegular):
            log.warning("drv_not_found", drv_path=str(drv_store_path), reason="not_regular_file")
            return None

        return parse_drv(node.contents.decode())


for _op, _method_name in {
    6: "query_referrers",
    7: "add_to_store",
    9: "build_paths",
    10: "ensure_path",
    11: "add_temp_root",
    12: "add_indirect_root",
    14: "find_roots",
    19: "set_options",
    20: "collect_garbage",
    23: "query_all_valid_paths",
    32: "query_substitutable_paths",
    33: "query_valid_derivers",
    34: "optimise_store",
    35: "verify_store",
    36: "build_derivation",
    37: "add_signatures",
    39: "add_to_store_nar",
    40: "query_missing",
    41: "query_derivation_output_map",
    42: "register_drv_output",
    43: "query_realisation",
    44: "add_multiple_to_store",
    45: "add_build_log",
    46: "build_paths_with_results",
    47: "add_perm_root",
    101: "pynixd_collect_garbage",
    103: "query_path_infos",
    104: "query_closure",
    105: "query_closure_with_info",
    106: "query_derivation_output_map_batch",
    107: "sign_path_info",
    108: "probe_systems",
    109: "probe_features",
}.items():
    DaemonStore._register_executor(_op, _method_name)
