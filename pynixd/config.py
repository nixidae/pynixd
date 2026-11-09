"""Pydantic-settings model for the PYNIXD_CONFIG JSON file and env vars."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd_store.store_layout import Overlay, StoreLayout

from .constants import STALL_TRACEBACK_MAX_BYTES
from .nix_config import NixConfig
from .op_routes import OPERATION_NAMES, OpRoute
from .s3_url import https_url


class ScheduleMode(StrEnum):
    """How pynixd assigns builds: auto-detect, proxy-only, or scheduler-only."""

    auto = "auto"
    proxy = "proxy"
    scheduler = "scheduler"


class NoLocalJobs(StrEnum):
    """What a client's `--max-jobs 0` makes of a build pynixd could run locally.

    `remote-only` is Nix: `Worker::waitForBuildSlot` at `worker.cc:262`
    grants no local slot under 0, and `worker.cc:351` fails when no builder
    can take the build. NIX-DEVIATION (#27): the other two build locally
    where Nix fails or delegates. `remote-first` builds locally only when no
    remote store could ever take the build; `one-slot` reads 0 as 1, which
    `src/libstore/build/worker.cc` never does. Each is opt-in, chosen by
    User, and the default stays Nix's. nixidae/nixidae#1003.
    """

    remote_only = "remote-only"
    remote_first = "remote-first"
    one_slot = "one-slot"


if TYPE_CHECKING:
    from .store import (
        DaemonStore,
        ExternalUnixStore,
        HTTPBinaryCacheStore,
        LocalBinaryCacheStore,
        LocalDBStore,
        LocalStore,
        S3BinaryCacheStore,
        SSHSocketStore,
        SSHSubprocessStore,
        Store,
    )


def _feature_matrix_from_config(
    systems: set[str] | None,
    system_features: set[str],
) -> dict[str, set[str]] | None:
    """Convert config-level systems + system_features into a feature_matrix.

    Returns None if both are empty/default (meaning: probe at startup).
    """
    if systems and system_features:
        return {s: set(system_features) for s in systems}
    if systems:
        return {s: set() for s in systems}
    if system_features:
        raise ValueError(
            "system_features specified without systems; specify systems to create a feature_matrix",
        )
    return None


class StoreRankingSettings(BaseModel):
    """Configurable weights for the telemetry-driven store ranking algorithm."""

    locality_weight: float = 500.0
    """Points for data locality: (common_paths / total_paths) * locality_weight."""

    cpu_idle_weight: float = 100.0
    """Points for CPU availability: (1.0 - cpu_utilization) * cpu_idle_weight."""

    cpu_pressure_penalty: float = 50.0
    """Penalty for CPU PSI `some avg10`, a percent: -(cpu_psi / 100 * cpu_pressure_penalty)."""

    io_pressure_penalty: float = 50.0
    """Penalty for IO PSI `some avg10`, a percent: -(io_psi / 100 * io_pressure_penalty)."""

    concurrency_penalty: float = 50.0
    """Penalty per running build: -(running_builds * concurrency_penalty)."""

    predicted_load_penalty_per_min: float = 10.0
    """Penalty per expected minute of in-flight work: -(predicted_mins * load_penalty)."""

    thundering_herd_penalty: float = 100.0
    """Penalty per build assigned in current scheduling pass: -(assigned * penalty)."""

    remote_setup_seconds: float = Field(default=1.0, ge=0.0)
    """Seconds every delegation to a remote store is expected to cost before
    and after its build, beside the bytes it moves: the connection, the
    handshake, the input query and the output copy. Measured phases leave this
    out: a trivial `runCommand` delegated to sg1 records 0 bytes to send, yet
    the client waits for each round trip (nixidae/nixidae#1125). A guess, not a
    measurement."""

    transfer_penalty_per_second: float = 10.0
    """Penalty per expected second of delegation, measured per store: -(seconds * penalty).

    The local store pays none. 10 makes a 10 s transfer weigh one fully busy
    CPU (`cpu_idle_weight`). A guess, not a measurement: nixidae/nixidae#1001.
    """

    time_penalty_per_second: float | None = None
    """Penalty per expected second of the whole build on each store, the local store included.

    The seconds are `StoreEstimate.total_seconds`: inputs, dispatch, build
    and pull, from the build's history. Where a store has one, it replaces
    `transfer_penalty_per_second`, which prices only a remote's transfer.
    `None` keeps the transfer cost alone. nixidae/nixidae#1125.
    """

    cpu_reference_seconds: float | None = None
    """The expected build seconds at which the CPU terms count in full.

    A shorter build gains less from an idle CPU and crowds a store less, so
    its CPU idle, CPU pressure and herd points scale by
    `expected / cpu_reference_seconds`. The scaled
    points order the stores; `min_schedule_score` reads the unscaled load,
    so a full store stays full. `None` counts them in full.
    nixidae/nixidae#1125.
    """

    per_core_load: bool = False
    """Price a running build, and one assigned in this pass, at one core's share of `cpu_idle_weight`.

    A store then fills when its builds match its cores, and not after the
    one or two builds that `concurrency_penalty` and `thundering_herd_penalty`
    allow an idle store at any size. Those two stay the price for a store
    whose cores are unknown. nixidae/nixidae#1125.
    """

    local_wins_ties: bool = False
    """The local store ranks first among equal scores; otherwise last, as a fallback (nixidae/nixidae#1001)."""

    explore_share: float = Field(default=0.0, ge=0.0, le=1.0)
    """The fraction of builds that go to a random roomy candidate other than
    the best-ranked one. A store's estimate for a derivation shape changes
    only when the store builds that shape, so with none an estimate that went
    stale stays stale (nixidae/nixidae#1125). Buildbarn and icecream do the
    same."""

    prefer_local_weight: float = 10.0
    """A `preferLocalBuild` derivation waits for a local slot until the wait
    passes this many times the best remote's delegation seconds. A guess, not
    a measurement: nixidae/nixidae#1001."""

    unmonitored_penalty: float = 1000.0
    """Penalty for a store with no CPU or pressure figure: -(unmonitored_penalty).

    Such a store still gets builds, and `min_schedule_score` does not apply
    to it, but it ranks under every store pynixd can see. That holds for a
    builder that refuses SFTP and exec, and for one with `monitor = false`.
    10 times `cpu_idle_weight`: a monitored store wins until its own
    penalties pass this. A guess, not a measurement: nixidae/nixidae#1016.
    """

    min_free_bytes: int = 2 * 1024**3
    """A store whose last sample shows less free disk than this gets no build. nixidae/nixidae#1013."""

    disk_full_hold_seconds: float = 600.0
    """How long a store that reports no disk figure is held back after "No space left on device"."""

    no_local_jobs: NoLocalJobs = NoLocalJobs.remote_only
    """What a client's `--max-jobs 0` means; `NoLocalJobs` has the three answers."""

    min_schedule_score: float = 0.0
    """Minimum score required for a store to be considered. If lower, builds stay queued."""


class PlacementTraceSettings(BaseModel):
    """The record of each placement decision in the store database. nixidae/nixidae#1070."""

    enabled: bool = False
    """Off unless asked for. A decision costs one row, and one more per store."""

    max_age: float = 7 * 24 * 3600.0
    """Seconds a decision stays. Older rows go in the next sweep."""

    sweep_interval: float = 600.0
    """Seconds between two sweeps of old rows."""

    sweep_chunk: int = 500
    """Decisions one sweep transaction deletes, with their stores.

    A sweep deletes the oldest rows in chunks of this size, one transaction
    each, so the write lock of the store database stays short and the freed
    pages are contiguous.
    """

    flush_interval: float = 5.0
    """Seconds between two writes of the buffered decisions, one transaction each."""

    repeat_interval: float = 30.0
    """A waiting build writes the same decision again after this many seconds.

    A pass ranks every pending build again, so each pass would otherwise
    write one decision for each waiting build. A changed outcome or reason
    writes at once.
    """

    buffer_limit: int = 10_000
    """Decisions held for the next flush. Past it, new decisions are dropped and counted."""


class PressureSettings(BaseModel):
    """How the pressure of a builder moves its build limit. Issue #98.

    A store runs at most its build limit of builds, between 1 and the size of
    its build pool. Each signal has a high mark and a low mark. Past a high
    mark the limit falls; under every low mark, with each slot in use, it
    rises; between the two it holds, so one noisy sample cannot flap it.

    Pressure is PSI `avg10` in percent of wall time, for the whole machine.
    """

    enabled: bool = True
    """Off: the limit is the build pool size, and nothing moves it."""

    interval: float = 2.0
    """Seconds between two samples. The kernel updates `avg10` every 2 s."""

    memory_full_high: float = 2.0
    memory_full_low: float = 0.5
    """`memory` `full`: each task stalls on memory. Past 2 % the machine thrashes."""

    io_full_high: float = 25.0
    io_full_low: float = 10.0
    """`io` `full`. Network storage idles at several percent; dynhetz measured 3 %."""

    cpu_some_high: float = 60.0
    """`cpu` `some` at or past this holds the limit. CPU pressure never lowers
    it: a builder with every core busy is the goal, and it does not thrash."""

    load_per_core_high: float = 1.5
    """The 1-minute load average per core that holds the limit, read only on
    a builder with no PSI."""

    min_available_memory_mb: int = 1024
    """`MemAvailable` under this lowers the limit, and twice this holds it."""

    decrease_factor: float = 0.5
    """A fall sets the limit to this share of the builds that run."""

    increase_step: int = 1
    """A rise past slow start adds this many builds."""

    settle: float = 10.0
    """Seconds after a change before the next one. `avg10` needs about 10 s to
    show what a change did; a shorter wait reacts to the old load."""

    agent: bool = True
    """Sample an SSH builder with `pynixd-agent`. Off, or on a builder that
    cannot run it, pynixd polls the same files over SFTP."""

    agents: dict[str, Path] | None = None
    """System to `pynixd-agent` binary. `None` reads `PYNIXD_AGENTS`, which the
    package sets to the agent of its own system."""

    initial_limit: int | None = None
    """The limit of the first sample. `None` takes the core count of the
    builder, or 1 when it is unknown. Until the first fall the limit doubles
    at each rise, from this number."""


class StoreSpecBase(BaseModel):
    """Common settings shared by all store specs."""

    store_id: StoreId | None = None
    systems: set[str] | None = None
    system_features: set[str] = Field(default_factory=set)
    feature_matrix: dict[str, set[str]] | None = None
    nix_bin: str = "nix"
    idle_ttl: float = 10.0
    read_timeout: float = 300.0
    """Seconds a pure database read may wait for this store's answer.

    Only reads that take no lock: a GC, a build and anything that waits on
    the GC lock can be silent for a long time and are never cut off.
    `pynixd state` shows those instead (#93).
    """
    max_lifetime: float = 300.0
    """How long a pooled connection may serve before the pool retires it.

    A worker of the daemon holds a temporary root for each path that it
    builds or substitutes, and it releases those roots when it exits. A
    pooled connection keeps that worker alive, so a connection in steady
    use holds every root it ever made. Zero turns the rule off. Issue #20.
    """
    scheduleable: bool = True
    priority: float = 1.0
    trusted: bool = True
    """Import a substitute from this store without checking its signatures, as Nix's `?trusted=1`.

    A store in the configuration is a builder or a peer, and its outputs are
    unsigned, so it is trusted by default (User's decision, nixidae/nixidae#1008).
    A binary cache is not. A store that a substituter URL opens takes the
    URL's `trusted`, false when absent, as in Nix (`store_url`).
    """
    score_penalty: int = 0
    mandatory_features: set[str] = Field(default_factory=set)
    """Features a derivation must require to build here: column 7 of a machines line (`Machine::mandatoryMet`)."""
    last_resort: bool = False
    """Build here only what no other store can: capability, never offload.

    A derivation goes to a last-resort store only when no other healthy,
    scheduleable store supports its system and features, or each that does
    already failed it. Load, disk and the ranking do not count: a busy
    regular store means wait, not move. For borrowed machines, such as the
    nix-community builders. nixidae/nixidae#1018.
    """
    gc_enabled: bool = True
    gc_max_age: int | None = None
    """Seconds since last reference after which a dead path may be collected.

    `None` keeps the substituter rule: nothing leaves unless a `gc_defer`
    store holds it. A number picks the age rule instead, which needs no
    substituter: dead and unreferenced for that long goes, live and fresh
    stays. Setting it is the operator taking ownership of the store's old
    paths, on a store whose losses do not matter or whose caches can replace
    them.
    """
    gc_target_usage: float | None = None
    """Disk usage fraction at which a collection pass stops early.

    `None` deletes the whole plan at once. A number bounds the pass by
    pressure instead: heaviest first, stopping once the freed bytes project
    usage under the fraction. The hourly loop then relieves a full disk over
    several passes rather than emptying the plan in one. A number at or above
    one deletes nothing until the disk is fuller than full, which is a way
    of saying dry-run on a schedule.
    """
    gc_allow_execute: bool = False
    """Whether the collector may delete: the operator's signature, nothing else.

    Off everywhere, so planning stays free and deleting stays refused. The
    liveness mirror must show sustained zero-divergence against what Nix
    reports alive before this flips on, and flipping it is the cutover
    decision, recorded in the config that carries it. `Collector.run`
    raises `GCNotPermittedError` on EXECUTE while this is off.
    """
    gc_defer: bool = False
    """Whether a path this store holds may leave the local store.

    Off everywhere, so a store that nothing declares drops nothing. An
    air-gapped lab therefore keeps every path until somebody names a
    substituter to defer to. The flag sits on the spec and not in a list of
    settings, because a substituter that `Server.__init__` reads out of
    `nix.conf` has no spec: naming one of those would need a second naming
    scheme over the generated store ids.
    """
    no_schedule: bool = False
    probe: bool | None = None
    store_path: Path = Path("/")
    """The root of a chroot store, which `nix daemon --store` takes.

    `/` is the store of the machine. Three specs declared this with the same
    default, and `store/daemon.py` read it with `getattr` because the base
    did not have it.
    """
    pressure: PressureSettings | None = None
    """This store's pressure settings; `None` takes `PynixdSettings.pressure`."""
    max_connections: int | None = None
    """Connections to this store at once, both pools. `None` takes the
    transport's default: 64 on a Unix socket, `MaxSessions` less two over SSH
    sessions. The build pool's share is the ceiling of the build limit, so a
    high number here lets pressure decide. An idle nix-daemon worker costs
    about 1 MiB (issue #98)."""
    transfer_connections: int | None = None
    """How many of `max_connections` serve transfers and reads; `None` is a
    quarter, at least 2."""
    settings: PynixdSettings | None = None
    reconnect: bool = True
    reconnect_min_delay: float = 1.0
    reconnect_max_delay: float = 300.0

    def layout(self) -> StoreLayout:
        """The three directories of the store that this spec names.

        A store that is not relocated has the layout of the chroot that
        `store_path` names, and `/` is the store of the machine.
        `LocalSocketStoreSpec` overrides this, because only a local store can
        put its store directory and its state directory in two places.
        """
        return StoreLayout.chroot(self.store_path)

    def _effective_feature_matrix(self) -> dict[str, set[str]] | None:
        if self.feature_matrix is not None:
            return self.feature_matrix
        return _feature_matrix_from_config(self.systems, self.system_features)

    def reference_key(self) -> object | None:
        """What makes two substituter URLs one store, or `None` when no URL names this kind.

        Nix's `StoreConfig::getReference`: another `?trusted=` is another store.
        `pynixd.store_url` compares a URL's spec with every running store by it.
        """
        return None

    def to_store(self, store_id: str) -> Store:
        raise NotImplementedError(f"{type(self).__name__} must implement to_store()")


class BuildVmSpec(BaseModel):
    """A sandboxed build in a libkrun microVM, around the namespace helper (nixidae/nixidae#1186).

    The guest sees only the build's input closure, through a read-only DAX share, so it keeps no copy of the
    store in its own memory. A fixed-output build, and one with a builder socket (`recursive-nix`,
    `builder-rpc-v0`), stays on the namespace helper alone.
    """

    memory_mib: int = Field(default=2048, gt=0)
    """The guest's memory. Freed pages go back to the host (free page reporting)."""
    cpus: int = Field(default=2, gt=0, le=255)
    dax_window_mib: int = Field(default=4096, ge=0)
    """The address space the guest maps the store through. It costs no host memory."""
    disk_gib: int = Field(default=1024, gt=0)
    """The build directory's size: a sparse XFS image on the host's disk, which costs what the build writes
    and a 64 MiB log. A build that fills it fails with ENOSPC; the host's memory never holds it."""


class OverlaySpec(BaseModel):
    """A `local-overlay://` store: `store_path` is the merged directory, an OverlayFS mount that pynixd does not make.

    pynixd builds the daemon's store URI from these, as it does `--store <root>` for a chroot store.
    """

    lower_store: Path
    """The root of the lower store, a plain local store: its paths are at `<lower_store>/nix/store`."""
    upper_layer: Path
    """OverlayFS's upper directory."""
    check_mount: bool = True
    """`check-mount`: the daemon refuses to start when the mount's lowerdir or upperdir is not these."""
    remount_hook: Path | None = None
    """`remount-hook`: run with the store directory after a delete under the upper layer."""


class LocalSocketStoreSpec(StoreSpecBase):
    """A local Nix daemon accessed via Unix domain socket.

    Optionally wraps the daemon with a local SQLite database (``use_db``)
    for fast-path resolution of queries like ``IsValidPath``, ``QueryPathInfo``,
    and ``QueryReferrers``.
    """

    type: Literal["local-socket"] = "local-socket"

    overlay: OverlaySpec | None = None
    """A `local-overlay://` store over a lower store, for a chroot root (`pynixd_store.store_layout.Overlay`)."""

    store_dir: Path | None = None
    """The directory in a store path, for a relocated store.

    Set this and `state_dir` together to serve a store that `NIX_STORE_DIR`
    moved, rather than one that `--store <root>` moved. The two shapes differ:
    a chroot store keeps `builtins.storeDir` at `/nix/store` and puts the
    files under the root, and a relocated store moves the store path itself.
    `pynixd_store/store_layout.py` states both. Issue nixidae/nixidae#215.
    """

    state_dir: Path | None = None
    """The directory that holds `db/` and `temproots/`, for a relocated store.

    Required with `store_dir`, and refused without it. Nix keeps the two
    independent, so neither one gives the other.
    """

    log_dir: Path | None = None
    """Nix's `logDir`, where `addBuildLog` writes `drvs/`, for a relocated store.

    Required with `store_dir`, and refused without it. Nix reads it from
    `NIX_LOG_DIR` independent of the state (`store-api.cc:1271`), so neither
    directory gives it. A chroot store takes it from the root instead.
    """

    socket_path: Path = Path("pynixd-nix")
    """Where the managed daemon listens.

    An absolute path is used as it is. A relative one is a name under
    `<state_dir>/daemon-socket/`, which is where Nix puts its own socket. The
    value was `nix/var/nix/daemon-socket/pynixd-nix` and it was joined to the
    store root, which gives the same path for a chroot store and no path at
    all for a relocated one.
    """

    managed: bool | None = None
    """Whether pynixd runs the daemon itself, rather than using the machine's.

    `None` derives it: a relocated store, or a store root that is not `/`, is
    always ours to serve. That inference reads a store at `/` as the store of
    the machine, whose database is protected and cannot safely be served by an
    unprivileged private daemon.

    Set `True` where that premise is false. A container can hold its own store
    at `/`, with its own `db/db.sqlite` and root in its own namespace, and no
    system daemon at all. Nothing about the store path says so, which is why
    the inference cannot see it. `False` demands the system daemon.

    nixkube's builder Pods are the case this exists for: each one mounts a
    copy-on-write volume seeded from the node store, keeping the same store
    prefix and the same path hashes, and runs no daemon.
    """

    nix_config: NixConfig | None = None
    extra_env: dict[str, str] | None = None
    extra_args: list[str] | None = None
    use_db: bool = True
    monitor: bool = True
    op_routes: dict[str, OpRoute] = Field(default_factory=dict)
    """The route of each named operation: `daemon`, `shadow` or `pynixd`.

    Keyed by the operation's name, `QueryReferrers`. An operation left out
    takes the default its answer declares; `pynixd/op_routes.py` says what
    each route does. The store refuses to start on a route it cannot take.
    """
    pynixd_writes: bool = False
    """Whether pynixd itself may change the store: its files, its database, its state directory.

    Off, every answer that writes goes to the daemon, whatever `op_routes`
    says, and a route to `pynixd` for one refuses to start. A read that
    tidies as Nix does (FindRoots unlinking a stale `gcroots/auto` link,
    reaping a dead process's temporary roots file) leaves that to the
    daemon. The test suites turn it on; a deployment turns it on by hand.
    """
    daemon: bool = True
    """Whether a Nix daemon runs under pynixd for this store.

    Off, pynixd opens the store itself (`pynixd_store.create_store`), starts
    no daemon and connects to none, and every operation takes the `pynixd`
    route. A request that only the daemon can answer fails with an error that
    names it. It needs `use_db`, `pynixd_writes` and a store pynixd manages.
    nixidae/nixidae#1155.
    """
    build_vm: BuildVmSpec | None = None
    """Run pynixd's sandboxed builds in a microVM; `None` runs them in the namespace helper alone."""
    usage_sample_seconds: float = Field(default=5.0, ge=0)
    """Seconds between two samples of a running build's cgroup (`builder/cgroup.py`); 0 takes none."""

    @model_validator(mode="after")
    def _check_the_op_routes(self) -> LocalSocketStoreSpec:
        unknown = sorted(set(self.op_routes) - OPERATION_NAMES)
        if unknown:
            raise ValueError(f"op_routes names no operation of the protocol: {', '.join(unknown)}")
        if not self.use_db and any(route is not OpRoute.daemon for route in self.op_routes.values()):
            raise ValueError("op_routes: a store with use_db = false answers every operation through the daemon")
        if not self.daemon:
            if not self.use_db:
                raise ValueError("daemon = false needs use_db: with no daemon, pynixd reads the database itself")
            if not self.pynixd_writes:
                raise ValueError("daemon = false needs pynixd_writes: with no daemon, pynixd writes the store itself")
            if self.managed is False:
                raise ValueError("daemon = false needs a store pynixd manages, not the machine's own daemon")
            if routed := sorted(op for op, route in self.op_routes.items() if route is not OpRoute.pynixd):
                raise ValueError(f"op_routes: with daemon = false, every route is pynixd, not for {', '.join(routed)}")
        return self

    @model_validator(mode="after")
    def _check_the_two_shapes(self) -> LocalSocketStoreSpec:
        """A relocated store names both of its directories, or neither.

        A default of `/nix/var/nix` for the state would put the temporary
        roots and the database of a relocated store in the store of the
        machine, and nothing would report it.
        """
        if self.store_dir is None and self.state_dir is not None:
            raise ValueError("state_dir needs store_dir: it describes a relocated store")
        if self.store_dir is not None and self.state_dir is None:
            raise ValueError("store_dir needs state_dir: Nix keeps the two independent")
        if self.store_dir is None and self.log_dir is not None:
            raise ValueError("log_dir needs store_dir: it describes a relocated store")
        if self.store_dir is not None and self.log_dir is None:
            raise ValueError("store_dir needs log_dir: Nix reads NIX_LOG_DIR independent of the state")
        if self.overlay is not None and self.store_dir is not None:
            raise ValueError("overlay needs a chroot root: a relocated store has none")
        return self

    def layout(self) -> StoreLayout:
        """The three directories of the store that this spec names."""
        if self.store_dir is not None and self.state_dir is not None:
            return StoreLayout.relocated_store(self.store_dir, self.state_dir, self.log_dir)
        layout = StoreLayout.chroot(self.store_path)
        if self.overlay is None:
            return layout
        return layout.with_overlay(
            Overlay(
                lower=StoreLayout.chroot(self.overlay.lower_store),
                upper_layer=self.overlay.upper_layer,
                check_mount=self.overlay.check_mount,
                remount_hook=self.overlay.remount_hook,
            )
        )

    def to_store(self, store_id: str) -> LocalStore | LocalDBStore:
        """Build a ``LocalStore`` or ``LocalDBStore`` from this spec."""
        from .store.local_daemon import LocalStore
        from .store.local_db import LocalDBStore

        spec = self.model_copy(update={"store_id": StoreId(store_id)})
        if spec.use_db:
            return LocalDBStore(spec)
        return LocalStore(spec)


class ExternalUnixStoreSpec(StoreSpecBase):
    """A remote Nix daemon connected via an external Unix socket.

    Used as a substitution source only — builds and GC are disabled.
    """

    type: Literal["external-unix"] = "external-unix"
    socket_path: Path = Path("/nix/var/nix/daemon-socket/socket")
    monitor: bool = False
    scheduleable: bool = False
    no_schedule: bool = True
    gc_enabled: bool = False
    probe: bool | None = False
    """A substituter builds nothing, so it is never sent the probe builds."""

    def reference_key(self) -> object | None:
        """`unix://<socket_path>?root=<store_path>`, and the trust."""
        return ("unix", self.socket_path, self.store_path, self.trusted)

    def to_store(self, store_id: str) -> ExternalUnixStore:
        """Build an ``ExternalUnixStore`` from this spec."""
        from .store import ExternalUnixStore

        return ExternalUnixStore(
            self.model_copy(update={"store_id": StoreId(store_id)}),
        )


class ReverseStoreSpec(StoreSpecBase):
    """Configuration for a reverse store (builder connects to controller).

    Reverse stores are created dynamically when a builder registers with
    the controller via the reverse server. This spec exists to provide a
    type in the StoreSpec union and document the expected fields.
    """

    type: Literal["reverse"] = "reverse"

    def to_store(self, store_id: str) -> DaemonStore:
        raise NotImplementedError("Reverse stores are created dynamically, not from config")


class ReverseAcceptorSettings(BaseModel):
    """Settings for the reverse acceptor (controller side).

    The reverse acceptor listens for builder-initiated SSH connections.
    Builders (reverse initiators) register themselves as build stores,
    enabling NAT traversal.
    """

    enabled: bool = False
    # Every interface, v4 and v6. See `PynixdSettings.http_host`.
    host: str = ""
    port: int = 2235
    host_key_path: Path | None = None
    authorized_builder_keys: list[Path] | None
    """Builder host public keys the acceptor pins, or `None` for none.

    This field has no default, so a configuration has to answer it.
    `None` accepts any builder key: reachable only for loopback tests
    and tunnels that authenticate elsewhere, never for a port the
    internet reaches. A builder the acceptor cannot name registers
    with an arbitrary store_id and receives real builds, so a
    reachable acceptor pins every builder. Each path holds OpenSSH
    public keys, several per file; a missing file fails the acceptor
    at startup, never silently. Issue #75.
    """
    registration_probe_timeout: float = 120.0
    """How long a registering builder's proof-of-life build may take.

    An echo build takes seconds. The bound only fires on a wedged serve
    path: SSH answers, the daemon never does. Past it the registration is
    refused instead of queueing client builds behind a builder that never
    answers. Issue #80.
    """


class ReverseInitiatorSettings(BaseModel):
    """Settings for the reverse initiator (builder side).

    The reverse initiator connects to a controller's reverse acceptor and
    registers the local pynixd instance as a build store.  After registration,
    the controller opens pooled ``nix-daemon --stdio`` sessions to execute
    builds and queries.
    """

    enabled: bool = False
    acceptor_host: str = "127.0.0.1"
    acceptor_port: int = 2235
    store_id: str | None = None
    systems: list[str] | None = None
    system_features: list[str] = Field(default_factory=list)
    nix_bin: str = "nix"
    max_jobs: int | None = Field(default=None, ge=1)
    """How many builds this builder takes at once; the controller sizes its
    build pool to it. `None` takes the CPUs this process may use."""
    server_host_key_paths: list[Path] = Field(default_factory=list)
    reconnect_min_delay: float = 1.0
    reconnect_max_delay: float = 60.0
    shutdown_on_connect_failure_seconds: float | None = None
    schedule_mode: ScheduleMode = ScheduleMode.proxy
    """How a session the controller opens over this link is served.

    `proxy` sends every build to this machine's local store. `scheduler`
    runs it through this instance's own scheduler, so a builder can offer a
    system only one of its stores builds: a macOS host offering
    `aarch64-linux` through a Linux VM store. `systems` must then name what
    those stores cover. A build that comes back to the instance that
    delegated it fails there at once (`DelegationLoopError`), and that
    instance tries another store.
    """


class SSHSubprocessStoreSpec(StoreSpecBase):
    """A remote Nix daemon accessed via ``ssh <host> nix-daemon --stdio``.

    Spawns a new ``nix-daemon`` process per connection over SSH.
    """

    type: Literal["ssh-subprocess"] = "ssh-subprocess"
    host: str
    port: int = 22
    username: str | None = None
    known_hosts: str | list[str] | None
    """The file that names the host key of this builder, or `null` for none.

    A list names files read when they exist, as OpenSSH reads its defaults.
    No file existing accepts no key.

    **This field has no default, so a configuration has to answer it.** A Nix
    build store sees the whole content of every build that a client pushes,
    and it returns the store paths that the client then registers as valid.
    The host key is the check that makes the far side the machine the
    configuration named, and a machine in the middle of that path substitutes
    a build output without it.

    `null` is `asyncssh`'s "accept any host key", which is what every SSH
    store did before this field existed. It stays reachable, because a
    loopback connection to a local virtual machine has no exposure worth the
    ceremony, and because pynixd often runs as root where the `known_hosts`
    of a user holds nothing. Writing it is the point: the choice is in the
    configuration rather than in a default.

    `nix` asks the same question. A `nix.buildMachines` entry carries
    `publicHostKey`, and `ssh-ng://` verifies against it, so a person moving
    a builder from `nix.buildMachines` to `stores` used to lose the check
    without being told. Issue nixidae/nixidae#204.
    """

    monitor: bool = True
    client_keys: list[Any] = Field(default_factory=list)
    persistent_connection: bool = True
    """Whether to hold one SSH connection open for the life of the store.

    True, the default, is what a normal remote builder wants. The connection
    is opened at startup and kept, so its state *is* the health of the store:
    the reconnect loop, the backoff and the circuit breaker all read it, and
    the resource monitor polls over it. A builder that is up looks up because
    the socket is there.

    False suits a builder that starts on demand -- a local VM behind a
    socket-activated unit, with a watchdog that stops it once the last
    connection closes. pynixd then connects on first use and drops the
    transport once its pool holds nothing, so the builder is free to go away.
    The cost is the measurement above: with no connection there is nothing to
    read the store's health from, and pynixd learns a backend is down by
    failing to reach it.

    Set `monitor = false` alongside it. The monitor polls over the same
    connection, so it would hold the builder awake by itself. Issue nixidae/nixidae#203.
    """

    host_public_key: str | None = None
    """The one host key accepted, an OpenSSH public key line; it overrides `known_hosts`.

    Nix's `base64-ssh-public-host-key`, decoded (`ssh.cc:11`).
    """
    remote_command: list[str] | None = None
    """The command that serves the daemon protocol on the host; `None` derives it from `nix_bin` and `store_path`.

    An `ssh-ng://` URL sets Nix's: `remote-program`, `--stdio`, then
    `--store` and `remote-store` when given (`ssh-store.cc:227`).
    """
    compress: bool = False
    """SSH compression, as Nix's `compress` (`ssh.cc:107`)."""
    connect_timeout: float | None = None
    """Seconds to reach the host and log in; `None` waits on TCP."""

    def reference_key(self) -> object | None:
        """Every setting an `ssh-ng://` URL names, and `no_schedule`, so a configured builder is never a substituter's store."""
        return (
            "ssh-ng",
            self.username,
            self.host,
            self.port,
            tuple(self.known_hosts) if isinstance(self.known_hosts, list) else self.known_hosts,
            self.host_public_key,
            tuple(str(key) for key in self.client_keys),
            tuple(self.remote_command or ()),
            self.compress,
            self.trusted,
            self.no_schedule,
        )

    def to_store(self, store_id: str) -> SSHSubprocessStore:
        """Build an ``SSHSubprocessStore`` from this spec."""
        from .store import SSHSubprocessStore

        return SSHSubprocessStore(
            self.model_copy(update={"store_id": StoreId(store_id)}),
        )


class SSHSocketStoreSpec(StoreSpecBase):
    """A remote Nix daemon accessed via SSH with Unix socket forwarding.

    Reuses an existing daemon socket on the remote host through an SSH
    tunnel, avoiding per-connection daemon startup overhead.
    """

    type: Literal["ssh-socket"] = "ssh-socket"
    host: str
    port: int = 22
    username: str | None = None
    socket_path: Path = Path("/nix/var/nix/daemon-socket/socket")
    known_hosts: str | None
    """The file that names the host key of this builder, or `null` for none.

    **This field has no default, so a configuration has to answer it.** A Nix
    build store sees the whole content of every build that a client pushes,
    and it returns the store paths that the client then registers as valid.
    The host key is the check that makes the far side the machine the
    configuration named, and a machine in the middle of that path substitutes
    a build output without it.

    `null` is `asyncssh`'s "accept any host key", which is what every SSH
    store did before this field existed. It stays reachable, because a
    loopback connection to a local virtual machine has no exposure worth the
    ceremony, and because pynixd often runs as root where the `known_hosts`
    of a user holds nothing. Writing it is the point: the choice is in the
    configuration rather than in a default.

    `nix` asks the same question. A `nix.buildMachines` entry carries
    `publicHostKey`, and `ssh-ng://` verifies against it, so a person moving
    a builder from `nix.buildMachines` to `stores` used to lose the check
    without being told. Issue nixidae/nixidae#204.
    """

    monitor: bool = True
    client_keys: list[Any] = Field(default_factory=list)
    persistent_connection: bool = True
    """Whether to hold one SSH connection open for the life of the store.

    True, the default, is what a normal remote builder wants. The connection
    is opened at startup and kept, so its state *is* the health of the store:
    the reconnect loop, the backoff and the circuit breaker all read it, and
    the resource monitor polls over it. A builder that is up looks up because
    the socket is there.

    False suits a builder that starts on demand -- a local VM behind a
    socket-activated unit, with a watchdog that stops it once the last
    connection closes. pynixd then connects on first use and drops the
    transport once its pool holds nothing, so the builder is free to go away.
    The cost is the measurement above: with no connection there is nothing to
    read the store's health from, and pynixd learns a backend is down by
    failing to reach it.

    Set `monitor = false` alongside it. The monitor polls over the same
    connection, so it would hold the builder awake by itself. Issue nixidae/nixidae#203.
    """

    def to_store(self, store_id: str) -> SSHSocketStore:
        """Build an ``SSHSocketStore`` from this spec."""
        from .store import SSHSocketStore

        return SSHSocketStore(
            self.model_copy(update={"store_id": StoreId(store_id)}),
        )


class BinaryCacheSpecBase(StoreSpecBase):
    """A read-only substitution source in Nix's binary cache layout, with health tracking."""

    trusted: bool = False
    """`False` lets the local daemon check each path against its own
    `trusted-public-keys`, as Nix does for a substituter (`substitution-goal.cc:236`)."""
    store_dir: str | None = None
    """`?store=`: the store directory the cache serves (`StoreDirConfig::storeDir`); `None` is pynixd's own."""
    max_fail_ratio: float = 0.5
    health_window: int = 10
    scheduleable: bool = False
    no_schedule: bool = True
    gc_enabled: bool = False


class HTTPBinaryCacheSpec(BinaryCacheSpecBase):
    """A remote binary cache accessed via HTTP, with a concurrency limit."""

    type: Literal["http-binary-cache"] = "http-binary-cache"
    url: str
    max_concurrent: int | None = None

    def reference_key(self) -> object | None:
        """The URL with no trailing slash, the store directory and the trust."""
        return ("http", self.url.rstrip("/"), self.store_dir, self.trusted)

    def to_store(self, store_id: str) -> HTTPBinaryCacheStore:
        """Build an ``HTTPBinaryCacheStore`` from this spec."""
        from .store import HTTPBinaryCacheStore

        return HTTPBinaryCacheStore(
            self.model_copy(update={"store_id": StoreId(store_id)}),
        )


class LocalBinaryCacheSpec(BinaryCacheSpecBase):
    """A binary cache in a directory, Nix's `file://` store."""

    type: Literal["local-binary-cache"] = "local-binary-cache"
    path: Path

    def reference_key(self) -> object | None:
        """The directory, the store directory and the trust."""
        return ("file", self.path, self.store_dir, self.trusted)

    def to_store(self, store_id: str) -> LocalBinaryCacheStore:
        """Build a ``LocalBinaryCacheStore`` from this spec."""
        from .store import LocalBinaryCacheStore

        return LocalBinaryCacheStore(
            self.model_copy(update={"store_id": StoreId(store_id)}),
        )


class S3BinaryCacheSpec(BinaryCacheSpecBase):
    """A binary cache in an S3 bucket, Nix's `s3://` store: HTTP with SigV4 signatures (`filetransfer.cc:728`)."""

    type: Literal["s3-binary-cache"] = "s3-binary-cache"
    bucket: str
    key: str = ""
    """The prefix inside the bucket that holds the cache."""
    region: str | None = None
    scheme: str | None = None
    endpoint: str | None = None
    addressing_style: Literal["auto", "path", "virtual"] = "auto"
    profile: str | None = None
    """The AWS configuration profile the credentials come from; `None` is botocore's default chain."""
    max_concurrent: int | None = None

    def url(self) -> str:
        """The HTTP base URL of the cache; `ValueError` with Nix's text for an address Nix refuses."""
        return https_url(
            self.bucket,
            self.key,
            region=self.region,
            scheme=self.scheme,
            endpoint=self.endpoint,
            style=self.addressing_style,
        )

    def reference_key(self) -> object | None:
        """Every setting the URL names, and the trust."""
        return (
            "s3",
            self.bucket,
            self.key.strip("/"),
            self.region,
            self.scheme,
            self.endpoint,
            self.addressing_style,
            self.profile,
            self.store_dir,
            self.trusted,
        )

    def to_store(self, store_id: str) -> S3BinaryCacheStore:
        """Build an ``S3BinaryCacheStore`` from this spec."""
        from .store import S3BinaryCacheStore

        return S3BinaryCacheStore(self.model_copy(update={"store_id": StoreId(store_id)}))


StoreSpec = Annotated[
    LocalSocketStoreSpec
    | ExternalUnixStoreSpec
    | SSHSubprocessStoreSpec
    | SSHSocketStoreSpec
    | ReverseStoreSpec
    | HTTPBinaryCacheSpec
    | LocalBinaryCacheSpec
    | S3BinaryCacheSpec,
    Field(discriminator="type"),
]


class _ConfigFileSource(PydanticBaseSettingsSource):
    """Read settings fields from a JSON config file (PYNIXD_CONFIG)."""

    def __init__(self, settings_cls: type[BaseSettings]) -> None:
        super().__init__(settings_cls)

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        """Return no value — all fields are loaded in ``__call__``."""
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Load settings from the JSON file at ``PYNIXD_CONFIG``.

        Returns only fields that exist in the settings model. Returns empty
        dict if the path is unset or does not exist.
        """
        config_path = self.settings_cls.model_fields.get("config")
        if config_path is None:
            return {}
        env_val = self.current_state.get("config")
        if env_val is None:
            return {}
        path = Path(env_val)
        if not path.exists():
            return {}
        with path.open() as f:
            data = json.load(f)
        return {k: v for k, v in data.items() if k in self.settings_cls.model_fields}


class PynixdSettings(BaseSettings):
    """Unified configuration from env vars (priority) and config file.

    Env var mapping: PYNIXD_<FIELD_NAME> (e.g. PYNIXD_SSH_PORT → ssh_port).
    Config file fields (lower priority) are loaded from the JSON file at
    PYNIXD_CONFIG (if it points to an existing path).
    """

    model_config = SettingsConfigDict(env_prefix="PYNIXD_")

    config: Path | None = None
    stores: dict[str, StoreSpec] = Field(default_factory=dict)

    ssh_host: str = "127.0.0.1"
    ssh_port: int | None = 2234
    ssh_host_key: Path | None = None
    ssh_user_keys: list[Path] = Field(default_factory=list)
    """`authorized_keys` files whose keys the SSH listener admits as untrusted users.

    Read at each authentication, so an edit applies to the next connection.
    No key in either list admits nobody. Issue #94.
    """
    ssh_admin_keys: list[Path] = Field(default_factory=list)
    """`authorized_keys` files whose keys the SSH listener admits as trusted.

    What Nix's `trusted-users` grants, tied to the key and never to the
    username a client sends.
    """

    unix_path: Path | None = Path("/run/pynixd/pynixd.sock")

    # `""`, and not `"0.0.0.0"`. asyncio reads an empty host as "every
    # interface" and binds one socket per family, so the server answers over
    # IPv6 too. `"0.0.0.0"` is IPv4 only: on a single-stack IPv6 cluster the
    # kubelet dials the Pod's v6 address, gets ECONNREFUSED, and the
    # `httpGet /healthz` probe never passes. Same value as `ssh_host` takes
    # in nixkube, for the same reason.
    http_host: str = ""
    http_port: int | None = None
    http_enable_cache: bool = True
    http_enable_metrics: bool = True
    http_metrics_no_auth: bool = True
    http_user: str | None = None
    http_pass: str | None = None
    http_htpasswd: Path | None = None
    http_priority: int = 30
    http_upstream_race: bool = True
    """Answer HTTP cache misses from a race of the HTTP substituters.

    Nix asks each substituter in turn for every path. pynixd asks all of
    its HTTP substituters at once instead: the `.narinfo` goes out from
    the first store that answers, and a NAR the local store does not hold
    streams, decompressed, from the highest-priority store that answered.
    Issue #85, nixidae/nixidae#1137.
    """
    http_upstream_negative_ttl: float = 60.0
    """How long a total upstream miss stays a miss for one `.narinfo` hash.

    A repeated miss would otherwise re-race every substituter on each
    request. Brief on purpose: an upstream that just fetched the path
    should become visible within about this long. Issue #85.
    """

    # How long the event loop may go without running a callback before
    # `/healthz` reports unhealthy. Everything this process serves is answered
    # by the loop, so a stall past this means it is serving nothing.
    #
    # 5 seconds is chosen to sit well under a probe's own timeout, so the
    # endpoint fails with a named reason before the probe fails with none. The
    # right value is a property of the workload -- a host pushing a large
    # closure stalls longer than a laptop -- so it is configurable, and
    # `pynixd_event_loop_lag_seconds` on /metrics is what to set it from.
    health_loop_lag_max: float = 5.0
    # The span the check looks back over. A stall must stop counting once the
    # process is serving again, or one bad transfer holds the pod unhealthy.
    health_loop_lag_window: float = 30.0
    # Dump every thread's Python stack when the loop stops beating for this
    # long. 0 disables it.
    #
    # **This exists because the failure is not reproducible.** A push on
    # nixlab2 sat at 100% CPU for about ten minutes with no progress, having
    # moved 42.9 MiB in total, so the cost is not proportional to the bytes and
    # a local benchmark has never produced it. The restart that follows
    # destroys the evidence.
    #
    # A watchdog thread reads a timestamp `LoopLagMonitor` writes every sample.
    # It works for the case that matters: CPU-bound Python releases the GIL
    # every switch interval, so the watchdog runs even while the loop does not.
    # A stall inside a C call that holds the GIL would defeat it.
    #
    # Well above `health_loop_lag_max`, because this writes a large amount to
    # stderr and a stall long enough to fail a probe is not by itself a fault
    # worth a stack dump. The ten-minute case is.
    stall_traceback_seconds: float = 30.0
    # A second place to write the stall dump, which a container restart cannot
    # take away. `None` writes to stderr only.
    #
    # **stderr is not a reliable sink for this one.** It reaches the container
    # log, which is the first place to look, but the stall this catches
    # typically ends in a liveness restart: that races the log shipper, and a
    # pod deleted and recreated takes the previous container's log with it. An
    # instrument whose only sink is destroyed by the event it records is not an
    # instrument.
    #
    # Point it at a persistent volume. On a pod that boots from a PVC, a path
    # under that volume survives both the restart and the recreation.
    stall_traceback_path: Path | None = None
    # Also arm `faulthandler`, whose dump names a C call that holds the GIL
    # while it runs. For debugging a stall only: that dump can segfault the
    # process when a thread exits during it (`StallWatchdog`).
    stall_traceback_gil_dump: bool = False
    # How large `stall_traceback_path` may grow. 0 removes the cap.
    #
    # A setting rather than a constant because a constant can only be changed
    # by a release, and the person who points the sink at a volume is the one
    # who knows what that volume can spare. Same argument as the path itself.
    #
    # 8 MiB is on the order of a thousand dumps, which is far past the point
    # where the file stops being a diagnostic and becomes a symptom. The cap
    # keeps the FIRST dumps and refuses later ones rather than rotating: the
    # first stall after a deployment usually explains the ones after it, and
    # rotation is what would throw it away.
    stall_traceback_max_bytes: int = STALL_TRACEBACK_MAX_BYTES
    http_upload_dir: Path | None = None

    https_port: int | None = None
    https_cert: Path | None = None
    https_key: Path | None = None

    reverse_acceptor: ReverseAcceptorSettings = Field(
        default_factory=lambda: ReverseAcceptorSettings(authorized_builder_keys=None)
    )
    reverse_initiator: ReverseInitiatorSettings = Field(default_factory=ReverseInitiatorSettings)

    admin_users: set[str] = Field(default_factory=set)
    """Deprecated: list the key in `ssh_admin_keys` instead.

    A session whose key is in `ssh_user_keys` and whose username is here is
    trusted. Every key that logs in under one name shares the grant, which
    is the problem `ssh_admin_keys` solves (nixidae/nixidae#532). Kept so a config
    that sets it keeps starting: an unknown key fails the whole settings.
    """

    session_builder_linger: float = 60.0
    """Seconds a store of a client's `builders` stays open with no session or build that needs it.

    A reopen costs a `nix daemon` start for a local machine and an SSH
    handshake for an `ssh-ng://` host, so a client that builds again within
    the linger keeps its store. nixidae/nixidae#1243.
    """

    gc_enabled: bool = True
    gc_interval: float = 3600.0
    gc_poll_interval: float = 60.0
    """Seconds between disk-pressure checks.

    The watch reads the disk usage, which costs a `statvfs` and no locks,
    against the full pass, which traces the roots under the garbage
    collector lock. A minute answers an hour of writes; the full pass stays
    the backstop for slow drift on `gc_interval`.
    """
    gc_high_watermark: float | None = None
    """Disk usage fraction that triggers a bounded pass between schedules.

    `None` keeps schedule-only collection. A number runs a pass as soon as
    the watch reads over it, so a fill spike does not wait out `gc_interval`.
    The pass is bounded by `gc_target_usage` on the store, which should sit
    below this: the watermark starts relief, the target ends it, and that
    gap is the hysteresis. Without a target the triggered pass is
    unbounded, like the scheduled one.
    """
    gc_watermark_max_passes: int = 20
    """Bounded passes per watermark trigger before the watch sleeps again.

    One pass relieves a little; a flood that lands all at once needs many,
    and a poll interval between each turns minutes of deleting into hours
    of waiting. The drive runs back to back while pressure stays over the
    store's target, stopping early when a pass frees nothing -- refused everything
    or planned nothing -- because more passes change nothing until the world
    does. Twenty covered a 95%-to-70% relief with room; raise it for a
    faster full drain, lower it to spread big reliefs across polls.
    """
    gc_cooldown: float = 900.0
    """Seconds between automatic GC triggers, watermark and scheduled alike.

    Reading usage is free, so the watch checks every poll -- but a hovering
    disk must not fire passes back to back forever. A watermark drive and a
    scheduled full pass each start a quiet window of this length during
    which further triggers stand down; the poll loop keeps watching, and the
    next trigger fires when the window lapses with pressure still over. Set
    it to a fraction of `gc_interval`: long enough that a covered hour is
    not re-collected, short enough that a fresh flood waits minutes, not an
    hour. Zero disables it, and every due trigger fires.
    """
    gc_liveness_interval: float | None = None
    """Seconds between Python liveness refreshes. `None` disables the background watch.

    Stable-root changes also trigger refreshes. Each collection refreshes liveness
    before planning, whether or not the background watch runs.
    """

    # Scheduling & Telemetry
    schedule_mode: ScheduleMode = ScheduleMode.auto
    ranking: StoreRankingSettings = Field(default_factory=StoreRankingSettings)
    placement_trace: PlacementTraceSettings = Field(default_factory=PlacementTraceSettings)

    # Substitution scheduling
    substitution_cache_maxsize: int = 100_000
    substitution_positive_ttl: float = 300.0
    substitution_negative_ttl: float = 15.0
    substitution_health_window: int = 64
    substitution_health_min_fill_ratio: float = 0.10
    substitution_health_min_success_ratio: float = 0.50
    substitution_query_timeout: float = 2.0
    substitution_import_timeout: float = 300.0
    force_substitute_on_destination: bool = False
    """Answer QueryValidPaths as if the client had set substitute=1.

    NIX-DEVIATION (#27): `daemon.cc:370` reads the substitute flag of op 31
    and runs `substitutePaths` before `queryValidPaths`, and `copyPaths` at
    `store-api.cc:1030` passes the client's `--substitute-on-destination`
    there. pynixd instead rewrites the flag to 1 on every client op 31 that
    did not set it, so each backend tries its own substituters for the
    missing paths before the client sends them. The difference is worth its
    cost where the destination sits next to a cache and the client does not:
    the flag is easy to leave out of a plain `nix copy` or a CI step, and
    then the whole closure crosses the slow link. It costs where the
    destination's substituter is slow or metered, because the bytes come
    from there instead of the client, so it stays off unless the operator
    takes that trade. To reverse it, measure the bytes on the client link
    against the bytes on the substituter link with the flag on and off; a
    parity run with it on reports the extra valid paths, which is the
    intended difference. Off keeps the gates byte-identical, so no
    `EXEMPTIONS` entry covers this. Issue #54.
    """

    pressure: PressureSettings = Field(default_factory=PressureSettings)

    # Plugins
    plugins: list[Path] = Field(default_factory=list)

    # Logging (the log-level threshold; filtering is handled by plugins)
    log_level: str = "WARNING"

    # Update Prometheus metrics (`/metrics`) on the hot paths: per-operation
    # counters and durations, per-transfer byte and path counters. On, because
    # a daemon nobody watches is a daemon nobody can explain. Off for a local
    # deployment that scrapes nothing: each `.labels()` takes a lock and a
    # lookup, each `.observe()` walks buckets, and a build pays both for
    # every one of its tens of thousands of operations. The internal
    # per-session timing (`client_op_timing`) is not this switch and stays:
    # it is a dict update, and the benchmark reads it.
    metrics_enabled: bool = True

    # Sign paths added through `AddToStore` with pynixd's own keys, by relay
    # (`SignPathInfo`) or by decomposing (local sign, then `AddSignatures`).
    # Off: nothing populates the signing keys in the standard deployment,
    # where the step is a pool acquire and an upstream round trip per added
    # path for zero signatures. An operator with signing keys sets this true,
    # and added paths carry pynixd's signature again.
    sign_added_paths: bool = False

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,  # noqa: ARG003
        file_secret_settings: PydanticBaseSettingsSource,  # noqa: ARG003
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Configure settings source priority: init > env > JSON config file."""
        return (init_settings, env_settings, _ConfigFileSource(settings_cls))

    def to_stores(self) -> dict[StoreId, Store]:
        """Convert all store specs to live Store instances."""
        stores: dict[StoreId, Store] = {}
        for key, spec in self.stores.items():
            spec.settings = self
            store = spec.to_store(store_id=key)
            stores[store.store_id] = store

        if LOCAL_STORE_ID not in stores:
            spec = LocalSocketStoreSpec(
                store_id=LOCAL_STORE_ID,
                monitor=False,
                settings=self,
            )
            # `spec.to_store`, so the implicit local store honours `use_db`
            # like a configured one. This line read `LocalStore(spec)`, which
            # ignored the option and hardcoded the answer. `use_db` defaults to
            # true, and a configured `stores.local` already reached the loop
            # above and got a `LocalDBStore` -- so the SQLite fast paths were
            # on for anyone who wrote the store out and off for everyone who
            # did not, including every deployment of `pynixd daemon`.
            #
            # `LocalStoreDB.open` degrades on its own when it cannot open the
            # database: it returns an inactive instance, every fast path of
            # `LocalDBStore` returns `None` for that, and `DaemonStore.execute`
            # falls through to the wire. So "on by default" costs a store that
            # cannot read the database nothing but one warning.
            stores[LOCAL_STORE_ID] = spec.to_store(store_id=str(LOCAL_STORE_ID))

        return stores


StoreSpecBase.model_rebuild()
LocalSocketStoreSpec.model_rebuild()
PynixdSettings.model_rebuild()
