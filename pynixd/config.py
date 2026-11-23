"""Pydantic-settings model for the PYNIXD_CONFIG JSON file and env vars."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd_store.s3_url import https_url
from pynixd_store.store_layout import Overlay, StoreLayout

from .constants import STALL_TRACEBACK_MAX_BYTES
from .experiments import Experiment
from .nix_config import NixConfig
from .op_routes import OPERATION_NAMES, OpRoute
from .store_mode import StoreMode

STABLE_CACHE = "https://cache.nixos.org"
"""The cache `gc_stable` holds stable by default: it removes nothing."""


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
        GhaCacheStore,
        HTTPBinaryCacheStore,
        LocalBinaryCacheStore,
        LocalDBStore,
        LocalStore,
        LocalStoreSubstituter,
        S3BinaryCacheStore,
        SSHSocketStore,
        SSHSubprocessStore,
        Store,
    )
    from .store.build_pods import BuildPodStore


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
    """A store whose last sample shows less free disk than this, over its host's own `min-free`, gets no build.

    A pynixd peer reports its `min-free` with each sample (`DiskSpace.floor_bytes`); other hosts count 0.
    nixidae/nixidae#1013.
    """

    memory_headroom_bytes: int = Field(default=1024**3, ge=0)
    """Memory left free beside a build's expected peak when placement picks its cores; the pressure limiter's
    `min_available_memory_mb` by default. nixidae/nixidae#1558."""

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


class TracingSettings(BaseModel):
    """OpenTelemetry spans of each build's life, sent over OTLP/HTTP. nixidae/nixidae#1570."""

    endpoint: str | None = None
    """The OTLP/HTTP traces URL, such as VictoriaTraces' `http://host:10428/insert/opentelemetry/v1/traces`.
    `None` sends nothing and starts no exporter."""

    bearer_token_file: Path | None = None
    """A file that holds a bearer token, sent as `Authorization: Bearer <token>`. Read once, at start."""

    service_name: str = "pynixd"

    queue_size: int = Field(default=2048, ge=1)
    """Ended spans the exporter's thread holds. Past it, new spans are dropped."""

    schedule_delay: float = Field(default=5.0, gt=0)
    """Seconds between two exports, unless a full batch goes first."""

    export_timeout: float = Field(default=10.0, gt=0)
    """Seconds one export request may take."""

    shutdown_timeout: float = Field(default=5.0, ge=0)
    """Seconds pynixd waits at exit for the last spans to go out. A dead endpoint delays exit by no more."""

    slow_op_seconds: float = Field(default=1.0, ge=0)
    """A client operation other than a build or a transfer becomes a span when it takes this long, or fails."""


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

    memory_full_below_available: float = 0.25
    """`memory` `full` counts only while `MemAvailable` is under this share of `MemTotal`.

    Swap-in of idle pages stalls their owners, and the kernel counts that as
    host-wide `full`: dynhetz read `full avg10=1.67` with 38 of 64 GB free and
    17 GB swapped out, which held the limit at 1. nixidae/nixidae#1260."""

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

    build_cgroup: Path | None = None
    """The cgroup v2 directory the local store's builds run in, such as
    `/sys/fs/cgroup/system.slice/nix-daemon.service`. Its memory limits, and its ancestors', bound the room, and its
    `memory.pressure` is the memory signal. `None`: the host's. The NixOS module sets the daemon's unit.
    nixidae/nixidae#1568."""

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
    priority: float = 1.0
    trusted: bool = True
    """Import a substitute from this store without checking its signatures, as Nix's `?trusted=1`.

    A store in the configuration is a builder or a peer, and its outputs are
    unsigned, so it is trusted by default (User's decision, nixidae/nixidae#1008).
    A binary cache is not. A store that a substituter URL opens takes the
    URL's `trusted`, false when absent, as in Nix (`store_url`).
    """
    want_mass_query: bool | None = None
    """`?want-mass-query=`: whether `QuerySubstitutablePaths` asks this store (`store-api.cc:526`).

    `None` is Nix's default: false, and a binary cache reads it from its `nix-cache-info`
    (`binary-cache-store.cc:66`, which only sets the default).
    """
    score_penalty: int = 0
    mandatory_features: set[str] = Field(default_factory=set)
    """Features a derivation must require to build here: column 7 of a machines line (`Machine::mandatoryMet`)."""
    pool: str | None = None
    """The provisioning pool that launched this builder (a reverse store); a client's `builders` may name it."""
    last_resort: bool = False
    """Build here only what no other store can: capability, never offload.

    A derivation goes to a last-resort store only when no other healthy
    store that takes builds supports its system and features, or each that does
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
    gc_stable: bool | None = None
    """Whether this store keeps every path it ever held, so a path it holds needs no cold-tier copy.

    `None` is the store kind's default: true for `https://cache.nixos.org`, which removes nothing,
    false for the rest. A cold tier (`gc_cold`) uploads a closure without the paths a stable store
    holds. nixidae/nixidae#1417.
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

    def stable_by_default(self) -> bool:
        """What `gc_stable` is when unset."""
        return False

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
    store in its own memory. A build without the sandbox, a fixed-output or impure build, a build for another
    system, and a build as a build user stay on the host (`pynixd_store.builder.site.choose_site`), and so do a
    fixed-output and an impure build unless `network` is on.

    On a Mac the guest builds aarch64-linux and, through Rosetta, x86_64-linux, and nothing for darwin
    (nixidae/nixidae#1149). `network` is Linux's alone there: pynixd leaves a fixed-output or impure Linux build
    to the store's daemon.
    """

    memory_mib: int = Field(default=2048, gt=0)
    """The guest's memory. Freed pages go back to the host (free page reporting)."""
    cpus: int = Field(default=2, gt=0, le=255)
    dax_window_mib: int = Field(default=4096, ge=0)
    """The address space the guest maps the store through. It costs no host memory. On a Mac it must be
    above 0: libkrun fails to map an empty window and the guest does not boot (measured)."""
    disk_gib: int = Field(default=1024, gt=0)
    """The build directory's size: a sparse image on the host's disk (XFS with a 64 MiB log; ext4 on a Mac),
    which costs what the build writes. A build that fills it fails with ENOSPC; the host's memory never holds
    it."""
    network: Annotated[
        bool,
        Experiment(
            replaces="a fixed-output or impure build runs in the namespace helper alone",
            issue=1186,
            tests=("tests/functional/test_build_vm.py",),
        ),
    ] = False
    """Run fixed-output and impure builds in the VM too, on a NIC that passt serves.

    passt carries the guest's TCP, UDP and ICMP out through the host's own sockets, and the guest's DNS to the
    host's resolver. NIX-DEVIATION (#27): Nix's fixed-output sandbox shares the host's network namespace
    (`linux-derivation-builder.cc:576`: `CLONE_NEWNET` for a sandboxed derivation alone), so a fetch reaches every service on the host's loopback and can listen
    on the host's interfaces. In the VM a fetch reaches neither: passt maps no host loopback and forwards no
    port to the guest. A fetch from `http://127.0.0.1:...` fails here and works in Nix. Measure how many
    fixed-output builds fetch from the host's loopback to reverse it. A sandboxed build gets no NIC.
    """


class DarwinBuildVmSpec(BaseModel):
    """A sandboxed darwin build in a macOS guest started from a snapshot (nixidae/nixidae#1168).

    The guest runs the darwin helper, profile and all, and sees the build's input closure alone through a
    read-only share. A build without the sandbox, a build for another system and a recursive build stay on the
    host (`pynixd_store.builder.site.choose_site`).
    """

    snapshot: Path
    """A directory `darwin_builder.py save` made with `PYNIXD=1` (`vivarium/docs/design/macos-guest-spike`)."""


class RunnerSandboxSpec(BaseModel):
    """A build pod's runner container (`pynixd_store.builder.runner`, nixidae/nixidae#1560)."""

    top: Path = Path("/build/runner")
    """The build's top directory; the runner mounts its `build` directory at `/build` by subPath."""
    ready: Path = Path("/build/runner-ready")
    """The file the controller's postStart hook waits for: the runner starts once it exists."""
    container: str = "runner"


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


class PostgresSpec(BaseModel):
    """The store database on a PostgreSQL server, in place of `db/db.sqlite`. nixidae/nixidae#1209."""

    conninfo: SecretStr
    """A libpq connection string or URI. pynixd logs and reports it as `**********`.

    A password can stay out of it: libpq reads `passfile`, `PGPASSFILE` and `PGPASSWORD`.
    """
    max_conns: int = Field(default=8, ge=1)
    """The connections pynixd holds open to the server."""
    open_seconds: float = Field(default=30.0, gt=0)
    """How long the start waits for the server. The store does not start without it."""


class LocalSocketStoreSpec(StoreSpecBase):
    """A local Nix daemon accessed via Unix domain socket.

    Optionally wraps the daemon with a local SQLite database (``use_db``)
    for fast-path resolution of queries like ``IsValidPath``, ``QueryPathInfo``,
    and ``QueryReferrers``.
    """

    type: Literal["local-socket"] = "local-socket"

    overlay: OverlaySpec | None = None
    """A `local-overlay://` store over a lower store, for a chroot root (`pynixd_store.store_layout.Overlay`)."""

    lower_store: Annotated[
        Path | None,
        Experiment(
            replaces="a store holds the files of each path it calls valid",
            issue=1560,
            tests=("tests/unit/test_lower_store.py",),
        ),
    ] = None
    """The root of an immutable store under this one, such as a build pod's nixkube farm.

    Its paths are valid here, and their files stay at `<lower_store>/nix/store`:
    no OverlayFS merges the two (`pynixd_store.lower_store_engine`). pynixd
    opens its database read-only and immutable, and writes only this store. It
    needs `daemon = false`, as a Nix daemon would not know the lower store.
    """

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
    op_routes: Annotated[
        dict[str, OpRoute],
        Experiment(
            replaces="each operation takes the route its answer declares",
            issue=1155,
            tests=("tests/unit/test_op_routes.py",),
        ),
    ] = Field(default_factory=dict)
    """The route of each named operation: `daemon`, `shadow` or `pynixd`.

    Keyed by the operation's name, `QueryReferrers`. An operation left out
    takes the default its answer declares; `pynixd/op_routes.py` says what
    each route does. The store refuses to start on a route it cannot take.
    """
    pynixd_writes: Annotated[
        bool,
        Experiment(
            replaces="the daemon makes every change to the store",
            issue=1155,
            tests=("tests/unit/test_write_router.py",),
        ),
    ] = False
    """Whether pynixd itself may change the store: its files, its database, its state directory.

    Off, every answer that writes goes to the daemon, whatever `op_routes`
    says, and a route to `pynixd` for one refuses to start. A read that
    tidies as Nix does (FindRoots unlinking a stale `gcroots/auto` link,
    reaping a dead process's temporary roots file) leaves that to the
    daemon. The test suites turn it on; a deployment turns it on by hand.
    """
    daemon: Annotated[
        bool,
        Experiment(
            replaces="a Nix daemon under pynixd answers what pynixd does not",
            issue=1155,
            tests=("tests/unit/test_no_daemon_spec.py", "tests/functional/test_no_daemon_refusals.py"),
        ),
    ] = True
    """Whether a Nix daemon runs under pynixd for this store.

    Off, pynixd opens the store itself (`pynixd_store.create_store`), starts
    no daemon and connects to none, and every operation takes the `pynixd`
    route. A request that only the daemon can answer fails with an error that
    names it. It needs `use_db`, `pynixd_writes` and a store pynixd manages.
    nixidae/nixidae#1155.
    """
    read_only: Annotated[
        bool,
        Experiment(
            replaces="pynixd and its daemon may change the store",
            issue=1309,
            tests=("tests/unit/test_read_only_db.py", "tests/functional/test_nixft_read_only_store.py"),
            details=("immutable",),
        ),
    ] = False
    """Serve the store as `local?read-only=true` serves it: pynixd changes nothing in it.

    pynixd takes no big lock and no temporary root, and opens the database
    `mode=ro`, so it sees what another writer commits. A store with no
    database refuses to start. A write of a path already valid succeeds, and
    any other write fails as Nix's does. With a daemon, the daemon opens the
    store read-only and takes every write. nixidae/nixidae#1309.

    NIX-DEVIATION (#27): `LocalStore::openDB` opens every read-only store's
    database `SQLiteOpenMode::Immutable` (`local-store.cc:514`), and its manual
    allows the mode only on a read-only file system. pynixd opens `mode=ro`
    unless `immutable` says the storage cannot change, so a read-only pynixd
    may serve a store another process writes. A managed daemon still opens it
    immutable. To reverse: a deployment that never serves a live store.
    """
    immutable: bool = False
    """The storage of a `read_only` store cannot change: a read-only medium, or a snapshot.

    SQLite then reads it with `immutable=1`, as a temporary file: no lock, no
    `-wal` and no `-shm` (`pager.c:5034` of SQLite 3.53). `mode=ro` cannot open
    a WAL database in a directory pynixd may not write once its `-shm` is gone:
    it must create one (`os_unix.c:5031`). Wrong when anything writes the store:
    an immutable reader sees no frame the `-wal` holds.
    """
    build_vm: Annotated[
        BuildVmSpec | None,
        Experiment(
            replaces="a sandboxed build runs in the namespace helper alone",
            issue=1186,
            tests=("tests/functional/test_build_vm.py",),
        ),
    ] = None
    """Run pynixd's sandboxed builds in a microVM; `None` runs them in the namespace helper alone."""
    darwin_build_vm: Annotated[
        DarwinBuildVmSpec | None,
        Experiment(
            replaces="a sandboxed darwin build runs under the darwin profile on the host",
            issue=1168,
            tests=("tests/functional/test_build_darwin_vm.py",),
        ),
    ] = None
    """Run pynixd's sandboxed darwin builds in a macOS guest; `None` runs them on the host."""
    sibling_sandbox: Annotated[
        Path | None,
        Experiment(
            replaces="a sandboxed build runs in the namespace helper, in pynixd's own process tree",
            issue=1560,
            tests=("tests/provisioning/kubernetes_sibling.py",),
        ),
    ] = None
    """Run pynixd's sandboxed builds in a pod's sandbox container, which serves `sibling run` in this directory;
    `None` runs them here. Takes the place of `build_vm`."""
    runner_sandbox: Annotated[
        RunnerSandboxSpec | None,
        Experiment(
            replaces="a build's builder is a process of pynixd's helper",
            issue=1560,
            tests=("tests/unit/test_runner_site.py",),
        ),
    ] = None
    """Run each build in the pod's runner container, which nixkube's NRI plugin makes Nix's sandbox, and whose
    spec central wrote. `None` runs it here. Takes the place of `build_vm` and `sibling_sandbox`."""
    postgres: Annotated[
        PostgresSpec | None,
        Experiment(
            replaces="the store database is SQLite, `db/db.sqlite` in the state directory",
            issue=1209,
            tests=("tests/unit/test_postgres_spec.py", "tests/unit/test_postgres_db.py"),
        ),
    ] = None
    """Keep the store database on a PostgreSQL server, so that more than one pynixd can serve the store.

    Only a store with no daemon takes it: a Nix daemon reads SQLite alone. The store's files,
    its big lock and its temporary roots stay in the state directory.
    """
    usage_sample_seconds: float = Field(default=5.0, ge=0)
    """Seconds between two samples of a running build's cgroup (`builder/cgroup.py`); 0 takes none."""

    @property
    def environment(self) -> dict[str, str]:
        """`extra_env`, with `nix_config` after any `NIX_CONFIG` it sets: what the store's Nix runs with.

        It renders as `NixConfig.to_nix_config_env` does: a setting it names replaces nix.conf's, except
        the few that method adds to with an `extra-` prefix.
        """
        env = dict(self.extra_env or {})
        if self.nix_config is not None and (rendered := self.nix_config.to_nix_config_env()):
            env["NIX_CONFIG"] = "\n".join(filter(None, (env.get("NIX_CONFIG"), rendered)))
        return env

    @model_validator(mode="after")
    def _check_the_op_routes(self) -> LocalSocketStoreSpec:
        unknown = sorted(set(self.op_routes) - OPERATION_NAMES)
        if unknown:
            raise ValueError(f"op_routes names no operation of the protocol: {', '.join(unknown)}")
        mode = self.mode
        if not mode.reads_database and any(route is not OpRoute.daemon for route in self.op_routes.values()):
            raise ValueError("op_routes: a store with use_db = false answers every operation through the daemon")
        if mode.read_only and self.overlay is not None:
            raise ValueError("read_only takes no overlay: an overlay store writes its upper layer")
        if not mode.has_daemon:
            if self.managed is False:
                raise ValueError("daemon = false needs a store pynixd manages, not the machine's own daemon")
            if routed := sorted(op for op, route in self.op_routes.items() if route is not OpRoute.pynixd):
                raise ValueError(f"op_routes: with daemon = false, every route is pynixd, not for {', '.join(routed)}")
        if self.postgres is not None:
            if mode.has_daemon:
                raise ValueError("postgres needs daemon = false: a Nix daemon reads its database from SQLite alone")
            if self.overlay is not None:
                raise ValueError("postgres takes no overlay: the lower store's database is SQLite")
            if mode.read_only:
                raise ValueError("postgres takes no read_only: pynixd opens no read-only store on a server")
        if self.lower_store is not None:
            if mode.has_daemon:
                raise ValueError("lower_store needs daemon = false: a Nix daemon would not know the lower store")
            if self.overlay is not None or mode.read_only or self.postgres is not None or self.store_dir is not None:
                raise ValueError("lower_store needs a writable chroot store with SQLite, and no overlay")
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

    @property
    def mode(self) -> StoreMode:
        """The mode the five flags name (`pynixd/store_mode.py`); `ValueError` when they name none."""
        return StoreMode.of(
            use_db=self.use_db,
            daemon=self.daemon,
            pynixd_writes=self.pynixd_writes,
            read_only=self.read_only,
            immutable=self.immutable,
        )

    def layout(self) -> StoreLayout:
        """The three directories of the store that this spec names."""
        if self.store_dir is not None and self.state_dir is not None:
            layout = StoreLayout.relocated_store(self.store_dir, self.state_dir, self.log_dir)
            return layout.as_read_only(immutable=self.immutable) if self.read_only else layout
        layout = StoreLayout.chroot(self.store_path)
        if self.lower_store is not None:
            return layout.with_lower(StoreLayout.chroot(self.lower_store).as_read_only(immutable=True))
        if self.overlay is None:
            return layout.as_read_only(immutable=self.immutable) if self.read_only else layout
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
        if spec.mode.reads_database:
            return LocalDBStore(spec)
        return LocalStore(spec)


class ExternalUnixStoreSpec(StoreSpecBase):
    """A remote Nix daemon connected via an external Unix socket.

    Used as a substitution source only — builds and GC are disabled.
    """

    type: Literal["external-unix"] = "external-unix"
    socket_path: Path = Path("/nix/var/nix/daemon-socket/socket")
    monitor: bool = False
    no_schedule: bool = True
    gc_enabled: bool = False
    probe: bool | None = False
    """A substituter builds nothing, so it is never sent the probe builds."""

    def reference_key(self) -> object | None:
        """`unix://<socket_path>?root=<store_path>`, the trust and `want-mass-query`."""
        return ("unix", self.socket_path, self.store_path, self.trusted, self.want_mass_query)

    def to_store(self, store_id: str) -> ExternalUnixStore:
        """Build an ``ExternalUnixStore`` from this spec."""
        from .store import ExternalUnixStore

        return ExternalUnixStore(
            self.model_copy(update={"store_id": StoreId(store_id)}),
        )


class LocalStoreSubstituterSpec(StoreSpecBase):
    """A local store at the chroot root `store_path` that pynixd only reads: a substituter URL's `local` store.

    Only a substituter URL makes one (`store_url`), so no configuration file names this type.
    """

    type: Literal["local-store-substituter"] = "local-store-substituter"
    no_schedule: bool = True
    gc_enabled: bool = False
    probe: bool | None = False

    def reference_key(self) -> object | None:
        """The root, the trust and `want-mass-query`."""
        return ("local", self.store_path, self.trusted, self.want_mass_query)

    def to_store(self, store_id: str) -> LocalStoreSubstituter:
        """Build a ``LocalStoreSubstituter`` from this spec."""
        from .store import LocalStoreSubstituter

        return LocalStoreSubstituter(self.model_copy(update={"store_id": StoreId(store_id)}))


class ReverseStoreSpec(StoreSpecBase):
    """Configuration for a reverse store (builder connects to controller).

    Reverse stores are created dynamically when a builder registers with
    the controller via the reverse server. This spec exists to provide a
    type in the StoreSpec union and document the expected fields.
    """

    type: Literal["reverse"] = "reverse"
    build_cores: int | None = Field(default=None, ge=1)
    """The most cores one build gets: the builder's own Nix `cores`. `None`: every core its host has."""

    def to_store(self, store_id: str) -> DaemonStore:
        raise NotImplementedError("Reverse stores are created dynamically, not from config")


class BuildPodFarmSpec(BaseModel):
    """Each build pod's `/nix` is a farm of one store path central makes for the build (spike, nixidae/nixidae#1560).

    The path is a text file whose references are the build's inputs and every store path the template names,
    so its closure is what the pod runs and builds from. A nixkube node realises it from central, its
    substituter, and binds each closure path read-only (`presentation: farm`). With nixkube's `storeVolume`
    the farm's store is writable, on the pod's own emptyDir, and nixkube's database holds every closure path
    valid: the pod's pynixd uses `/nix` as its store and copies no input in. A read-only farm instead takes
    `LocalSocketStoreSpec.lower_store`.
    """

    volume: str
    """The template's CSI volume that central points at the build's path."""


class BuildPodStoreSpec(StoreSpecBase):
    """Each build a pod of its own from `provisioning.kubernetes.templates`, which dials central (spike).

    The pod reaches central's SSH listener at `central_host`, so `ssh_port` must be set. nixidae/nixidae#1560.
    """

    type: Literal["kubernetes-build-pods"] = "kubernetes-build-pods"
    templates: list[str] = Field(min_length=1)
    """The templates of the store's pods, in order of preference. With more than one, each pod takes the first
    whose `nodeSelector` a Ready node satisfies, so central lists nodes (cluster-wide `list` on nodes). One
    store with a template per kind of node, not a store per kind: each store keeps its own queue and slots."""
    container: str = "pynixd"
    """The template's container that runs `pynixd build-one`: its termination message is the result."""
    central_host: str
    central_port: int = 2234
    gc_enabled: bool = False
    farm: BuildPodFarmSpec | None = None
    """Give each pod a nixkube farm of its build's inputs as its `/nix`; `None` copies the inputs in."""
    priority_classes: list[str] = []
    """The PriorityClass of a pod by its build's depth in recursive Nix: the first for a client's build, the next
    for a build nested in it, the last for every deeper one. Empty keeps the template's. Each deeper class
    takes a higher value, all below 0, and `PreemptLowerPriority`: a nested pod on a full cluster then evicts a
    shallower build pod and no ordinary pod (nixidae/nixidae#1576)."""
    runner_impure_env: dict[str, str] = {}
    """Nix's `impure-env` for a template with a runner container: what a fixed-output build's `impureEnvVars`
    take there. Central writes it into the pod spec, which anyone who reads the pod sees."""

    def priority_class(self, depth: int) -> str | None:
        """The PriorityClass of a build pod at *depth*, or `None` for the template's."""
        if not self.priority_classes:
            return None
        return self.priority_classes[min(depth, len(self.priority_classes) - 1)]

    def to_store(self, store_id: str) -> BuildPodStore:
        from .store.build_pods import BuildPodStore

        return BuildPodStore(self.model_copy(update={"store_id": StoreId(store_id)}))


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
    join_ticket: Path | None = None
    """A `JoinTicket` (JSON) a provisioner gave this machine, read at each dial.

    Its token goes in the registration, and its host, port and controller key
    replace the settings here. nixidae/nixidae#1198."""
    idle_lease: Path | None = None
    """Where the `Lease` a controller sends at registration goes, for the idle guard. `None`: nowhere."""
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


ProviderName = Literal["vivarium", "ec2", "gha", "nomac", "kubernetes"]
"""A provisioning provider: each has its own settings block in `ProvisioningSettings`."""


class ProvisioningPoolSettings(BaseModel):
    """One kind of builder machine the provisioner starts. nixidae/nixidae#1198."""

    model_config = ConfigDict(extra="forbid")

    name: str
    provider: ProviderName | None = None
    """Who starts this pool's machines; `None` takes `ProvisioningSettings.provider`."""
    template: str
    """The provider's name for the kind of machine: a vivarium template, an EC2 launch template."""
    systems: list[str] = Field(min_length=1)
    features: list[str] = Field(default_factory=list)
    mandatory_features: list[str] = Field(default_factory=list)
    """Features a build must need to launch a machine of this pool or to run on one (`Machine::mandatoryMet`).
    Empty: any build its systems and features cover."""
    pressure_wait: float = Field(default=60.0, ge=0)
    """Seconds a build that a busy store holds must wait before it counts as demand. A build that no
    store can take counts at once."""
    max_instances: int = Field(default=1, ge=0)
    min_instances: int = Field(default=0, ge=0)
    slots: int = Field(default=1, ge=1)
    """Builds one machine takes at once; its builder declares the same `max_jobs`."""
    idle_seconds: float = Field(default=300.0, ge=0)
    """The shortest idle spell that ends a machine. A machine is idle for as long as it took to boot,
    between this and `idle_max_seconds`: a second launch would cost that boot again."""
    idle_max_seconds: float | None = Field(default=None, ge=0)
    """The longest idle spell; `None`: no cap but `launch_timeout`, which bounds the boot."""
    launch_timeout: float = Field(default=180.0, gt=0)
    """Seconds a machine may take from launch to registration before it is ended. An EC2 builder took
    21-29 s on 2026-11-17."""
    single_use: bool = False
    """Each machine takes one build and is ended when that build ends, so no build sees what another left.
    `slots` must be 1. nixidae/nixidae#1560."""

    @model_validator(mode="after")
    def _single_use_has_one_slot(self) -> ProvisioningPoolSettings:
        if self.single_use and self.slots != 1:
            raise ValueError(f"pool {self.name}: a single_use pool has one slot, not {self.slots}")
        return self

    @model_validator(mode="after")
    def _mandatory_are_features(self) -> ProvisioningPoolSettings:
        if extra := set(self.mandatory_features) - set(self.features):
            raise ValueError(f"pool {self.name}: mandatory features not in its features: {sorted(extra)}")
        return self

    @model_validator(mode="after")
    def _idle_bounds(self) -> ProvisioningPoolSettings:
        if self.idle_max_seconds is not None and self.idle_max_seconds < self.idle_seconds:
            raise ValueError(f"pool {self.name}: idle_max_seconds is less than idle_seconds")
        return self


class Ec2TemplateSettings(BaseModel):
    """One kind of EC2 spot builder: a stock image that installs pynixd at boot. nixidae/nixidae#1198."""

    model_config = ConfigDict(extra="forbid")

    instance_types: list[str] = Field(min_length=1)
    """Tried in order: a type with no spot capacity now gives way to the next. All of one `arch`."""
    arch: Literal["x86_64", "aarch64"] = "x86_64"
    """The CPU of `instance_types`: it picks the default image and installer."""
    image: str | None = None
    """An AMI id, or `resolve:ssm:<parameter>`, which EC2 resolves at each launch. `None`: Ubuntu 26.04 for `arch`."""
    root_device: str = "/dev/sda1"
    """The image's root device name: Ubuntu's."""
    root_gib: int = Field(default=64, ge=8)
    subnet_id: str | None = None
    """`None` takes the default VPC's subnet in a zone EC2 picks."""
    security_group_ids: list[str] = Field(default_factory=list)
    """A builder only dials out; the default VPC's default group allows that."""
    installer_url: str | None = None
    """`None`: the `pynixd-installer` release's tarball for `arch`."""
    idle_poweroff: float = Field(default=600.0, gt=0)
    """The builder's own guard: it powers off, and so terminates, after this long with no build, until
    the controller sends its idle limit at registration (`Lease`). Then the guard uses that limit."""
    bootstrap_deadline: float = Field(default=180.0, gt=0)
    """The builder powers off if setting itself up takes longer."""

    @property
    def image_id(self) -> str:
        cpu = {"x86_64": "amd64", "aarch64": "arm64"}[self.arch]
        return (
            self.image
            or f"resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/{cpu}/hvm/ebs-gp3/ami-id"
        )

    @property
    def installer(self) -> str:
        return self.installer_url or (
            f"https://github.com/nixidae/nixidae/releases/download/pynixd-installer/pynixd-{self.arch}-linux.tar.zst"
        )


class Ec2Settings(BaseModel):
    """Where and as whom the EC2 provider launches. nixidae/nixidae#1198."""

    model_config = ConfigDict(extra="forbid")

    region: str
    profile: str | None = None
    """An AWS profile; `None` takes botocore's default chain (environment, instance role)."""
    templates: dict[str, Ec2TemplateSettings]
    resource_tags: dict[str, str] = Field(default_factory=lambda: {"pynixd-cloud-builder": "true"})
    """Added to every resource a launch makes, so one tag finds all of them."""
    endpoint: str | None = None
    """`None` takes the region's public endpoint."""


class GhaTemplateSettings(BaseModel):
    """One kind of GitHub Actions job builder: a runner that installs pynixd from the tarball. nixidae/nixidae#1473.

    Linux runners only: the job holds itself open with the idle guard, which needs systemd.
    """

    model_config = ConfigDict(extra="forbid")

    runs_on: list[str] = Field(default_factory=lambda: ["ubuntu-latest"], min_length=1)
    """The job's `runs-on` labels: GitHub's runners, or another service's that runs GitHub Actions jobs."""
    arch: Literal["x86_64", "aarch64"] = "x86_64"
    """The runners' CPU: it picks the default installer."""
    installer_url: str | None = None
    """`None`: the `pynixd-installer` release's tarball for `arch`."""
    idle_seconds: float = Field(default=600.0, gt=0)
    """The job's own guard ends it after this long with no build, until the controller sends its idle
    limit at registration (`Lease`). Then the guard uses that limit."""
    bootstrap_minutes: int = Field(default=10, ge=1)
    """The install step's `timeout-minutes`. A runner's queue time comes before it; the pool's
    `launch_timeout` must cover both."""

    @property
    def installer(self) -> str:
        return self.installer_url or (
            f"https://github.com/nixidae/nixidae/releases/download/pynixd-installer/pynixd-{self.arch}-linux.tar.zst"
        )


class GhaSettings(BaseModel):
    """Which workflow the GitHub Actions provider dispatches, and as whom. nixidae/nixidae#1473."""

    model_config = ConfigDict(extra="forbid")

    repository: str = Field(pattern=r"^[\w.-]+/[\w.-]+$")
    """`owner/name`: the repository that holds the workflow and the `PYNIXD_TICKET_KEY` secret."""
    workflow: str = "pynixd-builder.yml"
    """The workflow's file name, under `.github/workflows`."""
    ref: str = "main"
    token_file: Path
    """A GitHub token that may dispatch and cancel the repository's workflow runs (`actions: write`)."""
    ticket_key_file: Path
    """The key that seals each join ticket: the repository secret `PYNIXD_TICKET_KEY`, base64 of 32 bytes."""
    api_url: str = "https://api.github.com"
    templates: dict[str, GhaTemplateSettings]
    history_seconds: float = Field(default=86400.0, gt=0)
    """How far back the provider lists runs. A run older than this counts as gone; GitHub ends a hosted
    runner's job after 6 h."""


class NomacTemplateSettings(BaseModel):
    """One kind of NoMac Mac: its limits and how it becomes a builder. Every limit is set on each start, so
    NoMac's own defaults never apply."""

    model_config = ConfigDict(extra="forbid")

    installer_url: str = (
        "https://github.com/nixidae/nixidae/releases/download/pynixd-installer/pynixd-aarch64-darwin.tar.xz"
    )
    volume: Literal["case-insensitive", "case-sensitive"] = "case-insensitive"
    """The /nix volume's format, as ghanix's `macosVolume`."""
    max_duration_seconds: int = Field(default=8 * 3600, ge=60)
    """NoMac stops the Mac after this long, whatever it does."""
    idle_timeout_seconds: int = Field(default=1800, ge=60, le=3600)
    """NoMac stops the Mac after this long with no command, SSH or CPU use: the end of a Mac whose
    controller died. The controller ends an idle builder sooner, by the pool's `idle_seconds`."""
    spend_cap_usd: float = Field(default=2.0, gt=0)
    """NoMac stops the Mac once it has cost this much. A Mac costs $0.90 an hour (2026-11-19)."""
    parallel: bool = False
    """Start beside a Mac the account already runs. Without it, a start while one runs is `NoCapacity`."""
    bootstrap_seconds: int = Field(default=600, ge=60, le=3600)
    """The bootstrap job's own limit. The pool's `launch_timeout` ends a Mac that has not joined sooner."""

    @property
    def volume_format(self) -> str:
        """The format as `diskutil` names it."""
        return "APFS" if self.volume == "case-insensitive" else "Case-sensitive APFS"


class NomacSettings(BaseModel):
    """Which NoMac account the NoMac provider starts Macs in. nixidae/nixidae#1198."""

    model_config = ConfigDict(extra="forbid")

    api_key_file: Path
    """A NoMac API key (`nmk_...`)."""
    api_url: str = "https://nomac.app/api/v1"
    state_file: Path = Path("/var/lib/pynixd/nomac.json")
    """The tags of each start, which a NoMac session cannot carry, and its join ticket until it is used."""
    templates: dict[str, NomacTemplateSettings]
    history_seconds: float = Field(default=86400.0, gt=0)
    """How long the provider remembers a start whose session is gone."""


class KubernetesTemplateSettings(BaseModel):
    """One kind of builder pod. nixidae/nixidae#1560."""

    model_config = ConfigDict(extra="forbid")

    pod: dict[str, Any]  # A Pod manifest: its schema is the API server's, which validates it at each launch.
    """The pod, as `kubectl` takes it. The provider sets its name, adds a label, the tags annotation and the
    ticket volume, and `restartPolicy: Never` unless the pod sets one. Its pynixd runs the reverse initiator
    with `join_ticket` at `ticket_dir`/ticket. The container needs a passwd entry for its uid: without one, uid 0
    is no `root`, and the controller's session to the builder is an untrusted user's."""
    container: str | None = None
    """The container the ticket is mounted in; `None`: the first."""
    ticket_dir: Path = Path("/run/pynixd-join")


class KubernetesSettings(BaseModel):
    """Which cluster and namespace the Kubernetes provider starts builder pods in. nixidae/nixidae#1560."""

    model_config = ConfigDict(extra="forbid")

    kubeconfig: Path | None = None
    """`None`: kr8s' own search, the pod's service account first, then `KUBECONFIG` and `~/.kube/config`."""
    context: str | None = None
    namespace: str = "default"
    templates: dict[str, KubernetesTemplateSettings]


class ProvisioningSettings(BaseModel):
    """Builders started on demand, which register through the reverse acceptor. nixidae/nixidae#1198."""

    model_config = ConfigDict(extra="forbid")

    provider: ProviderName = "vivarium"
    """The provider of each pool that names none."""
    vivarium_socket: Path = Path("/run/vivarium/cloud.sock")
    ec2: Ec2Settings | None = None
    """Required with `provider = "ec2"`."""
    gha: GhaSettings | None = None
    """Required with `provider = "gha"`."""
    nomac: NomacSettings | None = None
    """Required with `provider = "nomac"`."""
    kubernetes: KubernetesSettings | None = None
    """Required with `provider = "kubernetes"`."""
    owner: str | None = None
    """The tag that marks this pynixd's machines, so a restart finds them. `None` takes the host name."""
    acceptor_host: str | None = None
    """The address a launched builder dials, in its join ticket. `None` leaves it to the builder's own settings."""
    acceptor_port: int | None = None
    pools: Annotated[
        list[ProvisioningPoolSettings],
        Experiment(
            replaces="every builder is configured or registers by itself",
            issue=1198,
            tests=("tests/unit/test_provisioner.py",),
        ),
    ] = Field(default_factory=list)
    """The machines to start, in order: a waiting build counts against the first pool that builds it."""
    heartbeat: float = Field(default=30.0, gt=0)
    """The longest the loop sleeps without a scheduling pass, a join or a leave to wake it."""

    @property
    def providers(self) -> frozenset[ProviderName]:
        """The providers the pools use."""
        return frozenset(pool.provider or self.provider for pool in self.pools)

    @model_validator(mode="after")
    def _provider_settings(self) -> ProvisioningSettings:
        # Without pools, `provider` alone names one: it must have its settings too.
        for name in self.providers | {self.provider}:
            if name == "ec2" and self.ec2 is None:
                raise ValueError('provider "ec2" needs the ec2 settings')
            if name == "gha" and self.gha is None:
                raise ValueError('provider "gha" needs the gha settings')
            if name == "nomac" and self.nomac is None:
                raise ValueError('provider "nomac" needs the nomac settings')
            if name == "kubernetes" and self.kubernetes is None:
                raise ValueError('provider "kubernetes" needs the kubernetes settings')
        return self


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
            self.want_mass_query,
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


S3Compression = Literal["zstd", "xz", "none"]
"""How an S3 store compresses the NARs it uploads; Nix's `compression` names."""


class BinaryCacheSpecBase(StoreSpecBase):
    """A read-only substitution source in Nix's binary cache layout, with health tracking."""

    trusted: bool = False
    """`False` lets the local daemon check each path against its own
    `trusted-public-keys`, as Nix does for a substituter (`substitution-goal.cc:236`)."""
    store_dir: str | None = None
    """`?store=`: the store directory the cache serves (`StoreDirConfig::storeDir`); `None` is pynixd's own."""
    max_fail_ratio: float = 0.5
    health_window: int = 10
    no_schedule: bool = True
    gc_enabled: bool = False
    drop_unopened: bool = False
    """An HTTP or S3 cache whose `nix-cache-info` cannot be read refuses to start, as Nix's does. A
    substituter that a client names sets it; a configured cache starts with Nix's defaults instead."""


class HTTPBinaryCacheSpec(BinaryCacheSpecBase):
    """A remote binary cache accessed via HTTP, with a concurrency limit."""

    type: Literal["http-binary-cache"] = "http-binary-cache"
    url: str
    max_concurrent: int | None = None

    def stable_by_default(self) -> bool:
        return self.url.rstrip("/") == STABLE_CACHE

    def reference_key(self) -> object | None:
        """The URL with no trailing slash, the store directory, the trust and `want-mass-query`."""
        return ("http", self.url.rstrip("/"), self.store_dir, self.trusted, self.want_mass_query)

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
        """The directory, the store directory, the trust and `want-mass-query`."""
        return ("file", self.path, self.store_dir, self.trusted, self.want_mass_query)

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
    endpoint_file: Path | None = None
    """A file that holds `endpoint`, read when the store is built: for an endpoint that names an account, which a
    settings file in the world-readable store must not carry. Not with `endpoint`."""
    addressing_style: Literal["auto", "path", "virtual"] = "auto"
    profile: str | None = None
    """The AWS configuration profile the credentials come from; `None` is botocore's default chain."""
    max_concurrent: int | None = None
    secret_key: Path | None = None
    """Nix's `secret-key`: the key file that signs each path pynixd uploads. Set, the store writes.

    Configuration only: a client's `s3://` substituter never makes pynixd write."""
    compression: S3Compression = "zstd"
    multipart_chunk_size: int = 5 * 1024 * 1024
    """Nix's `multipart-chunk-size`: the smallest part; a large NAR takes larger parts."""
    parts_in_flight: int = Field(default=8, ge=1)
    """Upload parts in flight at once, over every NAR the store sends. Each holds one part in memory."""
    gc_cold: bool = False
    """The collector's cold tier: a path leaves the local store once its closure is here or in a `gc_stable` store.

    Needs `secret_key`. One store at most carries it. nixidae/nixidae#1417.
    """
    gc_cold_verify: bool = False
    """Ask the bucket for each recorded path before a pass deletes it locally: one HEAD a path.

    Off, the record alone admits a delete. That holds while nothing removes objects from the bucket: no
    lifecycle rule, no hand. Give the bucket no lifecycle rule that expires objects. A rule that aborts
    incomplete multipart uploads removes no object and is safe. nixidae/nixidae#1600.
    """

    @model_validator(mode="after")
    def _cold_writes(self) -> Self:
        if self.gc_cold and self.secret_key is None:
            raise ValueError("gc_cold needs secret_key: the cold tier uploads what it keeps")
        if self.endpoint is not None and self.endpoint_file is not None:
            raise ValueError("endpoint and endpoint_file name the same thing: set one")
        return self

    @property
    def has_endpoint(self) -> bool:
        return self.endpoint is not None or self.endpoint_file is not None

    def resolved_endpoint(self) -> str | None:
        """`endpoint`, or what `endpoint_file` holds. Reads the file."""
        if self.endpoint_file is None:
            return self.endpoint
        return self.endpoint_file.read_text().strip()

    @property
    def writable(self) -> bool:
        return self.secret_key is not None

    def url(self) -> str:
        """The HTTP base URL of the cache; `ValueError` with Nix's text for an address Nix refuses."""
        return https_url(
            self.bucket,
            self.key,
            region=self.region,
            scheme=self.scheme,
            endpoint=self.resolved_endpoint(),
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
            self.endpoint_file,
            self.addressing_style,
            self.profile,
            self.store_dir,
            self.trusted,
            self.want_mass_query,
            self.secret_key,
            self.compression,
        )

    def to_store(self, store_id: str) -> S3BinaryCacheStore:
        """Build an ``S3BinaryCacheStore`` from this spec."""
        from .store import S3BinaryCacheStore

        return S3BinaryCacheStore(self.model_copy(update={"store_id": StoreId(store_id)}))


class GhaCacheSpec(BinaryCacheSpecBase):
    """The GitHub Actions cache of the job pynixd runs in: substitutes from it, uploads built outputs to it.

    nixidae/nixidae#1377. The runner gives the credentials only to an action, so a step writes
    them to *credentials_file* before pynixd starts; without the file the cache stays closed.
    """

    type: Literal["gha-cache"] = "gha-cache"
    credentials_file: Path = Path("/etc/pynixd/gha-cache.json")
    """A JSON object of `ACTIONS_RESULTS_URL`, `ACTIONS_RUNTIME_TOKEN`, `ACTIONS_CACHE_SERVICE_V2` and
    `ACTIONS_CACHE_MODE`, as the runner names them. It holds a token: mode 0600, root's."""
    trusted: bool = True
    """Its narinfos carry no signature, as magic-nix-cache's do (`main.rs:438`, `?trusted=1`).
    A job writes only to its own ref's scope, and any code that can write there can also change the job."""
    uploads: bool = True
    """Upload each output pynixd builds, when the job's cache mode allows writes."""
    version_salt: str = ""
    """Mixed into every entry's version: two salts are two caches in one repository."""
    max_concurrent_uploads: int = 4

    def reference_key(self) -> object | None:
        """The credentials file and the salt: one cache per pair."""
        return ("gha-cache", self.credentials_file, self.version_salt, self.store_dir)

    def to_store(self, store_id: str) -> GhaCacheStore:
        """Build a ``GhaCacheStore`` from this spec."""
        from .store import GhaCacheStore

        return GhaCacheStore(self.model_copy(update={"store_id": StoreId(store_id)}))


StoreSpec = Annotated[
    LocalSocketStoreSpec
    | ExternalUnixStoreSpec
    | SSHSubprocessStoreSpec
    | SSHSocketStoreSpec
    | ReverseStoreSpec
    | BuildPodStoreSpec
    | HTTPBinaryCacheSpec
    | LocalBinaryCacheSpec
    | S3BinaryCacheSpec
    | GhaCacheSpec,
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

    remote_settings: list[str] = Field(default_factory=lambda: ["timeout", "max-silent-time", "keep-going"])
    """The Nix settings pynixd takes from a client on another machine, by name.

    Such a client names `set-options-by-name` (huggorm's Nix) and sends every
    setting it overrides, its user nix.conf and `NIX_CONFIG` included. A
    delegating client's
    `max-jobs = 0` would stop this daemon building, so a setting not listed
    here is dropped. The trust rules apply first. nixidae/nixidae#1198.
    """

    unix_path: Path | None = Path("/run/pynixd/pynixd.sock")
    handover_path: Path | None = None
    """The control socket of a hand-over: `pynixd handover` takes this process's listeners through it.

    Root only (mode 0600). `None` serves none, and a reload then restarts. docs/design/upgrade-handover.md.
    """
    drain_limit: float = 86400.0
    """Seconds a process that handed over serves its remaining sessions and builds; then it closes as on a stop."""

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
    # long. 0 disables it. `None` is half of `health_loop_lag_max`
    # (`stall_traceback_threshold`).
    #
    # **At or under the health limit, never above it.** A stall that fails
    # `/healthz` is the stall that gets the pod killed, so it is the one that
    # needs a stack. Above the limit, a run of shorter stalls holds the probe
    # failing until the kubelet kills the pod, and nothing is dumped: on sg1
    # on 2026-11-14, three stalls of 5-10 s and fifteen of 2.5-5 s killed
    # pynixd, with the threshold at 30 s and no dump written.
    #
    # Half, and not equal: the watchdog checks once a second, so a dump
    # threshold at the limit itself misses most stalls just past it.
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
    stall_traceback_seconds: float | None = None
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
    # Also dump the loop thread's stack from a SIGALRM handler, which names a C
    # call that holds the GIL while it runs (`StallWatchdog`). It takes
    # SIGALRM, so nothing else in the process may use it.
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
    provisioning: ProvisioningSettings = Field(default_factory=ProvisioningSettings)
    """Off while `pools` is empty. A pool needs the reverse acceptor and the store database."""
    pod_name: str | None = None
    """The pod pynixd runs in, from the downward API. With `pod_uid` and `pod_namespace`, each build pod
    names it as its owner, and the garbage collector deletes the build pods when that pod is gone."""
    pod_uid: str | None = None
    pod_namespace: str | None = None
    """Must be the build pods' namespace: Kubernetes counts an owner in another namespace as absent, and
    deletes the pods it owns."""

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
    """Bounded passes per watermark drive before the watch reads the disk again.

    One pass relieves a little; a flood that lands all at once needs many,
    and a poll interval between each turns minutes of deleting into hours
    of waiting. The drive runs back to back while pressure stays over the
    store's target. A pass that frees nothing waits for the cold tier to
    cover what the next one would delete; without a cold tier the drive
    ends, and no new one starts until the store changes. Twenty covered a
    95%-to-70% relief with room.
    """
    gc_cold_ahead: float | None = None
    """The disk fraction a stalled drive puts first in the cold tier's upload queue.

    `None` is the gap between `gc_high_watermark` and the store's `gc_target_usage`, one watermark
    drive, or 0.05 when either is unset. nixidae/nixidae#1417.
    """
    gc_cold_workers: int = Field(default=16, ge=0)
    """Paths the cold tier uploads at once. Most paths are small, so one path's fixed cost, a few round trips,
    weighs more than its bytes: on sg1, 370 of 486 uploads were under 1 MB and took half the time. nixidae/nixidae#1600.

    Zero starts no uploader: the collector then deletes only what something else recorded as covered.
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
    tracing: TracingSettings = Field(default_factory=TracingSettings)

    # Substitution scheduling
    substitution_cache_maxsize: int = 100_000
    substitution_positive_ttl: float = 300.0
    substitution_negative_ttl: float = 15.0
    substitution_health_window: int = 64
    substitution_health_min_fill_ratio: float = 0.10
    substitution_health_min_success_ratio: float = 0.50
    substitution_query_timeout: float | None = None
    """Seconds one substituter query may take before it counts as "no substitute"; `None` is no deadline.

    Nix sets no deadline on a query (`substitution-goal.cc`), only the transfer's `connect-timeout` and
    `stalled-download-timeout`. A 2 s deadline under load made pynixd build glibc's bootstrap chain
    from source instead of substituting it. nixidae/nixidae#1265.
    """
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

    # Sign what an admin adds. pynixd's own store (`LocalDBStore`) signs
    # AddToStoreNar and AddMultipleToStore with its `secret-key-files`;
    # another store signs `AddToStore` alone, by relay (`SignPathInfo`) or by
    # decomposing (local sign, then `AddSignatures`). An AddToStore dump is
    # content-addressed, so its path vouches for it. Off, as Nix signs build
    # outputs alone. A store that others build in turns it on: a client
    # that trusts only this store's key takes back what admins pushed.
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

    @property
    def stall_traceback_threshold(self) -> float:
        """Seconds of stall before the watchdog dumps stacks; 0 when it is off."""
        if self.stall_traceback_seconds is None:
            return self.health_loop_lag_max / 2
        return self.stall_traceback_seconds

    @model_validator(mode="after")
    def _one_cold_tier(self) -> Self:
        cold = sorted(
            name for name, spec in self.stores.items() if isinstance(spec, S3BinaryCacheSpec) and spec.gc_cold
        )
        if len(cold) > 1:
            raise ValueError(f"one gc_cold store at most, and {', '.join(cold)} set it")
        return self

    @model_validator(mode="after")
    def _one_owner_pod(self) -> Self:
        given = [self.pod_name, self.pod_uid, self.pod_namespace]
        if not any(given):
            return self
        if not all(given):
            raise ValueError("pod_name, pod_uid and pod_namespace: set all three, or none")
        kubernetes = self.provisioning.kubernetes
        # Only build pods name this pod: reverse-builder pools may live in any namespace.
        builds_in_pods = any(isinstance(spec, BuildPodStoreSpec) for spec in self.stores.values())
        if builds_in_pods and kubernetes is not None and kubernetes.namespace != self.pod_namespace:
            raise ValueError(
                f"pod_namespace {self.pod_namespace} is not provisioning.kubernetes.namespace {kubernetes.namespace}:"
                " a build pod's owner in another namespace counts as absent, and Kubernetes deletes the pod"
            )
        return self

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


def _rebuild(model: type[BaseModel]) -> None:
    """Resolve the forward references of *model* and of its subclasses at import.

    A model left incomplete rebuilds at its first validation: pydantic parses
    and evaluates each annotation then, on the event loop.
    """
    model.model_rebuild()
    for sub in model.__subclasses__():
        _rebuild(sub)


_rebuild(StoreSpecBase)
PynixdSettings.model_rebuild()
