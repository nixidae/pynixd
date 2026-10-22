"""PSI (Pressure Stall Information) and system stats data model and parsing.

Reads cgroupv2 PSI, cpu.stat, cpu.max, and memory.current/max to gauge system
load and available resources on Linux backends. All paths are under
/sys/fs/cgroup/ — cgroupv2 is required, no fallback to procfs.

Future directions:
- Event-driven PSI: instead of polling, run a persistent SSH process that
  opens /sys/fs/cgroup/{cpu,memory,io}.pressure with O_RDWR, writes trigger
  thresholds (e.g. "some 150000 1000000"), and poll()s on the fds. On trigger
  fire, read current PSI state and print to stdout. This would let us gate
  builder admission instantly when a backend starts stalling.
- Derivation attributes could specify resource requirements (min memory,
  cpu count, etc.) to filter eligible builders before scheduling.
"""

from __future__ import annotations

import contextlib
import os
import time
from dataclasses import dataclass, field


@dataclass
class PsiMetric:
    """One resource from /proc/pressure/{cpu,memory,io}."""

    some_avg10: float = 0.0
    some_avg60: float = 0.0
    some_avg300: float = 0.0
    full_avg10: float = 0.0
    full_avg60: float = 0.0
    full_avg300: float = 0.0


@dataclass
class PsiWeights:
    """Configurable weights for pressure score calculation."""

    cpu: float = 0.4
    mem: float = 0.3
    io: float = 0.2
    memfull: float = 0.1

    @classmethod
    def from_env(cls) -> PsiWeights:
        """Parse PYNIXD_PSI_WEIGHTS='cpu=0.4,mem=0.3,io=0.2,memfull=0.1'."""
        raw = os.environ.get("PYNIXD_PSI_WEIGHTS", "")
        if not raw:
            return cls()
        kv = dict(part.split("=") for part in raw.split(","))
        return cls(**{k: float(v) for k, v in kv.items()})


PSI_WEIGHTS = PsiWeights.from_env()


@dataclass
class PsiSnapshot:
    """Complete PSI state for a host."""

    cpu: PsiMetric = field(default_factory=PsiMetric)
    memory: PsiMetric = field(default_factory=PsiMetric)
    io: PsiMetric = field(default_factory=PsiMetric)
    timestamp: float = 0.0

    def pressure_score(self, w: PsiWeights = PSI_WEIGHTS) -> float:
        """0.0 = idle, 100.0 = fully stalled.

        Weighted combination of pressure metrics, configurable via
        PYNIXD_PSI_WEIGHTS env var (default: cpu=0.4,mem=0.3,io=0.2,memfull=0.1).
        """
        return (
            w.cpu * self.cpu.some_avg10
            + w.mem * self.memory.some_avg10
            + w.io * self.io.some_avg10
            + w.memfull * self.memory.full_avg10
        )


@dataclass
class MemInfo:
    """Parsed subset of /proc/meminfo (values in kB)."""

    mem_total: int = 0
    mem_available: int = 0
    swap_total: int = 0
    swap_free: int = 0

    @property
    def available_mb(self) -> int:
        """Available memory in MiB (``MemAvailable`` / 1024)."""
        return self.mem_available // 1024

    @property
    def total_mb(self) -> int:
        """Total memory in MiB (``MemTotal`` / 1024)."""
        return self.mem_total // 1024


@dataclass
class CgroupCpuStat:
    """Parsed /sys/fs/cgroup/cpu.stat."""

    usage_usec: int = 0
    user_usec: int = 0
    system_usec: int = 0
    nr_periods: int = 0
    nr_throttled: int = 0
    throttled_usec: int = 0
    timestamp: float = 0.0


@dataclass
class CpuUtil:
    """CPU utilization derived from cgroupv2 cpu.stat + cpu.max deltas."""

    utilization: float
    cores: float
    throttled_pct: float


@dataclass(frozen=True)
class DiskSpace:
    """The filesystem that holds a store: what an unprivileged writer may still use, and its size."""

    free_bytes: int
    total_bytes: int

    @classmethod
    def from_statvfs(cls, stat: os.statvfs_result) -> DiskSpace:
        return cls(free_bytes=stat.f_bavail * stat.f_frsize, total_bytes=stat.f_blocks * stat.f_frsize)


@dataclass
class SystemHealth:
    """One sample of a host: pressure, memory, CPU, load, and the store's free disk."""

    psi: PsiSnapshot | None = None
    meminfo: MemInfo | None = None
    cpu_util: CpuUtil | None = None
    cores: float | None = None
    load1: float | None = None
    """The 1-minute load average."""
    disk: DiskSpace | None = None
    """The store's filesystem, when the monitor can see it. nixidae/nixidae#1013."""
    timestamp: float = field(default_factory=time.monotonic)

    @property
    def has_signal(self) -> bool:
        """Whether the sample says anything a build limit can act on."""
        return self.psi is not None or bool(self.meminfo and self.meminfo.mem_total) or self.load1 is not None


def parse_loadavg(text: str) -> float:
    """The 1-minute load average from `/proc/loadavg`."""
    return float(text.split()[0])


def parse_cpu_stat(text: str) -> CgroupCpuStat:
    """Parse /sys/fs/cgroup/cpu.stat output."""
    fields: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2:
            with contextlib.suppress(ValueError):
                fields[parts[0]] = int(parts[1])
    return CgroupCpuStat(
        usage_usec=fields.get("usage_usec", 0),
        user_usec=fields.get("user_usec", 0),
        system_usec=fields.get("system_usec", 0),
        nr_periods=fields.get("nr_periods", 0),
        nr_throttled=fields.get("nr_throttled", 0),
        throttled_usec=fields.get("throttled_usec", 0),
        timestamp=time.monotonic(),
    )


def parse_cpu_max(text: str) -> float | None:
    """Parse /sys/fs/cgroup/cpu.max -> core count.

    Format: 'quota period' or 'max period'. Returns cores as float.
    'max' means unlimited (returns None).
    """
    parts = text.strip().split()
    if len(parts) != 2:
        return None
    quota_str, period_str = parts
    if quota_str == "max":
        return None
    try:
        quota = int(quota_str)
        period = int(period_str)
    except ValueError:
        return None
    if period == 0:
        return None
    return quota / period


def count_cpus_from_proc_stat(text: str) -> int:
    """Count CPU cores from /proc/stat by counting 'cpuN' lines."""
    count = 0
    for line in text.splitlines():
        parts = line.split()
        if parts and parts[0].startswith("cpu") and parts[0][3:].isdigit():
            count += 1
    return max(count, 1)


def compute_cpu_util(
    prev: CgroupCpuStat,
    curr: CgroupCpuStat,
    cores: float | None,
) -> CpuUtil | None:
    """Compute CPU utilization between two cpu.stat snapshots.

    Uses the monotonic timestamp delta for wall-clock elapsed time,
    giving utilization as (cpu_time / wall_time * cores_capacity).
    """
    delta_usec = curr.usage_usec - prev.usage_usec
    if delta_usec <= 0:
        return None
    elapsed_wall_sec = curr.timestamp - prev.timestamp
    if elapsed_wall_sec <= 0:
        return None
    elapsed_wall_usec = int(elapsed_wall_sec * 1_000_000)
    total_capacity_usec = elapsed_wall_usec * (cores if cores is not None else 1.0)
    utilization = min((delta_usec / total_capacity_usec) * 100.0, 100.0)
    throttled_pct = (curr.nr_throttled / curr.nr_periods * 100.0) if curr.nr_periods > 0 else 0.0
    return CpuUtil(
        utilization=utilization,
        cores=cores if cores is not None else 1.0,
        throttled_pct=throttled_pct,
    )


def parse_meminfo(text: str) -> MemInfo:
    """Parse /proc/meminfo output into MemInfo."""
    fields: dict[str, int] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, rest = line.split(":", 1)
        # Values are in kB, strip the unit
        parts = rest.split()
        if parts:
            with contextlib.suppress(ValueError):
                fields[key.strip()] = int(parts[0])
    return MemInfo(
        mem_total=fields.get("MemTotal", 0),
        mem_available=fields.get("MemAvailable", 0),
        swap_total=fields.get("SwapTotal", 0),
        swap_free=fields.get("SwapFree", 0),
    )


def parse_psi_line(line: str) -> tuple[str, dict[str, float]]:
    """Parse 'some avg10=X avg60=Y avg300=Z total=N' -> ('some', {avg10: X, ...})"""
    parts = line.split()
    if not parts:
        raise ValueError(f"Empty PSI line: {line!r}")
    kind = parts[0]  # "some" or "full"
    values: dict[str, float] = {}
    for part in parts[1:]:
        if "=" in part:
            k, v = part.split("=", 1)
            if k != "total":
                values[k] = float(v)
    return kind, values


def parse_psi_output(text: str) -> PsiSnapshot:
    """Parse concatenated output of cat /proc/pressure/{cpu,memory,io}.

    Expected format (6 or 7 lines — cpu has no 'full' line on older kernels):
        some avg10=X avg60=Y avg300=Z total=N   <- cpu some
        [full avg10=X avg60=Y avg300=Z total=N]  <- cpu full (kernel 5.13+)
        some avg10=X avg60=Y avg300=Z total=N   <- memory some
        full avg10=X avg60=Y avg300=Z total=N   <- memory full
        some avg10=X avg60=Y avg300=Z total=N   <- io some
        full avg10=X avg60=Y avg300=Z total=N   <- io full
    """
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]

    cpu = PsiMetric()
    memory = PsiMetric()
    io = PsiMetric()

    # Parse lines sequentially — cpu first, then memory, then io.
    # CPU may have 1 or 2 lines, memory always 2, io always 2.
    idx = 0

    def apply(metric: PsiMetric, kind: str, values: dict[str, float]) -> None:
        prefix = f"{kind}_"
        for k, v in values.items():
            attr = prefix + k
            if hasattr(metric, attr):
                setattr(metric, attr, v)

    # CPU: always starts with "some", optionally followed by "full"
    if idx < len(lines):
        kind, vals = parse_psi_line(lines[idx])
        apply(cpu, kind, vals)
        idx += 1
    if idx < len(lines) and lines[idx].startswith("full"):
        kind, vals = parse_psi_line(lines[idx])
        apply(cpu, kind, vals)
        idx += 1

    # Memory: some then full
    if idx < len(lines):
        kind, vals = parse_psi_line(lines[idx])
        apply(memory, kind, vals)
        idx += 1
    if idx < len(lines):
        kind, vals = parse_psi_line(lines[idx])
        apply(memory, kind, vals)
        idx += 1

    # IO: some then full
    if idx < len(lines):
        kind, vals = parse_psi_line(lines[idx])
        apply(io, kind, vals)
        idx += 1
    if idx < len(lines):
        kind, vals = parse_psi_line(lines[idx])
        apply(io, kind, vals)
        idx += 1

    return PsiSnapshot(
        cpu=cpu,
        memory=memory,
        io=io,
        timestamp=time.monotonic(),
    )
