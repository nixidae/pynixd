"""What a build used, as a pynixd builder measured it (nixidae/nixidae#1187).

`BuildResult.resources` carries it, after every field of Nix's, and only when both peers name
`FEATURE_PYNIXD_BUILD_RESOURCES`. Nix names no such feature, so a Nix peer never sees these bytes.
"""

from __future__ import annotations

from .opt_microseconds import OptMicroseconds
from .wire_message import WireField, WireModel


class ResourceSample(WireModel):
    """The build's cgroup at one moment, *at_ms* after the builder started. The counters are cumulative."""

    at_ms: int = 0
    cpu_us: int = 0
    memory_bytes: int = 0
    """`memory.current`: what the build holds now, page cache included."""
    io_read_bytes: int = 0
    io_write_bytes: int = 0


class BuildResources(WireModel):
    """[measured uint64], then the rest when measured is 1.

    `source` says how far the numbers reach. `rusage` is `wait4` on the sandbox's first process: CPU and
    block IO of what it reaped, and the memory of its largest single process. `cgroup` is the build's own
    cgroup: the whole tree, and the stall times, which `rusage` leaves absent.
    """

    measured: int = 0
    source: str = WireField(default="", wire_depends_on=lambda self: self.measured == 1)
    cpu_user_us: int = WireField(default=0, wire_depends_on=lambda self: self.measured == 1)
    cpu_system_us: int = WireField(default=0, wire_depends_on=lambda self: self.measured == 1)
    peak_memory_bytes: int = WireField(default=0, wire_depends_on=lambda self: self.measured == 1)
    io_read_bytes: int = WireField(default=0, wire_depends_on=lambda self: self.measured == 1)
    io_write_bytes: int = WireField(default=0, wire_depends_on=lambda self: self.measured == 1)
    cpu_stall: OptMicroseconds = WireField(
        default_factory=OptMicroseconds, wire_depends_on=lambda self: self.measured == 1
    )
    """PSI `some` total of the build's cgroup: time some task waited for a CPU."""
    memory_stall: OptMicroseconds = WireField(
        default_factory=OptMicroseconds, wire_depends_on=lambda self: self.measured == 1
    )
    io_stall: OptMicroseconds = WireField(
        default_factory=OptMicroseconds, wire_depends_on=lambda self: self.measured == 1
    )
    samples: list[ResourceSample] = WireField(default_factory=list, wire_depends_on=lambda self: self.measured == 1)
    """The time series of a `cgroup` record, oldest first. A `rusage` record has none."""
    host_source: str = WireField(default="", wire_depends_on=lambda self: self.measured == 1)
    """How the host measured a VM build's VMM, `rusage` or `cgroup`; empty outside a VM, and then the three
    counters below are absent. The VMM's CPU time holds the guest's vCPUs, and its memory the guest's RAM."""
    host_cpu_user_us: int = WireField(
        default=0, wire_depends_on=lambda self: self.measured == 1 and self.host_source != ""
    )
    host_cpu_system_us: int = WireField(
        default=0, wire_depends_on=lambda self: self.measured == 1 and self.host_source != ""
    )
    host_peak_memory_bytes: int = WireField(
        default=0, wire_depends_on=lambda self: self.measured == 1 and self.host_source != ""
    )
    cores: int = WireField(default=0, wire_depends_on=lambda self: self.measured == 1)
    """`NIX_BUILD_CORES` of the build: the parallelism its peak memory belongs to. 0 when unknown."""
    oom_kills: int = WireField(default=0, wire_depends_on=lambda self: self.measured == 1)
    """Processes of the build that the kernel's OOM killer ended: `oom_kill` of the cgroup's `memory.events`.
    0 without a cgroup. A build with one failed for want of memory, and may pass with fewer cores."""
    killed_cores: int = WireField(default=0, wire_depends_on=lambda self: self.measured == 1)
    """The fewest cores of an earlier attempt of this build that the OOM killer ended; 0 when none did. The builder
    built again with fewer, and the fields above are the last attempt's. nixidae/nixidae#1558."""
    killed_peak_bytes: int = WireField(default=0, wire_depends_on=lambda self: self.measured == 1)
    """That attempt's peak memory: a lower bound on what its cores need."""
