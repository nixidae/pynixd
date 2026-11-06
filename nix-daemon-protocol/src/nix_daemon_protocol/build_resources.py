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
