"""What a build's processes used, as the helper measured it (nixidae/nixidae#1187).

The helper writes `USAGE_NAME` in the build's top directory after its builder ends; pynixd reads it.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import resource
    from pathlib import Path

USAGE_NAME = "usage.json"
BLOCK_BYTES = 512
"""Linux counts `ru_inblock` and `ru_oublock` in 512-byte units (`task_io_get_inblock`, `task_io_accounting_ops.h`)."""


class UsageSource(StrEnum):
    RUSAGE = "rusage"
    """`wait4` on the sandbox's pid 1: only what it reaped. Memory is the largest single process."""


@dataclass(frozen=True)
class BuildUsage:
    source: UsageSource
    cpu_user_us: int
    cpu_system_us: int
    peak_memory_bytes: int
    io_read_bytes: int
    """Bytes read from a block device; a page cache hit reads none."""
    io_write_bytes: int


def from_rusage(usage: resource.struct_rusage) -> BuildUsage:
    return BuildUsage(
        source=UsageSource.RUSAGE,
        cpu_user_us=round(usage.ru_utime * 1_000_000),
        cpu_system_us=round(usage.ru_stime * 1_000_000),
        # Linux counts `ru_maxrss` in KiB (`getrusage(2)`).
        peak_memory_bytes=usage.ru_maxrss * 1024,
        io_read_bytes=usage.ru_inblock * BLOCK_BYTES,
        io_write_bytes=usage.ru_oublock * BLOCK_BYTES,
    )


def write_usage(path: Path, usage: BuildUsage) -> None:
    path.write_text(json.dumps(asdict(usage)))


def read_usage(path: Path) -> BuildUsage | None:
    """The usage the helper wrote at *path*, or `None` when it wrote none: it failed to set up, or was killed."""
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    return BuildUsage(**{**data, "source": UsageSource(data["source"])})
