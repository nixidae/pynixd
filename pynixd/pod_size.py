"""What a build pod asks the scheduler for, and the cores its build gets, from the build's history.

The cores are a cap: a build that cannot use them leaves them idle, and Kubernetes lends idle CPU to a pod
past its request. The memory request belongs to those cores (`MemoryModel.peak`), as the memory of a build
grows with `-j`. The CPU request is what the build used, which is less than the cores it may use: the
scheduler then packs pods by their real load.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from pynixd_store.memory_model import MemoryModel

MIN_CPU_MILLIS: Final = 100
MIN_MEMORY_BYTES: Final = 128 * 2**20
"""The sandbox container's runner and helper, which its cgroup counts with the build."""


class SizeSource(StrEnum):
    HISTORY = "history"
    TEMPLATE = "template"
    """No history, or no node took the history's request: the template's own requests."""


@dataclass(frozen=True, slots=True)
class PodSize:
    cores: int
    """The build's `NIX_BUILD_CORES`."""
    cpu_millis: int | None
    """The CPU request; `None` keeps the template's."""
    memory_bytes: int | None
    """The memory request; `None` keeps the template's."""
    cpu_source: SizeSource
    memory_source: SizeSource


def size_pod(
    *,
    cores: int,
    memory_bytes: int | None,
    model: MemoryModel | None,
    cores_used: float | None,
    ceilings: tuple[int | None, ...] = (),
    unplaced: bool = False,
) -> PodSize:
    """The size of a pod whose store gives a build at most *cores* and *memory_bytes*.

    *model* gives the most cores whose peak fits *memory_bytes*, and the request for them. *cores_used* is the
    median of what the build's history used, in cores. *ceilings*: the client's cores and the cores after an OOM
    kill. *unplaced*: no node took the last pod of the build, so this one keeps the template's requests.
    """
    top = min((c for c in (cores, *ceilings) if c), default=cores)
    built = top
    memory = None
    if model is not None:
        if memory_bytes is not None:
            built = model.most_cores_within(memory_bytes, top) or 1
        memory = max(MIN_MEMORY_BYTES, model.peak(built))
        if memory_bytes is not None:
            memory = min(memory, memory_bytes)
    cpu = None
    if cores_used is not None:
        cpu = max(MIN_CPU_MILLIS, min(built * 1000, math.ceil(cores_used * 10) * 100))
    if unplaced:
        cpu = memory = None
    return PodSize(
        cores=built,
        cpu_millis=cpu,
        memory_bytes=memory,
        cpu_source=SizeSource.TEMPLATE if cpu is None else SizeSource.HISTORY,
        memory_source=SizeSource.TEMPLATE if memory is None else SizeSource.HISTORY,
    )
