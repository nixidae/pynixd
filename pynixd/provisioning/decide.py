"""What a provisioner launches and terminates, from the queue and the instances it holds. nixidae/nixidae#1198.

Pure: the provisioner loop reads the queue and its instance table, calls
`decide`, and does what the `Decision` says.

A waiting build counts against the first pool, in configuration order, that
can build it. A pool launches enough instances to give each waiting build a
slot, less the slots of instances still booting or idle, up to its maximum.
An instance under an interruption notice gives no slot: a replacement starts
before it ends. An idle instance is terminated once it has been idle for the
pool's `idle_seconds` and no build waits for its pool; one stuck booting past
`launch_timeout` is terminated too.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


class InstanceState(StrEnum):
    PENDING = "pending"
    """Launched; its builder has not registered yet."""
    RUNNING = "running"
    """Its builder is registered and takes builds."""


class EndReason(StrEnum):
    IDLE = "idle"
    LAUNCH_TIMEOUT = "launch-timeout"
    DISCONNECTED = "disconnected"
    """Its builder left; its token admits no second registration."""
    NO_CAPACITY = "no-capacity"
    LAUNCH_FAILED = "launch-failed"
    ORPHAN = "orphan"
    """Live when this pynixd started: its builder lost the pynixd that admitted it."""
    SHUTDOWN = "shutdown"


@dataclass(frozen=True, slots=True)
class Pool:
    """One kind of machine a provider starts."""

    name: str
    systems: frozenset[str]
    features: frozenset[str] = frozenset()
    max_instances: int = 1
    min_instances: int = 0
    slots: int = 1
    """Builds one instance takes at once."""
    idle_seconds: float = 300.0
    launch_timeout: float = 600.0
    """Seconds an instance may stay pending before it is given up."""

    def builds(self, want: Want) -> bool:
        return want.system in self.systems and want.features <= self.features


@dataclass(frozen=True, slots=True)
class Want:
    """One build that waits for a store."""

    system: str
    features: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class Held:
    """One instance the provisioner holds, as its table and the scheduler know it."""

    id: str
    pool: str
    state: InstanceState
    launched_at: float
    builds: int = 0
    """Builds it runs now."""
    idle_since: float | None = None
    """When its last build ended, or its builder registered; None while it builds."""
    notice: float | None = None
    """When an interruption ends it."""


@dataclass(frozen=True, slots=True)
class Decision:
    launch: dict[str, int] = field(default_factory=dict)
    """Instances to launch, by pool."""
    terminate: dict[str, EndReason] = field(default_factory=dict)
    """Instances to end, by id."""


def attribute(pools: Sequence[Pool], waiting: Sequence[Want]) -> Counter[str]:
    """Waiting builds by the first pool that builds each; a build no pool builds counts nowhere."""
    counts: Counter[str] = Counter()
    for want in waiting:
        pool = next((pool for pool in pools if pool.builds(want)), None)
        if pool is not None:
            counts[pool.name] += 1
    return counts


def decide(pools: Sequence[Pool], waiting: Sequence[Want], held: Sequence[Held], now: float) -> Decision:
    demand = attribute(pools, waiting)
    launch: dict[str, int] = {}
    terminate: dict[str, EndReason] = {}
    for pool in pools:
        mine = [one for one in held if one.pool == pool.name]
        for one in mine:
            if one.state is InstanceState.PENDING and now - one.launched_at > pool.launch_timeout:
                terminate[one.id] = EndReason.LAUNCH_TIMEOUT
        live = [one for one in mine if one.id not in terminate]
        lasting = [one for one in live if one.notice is None]
        free = sum(
            pool.slots if one.state is InstanceState.PENDING else max(pool.slots - one.builds, 0) for one in lasting
        )
        need = math.ceil(max(demand[pool.name] - free, 0) / pool.slots)
        room = pool.max_instances - len(lasting)
        short = max(pool.min_instances - len(lasting), 0)
        count = max(min(need, room), short, 0)
        if count:
            launch[pool.name] = count
        if demand[pool.name]:
            continue
        spare = len(lasting) - pool.min_instances
        idle = sorted(
            (
                one
                for one in lasting
                if one.state is InstanceState.RUNNING
                and one.builds == 0
                and one.idle_since is not None
                and now - one.idle_since >= pool.idle_seconds
            ),
            key=lambda one: one.idle_since or 0.0,
        )
        for one in idle[: max(spare, 0)]:
            terminate[one.id] = EndReason.IDLE
    return Decision(launch=launch, terminate=terminate)
