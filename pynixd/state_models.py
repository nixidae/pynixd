"""The shape of a `pynixd state` answer.

The daemon builds a `StateReport` and sends its JSON; the CLI parses the same
model back. Every section is optional and unknown keys are ignored, so a
client reads an older or newer daemon's answer without failing on it.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

WaitReason = Literal["unwanted", "local_slot", "pressure", "scheduling"]
"""What a pending build waits for. `WAITS_FOR` words each one."""

WAITS_FOR: dict[str, str] = {
    "unwanted": "nothing: every request that wanted it gave up, the next pass cancels it",
    "local_slot": "a local slot: each request that wants it runs its max-jobs of local builds",
    "pressure": "a build limit: each store it can build on runs as many builds as its host takes now (#98)",
    "scheduling": "a scheduling pass or a free compatible store",
}


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


class QueueCounts(_Model):
    """Builds by state, from a walk of the queue (#89)."""

    scheduler: bool = True
    pending: int = 0
    building: int = 0
    done: int = 0


class BuildRow(_Model):
    """One unfinished build, and what holds it (#92)."""

    id: int
    drv: str
    state: Literal["pending", "building"]
    age_s: float
    store: str | None = None
    requests: dict[int, int | None] = {}
    """Each live request that wants it, and that request's `max-jobs`."""
    goal_holders: int = 0
    """Goal requests that hold the build, given-up ones included."""
    subscribers: int = 0
    retries: int = 0
    building_s: float | None = None
    waits_for: str | None = None
    """A `WaitReason` for a pending build. A plain string, because a daemon
    from before the keys sent the sentence itself."""


class ClientRow(_Model):
    """One live session, and the operation it runs now (#92)."""

    transport: str
    user: str
    connected_s: float
    op: str | None = None
    op_s: float | None = None


class UpstreamRow(_Model):
    """One operation waiting on a store, and how long the store is silent (#93)."""

    store: str
    conn: str
    op: str
    op_s: float
    silent_s: float


class PoolRow(_Model):
    """One connection pool of a store: `build` or `transfer`. Issue #97."""

    slots: int
    in_use: int = 0
    waiting: int = 0
    """Operations waiting for a free slot now."""
    longest_wait_s: float = 0.0


class LimitRow(_Model):
    """A store's build limit and the sample that set it. Issue #98."""

    limit: int
    ceiling: int
    reason: str
    sample_age_s: float | None = None
    cpu_some: float | None = None
    memory_full: float | None = None
    io_full: float | None = None
    """PSI `avg10`, in percent."""
    available_mb: int | None = None
    load1: float | None = None
    disk_free_mb: int | None = None
    """Free space of the store's filesystem. nixidae/nixidae#1013."""
    disk_total_mb: int | None = None


class StoreRow(_Model):
    healthy: bool
    systems: list[str] = []
    pools: dict[str, PoolRow] = {}
    limit: LimitRow | None = None


class StoreTransfer(_Model):
    bytes_in: int = 0
    bytes_out: int = 0
    paths_in: int = 0
    paths_out: int = 0
    wire_bytes_in: int = 0
    wire_bytes_out: int = 0


class Transfers(_Model):
    bytes_received: int = 0
    paths_received: int = 0
    bytes_served: int = 0
    paths_served: int = 0
    stores: dict[str, StoreTransfer] = {}


class Totals(_Model):
    builds_completed: dict[str, int] = {}
    sessions_accepted: dict[str, int] = {}


class Benchmark(_Model):
    tracing: bool = False
    tracemalloc_current: int | None = None
    tracemalloc_peak: int | None = None
    gc_objects: int | None = None


class FederatedError(_Model):
    error: str


class StateReport(_Model):
    queue: QueueCounts | None = None
    builds: list[BuildRow] | None = None
    stores: dict[str, StoreRow] | None = None
    sessions: dict[str, int] | None = None
    clients: list[ClientRow] | None = None
    upstream: list[UpstreamRow] | None = None
    transfers: Transfers | None = None
    totals: Totals | None = None
    benchmark: Benchmark | None = None
    federated: dict[str, FederatedError | StateReport] | None = None
    """Each other store's answer. `FederatedError` first: with unknown keys
    ignored, `{"error": ...}` would also parse as an empty report."""
