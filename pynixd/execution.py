"""Running a placed build on its store, typed over the stores it reads.

`Scheduler.schedule` places a build through `Placer`, and the placer's spawn
callback runs it here. Each dependency is a Protocol or a real object, so a
test's fake that lacks a member fails `checks.types`, not the guest's unit
phase. nixidae/nixidae#1246.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Protocol

import structlog

from .allocator import outside_the_session
from .goals.results import result_succeeded
from .placement import client_forbids_local
from .serde import BuildResultStatus, LogNext
from .system_features import effective_required_features

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from nix_daemon_protocol.ids import StoreId

    from .build_queue import BuildQueue, QueuedBuild
    from .config import StoreRankingSettings
    from .serde import BuildDerivationResponse, BuildResult

log = structlog.get_logger(__name__)

_MACHINE_FAULTS = (
    "No space left on device",
    "Disk quota exceeded",
    "Cannot allocate memory",
    "Too many open files",
    "Read-only file system",
    "Input/output error",
    "getting status of",
    "is not valid",
    "Killed",
)
"""Words in a failed build's message that blame the builder's machine, not the derivation."""


def machine_fault(result: BuildResult) -> str | None:
    """Why a failed result is the machine's fault, or `None` for the derivation's own failure.

    Nix marks a build that failed with a full disk, or any unsandboxed build,
    `TransientFailure` (`derivation-builder.cc:242-250`), and an error outside
    the builder `MiscFailure` (`derivation-building-goal.cc:805`). The text
    catches a disk that filled and emptied again before Nix looked.
    """
    if result_succeeded(result):
        return None
    status = BuildResultStatus(result.status)
    if status in (BuildResultStatus.TRANSIENT_FAILURE, BuildResultStatus.MISC_FAILURE):
        return status.name.lower()
    message = str(result.error_msg)
    return next((words for words in _MACHINE_FAULTS if words in message), None)


class Builder(Protocol):
    """What execution reads of a store that builds."""

    @property
    def store_id(self) -> StoreId: ...
    @property
    def session_only(self) -> bool: ...

    disk_full_until: float

    def supports_derivation(self, system: str, features: set[str], /) -> bool: ...


class Executor[S: Builder]:
    """Runs a build on the store the placer chose, and moves it on when the machine fails."""

    def __init__(
        self,
        *,
        queue: BuildQueue,
        local_id: StoreId,
        stores: Callable[[], Mapping[StoreId, S]],
        settings: StoreRankingSettings,
        trigger: Callable[[], None],
    ) -> None:
        self.queue = queue
        self.local_id = local_id
        self._stores = stores
        self.settings = settings
        self.trigger = trigger

    def note_disk_full(self, store: S, message: str) -> None:
        """Hold *store* back from builds when it ran out of disk. nixidae/nixidae#1013."""
        if "No space left on device" in message:
            store.disk_full_until = time.monotonic() + self.settings.disk_full_hold_seconds
            log.warning("store_held_back_for_disk", store_id=store.store_id)

    def retryable_elsewhere(self, build: QueuedBuild, store: S, resp: BuildDerivationResponse) -> str | None:
        """The machine fault behind a failed result, when another store has not tried yet.

        The local store is a store like the others here: a remote's fault
        may fall back to it, and its own fault may move to a remote. It stays
        out when the client forbids local builds (`--max-jobs 0`).
        """
        fault = machine_fault(resp.result)
        if fault is None:
            return None
        platform = build.request.derivation.platform
        features = effective_required_features(build.request.derivation)
        local_forbidden = client_forbids_local(self.queue, build)
        untried = [
            other
            for other in self._stores().values()
            if other.store_id != store.store_id
            and (other.store_id == self.local_id or outside_the_session(other, build) is None)
            and other.store_id not in build.store_failures
            and not (other.store_id == self.local_id and local_forbidden)
            and other.supports_derivation(platform, features)
        ]
        return fault if untried else None

    async def retry_elsewhere(self, build: QueuedBuild, store: S, resp: BuildDerivationResponse, fault: str) -> None:
        """The builder's machine failed, not the derivation: try another store.

        The local one is included, as a crashed connection already does.
        nixidae/nixidae#1015.
        """
        log.warning("build_failed_on_the_machine", build_id=build.build_id, store_id=store.store_id, fault=fault)
        self.note_disk_full(store, str(resp.result.error_msg))
        await build.post_log_and_fanout(
            LogNext(text=f"pynixd: build failed on {store.store_id} ({fault}), retrying elsewhere\n")
        )
        build.reset_for_retry(store.store_id, str(resp.result.error_msg) or fault)
        self.trigger()
