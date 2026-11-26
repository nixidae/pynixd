"""A build that fails for its builder's machine is tried on another builder.

nixidae/nixidae#1015: sg1-n3 failed croshome's `kubecove` with "No space
left on device", and the build failed although sg1-n2 had room. Nix marks a
full-disk failure `TransientFailure` (`derivation-builder.cc:242-250`).
"""

from __future__ import annotations

import pytest
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.build_queue import BuildQueue
from pynixd.execution import machine_fault
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus
from tests._conftest.execution import fake_executor, queued
from tests._conftest.placement import FakeStore, options

FULL = "cp: cannot create symbolic link '/nix/store/x': No space left on device"


@pytest.mark.parametrize(
    ("status", "message", "fault"),
    [
        (BuildResultStatus.TRANSIENT_FAILURE, "", "transient_failure"),
        (BuildResultStatus.MISC_FAILURE, "", "misc_failure"),
        (BuildResultStatus.PERMANENT_FAILURE, FULL, "No space left on device"),
        (BuildResultStatus.PERMANENT_FAILURE, "error: expected ';'", None),
        (BuildResultStatus.BUILT, "", None),
    ],
)
def test_a_machine_fault_is_told_from_a_build_error(status: BuildResultStatus, message: str, fault: str | None) -> None:
    assert machine_fault(BuildResult(status=int(status), error_msg=message)) == fault


SG1 = StoreId("sg1-n1")
STORES = {store_id: FakeStore(store_id) for store_id in (LOCAL_STORE_ID, SG1, StoreId("sg1-n2"))}


async def _retry(
    failed: dict[str, int],
    on: StoreId,
    message: str,
    *,
    forbids_local: bool = False,
    stores: dict[StoreId, FakeStore] = STORES,
    coming: bool = False,
) -> str | None:
    """Whether a build that failed with *message* on *on* moves on, after failing on each store in *failed*.

    *coming* is whether a builder that has not registered yet may still take it.
    """
    queue = BuildQueue()
    build = await queued(queue, options=options(0) if forbids_local else None)
    build.store_failures = {StoreId(k): v for k, v in failed.items()}
    resp = BuildDerivationResponse(
        result=BuildResult(status=int(BuildResultStatus.PERMANENT_FAILURE), error_msg=message)
    )
    executor = fake_executor(queue, stores, builder_may_come=lambda _build, _features: coming)
    return executor.retryable_elsewhere(build, stores[on], resp)


ALONE = {LOCAL_STORE_ID: STORES[LOCAL_STORE_ID]}
"""The local store, with no builder registered yet: sg1's pynixd-0 48 s after a restart."""


async def test_a_local_fault_waits_for_a_builder_still_to_come() -> None:
    """nixidae/nixidae#1542: the failure reached the client 48 s before the first builder pod registered."""
    assert await _retry({}, LOCAL_STORE_ID, FULL, stores=ALONE, coming=True) == "No space left on device"


async def test_a_local_fault_with_no_builder_to_come_fails() -> None:
    assert await _retry({}, LOCAL_STORE_ID, FULL, stores=ALONE) is None


async def test_a_build_waits_for_a_coming_builder_only_so_often() -> None:
    """Each builder pod is a new store, so a build failed on every machine would move from pod to pod forever."""
    failed = {"pod-a": 1, "pod-b": 1}
    assert await _retry(failed, LOCAL_STORE_ID, FULL, stores=ALONE, coming=True) is None


async def test_a_full_disk_is_retried_while_another_remote_has_not_tried() -> None:
    assert await _retry({}, SG1, FULL) == "No space left on device"


async def test_the_local_store_is_tried_after_every_remote() -> None:
    """User: trying locally should also happen, not only the remotes."""
    assert await _retry({"sg1-n2": 1}, SG1, FULL) == "No space left on device"


async def test_a_local_fault_moves_to_a_remote() -> None:
    assert await _retry({}, LOCAL_STORE_ID, FULL) == "No space left on device"


async def test_it_stops_once_every_store_has_failed() -> None:
    assert await _retry({"sg1-n2": 1, "local": 1}, SG1, FULL) is None


async def test_max_jobs_zero_keeps_the_local_store_out() -> None:
    assert await _retry({"sg1-n2": 1}, SG1, FULL, forbids_local=True) is None


async def test_a_compile_error_is_not_retried() -> None:
    assert await _retry({}, SG1, "error: expected ';'") is None


async def test_the_store_that_failed_is_not_tried_again() -> None:
    """`retry_elsewhere` means another store: sg1 took one build three times per node. nixidae/nixidae#1102."""
    queue = BuildQueue()
    build = await queued(queue)
    resp = BuildDerivationResponse(result=BuildResult(status=int(BuildResultStatus.TRANSIENT_FAILURE)))

    await fake_executor(queue, STORES).retry_elsewhere(build, STORES[SG1], resp, "transient_failure")

    assert build.is_blacklisted(SG1)
    assert not build.is_blacklisted(StoreId("sg1-n2"))


SANDBOX = "error: setting up the sandbox: [Errno 1] Operation not permitted"


async def test_a_machine_fault_says_why_in_the_log_and_to_the_client() -> None:
    """sg1's pynixd-0 failed every local build in 0.33 s, and the log and the client said only "misc_failure"."""
    queue = BuildQueue()
    build = await queued(queue)
    resp = BuildDerivationResponse(result=BuildResult(status=int(BuildResultStatus.MISC_FAILURE), error_msg=SANDBOX))

    with structlog.testing.capture_logs() as logs:
        await fake_executor(queue, STORES).retry_elsewhere(build, STORES[LOCAL_STORE_ID], resp, "misc_failure")

    [line] = [entry for entry in logs if entry["event"] == "build_failed_on_the_machine"]
    assert line["error"] == SANDBOX
    assert SANDBOX.encode() in build.log_bytes()
