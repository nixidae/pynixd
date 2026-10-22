"""A build that fails for its builder's machine is tried on another builder.

nixidae/nixidae#1015: sg1-n3 failed croshome's `kubecove` with "No space
left on device", and the build failed although sg1-n2 had room. Nix marks a
full-disk failure `TransientFailure` (`derivation-builder.cc:242-250`).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.scheduler import Scheduler, machine_fault
from pynixd.serde import BasicDerivation, BuildResult, BuildResultStatus

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
def test_a_machine_fault_is_told_from_a_build_error(status: BuildResultStatus, message: str, fault: str | None):
    assert machine_fault(BuildResult(status=int(status), error_msg=message)) == fault


class Store:
    def __init__(self, store_id: str) -> None:
        self.store_id = StoreId(store_id)

    def supports_derivation(self, *_args: Any) -> bool:
        return True


def _scheduler(*, forbids_local: bool = False) -> Any:
    stores = {StoreId(n): Store(n) for n in ("local", "sg1-n1", "sg1-n2")}
    return SimpleNamespace(
        local_store=stores[LOCAL_STORE_ID], stores=stores, _client_forbids_local=lambda _build: forbids_local
    )


def _build(failed: dict[str, int]) -> Any:
    return SimpleNamespace(
        request=SimpleNamespace(derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh")),
        store_failures={StoreId(k): v for k, v in failed.items()},
    )


def _resp(message: str) -> Any:
    return SimpleNamespace(result=BuildResult(status=int(BuildResultStatus.PERMANENT_FAILURE), error_msg=message))


def test_a_full_disk_is_retried_while_another_remote_has_not_tried():
    scheduler = _scheduler()
    retry = Scheduler._retryable_elsewhere(  # noqa: SLF001 -- the decision is the unit
        cast("Any", scheduler), _build({}), scheduler.stores[StoreId("sg1-n1")], _resp(FULL)
    )
    assert retry == "No space left on device"


def test_the_local_store_is_tried_after_every_remote():
    """User: trying locally should also happen, not only the remotes."""
    scheduler = _scheduler()
    retry = Scheduler._retryable_elsewhere(  # noqa: SLF001
        cast("Any", scheduler), _build({"sg1-n2": 1}), scheduler.stores[StoreId("sg1-n1")], _resp(FULL)
    )
    assert retry == "No space left on device"


def test_a_local_fault_moves_to_a_remote():
    scheduler = _scheduler()
    retry = Scheduler._retryable_elsewhere(  # noqa: SLF001
        cast("Any", scheduler), _build({}), scheduler.stores[LOCAL_STORE_ID], _resp(FULL)
    )
    assert retry == "No space left on device"


def test_it_stops_once_every_store_has_failed():
    scheduler = _scheduler()
    retry = Scheduler._retryable_elsewhere(  # noqa: SLF001
        cast("Any", scheduler), _build({"sg1-n2": 1, "local": 1}), scheduler.stores[StoreId("sg1-n1")], _resp(FULL)
    )
    assert retry is None


def test_max_jobs_zero_keeps_the_local_store_out():
    scheduler = _scheduler(forbids_local=True)
    retry = Scheduler._retryable_elsewhere(  # noqa: SLF001
        cast("Any", scheduler), _build({"sg1-n2": 1}), scheduler.stores[StoreId("sg1-n1")], _resp(FULL)
    )
    assert retry is None


def test_a_compile_error_is_not_retried():
    scheduler = _scheduler()
    retry = Scheduler._retryable_elsewhere(  # noqa: SLF001
        cast("Any", scheduler), _build({}), scheduler.stores[StoreId("sg1-n1")], _resp("error: expected ';'")
    )
    assert retry is None
