"""A Determinate builder's `Cancelled` (16): a goal that never finished.

Determinate v3.24.0 writes it to every peer (`common-protocol.cc:111`), and
it is each goal's result until the goal ends (`goal.hh:172`). Upstream reads
no status above 14. nixidae/nixidae#1279.
"""

from __future__ import annotations

from pynixd.execution import machine_fault
from pynixd.goals.results import result_succeeded
from pynixd.serde import MAX_WIRE_STATUS, BuildResult, BuildResultStatus

CANCELLED = 16


def test_the_status_is_known() -> None:
    assert BuildResultStatus(CANCELLED) is BuildResultStatus.CANCELLED


def test_a_cancelled_build_is_the_machines_fault() -> None:
    """The builder stopped before the build ended, so another machine may finish it."""
    result = BuildResult(status=CANCELLED)
    assert not result_succeeded(result)
    assert machine_fault(result) == "cancelled"


def test_the_wire_carries_misc_failure() -> None:
    """Determinate's client counts `MiscFailure` as cancelled too (`build-result.hh:218`)."""
    assert BuildResult(status=CANCELLED).wire_status() == int(BuildResultStatus.MISC_FAILURE)
    assert BuildResult(status=CANCELLED).wire_status() <= MAX_WIRE_STATUS
