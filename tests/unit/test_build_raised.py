"""A builder's `MiscFailure` with a message is an `Error` its Nix raised.

`Store::buildDerivation` (`entry-points.cc:85`) answers the `Error` as a result;
`buildPathsWithResults` lets it end the operation. `provenance.sh:371` of the
functional suite reads the text. nixidae/nixidae#1277.
"""

from __future__ import annotations

import pytest

from pynixd.execution import machine_fault, marked_raised
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus

_MESSAGE = "tag name '\x1b[35;1m123-invalid\x1b[0m' is invalid"


def _answer(status: BuildResultStatus, message: str) -> BuildDerivationResponse:
    return BuildDerivationResponse(
        result=BuildResult(
            status=status,
            error_msg=message,
            times_built=0,
            is_non_deterministic=0,
            start_time=0,
            stop_time=0,
            built_outputs={},
        )
    )


def test_a_misc_failure_with_a_message_is_raised() -> None:
    resp = marked_raised(_answer(BuildResultStatus.MISC_FAILURE, _MESSAGE))
    assert resp.result.status == BuildResultStatus.RAISED
    assert resp.result.error_msg == _MESSAGE


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (BuildResultStatus.MISC_FAILURE, ""),
        (BuildResultStatus.PERMANENT_FAILURE, "builder for '/nix/store/x.drv' failed with exit code 1"),
        (BuildResultStatus.BUILT, ""),
    ],
    ids=["no-message", "permanent", "built"],
)
def test_every_other_answer_stays(status: BuildResultStatus, message: str) -> None:
    assert marked_raised(_answer(status, message)).result.status == status


def test_the_wire_carries_misc_failure() -> None:
    """`buildResultStatusTable` (`common-protocol.cc:104`) has no entry for pynixd's mark."""
    resp = marked_raised(_answer(BuildResultStatus.MISC_FAILURE, _MESSAGE))
    assert resp.result.for_the_wire().status == BuildResultStatus.MISC_FAILURE


def test_a_raised_answer_is_a_machine_fault_as_misc_failure_is() -> None:
    """`retry_elsewhere` treats the mark as the `MiscFailure` it was. nixidae/nixidae#1015."""
    resp = marked_raised(_answer(BuildResultStatus.MISC_FAILURE, _MESSAGE))
    assert machine_fault(resp.result) == machine_fault(_answer(BuildResultStatus.MISC_FAILURE, _MESSAGE).result)
