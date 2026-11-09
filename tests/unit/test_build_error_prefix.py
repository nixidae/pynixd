"""A daemon's `BuildDerivation` failure reaches the client with one `error: ` prefix.

`Store::buildDerivation` (`entry-points.cc:95`) fills `errorMsg` with `e.msg()`, which holds the prefix, and the
client adds its own: `error: error: some outputs are unexpectedly invalid`. nixidae/nixidae#1204.
"""

from __future__ import annotations

from pynixd.execution import without_error_prefix
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus

_RED = "\x1b[31;1m"
_NORMAL = "\x1b[0m"


def _failed(message: str) -> BuildDerivationResponse:
    return BuildDerivationResponse(
        result=BuildResult(
            status=BuildResultStatus.MISC_FAILURE,
            error_msg=message,
            times_built=0,
            is_non_deterministic=0,
            start_time=0,
            stop_time=0,
            built_outputs={},
        )
    )


def test_the_prefix_of_e_msg_is_removed() -> None:
    resp = without_error_prefix(_failed(f"{_RED}error:{_NORMAL} some outputs are unexpectedly invalid"))
    assert resp.result.error_msg == "some outputs are unexpectedly invalid"


def test_a_prefix_that_names_a_program_is_removed() -> None:
    """`showErrorInfo` (`error.cc:262`) writes `error [name]:` for another program's error."""
    resp = without_error_prefix(_failed(f"{_RED}error [nix-daemon]:{_NORMAL} it failed"))
    assert resp.result.error_msg == "it failed"


def test_a_message_without_the_prefix_stays() -> None:
    """A goal's own failure holds `message()`, which has no prefix (`error.hh:164`)."""
    for message in ("builder for '/nix/store/x.drv' failed with exit code 1", "error: in the text", ""):
        assert without_error_prefix(_failed(message)).result.error_msg == message


def test_only_the_leading_prefix_goes() -> None:
    inner = f"{_RED}error:{_NORMAL} outer\n{_RED}error:{_NORMAL} inner"
    assert without_error_prefix(_failed(inner)).result.error_msg == f"outer\n{_RED}error:{_NORMAL} inner"
