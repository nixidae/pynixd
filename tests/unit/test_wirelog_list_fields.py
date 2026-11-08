"""The parity diff names each field inside a list of models, so an exemption reaches it (nixidae/nixidae#1217)."""

from __future__ import annotations

from nix_daemon_protocol.wirelog.decode import _flatten
from nix_daemon_protocol.wirelog.diff import _is_exempt_leaf
from pynixd.serde import BuildPathsWithResultsResponse, BuildResult, DerivedPath, KeyedBuildResult


def _fields(start: int) -> dict[str, str]:
    result = BuildResult(status=0, error_msg="", times_built=0, is_non_deterministic=0, start_time=start, stop_time=0)
    response = BuildPathsWithResultsResponse(
        results=[KeyedBuildResult(path=DerivedPath(value="/nix/store/a-b.drv!out"), result=result)]
    )
    out: dict[str, str] = {}
    _flatten(response, "", out)
    return out


def test_a_field_inside_a_list_has_a_name_of_its_own() -> None:
    fields = _fields(5)
    assert fields["results.0.result.start_time"] == "5"
    assert "results" not in fields


def test_only_the_times_of_a_result_are_exempt() -> None:
    one, two = _fields(0), _fields(5)
    differing = {name for name in one.keys() | two.keys() if one.get(name) != two.get(name)}
    assert differing == {"results.0.result.start_time"}
    assert all(_is_exempt_leaf(name) for name in differing)
    assert not _is_exempt_leaf("results.0.result.status")
