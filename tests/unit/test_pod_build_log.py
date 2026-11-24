"""What a build pod writes to its log for each of the build's log messages (nixidae/nixidae#1560, spike).

Central relays the pod's log to the client, so a line the builder wrote has to be there as text.
"""

from __future__ import annotations

from nix_daemon_protocol.logs import ActivityField, LogNext, LogResult, LogStartActivity
from nix_daemon_protocol.protocol import FieldType, ResultType
from pynixd.pod_build import shown


def _result(result_type: ResultType, text: str) -> LogResult:
    return LogResult(act_id=1, result_type=result_type, fields=[ActivityField(type=FieldType.STRING, valstr=text)])


def test_a_line_the_builder_wrote_is_its_text() -> None:
    assert shown(_result(ResultType.BUILD_LOG_LINE, "to-stderr")) == "to-stderr"
    assert shown(_result(ResultType.POST_BUILD_LOG_LINE, "hook said")) == "hook said"


def test_a_log_line_is_its_text_and_anything_else_its_kind() -> None:
    assert shown(LogNext(text="building")) == "building"
    assert shown(_result(ResultType.SET_PHASE, "buildPhase")) == "LogResult"
    assert shown(LogStartActivity(act_id=1)) == "LogStartActivity"
