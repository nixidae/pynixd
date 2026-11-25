"""What a build pod writes to its log for each of the build's log messages (nixidae/nixidae#1560, spike).

Central relays the pod's log to the client, so a line the builder wrote has to be there as text.
"""

from __future__ import annotations

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from nix_daemon_protocol.logs import ActivityField, LogNext, LogResult, LogStartActivity
from nix_daemon_protocol.protocol import FieldType, ResultType
from pynixd.config import LocalSocketStoreSpec, PynixdSettings
from pynixd.pod_build import shown, without_probe
from pynixd.store.no_daemon import NoDaemonStore


def _result(result_type: ResultType, text: str) -> LogResult:
    return LogResult(act_id=1, result_type=result_type, fields=[ActivityField(type=FieldType.STRING, valstr=text)])


def test_a_line_the_builder_wrote_is_its_text() -> None:
    assert shown(_result(ResultType.BUILD_LOG_LINE, "to-stderr")) == "to-stderr"
    assert shown(_result(ResultType.POST_BUILD_LOG_LINE, "hook said")) == "hook said"


def test_a_log_line_is_its_text_and_anything_else_its_kind() -> None:
    assert shown(LogNext(text="building")) == "building"
    assert shown(_result(ResultType.SET_PHASE, "buildPhase")) == "LogResult"
    assert shown(LogStartActivity(act_id=1)) == "LogStartActivity"


def test_the_pods_store_builds_no_probes() -> None:
    """A store with no daemon probes by default; the pod's store takes central's one build without probes."""
    spec = LocalSocketStoreSpec(store_id=LOCAL_STORE_ID, monitor=False, daemon=False, managed=True, pynixd_writes=True)
    assert NoDaemonStore(spec).probes_own_builder
    settings = PynixdSettings(stores={str(LOCAL_STORE_ID): spec})
    without_probe(settings)
    unprobed = settings.stores[str(LOCAL_STORE_ID)]
    assert isinstance(unprobed, LocalSocketStoreSpec)
    assert not NoDaemonStore(unprobed).probes_own_builder
