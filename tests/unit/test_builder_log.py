"""A build on a builder leaves its log in the local store, as Nix's build hook does.

`build-remote.sh:74` of Nix's functional suite reads it back with
`nix log --store $TEST_ROOT/machine0`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from nix_daemon_protocol.logs import ActivityField, LogNext, LogResult
from nix_daemon_protocol.protocol import FieldType, ResultType
from nix_daemon_protocol.store_path import StorePath
from pynixd import scheduler
from pynixd.scheduler import Scheduler, builder_log

if TYPE_CHECKING:
    import pytest

DRV = "/nix/store/xfb3ya060dvzsa0d4i6wi4xnmf82m4qh-build-remote-input-1.drv"


def _line(text: str, result_type: int = ResultType.BUILD_LOG_LINE) -> LogResult:
    return LogResult(act_id=1, result_type=result_type, fields=[ActivityField(type=FieldType.STRING, valstr=text)])


def _build(overrides: dict[str, str] | None = None) -> Any:
    options = None if overrides is None else SimpleNamespace(overrides=overrides)
    return cast("Any", SimpleNamespace(build_id=1, options=options, request=SimpleNamespace(drv_path=StorePath(DRV))))


def _resp(*messages: object) -> Any:
    return cast("Any", SimpleNamespace(logs=SimpleNamespace(messages=list(messages))))


def test_the_builder_lines_make_the_log() -> None:
    resp = _resp(
        LogNext(text="building '/nix/store/x.drv' on 'ssh-ng://localhost'\n"),
        _line("hi-input1"),
        _line(""),
        _line("post-build", ResultType.POST_BUILD_LOG_LINE),
        _line("buildPhase", ResultType.SET_PHASE),
        _line("done"),
    )
    assert builder_log(_build(), resp) == b'hi-input1\n\n@nix {"action":"setPhase","phase":"buildPhase"}\ndone\n'


def test_a_silent_build_keeps_an_empty_log() -> None:
    assert builder_log(_build({}), _resp()) == b""


def test_keep_build_log_false_keeps_none() -> None:
    assert builder_log(_build({"keep-build-log": "false"}), _resp(_line("hi"))) is None


async def test_the_log_goes_to_the_local_store_under_the_base_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """`AddBuildLog` carries the base name (`daemon.cc:1013`); a whole path made Nix refuse it."""
    written: list[tuple[str, bytes]] = []

    class Router:
        async def add_build_log(self, path: str, log: bytes) -> None:
            written.append((path, log))

    monkeypatch.setattr(scheduler, "write_router", lambda _store: Router())
    fake = SimpleNamespace(local_store=object())
    await Scheduler._keep_log(cast("Any", fake), _build(), _resp(_line("hi-input1")))  # noqa: SLF001 -- the write is the unit
    assert written == [("xfb3ya060dvzsa0d4i6wi4xnmf82m4qh-build-remote-input-1.drv", b"hi-input1\n")]
