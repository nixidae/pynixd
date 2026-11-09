"""A build on a builder leaves its log in the local store, as Nix's build hook does.

`build-remote.sh:74` of Nix's functional suite reads it back with
`nix log --store $TEST_ROOT/machine0`.
"""

from __future__ import annotations

from nix_daemon_protocol.logs import ActivityField, LogNext, LogResult
from nix_daemon_protocol.protocol import FieldType, ResultType
from pynixd.build_queue import BuildQueue, QueuedBuild
from pynixd.execution import builder_log, kept_on_the_builder
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus
from pynixd.store.base import get_current_system
from tests._conftest.execution import Logs, fake_executor, queued
from tests._conftest.placement import options

DRV_NAME = "0000000000000000000000000000000x-x.drv"


def _line(text: str, result_type: int = ResultType.BUILD_LOG_LINE) -> LogResult:
    return LogResult(act_id=1, result_type=result_type, fields=[ActivityField(type=FieldType.STRING, valstr=text)])


async def _build(
    overrides: dict[str, str] | None = None, *, platform: str = "x86_64-linux", keep_failed: bool = False
) -> QueuedBuild:
    sent = options(1)
    sent.overrides = overrides or {}
    sent.keep_failed = keep_failed
    return await queued(
        BuildQueue(), platform=platform, options=None if overrides is None and not keep_failed else sent
    )


def _resp(*messages: LogNext | LogResult) -> BuildDerivationResponse:
    resp = BuildDerivationResponse(result=BuildResult(status=int(BuildResultStatus.BUILT)))
    for message in messages:
        resp.logs.add(message)
    return resp


async def test_the_builder_lines_make_the_log() -> None:
    resp = _resp(
        LogNext(text="building '/nix/store/x.drv' on 'ssh-ng://localhost'\n"),
        _line("hi-input1"),
        _line(""),
        _line("post-build", ResultType.POST_BUILD_LOG_LINE),
        _line("buildPhase", ResultType.SET_PHASE),
        _line("done"),
    )
    assert builder_log(await _build(), resp) == b'hi-input1\n\n@nix {"action":"setPhase","phase":"buildPhase"}\ndone\n'


async def test_a_line_that_is_not_utf8_keeps_its_bytes() -> None:
    """`test_build_equivalence.py`'s `binary-log`: a gzip header on stderr failed the build with UnicodeEncodeError.

    A log field reads with `surrogateescape`, so the encode takes the same handler back to the builder's bytes.
    """
    line = b"\x1f\x8b\x08not-utf8".decode("utf-8", errors="surrogateescape")
    assert builder_log(await _build(), _resp(_line(line))) == b"\x1f\x8b\x08not-utf8\n"


async def test_a_silent_build_keeps_an_empty_log() -> None:
    assert builder_log(await _build({}), _resp()) == b""


async def test_keep_build_log_false_keeps_none() -> None:
    assert builder_log(await _build({"keep-build-log": "false"}), _resp(_line("hi"))) is None


async def test_the_log_goes_to_the_local_store_under_the_base_name() -> None:
    """`AddBuildLog` carries the base name (`daemon.cc:1013`); a whole path made Nix refuse it."""
    logs = Logs()
    queue = BuildQueue()
    build = await queued(queue)
    await fake_executor(queue, {}, logs=logs).keep_log(build, _resp(_line("hi-input1")))
    assert logs.written == [(DRV_NAME, b"hi-input1\n")]


async def test_a_log_the_store_refuses_keeps_the_build() -> None:
    """`LogFile` failing ends Nix's goal; pynixd keeps the build, which has its outputs."""
    queue = BuildQueue()
    build = await queued(queue)
    await fake_executor(queue, {}, logs=Logs(fails=True)).keep_log(build, _resp(_line("hi-input1")))


async def test_keep_failed_warns_that_the_builder_kept_the_directory() -> None:
    """`build-remote.sh:113`: the hook's warning, with its hint for a build this machine could run."""
    warning = kept_on_the_builder(await _build(platform=get_current_system(), keep_failed=True))
    assert warning is not None
    assert "The failed build directory was kept on the remote builder due to `--keep-failed`." in warning
    assert "--builders ''" in warning


async def test_a_foreign_platform_gets_no_hint() -> None:
    warning = kept_on_the_builder(await _build(platform="riscv64-none", keep_failed=True))
    assert warning is not None
    assert "--builders" not in warning


async def test_no_warning_without_keep_failed() -> None:
    assert kept_on_the_builder(await _build(platform=get_current_system())) is None
