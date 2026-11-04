"""A builder's `@nix` lines, as `handleJSONLogMessage` reads them with `trusted = false` (`logging.cc:437`).

nixpkgs' stdenv writes a `setPhase` line for each phase to `$NIX_LOG_FD`, so
every stdenv build makes them.
"""

from __future__ import annotations

from typing import Any

from nix_daemon_protocol.logs import LogNext, LogResult, LogStartActivity, LogStopActivity
from nix_daemon_protocol.protocol import ResultType, Verbosity
from pynixd.activity import BuildActivity
from pynixd_store.builder import jsonlog
from pynixd_store.builder.buildlog import BuildLog

DRV = "/nix/store/zg01p02q4lq6g9p7v3s6xk0bq0zs0y8c-hello.drv"


def _outputs(text: bytes) -> tuple[list[Any], list[str]]:
    log = BuildLog(10)
    outputs = log.feed(text) + log.flush()
    return outputs, list(log.tail)


def test_a_consumed_line_is_no_line_and_not_in_the_tail() -> None:
    outputs, tail = _outputs(b'one\n@nix {"action": "setPhase", "phase": "buildPhase"}\ntwo\n')
    assert outputs == ["one", jsonlog.SetPhase("buildPhase"), "two"]
    assert tail == ["one", "two"]


def test_a_start_of_another_type_is_consumed_silently() -> None:
    outputs, tail = _outputs(b'@nix {"action": "start", "id": 1, "type": 105, "level": 3, "text": "x"}\n')
    assert (outputs, tail) == ([], [])


def test_an_unknown_action_is_consumed() -> None:
    assert _outputs(b'@nix {"action": "dance"}\n') == ([], [])


def test_bad_json_is_a_line_after_an_error() -> None:
    line = "@nix {not json"
    outputs, tail = _outputs(line.encode() + b"\n")
    [error, plain] = outputs
    assert isinstance(error, jsonlog.Message)
    assert error.level == Verbosity.ERROR
    assert error.text.startswith("bad JSON log message from the derivation builder: ")
    assert (plain, tail) == (line, [line])


def test_a_wrong_shape_is_a_line_after_a_warning() -> None:
    line = '@nix {"action": "msg", "level": "loud", "msg": "hi"}'
    outputs, tail = _outputs(line.encode() + b"\n")
    [warning, plain] = outputs
    assert isinstance(warning, jsonlog.Message)
    assert warning.level == Verbosity.WARN
    assert warning.text == (
        "\x1b[35;1mwarning:\x1b[0m Unable to handle a JSON message from the derivation builder: "
        "[json.exception.type_error.302] type must be number, but is string"
    )
    assert (plain, tail) == (line, [line])


def test_a_field_getfields_refuses_is_a_line_not_a_failed_build() -> None:
    """NIX-DEVIATION (#27) in `jsonlog._fields`."""
    line = '@nix {"action": "start", "id": 1, "type": 101, "level": 3, "text": "x", "fields": [-1]}'
    outputs, tail = _outputs(line.encode() + b"\n")
    assert outputs[-1] == line
    assert "unsupported JSON type 5" in outputs[0].text
    assert tail == [line]


async def _wire(outputs: list[Any], verbosity: int = Verbosity.INFO) -> tuple[int, list[Any]]:
    sent: list[Any] = []

    async def on_log(message: Any) -> None:
        sent.append(message)

    activity = BuildActivity(DRV, on_log, verbosity)
    await activity.start()
    await activity.emit(outputs)
    await activity.stop()
    return activity.id, sent


async def test_a_file_transfer_gets_a_fresh_id_under_the_build() -> None:
    start = jsonlog.Start(id=7, level=3, type=101, text="fetching", fields=("url",))
    build, sent = await _wire([start, jsonlog.Result(7, 105, (1, 2)), jsonlog.Stop(7)])
    [_build_start, child_start, result, child_stop, build_stop] = sent
    assert isinstance(child_start, LogStartActivity)
    assert child_start.parent == build
    assert child_start.act_id not in (7, build)
    assert isinstance(result, LogResult)
    assert (result.act_id, result.result_type, [f.valint for f in result.fields]) == (child_start.act_id, 105, [1, 2])
    assert child_stop == LogStopActivity(act_id=child_start.act_id)
    assert build_stop == LogStopActivity(act_id=build)


async def test_unknown_ids_send_nothing() -> None:
    _build, sent = await _wire([jsonlog.Result(9, 105, ()), jsonlog.Stop(9)])
    assert len(sent) == 2


async def test_a_second_start_of_one_id_starts_and_stops_at_once() -> None:
    """`std::map::emplace` constructs the `Activity` before it finds the key taken."""
    start = jsonlog.Start(id=7, level=3, type=101, text="x", fields=())
    _build, sent = await _wire([start, start])
    [_b, first, second, second_stop, first_stop, _build_stop] = sent
    assert second_stop == LogStopActivity(act_id=second.act_id)
    assert first_stop == LogStopActivity(act_id=first.act_id)


async def test_leftover_activities_stop_before_the_build() -> None:
    starts = [jsonlog.Start(id=i, level=3, type=101, text="x", fields=()) for i in (1, 2)]
    build, sent = await _wire(starts)
    assert [type(m) for m in sent[-3:]] == [LogStopActivity] * 3
    assert sent[-1].act_id == build
    assert [m.act_id for m in sent[-3:-1]] == [sent[2].act_id, sent[1].act_id]


async def test_set_phase_is_a_result_of_the_build() -> None:
    build, sent = await _wire([jsonlog.SetPhase("installPhase")])
    result = sent[1]
    assert isinstance(result, LogResult)
    assert (result.act_id, result.result_type, result.fields[0].valstr) == (
        build,
        ResultType.SET_PHASE,
        "installPhase",
    )


async def test_a_message_above_the_clients_verbosity_is_dropped() -> None:
    outputs = [jsonlog.Message(Verbosity.INFO, "shown"), jsonlog.Message(Verbosity.DEBUG, "hidden")]
    _build, sent = await _wire(outputs, verbosity=Verbosity.INFO)
    assert [m for m in sent if isinstance(m, LogNext)] == [LogNext(text="shown\n")]
