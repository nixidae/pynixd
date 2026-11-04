"""The builder's `@nix` lines: `handleJSONLogMessage` with `trusted = false` (`logging.cc:437`).

A line the handler consumes is no log line and stays out of the tail. A line
it cannot read is a log line, after one line of Nix's complaint.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from nix_daemon_protocol.protocol import ActivityType, Verbosity

from ..hintfmt import ANSI_MAGENTA, ANSI_NORMAL

SOURCE = "the derivation builder"
PREFIX = "@nix "

Field = int | str


@dataclass(frozen=True)
class Start:
    """A file transfer the builder starts; the only activity an untrusted builder may start."""

    id: int
    level: int
    type: int
    text: str
    fields: tuple[Field, ...]


@dataclass(frozen=True)
class Stop:
    id: int


@dataclass(frozen=True)
class Result:
    id: int
    type: int
    fields: tuple[Field, ...]


@dataclass(frozen=True)
class SetPhase:
    phase: str


@dataclass(frozen=True)
class Message:
    """A line for `logger->log`: the builder's own `msg`, or Nix's complaint about a line."""

    level: int
    text: str


Event = Start | Stop | Result | SetPhase | Message


class _Unreadable(Exception):
    pass


def handle(line: str) -> tuple[list[Event], bool]:
    """The events *line* makes, and whether the handler consumed it."""
    if not line.startswith(PREFIX):
        return [], False
    try:
        message = json.loads(line[len(PREFIX) :])
    except ValueError as e:
        # NIX-DEVIATION (#27): `parseJSONMessage` (`logging.cc:425`) prints nlohmann's
        # parse error; pynixd prints Python's, which names the same place in other words.
        # Matching it means porting nlohmann's lexer for one line of a broken builder.
        return [Message(Verbosity.ERROR, f"bad JSON log message from {SOURCE}: {e}")], False
    try:
        return _events(message), True
    except _Unreadable as e:
        warning = f"{ANSI_MAGENTA}warning:{ANSI_NORMAL} Unable to handle a JSON message from {SOURCE}: {e}"
        return [Message(Verbosity.WARN, warning)], False


def _events(message: Any) -> list[Event]:
    if message is None:
        message = {}  # `operator[]` makes a null an object
    if not isinstance(message, dict):
        raise _Unreadable(
            f"[json.exception.type_error.305] cannot use operator[] with a string argument with {_kind(message)}"
        )
    action = _string(message, "action")
    if action == "start":
        kind = _number(message, "type")
        if kind != ActivityType.FILE_TRANSFER:
            return []
        return [
            Start(
                _number(message, "id"),
                _number(message, "level"),
                kind,
                _string(message, "text"),
                _fields(message.get("fields")),
            )
        ]
    if action == "stop":
        return [Stop(_number(message, "id"))]
    if action == "result":
        return [Result(_number(message, "id"), _number(message, "type"), _fields(message.get("fields")))]
    if action == "setPhase":
        return [SetPhase(_string(message, "phase"))]
    if action == "msg":
        text = _string(message, "msg")
        return [Message(_number(message, "level"), text)]
    return []


def _string(message: dict[str, Any], key: str) -> str:
    value = message.get(key)
    if not isinstance(value, str):
        raise _Unreadable(f"[json.exception.type_error.302] type must be string, but is {_kind(value)}")
    return value


def _number(message: dict[str, Any], key: str) -> int:
    """nlohmann's arithmetic `get`: a number or a boolean, a float truncated."""
    value = message.get(key)
    if not isinstance(value, int | float):
        raise _Unreadable(f"[json.exception.type_error.302] type must be number, but is {_kind(value)}")
    return int(value)


def _fields(raw: Any) -> tuple[Field, ...]:
    """`getFields` (`logging.cc:411`): an unsigned number or a string each.

    NIX-DEVIATION (#27): `getFields` throws a plain `Error` on any other
    type, which the handler's `json::exception` catch misses, so it escapes
    `BuildLog::flushLine` and ends the build. pynixd treats the line as one
    it cannot read: the build goes on and the line is a log line. A builder
    controls this text, and ending a sandboxed build over a log line buys
    nothing. Measure: a builder that prints `@nix {"action":"result",
    "id":1,"type":1,"fields":[-1]}` after starting activity 1, on both routes.
    """
    values = list(raw.values()) if isinstance(raw, dict) else [] if raw is None else raw
    if not isinstance(values, list):
        values = [values]
    fields: list[Field] = []
    for value in values:
        if isinstance(value, str) or (isinstance(value, int) and not isinstance(value, bool) and value >= 0):
            fields.append(value)
        else:
            raise _Unreadable(f"unsupported JSON type {_json_type(value)}")
    return tuple(fields)


def _kind(value: Any) -> str:
    """nlohmann's `type_name`."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int | float):
        return "number"
    if isinstance(value, str):
        return "string"
    return "array" if isinstance(value, list) else "object"


def _json_type(value: Any) -> int:
    """nlohmann's `value_t`, which `getFields` prints."""
    if value is None:
        return 0
    if isinstance(value, dict):
        return 1
    if isinstance(value, list):
        return 2
    if isinstance(value, bool):
        return 4
    if isinstance(value, int):
        return 5 if value < 0 else 6
    return 7
