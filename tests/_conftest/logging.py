"""Structured logging configuration and per-test log folder fixtures.

Uses a single autouse fixture (``test_logging``) that:
1. Captures every structlog event into a per-test list
2. Writes JSON events to a per-test folder (filtered.log + unfiltered.log)
3. Logs per-test statistics to logstats.txt on teardown
"""

from __future__ import annotations

import json
import linecache
import logging
import re
import sys
import time
import traceback
from collections import Counter
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path
    from typing import TextIO

    import pytest

import pytest
import structlog
from structlog import DropEvent

from tests._conftest.constants import _log_dir_key

# ── Life counter: cooperative vote-to-drop system ────────────────

_LIFE_KEY = "_pynixd_life"

_dropped_counter: Counter[tuple[str, str, str]] = Counter()


def _life_check_processor(logger: Any, method_name: str, event_dict: Any) -> Any:
    """Final gate — drop events whose life counter is negative."""
    life = event_dict.pop(_LIFE_KEY, 0)
    if life < 0:
        key = (method_name, str(event_dict.get("logger", "")), str(event_dict.get("event", "")))
        _dropped_counter[key] += 1
        raise DropEvent
    return event_dict


# ── Capture: snapshot every event for per-test statistics ─────────

_captured_events: list[dict[str, Any]] = []


def _capture_processor(logger: Any, method_name: str, event_dict: Any) -> Any:
    """Snapshot every event before _life_check_processor can drop it.

    Placed right before _life_check_processor in the chain so dropped
    events are also countable.  Always passes through — never raises DropEvent.
    """
    _captured_events.append(dict(event_dict))
    return event_dict


# ── Exception formatting for JSON output ─────────────────────────


def _format_exception_for_json(logger: Any, method_name: str, event_dict: Any) -> Any:
    """Convert exc_info to a structured dict for JSON serialization."""
    import sys

    exc_info = event_dict.pop("exc_info", None)
    if exc_info is True:
        exc_info = sys.exc_info()
    if exc_info and not isinstance(exc_info, str):
        event_dict["exception"] = {
            "type": type(exc_info[1]).__name__,
            "message": str(exc_info[1]),
            "traceback": [
                {
                    "file": frame.filename,
                    "line": frame.lineno or 0,
                    "function": frame.name,
                    "code": linecache.getline(frame.filename, frame.lineno or 0).strip() or None,
                }
                for frame in traceback.extract_tb(exc_info[2])
            ],
        }
    return event_dict


# ── Foreign stdlib log enrichment ─────────────────────────────────


def _stdlib_to_event_dict(logger: Any, method_name: str, event_dict: Any) -> Any:
    """Enrich a foreign (non-structlog) LogRecord with logger/level keys.

    Called by ProcessorFormatter.foreign_pre_chain for stdlib loggers
    like asyncssh that don't use structlog.
    """
    record = event_dict["_record"]
    event_dict["message"] = event_dict.pop("event")  # rename for foreign records
    event_dict["logger"] = record.name
    event_dict["level"] = record.levelno
    event_dict["log_level"] = record.levelname.lower()
    return event_dict


# ── Time stampers ────────────────────────────────────────────────

_session_start_time = time.monotonic()
_test_start_time: float = _session_start_time
_original_excepthook = sys.excepthook


def _abs_time_stamper(logger: Any, method_name: str, event_dict: Any) -> Any:
    event_dict["_abs_time"] = time.monotonic()
    return event_dict


def _relative_time_stamper(logger: Any, method_name: str, event_dict: Any) -> Any:
    abs_time = event_dict.pop("_abs_time", None) or time.monotonic()
    start = event_dict.pop("test_start_time", None) or _session_start_time
    elapsed = abs_time - start
    seconds = int(elapsed)
    milliseconds = int((elapsed - seconds) * 1000)
    event_dict["timestamp"] = f"{seconds:03d}.{milliseconds:03d}"
    return event_dict


# ── Event dict key sorter for consistent JSON output ─────────────

_KEY_PRIORITY = ("timestamp", "level", "event", "logger")


def _sort_event_dict_keys(logger: Any, method_name: str, event_dict: Any) -> dict[str, Any]:
    """Reorder event_dict keys: priority keys first, rest sorted alphabetically."""
    prioritized = {k: event_dict.pop(k) for k in _KEY_PRIORITY if k in event_dict}
    sorted_keys = sorted(event_dict)
    return {**prioritized, **{k: event_dict[k] for k in sorted_keys}}


# ── Default processor chain ──────────────────────────────────────

_BASE_PROCESSORS: list[Callable[[Any, str, Any], Any]] = [
    structlog.stdlib.filter_by_level,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.contextvars.merge_contextvars,
    _abs_time_stamper,
    _relative_time_stamper,
    structlog.processors.StackInfoRenderer(),
    _capture_processor,  # ← snapshot BEFORE the drop gate
    _life_check_processor,  # ← may DropEvent here
    _format_exception_for_json,  # ← convert exc_info to structured dict
    _sort_event_dict_keys,  # ← consistent key ordering before JSON
    structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
]


_AUTO_PROCESSOR_INDEX = 3  # insertion point for extra_processors (after merge_contextvars)


def configure_test_logging(*, extra_processors: list[Callable[[Any, str, Any], Any]] | None = None) -> None:
    """Reset structlog to test defaults, optionally injecting extra processors.

    Extra processors are inserted after ``merge_contextvars`` (position
    ``_AUTO_PROCESSOR_INDEX``) so they
    have access to ``event``, ``logger``, and ``level`` fields, and execute
    before ``_capture_processor`` (so captured events include any mutations).
    """
    if extra_processors:
        processors = (
            _BASE_PROCESSORS[:_AUTO_PROCESSOR_INDEX] + extra_processors + _BASE_PROCESSORS[_AUTO_PROCESSOR_INDEX:]
        )
    else:
        processors = _BASE_PROCESSORS

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )


# ── Processor factories (vote-to-drop helpers) ───────────────────


def suppress_messages_containing(*substrings: str) -> Callable[[Any, str, Any], Any]:
    lowered = tuple(s.lower() for s in substrings)

    def _suppress(logger: Any, method_name: str, event_dict: Any) -> Any:
        msg = str(event_dict.get("event", "")).lower()
        if any(s in msg for s in lowered):
            event_dict[_LIFE_KEY] = event_dict.get(_LIFE_KEY, 0) - 1
        return event_dict

    return _suppress


def revive_messages_containing(*substrings: str) -> Callable[[Any, str, Any], Any]:
    lowered = tuple(s.lower() for s in substrings)

    def _revive(logger: Any, method_name: str, event_dict: Any) -> Any:
        msg = str(event_dict.get("event", "")).lower()
        if any(s in msg for s in lowered):
            event_dict[_LIFE_KEY] = event_dict.get(_LIFE_KEY, 0) + 1
        return event_dict

    return _revive


def suppress_messages_matching(pattern: str) -> Callable[[Any, str, Any], Any]:
    regex = re.compile(pattern)

    def _suppress(logger: Any, method_name: str, event_dict: Any) -> Any:
        msg = str(event_dict.get("event", ""))
        if regex.search(msg):
            event_dict[_LIFE_KEY] = event_dict.get(_LIFE_KEY, 0) - 1
        return event_dict

    return _suppress


def revive_messages_matching(pattern: str) -> Callable[[Any, str, Any], Any]:
    regex = re.compile(pattern)

    def _revive(logger: Any, method_name: str, event_dict: Any) -> Any:
        msg = str(event_dict.get("event", ""))
        if regex.search(msg):
            event_dict[_LIFE_KEY] = event_dict.get(_LIFE_KEY, 0) + 1
        return event_dict

    return _revive


# ── Apply initial configuration ──────────────────────────────────

configure_test_logging()

log = structlog.get_logger(__name__)

# Suppress noisy loggers globally.
logging.getLogger("asyncio").setLevel(logging.INFO)
logging.getLogger("aiosqlite").setLevel(logging.INFO)
logging.getLogger("pynixd.store.pool").setLevel(logging.INFO)


# ── Level management ──────────────────────────────────────────────


@contextmanager
def set_log_levels(levels: dict[str, int]) -> Generator[None]:
    """Temporarily set logger levels, restoring them on exit."""
    saved = {}
    for name, level in levels.items():
        logger = logging.getLogger(name)
        saved[name] = logger.level
        logger.setLevel(level)
    try:
        yield
    finally:
        for name, level in saved.items():
            logging.getLogger(name).setLevel(level)


# ── Log directory fixture ────────────────────────────────────────


@pytest.fixture(scope="session")
def test_log_dir(request: pytest.FixtureRequest) -> Path:
    """Return the session-wide log directory."""
    return request.session.config.stash[_log_dir_key]


# ── Human-readable stderr renderer ───────────────────────────────


def _human_readable_renderer(logger: Any, method_name: str, event_dict: Any) -> str:
    """Render event_dict as a human-readable string for stderr output."""
    ts = event_dict.get("timestamp", "")
    level = str(event_dict.get("log_level", event_dict.get("level", "")))
    logger_name = event_dict.get("logger", "")
    event = event_dict.get("event") or event_dict.get("message", "")
    skip = {"event", "logger", "level", "log_level", "timestamp", "exception"}
    extras = "    ".join(f"{k}={v}" for k, v in event_dict.items() if k not in skip)
    return f"[{ts}] {level.upper()} {logger_name}    {event}    {extras}"


# ── Setup / teardown helpers ─────────────────────────────────────


def _setup_test_logging(log_dir: Path) -> tuple[logging.FileHandler, logging.StreamHandler[TextIO]]:
    """Attach per-test log handlers and configure structlog.

    Creates two handlers:
    - FileHandler → filtered.log (JSON, real-time, DEBUG+)
    - StreamHandler → stderr (human-readable, WARNING+)

    Returns (file_handler, stderr_handler) for teardown.
    """
    log_dir.mkdir(parents=True, exist_ok=True)

    global _test_start_time, _original_excepthook
    _test_start_time = time.monotonic()

    _original_excepthook = sys.excepthook

    def _jsonl_excepthook(exc_type: type[BaseException], exc_value: BaseException, exc_tb: Any) -> None:
        entry = {
            "timestamp": f"{time.monotonic() - _test_start_time:09.3f}",
            "type": exc_type.__name__,
            "message": str(exc_value),
            "traceback": "".join(traceback.format_exception(exc_type, exc_value, exc_tb)),
        }
        with (log_dir / "exceptions.jsonl").open("a") as f:
            f.write(json.dumps(entry, default=str) + "\n")
        _original_excepthook(exc_type, exc_value, exc_tb)

    sys.excepthook = _jsonl_excepthook

    foreign_pre: list[Callable[[Any, str, Any], Any]] = [
        _stdlib_to_event_dict,
        _abs_time_stamper,
        _relative_time_stamper,
    ]

    # File handler → JSON output via ProcessorFormatter
    json_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=foreign_pre,
    )
    file_handler = logging.FileHandler(log_dir / "filtered.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(json_formatter)

    # Stderr handler → human-readable, WARNING+
    human_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            _human_readable_renderer,
        ],
        foreign_pre_chain=foreign_pre,
    )
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(human_formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    root.addHandler(stderr_handler)

    configure_test_logging()

    return file_handler, stderr_handler


def _teardown_test_logging(
    file_handler: logging.FileHandler,
    stderr_handler: logging.StreamHandler[TextIO],
    events: list[dict[str, Any]],
    log_dir: Path,
) -> None:
    """Remove per-test handlers and write unfiltered log + stats."""
    root = logging.getLogger()
    global _original_excepthook
    sys.excepthook = _original_excepthook

    root.removeHandler(file_handler)
    root.removeHandler(stderr_handler)
    # **The level that was in effect before the test, and not a literal.**
    # This reset was `WARNING`, so every INFO record between two tests was
    # dropped by the logger before any handler saw it -- including the whole
    # startup of the session server. `session_logging` raises this to INFO
    # for exactly that window. Issue #47.
    root.setLevel(_root_level_outside_tests)
    file_handler.close()
    stderr_handler.close()

    # Write unfiltered.log — all captured events as JSON
    with (log_dir / "unfiltered.log").open("w") as f:
        f.writelines(json.dumps(e, default=str) + "\n" for e in events)

    # Write logstats.txt
    if events:
        _write_stats(events, log_dir / "logstats.txt")


def _write_stats(events: list[dict[str, Any]], path: Path) -> None:
    """Write per-test log statistics to a file."""
    by_source: Counter[tuple[str, str]] = Counter()
    for ed in events:
        by_source[(ed.get("logger", ""), ed.get("event", ""))] += 1

    dropped = sum(1 for e in events if e.get(_LIFE_KEY, 0) < 0)

    with path.open("w") as f:
        f.write(f"total: {len(events)}\n")
        f.write(f"dropped: {dropped}\n")
        f.write(f"unique (logger, event): {len(by_source)}\n")
        f.write("\ntop (logger, event) pairs:\n")
        f.writelines(f"  {logger_}:{event} x{count}\n" for (logger_, event), count in by_source.most_common(10))


# ── Single autouse fixture ───────────────────────────────────────

_in_test: bool = False
"""True while a per-test handler is attached. `session_logging` reads it."""

_root_level_outside_tests: int = logging.WARNING
"""What the root logger goes back to when a test ends.

`session_logging` raises it, so that what happens between two tests is
recorded rather than filtered away before a handler sees it."""


@pytest.fixture(scope="session", autouse=True)
def session_logging(test_log_dir: Path) -> Generator[Path, Any, Any]:
    """Capture what happens outside any test, under `_session/`.

    **The store every scheduling test builds on is the one store whose
    startup nothing recorded.** `test_logging` below is function-scoped, and
    pytest sets a session fixture up before it, so the pynixd server of
    `tests/_conftest/config.py` spawns its daemons and runs its capability
    probe with no file handler attached. Nothing it logs reaches a per-test
    folder, and nothing reaches the CI artifact.

    Measured on CI run 35191328467: across 773 test folders, every
    `spawning_managed_daemon` belongs to a per-test store and not one belongs
    to the session `local` or `builder`. Reading that absence as "the session
    stores do not probe" is a mistake this directory should not make twice.
    Issue #47.

    INFO and not DEBUG, because a test's own events already go to its own
    folder and this would otherwise copy all of them. What is wanted here is
    the startup, the probe verdicts and the warnings.
    """
    log_dir = test_log_dir / "_session"
    log_dir.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(log_dir / "filtered.log")
    handler.setLevel(logging.INFO)
    # Only what happens outside a test. A test's own events already have a
    # folder, and copying them here doubles the artifact for nothing.
    handler.addFilter(lambda _record: not _in_test)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
            foreign_pre_chain=[
                _stdlib_to_event_dict,
                _abs_time_stamper,
                _relative_time_stamper,
            ],
        ),
    )
    root = logging.getLogger()
    root.addHandler(handler)
    # **The logger filters before the handler does.** Outside a test the root
    # sits at WARNING, so an INFO record is dropped before it can reach this
    # handler at all -- which is why the session server's startup produced two
    # warnings and nothing else on the first attempt at this.
    global _root_level_outside_tests
    previous, _root_level_outside_tests = _root_level_outside_tests, logging.INFO
    root.setLevel(logging.INFO)
    try:
        yield log_dir
    finally:
        _root_level_outside_tests = previous
        root.removeHandler(handler)
        handler.close()


@pytest.fixture(autouse=True)
def test_logging(request: pytest.FixtureRequest, test_log_dir: Path) -> Generator[Path, Any, Any]:
    """Per-test log capture, folder output, and statistics.

    Creates a per-test folder under ``test_log_dir``, sets up JSON file
    logging and WARNING+ stderr logging, and on teardown writes
    unfiltered events and statistics to the folder.

    Yields the per-test folder path for other fixtures to use.
    """
    events: list[dict[str, Any]] = []
    global _captured_events
    _captured_events = events

    # Build folder path: <run_id>/<test_file>/<test_name>/
    test_file = request.node.path.stem
    test_name = request.node.name.replace("/", "_").replace("[", "_").replace("]", "_")
    log_dir = test_log_dir / test_file / test_name

    file_handler, stderr_handler = _setup_test_logging(log_dir)
    structlog.contextvars.bind_contextvars(test_start_time=time.monotonic())

    global _in_test
    _in_test = True
    try:
        yield log_dir  # yield the folder path for other fixtures to use
    finally:
        _in_test = False

    _teardown_test_logging(file_handler, stderr_handler, events, log_dir)
    _captured_events = []
