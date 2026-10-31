"""Pytest hooks: collection, session lifecycle, and reporting."""

from __future__ import annotations

import functools
import inspect
import json
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import pytest
import structlog

from tests._conftest.constants import TMP_PREFIX, _log_dir_key, log_folder_name

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator

    import pluggy

log = structlog.get_logger(__name__)


# ── Collection filtering ──────────────────────────────────────────


def pytest_ignore_collect(collection_path: Path) -> bool:
    """Skip integration test directories during normal collection."""
    return "nar_integration" in str(collection_path) or "drv_integration" in str(collection_path)


# ── Session lifecycle ─────────────────────────────────────────────


def pytest_sessionstart(session: pytest.Session) -> None:
    """Create session-wide log directory and clean up leftovers."""
    run_id = str(int(time.time()))
    # `$PYNIXD_TEST_LOG_DIR` names the root, and `/tmp` is only the
    # default. A run inside a guest or a build sandbox needs these files
    # somewhere that outlives it: `/tmp` there is thrown away with the
    # machine, and a parity failure says nothing but "logs: /tmp/..." --
    # a path to a file that no longer exists. See tests/guest/suite.py.
    root = Path(os.environ.get("PYNIXD_TEST_LOG_DIR", "/tmp/pynixd-logs"))
    log_dir = root / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    session.config.stash[_log_dir_key] = log_dir

    tr = session.config.pluginmanager.get_plugin("terminalreporter")
    if tr:
        tr.write_line(f"\nIMPORTANT: Test run logs: {log_dir}")

    from tests._conftest.helpers import rmtree_robust_glob

    # Only the directories that this project makes. `/tmp/pytest-of-lillecarl/*`
    # was here as well, and that directory is the shared temporary root of
    # pytest: every suite of this repository puts its `tmp_path` under it. The
    # line therefore deleted the leftovers of another project at the start of
    # each run, and one of those leftovers was a root-owned overlayfs work
    # directory that no cleanup can remove.
    #
    # pytest keeps the last three roots of its own and removes the rest, so
    # nothing here has to.
    rmtree_robust_glob(f"{TMP_PREFIX}*")


def pytest_terminal_summary(
    terminalreporter: pytest.TerminalReporter,
    exitstatus: int,
    config: pytest.Config,
) -> None:
    """Print log directory path at the end of the test run."""
    log_dir = config.stash.get(_log_dir_key, None)
    if log_dir:
        terminalreporter.write_line(f"\nIMPORTANT: Test run logs: {log_dir}")


# ── Collection / modification ─────────────────────────────────────


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Wrap each async test in a deadline."""

    for item in items:
        if (
            isinstance(item, pytest.Function)
            and inspect.iscoroutinefunction(item.obj)
            and not getattr(item.obj, "_pynixd_timeout_wrapped", False)
        ):
            item.obj = _wrap_with_timeout(item)
            item.obj._pynixd_timeout_wrapped = True  # type: ignore[attr-defined]  # pytest types item.obj as a plain callable


def _wrap_with_timeout(item: pytest.Function) -> Callable[..., Awaitable[object]]:
    """Wrap an async test function in `anyio.fail_after`, so that it cannot hang."""
    original_func = item.obj

    @functools.wraps(original_func)
    async def wrapped(*args: object, **kwargs: object) -> object:
        timeout = item.config.getoption("async_test_timeout")
        try:
            with anyio.fail_after(timeout):
                return await original_func(*args, **kwargs)
        except TimeoutError:
            log.exception(
                "test_timeout_triggered",
                test=item.nodeid,
                timeout=timeout,
            )
            raise

    return wrapped


# ── Test reporting ────────────────────────────────────────────────


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item,
    call: pytest.CallInfo[None],
) -> Generator[None, pluggy.Result[pytest.TestReport]]:
    """Write failure details to the log file."""
    outcome = yield
    report = outcome.get_result()

    if report.when == "call" and report.failed:
        log_dir = item.config.stash.get(_log_dir_key, None)
        if log_dir:
            test_file = item.path.stem
            test_name = log_folder_name(item.name)
            folder = log_dir / test_file / test_name
            folder.mkdir(parents=True, exist_ok=True)

            # Write exception to JSONL for machine consumption
            exc_info = call.excinfo
            if exc_info:
                import traceback as tb

                entry = {
                    "timestamp": f"{time.monotonic():.3f}",
                    "type": exc_info.type.__name__,
                    "message": str(exc_info.value),
                    "traceback": "".join(tb.format_exception(exc_info.type, exc_info.value, exc_info.tb)),
                }
                with (folder / "exceptions.jsonl").open("a") as f:
                    f.write(json.dumps(entry, default=str) + "\n")

            # Suppress traceback from CLI output; it's in exceptions.jsonl
            report.longrepr = f"logs: {folder}"
            report.sections = []
