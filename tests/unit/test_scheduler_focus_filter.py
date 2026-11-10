"""`pynixd.filters.scheduler_focus`, the log filter dynhetz runs: a name it keeps must still log."""

from __future__ import annotations

import importlib.util
from types import SimpleNamespace

import pytest

from pynixd.filters import scheduler_focus


@pytest.mark.parametrize("name", sorted(scheduler_focus.KEEP_LOGGERS | scheduler_focus.KEEP_LOGGERS_DEBUG))
def test_each_kept_logger_is_a_module(name: str) -> None:
    """A logger is its module's `__name__`; a moved module silences the entry."""
    assert importlib.util.find_spec(name) is not None


def test_the_wal_checkpoint_is_kept() -> None:
    """dynhetz dropped it, so the checkpoint looked as if it never ran (nixidae/nixidae#1136)."""
    logger = SimpleNamespace(name="pynixd_store.local_store_db")
    event = {"event": "db_wal_checkpoint", "level": "info"}
    assert scheduler_focus.filter(logger, "info", event) == event
