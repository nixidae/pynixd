"""A test calls no `Scheduler` method unbound on a fake scheduler.

`Scheduler.method(cast("Any", fake), ...)` checks nothing: a fake that lacks
a member fails in the guest's unit phase, and nixidae/nixidae#1242 broke nine
tests that way. Placement and execution take typed dependencies instead
(`pynixd/placement.py`, `pynixd/execution.py`), and their tests pass fakes the
type checker holds to Protocols. nixidae/nixidae#1246.
"""

from __future__ import annotations

import ast
from pathlib import Path

TESTS = Path(__file__).resolve().parent.parent

ALLOWED = {
    ("unit/test_store_choice.py", "schedule"): "the scheduling loop itself, driven over a fake scheduler",
    ("unit/test_store_choice.py", "on_sample"): "the loop's wake-up on a store sample",
    ("unit/test_local_build_slots.py", "_filter_schedulable"): "the loop's count of running builds",
}


def unbound_calls(source: str) -> set[str]:
    """The names of each `Scheduler.<name>(...)` call in *source*."""
    return {
        node.func.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "Scheduler"
    }


def _found() -> set[tuple[str, str]]:
    return {
        (str(path.relative_to(TESTS)), name)
        for path in TESTS.rglob("test_*.py")
        for name in unbound_calls(path.read_text())
    }


def test_the_scanner_finds_an_unbound_call() -> None:
    assert unbound_calls('Scheduler.execute_build(cast("Any", fake), build, store)') == {"execute_build"}


def test_the_scanner_ignores_a_bound_call_and_a_constructor() -> None:
    assert unbound_calls("scheduler.schedule()\nScheduler(ctx)\nother.Scheduler.schedule()") == set()


def test_the_scanner_reads_the_test_tree() -> None:
    """The runner may work in a store copy; a scan that found nothing proves nothing."""
    assert len(list(TESTS.rglob("test_*.py"))) > 100


def test_only_the_allowed_calls_are_unbound() -> None:
    found = _found()
    assert found - ALLOWED.keys() == set(), "type the dependency instead of calling `Scheduler` unbound on a fake"
    assert ALLOWED.keys() - found == set(), "a stale entry: the call is gone, so its permission goes too"
