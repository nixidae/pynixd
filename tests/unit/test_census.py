"""The process census a test writes before its deadline (`tests/_conftest/census.py`, nixidae/nixidae#1283).

A broken census makes no other test fail: it runs only when a test hangs.
"""

from __future__ import annotations

import os
import subprocess
from typing import TYPE_CHECKING

from tests._conftest.census import census, descendants

if TYPE_CHECKING:
    from pathlib import Path


def _fake(proc: Path, pid: int, ppid: int) -> None:
    (proc / str(pid)).mkdir()
    (proc / str(pid) / "status").write_text(f"Name:\tx\nPPid:\t{ppid}\n")


def test_descendants_come_parents_first_and_skip_others(tmp_path: Path) -> None:
    for pid, ppid in ((10, 1), (11, 10), (12, 11), (13, 10), (20, 1)):
        _fake(tmp_path, pid, ppid)
    assert descendants(10, tmp_path) == [11, 13, 12]
    assert descendants(20, tmp_path) == []


def test_a_waiting_child_is_named_with_what_it_waits_in() -> None:
    with subprocess.Popen(["sleep", "30"]) as child:
        try:
            text = census(os.getpid())
        finally:
            child.kill()
    [entry] = [part for part in text.split("\n\n") if part.startswith(f"pid {child.pid} ")]
    assert "sleep 30" in entry
    assert f"ppid {os.getpid()} " in entry
    assert "wchan=" in entry
    assert text.rstrip().split("\n\n")[-1].startswith("/proc/locks:")
