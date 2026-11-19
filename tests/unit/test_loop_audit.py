"""`tests/_conftest/loop_audit.py` records pynixd's file calls on the loop, and nothing else."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import anyio.to_thread
import pytest

from tests._conftest import loop_files
from tests._conftest.loop_audit import Frame, Kind, LoopAudit, Mode, Roots, Site, classify, stale, violations

if TYPE_CHECKING:
    from collections.abc import Iterator

ROOTS = Roots(
    first_party=("/work/pynixd/", "/work/pynixd-store/src/pynixd_store/"),
    tests="/work/tests/",
    skip=frozenset({"/work/tests/_conftest/loop_files.py"}),
)


def test_the_site_is_the_innermost_pynixd_frame() -> None:
    frames = [
        Frame("/work/tests/_conftest/loop_files.py", 90, "wrapper"),
        Frame("/usr/lib/python3.14/pathlib/_local.py", 10, "exists"),
        Frame("/work/pynixd/store/local_host.py", 42, "open_db"),
        Frame("/work/pynixd/server.py", 7, "serve"),
    ]
    site = classify("os.stat", "/work/f", frames, ROOTS)
    assert site is not None
    assert site.site == "pynixd/store/local_host.py:42 open_db"
    assert site.leaf == "/usr/lib/python3.14/pathlib/_local.py:10 exists"
    assert site.kind is Kind.FILE
    assert not site.from_test


def test_a_call_with_no_pynixd_frame_is_the_tests_own() -> None:
    frames = [Frame("/work/tests/unit/test_x.py", 3, "test_x"), Frame("/usr/lib/x.py", 1, "run")]
    assert classify("os.listdir", "/work/f", frames, ROOTS) is None


def test_a_test_frame_marks_the_site() -> None:
    frames = [Frame("/work/pynixd-store/src/pynixd_store/add.py", 5, "add"), Frame("/work/tests/unit/t.py", 9, "t")]
    site = classify("open", "/work/f", frames, ROOTS)
    assert site is not None
    assert site.from_test
    assert site.site == "pynixd_store/add.py:5 add"


def test_an_import_and_a_spawn_are_kinds_of_their_own() -> None:
    pynixd = Frame("/work/pynixd/cli/x.py", 1, "f")
    imported = classify("open", "/work/f", [Frame("<frozen importlib._bootstrap>", 1, "_load"), pynixd], ROOTS)
    spawned = classify("os.posix_spawn", "/work/f", [pynixd], ROOTS)
    assert imported is not None
    assert imported.kind is Kind.IMPORT
    assert spawned is not None
    assert spawned.kind is Kind.SPAWN


def test_a_traceback_and_a_proc_file_are_kinds_of_their_own() -> None:
    pynixd = Frame("/work/pynixd/monitor.py", 1, "sample")
    traced = classify("os.stat", "/work/pynixd/x.py", [Frame("/usr/lib/linecache.py", 9, "checkcache"), pynixd], ROOTS)
    pseudo = classify("open", "/proc/pressure/cpu", [pynixd], ROOTS)
    assert traced is not None
    assert traced.kind is Kind.TRACEBACK
    assert pseudo is not None
    assert pseudo.kind is Kind.PSEUDO


def _site(kind: Kind, site: str, *, from_test: bool = False) -> Site:
    return Site(kind=kind, event="open", site=site, leaf=site, from_test=from_test)


def test_the_gate_fails_on_a_server_file_site_no_exemption_names() -> None:
    server = _site(Kind.FILE, "pynixd/a.py:3 read")
    moved = _site(Kind.FILE, "pynixd/b.py:9 write")
    via_test = _site(Kind.FILE, "pynixd/c.py:1 f", from_test=True)
    pseudo = _site(Kind.PSEUDO, "pynixd/d.py:1 g")
    exempt = {"pynixd/b.py write": "a debt", "pynixd/gone.py h": "fixed"}
    assert violations([server, moved, via_test, pseudo], exempt) == [server]
    assert stale([server, moved, via_test, pseudo], exempt) == ["pynixd/gone.py h"]


@pytest.fixture
def audit(monkeypatch: pytest.MonkeyPatch) -> Iterator[LoopAudit]:
    """A recorder whose audited code is this file, with the `os` wrappers undone afterwards."""
    for name in loop_files.UNAUDITED:
        if hasattr(os, name):
            monkeypatch.setattr(os, name, getattr(os, name))
    for supports in ("supports_fd", "supports_dir_fd", "supports_follow_symlinks", "supports_effective_ids"):
        monkeypatch.setattr(os, supports, set(getattr(os, supports)))
    here = str(Path(__file__).resolve())
    audit = LoopAudit(roots=Roots(first_party=(here,), tests="/nowhere/", skip=ROOTS.skip), mode=Mode.RECORD, exempt={})
    loop_files.watch_unaudited()
    loop_files.set_session_sink(audit.sink)
    try:
        yield audit
    finally:
        loop_files.set_session_sink(None)


def _stat_here() -> None:
    Path(__file__).exists()


async def test_a_stat_on_the_loop_is_recorded_and_one_in_a_thread_is_not(audit: LoopAudit) -> None:
    await anyio.to_thread.run_sync(_stat_here)
    assert not audit.sites
    _stat_here()
    [site] = audit.sites
    assert site.event == "os.stat"
    assert site.site.endswith(" _stat_here")


async def test_shutil_still_copies_with_the_wrappers(audit: LoopAudit, tmp_path: Path) -> None:
    """`shutil` asks `os.supports_*` for the function objects the wrappers replace."""
    (tmp_path / "a").write_bytes(b"x")
    await anyio.to_thread.run_sync(lambda: shutil.copy2(tmp_path / "a", tmp_path / "b"))
    assert (tmp_path / "b").read_bytes() == b"x"
    assert os.stat in os.supports_follow_symlinks
