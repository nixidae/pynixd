"""The extended set and `tests/suites.toml` stay in step (nixidae/nixidae#1247).

A suite holds tests marked `@pytest.mark.extended` exactly when
`tests/suites.toml` sets `extended = true` for it. The extended run takes only
the suites flagged there, so a marked test in an unflagged suite runs nowhere;
and pytest exits 5 on a flagged suite with nothing marked, which fails the run.

Each project with a suite deselects the set by default. Whether a test belongs
in the set (slow or exhaustive past the budget in `CLAUDE.md`) is a judgement
this file does not check.
"""

from __future__ import annotations

import ast
import shlex
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SUITES = ROOT / "tests" / "suites.toml"
MARKER = "extended"


def marks_extended(source: str) -> bool:
    """Whether `source` names `pytest.mark.extended` anywhere: a decorator, `pytestmark`, or `pytest.param(marks=...)`."""
    for node in ast.walk(ast.parse(source)):
        if (
            isinstance(node, ast.Attribute)
            and node.attr == MARKER
            and isinstance(node.value, ast.Attribute)
            and node.value.attr == "mark"
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "pytest"
        ):
            return True
    return False


def marked_files(directory: Path) -> list[Path]:
    return sorted(path for path in directory.rglob("*.py") if marks_extended(path.read_text()))


def _suites() -> dict[str, dict[str, object]]:
    with SUITES.open("rb") as file:
        return tomllib.load(file)


def test_the_scanner_finds_the_suites() -> None:
    """A store copy without the sources would pass every check below with nothing scanned."""
    suites = _suites()
    assert {"unit", "functional", "protocol"} <= suites.keys()
    for name, suite in suites.items():
        directory = ROOT / str(suite["path"])
        assert any(directory.rglob("test_*.py")), f"{name}: no test file under {directory}"


def test_the_scanner_sees_each_spelling() -> None:
    assert marks_extended("import pytest\n@pytest.mark.extended\ndef test_x(): ...\n")
    assert marks_extended("import pytest\npytestmark = [pytest.mark.anyio, pytest.mark.extended]\n")
    assert marks_extended("import pytest\nP = pytest.param(1, marks=pytest.mark.extended)\n")


def test_the_scanner_ignores_what_is_not_the_marker() -> None:
    assert not marks_extended("import pytest\n@pytest.mark.slow\ndef test_x(): ...\n")
    assert not marks_extended("# pytest.mark.extended\nX = 'pytest.mark.extended'\n")
    assert not marks_extended("extended = 1\nmark.extended = 2\n")


def test_a_suite_is_flagged_exactly_when_it_holds_marked_tests() -> None:
    for name, suite in _suites().items():
        flagged = suite.get("extended", False) is True
        marked = marked_files(ROOT / str(suite["path"]))
        if marked and not flagged:
            listed = ", ".join(str(path.relative_to(ROOT)) for path in marked)
            raise AssertionError(f"{name}: {listed} mark tests extended; set `extended = true` in {SUITES.name}")
        if flagged and not marked:
            raise AssertionError(f"{name}: `extended = true` in {SUITES.name}, and no test is marked; pytest exits 5")


def project_of(directory: Path) -> Path:
    """The `pyproject.toml` pytest reads for a suite: the nearest one at or above it."""
    for candidate in (directory, *directory.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate / "pyproject.toml"
    raise AssertionError(f"no pyproject.toml above {directory}")


def test_each_project_deselects_the_set_by_default() -> None:
    projects = {project_of(ROOT / str(suite["path"])) for suite in _suites().values()}
    assert len(projects) >= 2, projects
    for pyproject in sorted(projects):
        with pyproject.open("rb") as file:
            options = tomllib.load(file)["tool"]["pytest"]["ini_options"]
        addopts = shlex.split(options["addopts"])
        assert "--strict-markers" in addopts, pyproject
        assert addopts[addopts.index("-m") + 1] == f"not {MARKER}", pyproject
        assert any(line.startswith(f"{MARKER}:") for line in options["markers"]), pyproject
