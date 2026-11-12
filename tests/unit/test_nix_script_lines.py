"""Each `<script>.sh:<line>` a pynixd file names is a line of Nix's functional suite.

A port of a script (nixidae/nixidae#1248) names the lines it stands for, and so does a correction the
suite found. When Nix moves a line, the reference goes stale with no failure. The guest sets
`NIX_FUNCTIONAL_TESTS` to the `tests/functional` of each Nix whose suite pynixd runs, separated by
`:`, and a reference holds when one of them has the line.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCANNED = ("pynixd", "pynixd-store", "nix-daemon-protocol", "tests")
REFERENCE = re.compile(r"`(?:tests/functional/)?([A-Za-z0-9_./-]+\.sh):(\d+)`")


def references(root: Path) -> list[tuple[Path, int, str, int]]:
    """Each reference under *root*: the file and line that names it, the script and the script's line."""
    found = []
    for top in SCANNED:
        for path in sorted((root / top).rglob("*.py")):
            for number, text in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                found.extend((path, number, m[1], int(m[2])) for m in REFERENCE.finditer(text))
    return found


def resolves(script: str, line: int, sources: list[Path]) -> bool:
    """Whether a source holds *script* with a non-blank *line*; a bare name may sit in a subdirectory."""
    for source in sources:
        candidates = [source / script] if "/" in script else [source / script, *source.glob(f"*/{script}")]
        for candidate in candidates:
            if candidate.is_file():
                lines = candidate.read_text(encoding="utf-8", errors="replace").splitlines()
                if 0 < line <= len(lines) and lines[line - 1].strip():
                    return True
    return False


def test_the_scanner_finds_references() -> None:
    assert len(references(ROOT)) > 50


def test_a_reference_resolves_by_name_or_in_a_subdirectory(tmp_path: Path) -> None:
    (tmp_path / "ca").mkdir()
    (tmp_path / "build.sh").write_text("one\n\nthree\n")
    (tmp_path / "ca" / "substitute.sh").write_text("one\n")
    assert resolves("build.sh", 3, [tmp_path])
    assert resolves("substitute.sh", 1, [tmp_path])
    assert resolves("ca/substitute.sh", 1, [tmp_path])
    assert not resolves("build.sh", 2, [tmp_path]), "a blank line is no assertion"
    assert not resolves("build.sh", 4, [tmp_path])
    assert not resolves("gone.sh", 1, [tmp_path])


@pytest.mark.skipif("NIX_FUNCTIONAL_TESTS" not in os.environ, reason="only the guest knows the Nix sources")
def test_every_reference_names_a_line_of_the_suite() -> None:
    sources = [Path(p) for p in os.environ["NIX_FUNCTIONAL_TESTS"].split(":")]
    assert all(source.is_dir() for source in sources), sources
    stale = [
        f"{path.relative_to(ROOT)}:{number}: {script}:{line}"
        for path, number, script, line in references(ROOT)
        if not resolves(script, line, sources)
    ]
    assert not stale, "\n".join(stale)
