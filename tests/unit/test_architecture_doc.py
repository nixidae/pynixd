"""`docs/architecture.md` names only what exists.

The map it replaced named `BackendPool`, `OutputPuller` and `PathTracker`, and none of them existed. Each name
in back quotes of one of three shapes must resolve: a file or directory path, a class (or an operation whose
`Request` class exists), and a module-level constant. Other back-quoted text is a setting or a command, and
this test does not read it. It does not check that a sentence is true (ASD-STE100 and review do that).
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "architecture.md"
SCANNED = ("pynixd", "pynixd-store", "nix-daemon-protocol")
QUOTED = re.compile(r"`([^`\n]+)`")
PATH = re.compile(r"^[\w.-]+(?:/[\w.-]+)*(?:/|\.(?:py|md))$")
CLASS = re.compile(r"^[A-Z][a-z][A-Za-z0-9]*$")
CONSTANT = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$")


def defined_names(root: Path) -> tuple[set[str], set[str]]:
    """The class names, and the module-level constant names, of the scanned packages under *root*."""
    classes: set[str] = set()
    constants: set[str] = set()
    for top in SCANNED:
        for path in (root / top).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            classes.update(node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef))
            for node in tree.body:
                targets = (
                    node.targets
                    if isinstance(node, ast.Assign)
                    else [node.target]
                    if isinstance(node, ast.AnnAssign)
                    else []
                )
                constants.update(t.id for t in targets if isinstance(t, ast.Name))
    return classes, constants


def unresolved(text: str, root: Path, classes: set[str], constants: set[str]) -> list[str]:
    """Each back-quoted name in *text* of a checked shape that does not resolve."""
    missing = []
    for name in QUOTED.findall(text):
        if PATH.match(name):
            ok = (root / name).is_dir() if name.endswith("/") else (root / name).is_file()
        elif CLASS.match(name):
            ok = name in classes or f"{name}Request" in classes
        elif CONSTANT.match(name):
            ok = name in constants
        else:
            continue
        if not ok:
            missing.append(name)
    return missing


def test_the_scanner_found_the_doc_and_the_code() -> None:
    classes, constants = defined_names(ROOT)
    assert DOC.is_file()
    assert {"LocalDBStore", "StoreEngine"} <= classes
    assert "WIRE_REGISTRY" in constants


def test_the_scanner_reads_each_shape_both_ways(tmp_path: Path) -> None:
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "real.py").write_text("")
    text = "`pkg/real.py` `pkg/gone.py` `pkg/` `gone/` `Here` `Gone` `Op` `HERE_TOO` `GONE_TOO` `daemon = false` `min-free`"
    assert unresolved(text, tmp_path, {"Here", "OpRequest"}, {"HERE_TOO"}) == [
        "pkg/gone.py",
        "gone/",
        "Gone",
        "GONE_TOO",
    ]


def test_every_name_the_architecture_doc_quotes_exists() -> None:
    classes, constants = defined_names(ROOT)
    assert unresolved(DOC.read_text(encoding="utf-8"), ROOT, classes, constants) == []
