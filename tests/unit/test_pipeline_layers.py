"""The build pipeline's stages import downward only: goals, then the scheduler, then placement and execution, then the queue.

A goal asks the scheduler for a build; the scheduler places it and runs it;
every stage reads the queue. A stage that imports one above it couples the two
ways, and the layers stop being readable one at a time. Type-only imports
count: they are coupling as well. nixidae/nixidae#1318 (step 4).
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent

STAGES = {
    "pynixd.build_queue": 0,
    "pynixd.allocator": 1,
    "pynixd.placement": 1,
    "pynixd.placement_trace": 1,
    "pynixd.execution": 1,
    "pynixd.scheduler": 2,
    "pynixd.goals": 3,
}

ALLOWED = {
    ("pynixd.build_queue", "pynixd.placement_trace"): "a queued build holds the decision placement made for it; a type",
}


def stage_of(module: str) -> int | None:
    for prefix, stage in STAGES.items():
        if module == prefix or module.startswith(prefix + "."):
            return stage
    return None


def _is_module(name: str) -> bool:
    base = ROOT / name.replace(".", "/")
    return base.with_suffix(".py").exists() or (base / "__init__.py").exists()


def imported(module: str, *, package: bool, source: str) -> set[str]:
    """The modules *module* imports, the type-only and the lazy ones included."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = module.split(".") if package else module.split(".")[:-1]
                base = base[: len(base) - (node.level - 1)]
                target = ".".join([*base, *([node.module] if node.module else [])])
            else:
                target = node.module or ""
            submodules = {f"{target}.{alias.name}" for alias in node.names if _is_module(f"{target}.{alias.name}")}
            found |= submodules if len(submodules) == len(node.names) else {target, *submodules}
    return found


def _upward() -> set[tuple[str, str]]:
    edges: set[tuple[str, str]] = set()
    for path in (ROOT / "pynixd").rglob("*.py"):
        parts = list(path.relative_to(ROOT).with_suffix("").parts)
        package = parts[-1] == "__init__"
        module = ".".join(parts[:-1] if package else parts)
        own = stage_of(module)
        if own is None:
            continue
        for target in imported(module, package=package, source=path.read_text()):
            stage = stage_of(target)
            if stage is not None and stage > own:
                edges.add((module, target))
    return edges


def test_the_scanner_resolves_relative_and_submodule_imports() -> None:
    source = "from .goals.results import GoalResult\nfrom . import build_queue\nif TYPE_CHECKING:\n    from .scheduler import Scheduler\n"
    assert imported("pynixd.execution", package=False, source=source) == {
        "pynixd.goals.results",
        "pynixd.build_queue",
        "pynixd.scheduler",
    }


def test_the_scanner_finds_each_stage() -> None:
    """The runner may work in a store copy; a scan that found no stage proves nothing."""
    for prefix in STAGES:
        assert _is_module(prefix), prefix


def test_no_stage_imports_one_above_it() -> None:
    found = _upward()
    assert found - ALLOWED.keys() == set(), "move what both stages need below the lower one"
    assert ALLOWED.keys() - found == set(), "a stale entry: the import is gone, so its permission goes too"
