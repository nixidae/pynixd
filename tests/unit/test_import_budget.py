"""``pynixd daemon`` must load neither ``asyncssh`` nor ``aiohttp``, and stay under a budget.

Issue #30 measured why this matters. Nix's functional suite restarts the
daemon for every configuration change, **344 times over the suite**, and each
start took 1.28 s where ``nix daemon`` takes 0.2 s. Of that 1.28 s only 0.20 s
is work: spawn the upstream daemon, connect, bind the socket. The rest is
``import``. Those 344 starts were 442 s of a 551 s gap.

``asyncssh`` and ``aiohttp`` were 232 of the 925 modules and 0.28 s of every
start, and a daemon that serves a Unix socket runs neither an SSH server nor
an HTTP cache. ``pynixd/store/__init__.py`` and ``pynixd/_optional.py`` now
resolve their names on first read, and ``pynixd/_lazy.py`` answers the two
``except`` clauses and the one ``isinstance`` that named a class without
needing one. ``environs`` went with them: it answered four calls and pulled
``marshmallow``, for 64 ms more.

**Without this file the next eager import puts the cost back in silence.**
Nothing else fails. An eager import makes every daemon start slower and no
assertion anywhere reads the import graph.

## Why this asserts a name and a count, and not a duration

A wall-clock budget is the direct measurement and the wrong gate. This suite
runs on a shared runner and on a laptop, and a millisecond figure that holds
on one is noise on the next. A module count is the same number everywhere.

The name check is the sharper half, and it has no headroom at all: the two
libraries are either in ``sys.modules`` or they are not.
"""

from __future__ import annotations

import ast
import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

from pynixd import _optional, store
from pynixd.config import HTTPBinaryCacheSpec

#: The most modules ``import pynixd.instance`` may load in a clean interpreter.
#:
#: Measured at 703 when issue #30 made both stacks lazy and dropped
#: `environs`, down from 925.
#: The headroom is small on purpose. A legitimate new dependency raises this
#: number in the commit that adds it, and a reader sees the cost. Raise it with
#: a measurement in the commit message, and never to make a red run green.
MODULE_BUDGET = 740

#: The libraries that only an SSH or an HTTP configuration may load.
#:
#: ``passlib`` comes with ``http_server``, and ``cryptography`` with
#: ``asyncssh``. Both are here because each one names its parent's cost.
FORBIDDEN = ("asyncssh", "aiohttp", "passlib", "cryptography")

#: The submodules of pynixd that only a configuration naming them may load.
OPTIONAL_MODULES = sorted(_optional.MODULES | {"ssh", "http_binary_cache", "reverse"})

_PROBE = f"""
import json, sys
import pynixd.instance
print(json.dumps({{
    "count": len(sys.modules),
    "present": sorted(n for n in sys.modules if n.split(".")[0] in {FORBIDDEN!r}),
    "optional_loaded": sorted(
        n for n in sys.modules
        if n.startswith("pynixd.") and n.split(".")[-1] in {OPTIONAL_MODULES!r}
    ),
}}))
"""


def _probe() -> dict[str, object]:
    """Import ``pynixd.instance`` in a clean interpreter and report what it loaded.

    ``PYTHONPATH`` and ``BEARTYPING`` are removed rather than
    inherited. ``nanopynix_testing.beartype_hook`` puts a startup directory on
    the first so that a spawned interpreter starts beartype and coverage, and
    sets the second for it. Either one would make the count depend on how the
    suite ran, which is the one thing a budget must not do.
    """
    env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "BEARTYPING"}}
    completed = subprocess.run(  # noqa: S603 -- sys.executable and a literal script, no shell
        [sys.executable, "-c", _PROBE],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if completed.returncode != 0:
        pytest.fail(f"the import probe exited {completed.returncode}:\n{completed.stderr}")
    loaded: dict[str, object] = json.loads(completed.stdout)
    return loaded


def test_the_daemon_loads_no_ssh_and_no_http_stack() -> None:
    """A Unix-socket daemon runs neither server, so it must load neither library."""
    result = _probe()

    assert result["present"] == [], (
        f"a fresh `import pynixd.instance` loaded {result['present']}. "
        "Something reached one of those at module level again; issue #30 says why that costs 0.28 s "
        "of every daemon start."
    )


def test_no_optional_module_of_pynixd_loads() -> None:
    """The four servers and the three optional stores wait for a configuration."""
    result = _probe()

    assert result["optional_loaded"] == []


def test_the_daemon_stays_under_the_module_budget() -> None:
    result = _probe()
    count = result["count"]
    assert isinstance(count, int)

    assert count <= MODULE_BUDGET, (
        f"a fresh `import pynixd.instance` loaded {count} modules, over the budget of {MODULE_BUDGET}. "
        "Raise the budget only with a measurement in the commit message."
    )


# ── The two tables, and the copy of each that the type checker reads ─


def _type_checking_names(module_path: Path) -> set[str]:
    """Every name that the `if TYPE_CHECKING:` blocks of *module_path* bind."""
    tree = ast.parse(module_path.read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if not (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING"):
            continue
        for child in ast.walk(node):
            if isinstance(child, ast.ImportFrom):
                names.update(alias.asname or alias.name for alias in child.names)
    return names


def test_the_store_table_and_the_type_checker_copy_agree() -> None:
    """`ORIGIN` runs, and the `if TYPE_CHECKING:` imports are what the type checker reads.

    The two are written apart, so they drift. A name in one and not the other
    means either a name nobody can import or a name the type checker cannot see.
    """
    path = Path(str(store.__file__))
    declared = _type_checking_names(path)
    # `Store` is the annotation of `is_http_binary_cache`, so it is in both.
    assert declared == set(store.ORIGIN)


def test_every_store_name_resolves_from_the_module_the_table_names() -> None:
    for name, origin in store.ORIGIN.items():
        module = importlib.import_module(f"pynixd.store.{origin}")
        assert getattr(store, name) is getattr(module, name)


def test_the_store_all_is_the_table_plus_the_one_predicate() -> None:
    assert set(store.__all__) == set(store.ORIGIN) | {"is_http_binary_cache"}


def test_an_unknown_store_name_raises_attribute_error() -> None:
    with pytest.raises(AttributeError):
        store.NoSuchStore  # type: ignore[attr-defined]  # that is the assertion  # noqa: B018


def test_the_optional_table_and_the_type_checker_copy_agree() -> None:
    path = Path(str(_optional.__file__))
    assert _type_checking_names(path) - {"ModuleType"} == _optional.MODULES


def test_every_optional_module_resolves() -> None:
    for name in _optional.MODULES:
        assert getattr(_optional, name) is importlib.import_module(f"pynixd.{name}")


def test_an_unknown_optional_module_raises_attribute_error() -> None:
    with pytest.raises(AttributeError):
        _optional.no_such_server  # type: ignore[attr-defined]  # that is the assertion  # noqa: B018


# ── The predicate that answers without importing ─────────────────────


def test_the_http_predicate_says_no_while_the_module_is_absent() -> None:
    """A class that nothing imported has no instance, so `False` is exact.

    The probe is a subprocess, because this suite has already imported the
    module by the time it reaches here.
    """
    probe = """
import json, sys
import pynixd.store as store
answer = store.is_http_binary_cache(object())
print(json.dumps({"answer": answer, "loaded": "pynixd.store.http_binary_cache" in sys.modules}))
"""
    env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "BEARTYPING"}}
    completed = subprocess.run(  # noqa: S603 -- sys.executable and a literal script, no shell
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if completed.returncode != 0:
        pytest.fail(f"the probe exited {completed.returncode}:\n{completed.stderr}")
    result = json.loads(completed.stdout)

    assert result["answer"] is False
    assert result["loaded"] is False, "asking the question must not load the module"


def test_the_http_predicate_says_yes_for_a_real_cache_store() -> None:
    """And it answers correctly once a configuration made one."""
    cache = HTTPBinaryCacheSpec(url="https://cache.example.org").to_store("cache")

    assert store.is_http_binary_cache(cache)
    assert not store.is_http_binary_cache(cast("store.Store", object()))
