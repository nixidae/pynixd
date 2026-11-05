"""The sandbox helper finds pynixd's packages however this process found them.

The packaged `pynixd` is a Nix-wrapped application: its wrapper adds the
packages with `site.addsitedir`, so `sys.executable -m pynixd_store.builder.child`
failed every build with `No module named 'pynixd_store'`. The test suites run
in a venv, where the interpreter finds them alone. Found by pydd's suite.
"""

from __future__ import annotations

import os
import subprocess
import sys

from pynixd_store.builder.local import helper_environment

IMPORT = ["-c", "import pynixd_store.builder.child"]


def _bare() -> str:
    """An interpreter that knows only its own standard library, as the wrapped application's is."""
    return getattr(sys, "_base_executable", sys.executable)


def test_a_bare_interpreter_imports_the_helper_with_its_environment() -> None:
    result = subprocess.run([_bare(), *IMPORT], env=helper_environment(), capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


def test_a_bare_interpreter_cannot_without_it() -> None:
    """The control: the case above proves nothing if the bare interpreter finds the package alone."""
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    result = subprocess.run([_bare(), *IMPORT], env=env, capture_output=True, text=True, check=False)
    assert result.returncode != 0
    assert "No module named 'pynixd_store'" in result.stderr
