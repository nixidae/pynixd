"""Nix's `external-builders.sh`, which skips on `needLocalStore` ("'--external-builders' can't be used with the daemon").

Held to three arms (`_nixft.py`). `test_external_builder.py` compares what the program sees on pynixd's
route and on the daemon's; this holds the script's own build to the local store as well.
"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

import anyio
import pytest

from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, arm_store, copy_nix_tests
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["_keep_store_dirs"]

# `external-builders.sh:10-19`.
EXPR = """with import {root}/config.nix;
mkDerivation {{
  name = "external";
  system = "x68_46-xunil";
  buildCommand = ''
    echo xyzzy
    printf foo > $out
  '';
}}"""

# `external-builders.sh:23-43`, in Python: the program checks its argument and the system, runs the
# derivation's builder with `buildCommand` and `out`, and appends to the output.
PROGRAM = """#!{python}
import json, os, subprocess, sys
assert sys.argv[1] == "bla", sys.argv
with open(sys.argv[2], encoding="utf-8") as f:
    doc = json.load(f)
assert doc["system"] == "x68_46-xunil", doc["system"]
print("\\x02", flush=True)
env = {{"PATH": os.environ.get("PATH", ""), "buildCommand": doc["env"]["buildCommand"], "out": doc["env"]["out"]}}
subprocess.run([doc["builder"], *doc["args"]], env=env, check=True)
with open(doc["env"]["out"], "a", encoding="utf-8") as f:
    f.write("bar")
"""


@pytest.mark.parametrize("arm", list(Arm))
async def test_an_external_builder_builds_a_system_no_store_has(tmp_path: Path, arm: Arm) -> None:
    """`external-builders.sh:46-50`: the program in `--external-builders` builds the derivation, and the output reads `foobar`."""
    copy_nix_tests(tmp_path)
    (tmp_path / "expr.nix").write_text(EXPR.format(root=tmp_path))
    program = tmp_path / "external-builder"
    program.write_text(PROGRAM.format(python=sys.executable))
    program.chmod(0o755)
    builders = [{"systems": ["x68_46-xunil"], "args": ["bla"], "program": str(program)}]
    config = for_test_store(substituters=(), experimental_features=("external-builders",))
    config.sandbox = "false"
    async with arm_store(tmp_path, arm, config) as store:
        with anyio.fail_after(60):
            rc, _out, err, _both = await run_subproc(
                [
                    NIX_BIN,
                    "build",
                    "-L",
                    "--file",
                    tmp_path / "expr.nix",
                    "--out-link",
                    tmp_path / "result",
                    "--store",
                    store.url,
                    "--extra-experimental-features",
                    "external-builders",
                    "--external-builders",
                    json.dumps(builders),
                ],
                expected_retcode=None,
                env=dict(store.env),
                nix_config=store.config,
            )
    assert rc == 0, err
    assert (tmp_path / "result").read_text() == "foobar"
