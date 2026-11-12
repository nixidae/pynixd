"""Nix's `supplementary-groups.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

In `unshare --mount --map-root-user` the client, pynixd and its daemon run as root in a user namespace whose
`setgroups` is denied. A sandboxed build then fails with "setgroups failed", by default and with
`require-drop-supplementary-groups = true`, and builds with it `false` (`linux-derivation-builder.cc:566`).
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import anyio
import pytest

from tests.conftest import run_subproc
from tests.functional._nixft import Arm
from tests.functional.test_build_without_sandbox import _keep_store_dirs

__all__ = ["_keep_store_dirs"]

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_build_that_cannot_drop_its_groups_fails_unless_allowed(tmp_path: Path, arm: Arm) -> None:
    """`supplementary-groups.sh:26`, `:30`, `:35`: fails, fails with the option `true`, builds with it `false`."""
    unshare = shutil.which("unshare")
    assert unshare is not None
    out = tmp_path / "results.json"
    with anyio.fail_after(110):
        # `python -m` finds `tests` from its working directory.
        await run_subproc(
            [unshare, "--mount", "--map-root-user", "--wd", ROOT, sys.executable, "-m",
             "tests.functional._unshared_groups", tmp_path, arm, out],
        )  # fmt: skip
    results = json.loads(out.read_text())
    got = [(r["rc"], "setgroups failed" in r["stderr"]) for r in results]
    assert got == [(1, True), (1, True), (0, False)], [r["stderr"][-800:] for r in results]
