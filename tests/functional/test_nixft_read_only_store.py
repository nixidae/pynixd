"""Nix's `read-only-store.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

The client opens the store itself with `local?read-only=true`, so the script holds a mode of Nix's local
store. pynixd cannot serve a store read-only yet, so its two arms skip (nixidae/nixidae#1309).
"""

from __future__ import annotations

import contextlib
import stat
from typing import TYPE_CHECKING

import anyio
import pytest

from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, ArmStore, arm_store, nix_tests
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

__all__ = ["_keep_store_dirs"]

READ_ONLY = "local?read-only=true"
NO_ARM = pytest.mark.skip(reason="pynixd cannot serve a store read-only: nixidae/nixidae#1309")
ARMS = [Arm.local, pytest.param(Arm.daemon, marks=NO_ARM), pytest.param(Arm.pynixd, marks=NO_ARM)]


async def _run(store: ArmStore, program: str, url: str, *args: str | Path) -> tuple[int, str, str]:
    with anyio.fail_after(60):
        rc, out, err, _both = await run_subproc(
            [NIX_BIN.with_name(program), "--store", url, *args],
            expected_retcode=None,
            env=dict(store.env) | {"NIX_CONFIG": "extra-experimental-features = read-only-local-store"},
            nix_config=store.config,
        )
    return rc, out, err


async def _happy(store: ArmStore, src: Path, dummy_path: str, line: int) -> None:
    """`read-only-store.sh:13` (`happy`), called at *line*: a query, `local://`, and an add of a present path."""
    for url, script_line in ((READ_ONLY, 15), ("local://?read-only=true", 18)):
        rc, _out, err = await _run(store, "nix", url, "path-info", dummy_path)
        assert rc == 0, f"read-only-store.sh:{script_line} from :{line}: {err}"
    rc, _out, err = await _run(store, "nix-store", READ_ONLY, "--add", src / "dummy")
    assert rc == 0, f"read-only-store.sh:21 from :{line}: {err}"


@contextlib.contextmanager
def _read_only(*trees: Path) -> Iterator[None]:
    """`chmod -R -w` (`read-only-store.sh:40`, `:41`), undone after so that the test's directory can go."""
    paths = [p for tree in trees for p in (tree, *tree.rglob("*")) if not p.is_symlink()]
    for p in paths:
        p.chmod(p.stat().st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
    try:
        yield
    finally:
        for p in paths:
            p.chmod(p.stat().st_mode | stat.S_IWUSR)


@pytest.mark.parametrize("arm", ARMS)
async def test_a_read_only_store_reads_and_refuses_writes(tmp_path: Path, arm: Arm) -> None:
    """`read-only-store.sh:26` to `:50`: no database, a database, then a store whose files are read-only."""
    src = tmp_path / "src"
    src.mkdir()
    for name in ("dummy", "eval.nix"):
        (src / name).write_bytes((nix_tests() / name).read_bytes())
    async with arm_store(tmp_path, arm, for_test_store(substituters=())) as store:
        rc, _out, err = await _run(store, "nix-store", READ_ONLY, "--add", src / "dummy")
        assert (rc, "database does not exist, and cannot be created in read-only mode" in err) == (1, True), err
        rc, out, err = await _run(store, "nix-store", store.url, "--add", src / "dummy")
        assert rc == 0, f"read-only-store.sh:29: {err}"
        dummy_path = out.strip()
        rc, _out, err = await _run(store, "nix-store", READ_ONLY, "--add", src / "eval.nix")
        assert (rc, "attempt to write a readonly database" in err) == (1, True), err
        await _happy(store, src, dummy_path, 35)
        with _read_only(tmp_path / "store", tmp_path / "var"):
            rc, _out, err = await _run(store, "nix-store", store.url, "--add", src / "eval.nix")
            phrase = "This command may have been run as non-root in a single-user Nix installation"
            assert (rc, phrase in err) == (1, True), f"read-only-store.sh:46: {err}"
            rc, _out, err = await _run(store, "nix-store", READ_ONLY, "--add", src / "eval.nix")
            assert (rc, "Permission denied" in err) == (1, True), f"read-only-store.sh:47: {err}"
            await _happy(store, src, dummy_path, 50)
