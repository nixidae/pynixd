"""Nix's `read-only-store.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

On the local arm the client opens the store itself with `local?read-only=true`. On the two pynixd arms a
read-only pynixd serves it (`LocalSocketStoreSpec.read_only`, nixidae/nixidae#1309). Each command opens the
store again, as each of the script's commands does: a read-only server and a writing one never share the
store, which is the script's reason to skip on a daemon. A store-open error is the server's refusal to start.
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

    from pynixd.nix_config import NixConfig

__all__ = ["_keep_store_dirs"]

READ_ONLY = "local?read-only=true"
FEATURE = {"NIX_CONFIG": "extra-experimental-features = read-only-local-store"}


def _flat(error: BaseException) -> str:
    if isinstance(error, BaseExceptionGroup):
        return "\n".join(_flat(inner) for inner in error.exceptions)
    return f"{type(error).__name__}: {error}"


class _Steps:
    """Each of the script's commands, on one arm: the store it opens, then the command."""

    def __init__(self, root: Path, arm: Arm, config: NixConfig) -> None:
        self.root, self.arm, self.config = root, arm, config

    async def run(
        self, program: str, *args: str | Path, read_only: bool = False, immutable: bool = False
    ) -> tuple[int, str, str]:
        """The exit code, stdout and stderr; a pynixd store that refuses to start gives 1 and its error."""
        try:
            async with arm_store(self.root, self.arm, self.config, read_only=read_only, immutable=immutable) as store:
                url = READ_ONLY if read_only and self.arm is Arm.local else store.url
                return await self._command(store, program, url, *args)
        except Exception as error:  # noqa: BLE001 -- the refusal is the answer the step asserts
            if self.arm is Arm.local:
                raise
            return 1, "", _flat(error)

    async def _command(self, store: ArmStore, program: str, url: str, *args: str | Path) -> tuple[int, str, str]:
        with anyio.fail_after(60):
            rc, out, err, _both = await run_subproc(
                [NIX_BIN.with_name(program), "--store", url, *args],
                expected_retcode=None,
                env=dict(store.env) | FEATURE,
                nix_config=store.config,
            )
        return rc, out, err

    async def happy(self, src: Path, dummy_path: str, line: int, *, immutable: bool = False) -> None:
        """`read-only-store.sh:13` (`happy`), called at *line*: a query, `local://`, and an add of a present path."""
        rc, _out, err = await self.run("nix", "path-info", dummy_path, read_only=True, immutable=immutable)
        assert rc == 0, f"read-only-store.sh:15 from :{line}: {err}"
        if self.arm is Arm.local:
            async with arm_store(self.root, self.arm, self.config) as store:
                rc, _out, err = await self._command(store, "nix", "local://?read-only=true", "path-info", dummy_path)
            assert rc == 0, f"read-only-store.sh:18 from :{line}: {err}"
        rc, _out, err = await self.run("nix-store", "--add", src / "dummy", read_only=True, immutable=immutable)
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


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_read_only_store_reads_and_refuses_writes(tmp_path: Path, arm: Arm) -> None:
    """`read-only-store.sh:26` to `:50`: no database, a database, then a store whose files are read-only.

    The last part opens the store `immutable` on the pynixd arms: its storage cannot change, and `mode=ro`
    opens no WAL database in a directory pynixd may not write.
    """
    src = tmp_path / "src"
    src.mkdir()
    for name in ("dummy", "eval.nix"):
        (src / name).write_bytes((nix_tests() / name).read_bytes())
    steps = _Steps(tmp_path, arm, for_test_store(substituters=()))
    rc, _out, err = await steps.run("nix-store", "--add", src / "dummy", read_only=True)
    assert (rc, "database does not exist, and cannot be created in read-only mode" in err) == (1, True), err
    rc, out, err = await steps.run("nix-store", "--add", src / "dummy")
    assert rc == 0, f"read-only-store.sh:29: {err}"
    dummy_path = out.strip()
    rc, _out, err = await steps.run("nix-store", "--add", src / "eval.nix", read_only=True)
    assert (rc, "attempt to write a readonly database" in err) == (1, True), err
    await steps.happy(src, dummy_path, 35)
    with _read_only(tmp_path / "store", tmp_path / "var"):
        rc, _out, err = await steps.run("nix-store", "--add", src / "eval.nix")
        # NIX-DEVIATION (#27): with no daemon, pynixd refuses a store directory it cannot write before it takes
        # the big lock (`create_store._create_dirs`), so the hint of `local-store.cc:244` is not its answer.
        phrase = (
            "pynixd cannot write the store directory"
            if arm is Arm.pynixd
            else "This command may have been run as non-root in a single-user Nix installation"
        )
        assert (rc, phrase in err) == (1, True), f"read-only-store.sh:46: {err}"
        rc, _out, err = await steps.run("nix-store", "--add", src / "eval.nix", read_only=True, immutable=True)
        assert (rc, "Permission denied" in err) == (1, True), f"read-only-store.sh:47: {err}"
        await steps.happy(src, dummy_path, 50, immutable=True)
