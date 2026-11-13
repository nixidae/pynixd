"""Nix's `gc-non-blocking.sh`, which skips on `needLocalStore` ("the GC test needs a synchronisation point"), held to three arms.

`tests/unit/test_engine_gc.py` holds pynixd's collector to the same two synchronisation points without a build.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import anyio
import pytest

from pynixd_store.collect import SYNC_AFTER_LOCK, SYNC_AFTER_ROOTS
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, arm_store, copy_nix_tests
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["_keep_store_dirs"]

# `gc-non-blocking.sh:38-43`: the build runs while the collector holds its lock, and lets it go on.
BUILD = """with import {root}/config.nix; mkDerivation {{
  name = "non-blocking";
  buildCommand = "set -x; test -e {running}; mkdir $out; echo > {after_roots}";
}}"""


def _release(fifo: Path) -> None:
    """`gc-non-blocking.sh:34`: the open waits for the collector to read, as `echo > fifo` does."""
    with fifo.open("w") as handle:
        handle.write("\n")


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_build_runs_while_the_collector_holds_its_lock(
    tmp_path: Path, arm: Arm, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`gc-non-blocking.sh:11-52`: the build ends before the collector, which then deletes the dead path only.

    The collector reads the first fifo with its lock held and no roots socket (`gc.cc:399`), so the build's
    first connection to the socket finds no file. It reads the second after the roots (`gc.cc:537`), and
    the build writes it, so the collection cannot end before the build does.
    """
    copy_nix_tests(tmp_path, "simple.nix")
    after_lock, after_roots, running = tmp_path / "test2.fifo", tmp_path / "test.fifo", tmp_path / "running"
    os.mkfifo(after_lock)
    os.mkfifo(after_roots)
    config = for_test_store(substituters=())
    config.sandbox = "false"
    async with arm_store(tmp_path, arm, config) as store:
        _rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "store", "add-path", "--store", store.url, tmp_path / "simple.nix"],
            env=dict(store.env),
            nix_config=store.config,
        )
        dummy = out.strip()
        running.touch()
        # pynixd's collector reads the two from its own environment; the local arm's `nix-store` from its own.
        monkeypatch.setenv(SYNC_AFTER_LOCK, str(after_lock))
        monkeypatch.setenv(SYNC_AFTER_ROOTS, str(after_roots))
        built: list[str] = []

        async def collect() -> None:
            await run_subproc(
                [NIX_BIN.with_name("nix-store"), "--store", store.url, "--gc", "-vvvvv"],
                env=dict(store.env),
                nix_config=store.config,
            )
            running.unlink()

        async def release() -> None:
            await anyio.sleep(1)
            await anyio.to_thread.run_sync(_release, after_lock, abandon_on_cancel=True)

        async def build() -> None:
            _rc, out, _err, _both = await run_subproc(
                [
                    NIX_BIN.with_name("nix-build"),
                    "--store",
                    store.url,
                    "--max-silent-time",
                    "60",
                    "-o",
                    tmp_path / "result",
                    "-E",
                    BUILD.format(root=tmp_path, running=running, after_roots=after_roots),
                ],
                env=dict(store.env),
                nix_config=store.config,
            )
            built.append(out.strip())

        with anyio.fail_after(90):
            async with anyio.create_task_group() as tg:
                tg.start_soon(collect)
                # `gc-non-blocking.sh:30`: the collector has no signal that it waits at the first fifo.
                await anyio.sleep(2)
                tg.start_soon(release)
                tg.start_soon(build)
    assert not running.exists()
    assert not os.path.lexists(store_file(tmp_path, dummy))
    assert os.path.lexists(store_file(tmp_path, built[0]))


def store_file(root: Path, path: str) -> Path:
    """Where *path* is on disk: every arm keeps its store in `<root>/store`, as its store directory."""
    return root / "store" / path.rsplit("/", 1)[-1]
