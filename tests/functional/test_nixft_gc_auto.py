"""Nix's `gc-auto.sh`, which skips on `needLocalStore` ("min-free and max-free are daemon options"), held to three arms.

`tests/unit/test_auto_gc.py` holds pynixd's collector to the same numbers without a build.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import anyio
import pytest

from pynixd_store.autogc import FREE_SPACE_FILE
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, arm_store, copy_nix_tests
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from pathlib import Path

    from tests.functional._nixft import ArmStore

__all__ = ["_keep_store_dirs"]

# `gc-auto.sh:27-53`: A sees the three dead paths, reports 100 bytes free, and waits for two to go.
BUILD_A = """with import {root}/config.nix; mkDerivation {{
  name = "gc-A";
  buildCommand = ''
    set -x
    [[ $(ls $NIX_STORE/*-garbage? | wc -l) = 3 ]]
    mkdir $out
    echo foo > $out/bar
    echo 100 > {free}.tmp1
    mv {free}.tmp1 {free}
    for i in {{1..20}}; do
        echo ''${{i}}...
        if [[ $(ls $NIX_STORE/*-garbage? | wc -l) = 1 ]]; then
            exit 0
        fi
        sleep 1
    done
    exit 1
  '';
}}"""

# `gc-auto.sh:55-68`: B builds while A waits, and ends when A has ended.
BUILD_B = """with import {root}/config.nix; mkDerivation {{
  name = "gc-B";
  buildCommand = ''
    set -x
    mkdir $out
    echo foo > $out/bar
    cat "{fifo}"
  '';
}}"""

# `gc-auto.sh:70`, `:74`.
LIMITS = ("--min-free", "1K", "--max-free", "2K", "--min-free-check-interval", "1")


async def _build(store: ArmStore, expr: str, result: Path) -> tuple[int, str]:
    got, _out, err, _both = await run_subproc(
        [NIX_BIN, "build", "--impure", "-v", "-L", "-o", result, "--expr", expr, "--store", store.url, *LIMITS],
        expected_retcode=None,
        env=dict(store.env),
        nix_config=store.config,
    )
    return got, err


def _unlock(fifo: Path) -> None:
    """`gc-auto.sh:82`: the open waits for B's `cat`, as `echo unlock > fifo` does."""
    with fifo.open("w") as handle:
        handle.write("unlock\n")


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_build_short_of_min_free_collects_dead_paths_to_max_free(
    tmp_path: Path, arm: Arm, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`gc-auto.sh:11-87`: of three dead paths one is left, and both concurrent builds succeed.

    The free space comes from `_NIX_TEST_FREE_SPACE_FILE` (`gc.cc:1020`), which the client, pynixd and
    nix-daemon all read from this process's environment.
    """
    copy_nix_tests(tmp_path, "nar-access.sh")
    free, fifo = tmp_path / "fake-free", tmp_path / "fifoLock"
    free.write_text("1100\n")
    os.mkfifo(fifo)
    monkeypatch.setenv(FREE_SPACE_FILE, str(free))
    config = for_test_store(substituters=())
    config.sandbox = "false"
    async with arm_store(tmp_path, arm, config) as store:
        for n in (1, 2, 3):
            await run_subproc(
                [
                    NIX_BIN,
                    "store",
                    "add-path",
                    "--store",
                    store.url,
                    "--name",
                    f"garbage{n}",
                    tmp_path / "nar-access.sh",
                ],
                env=dict(store.env),
                nix_config=store.config,
            )
        a_expr = BUILD_A.format(root=tmp_path, free=free)
        b_expr = BUILD_B.format(root=tmp_path, fifo=fifo)
        results: dict[str, tuple[int, str]] = {}

        async def build_a() -> None:
            results["A"] = await _build(store, a_expr, tmp_path / "result-A")
            await anyio.to_thread.run_sync(_unlock, fifo, abandon_on_cancel=True)

        async def build_b() -> None:
            results["B"] = await _build(store, b_expr, tmp_path / "result-B")

        with anyio.fail_after(90):
            async with anyio.create_task_group() as tg:
                tg.start_soon(build_a)
                tg.start_soon(build_b)
    assert results["A"][0] == 0, results["A"][1]
    assert results["B"][0] == 0, results["B"][1]
    assert (tmp_path / "result-A/bar").read_text() == "foo\n"
    assert (tmp_path / "result-B/bar").read_text() == "foo\n"
