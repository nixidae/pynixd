"""The scheduler refuses a build no store can run with each reason that applies.

`reject` (`derivation-building-goal.cc:289`) adds a block for a platform mismatch and another for missing
system features, so a derivation with both prints both. Measured with Nix 2.35.3 on a `riscv64-none`
derivation that requires `parity-missing`. nixidae/nixidae#1216.
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any, cast

import pytest

from pynixd.scheduler import Scheduler
from pynixd.serde import BuildResultStatus
from pynixd_store.store_path import StorePath

ANSI = re.compile(r"\x1b\[[0-9;]*m")
DRV = StorePath("/nix/store/00000000000000000000000000000001-probe.drv")


class Queue:
    def __init__(self) -> None:
        self.failed: list[tuple[str, BuildResultStatus]] = []

    async def fail(self, _build_id: int, message: str, status: BuildResultStatus) -> None:
        self.failed.append((message, status))


async def _refusal(platform: str, features: set[str]) -> list[str]:
    queue = Queue()
    stores = {
        "a": SimpleNamespace(_feature_matrix={"x86_64-linux": {"kvm"}}),
        "b": SimpleNamespace(_feature_matrix={"aarch64-linux": {"big-parallel"}}),
    }
    scheduler = SimpleNamespace(stores=stores, queue=queue)

    async def post(_message: Any) -> None:
        return None

    build = SimpleNamespace(
        build_id=1,
        request=SimpleNamespace(drv_path=DRV, derivation=SimpleNamespace(platform=platform)),
        post_log_and_fanout=post,
    )
    await Scheduler._fail_no_compatible_store(cast("Any", scheduler), cast("Any", build), features)  # noqa: SLF001 -- the refusal is the unit
    [(message, status)] = queue.failed
    assert status == BuildResultStatus.INPUT_REJECTED
    return ANSI.sub("", message).splitlines()


@pytest.mark.anyio
async def test_a_foreign_build_that_lacks_features_names_both_reasons() -> None:
    lines = await _refusal("riscv64-none", {"parity-missing"})

    assert lines == [
        f"Cannot build '{DRV}'.",
        "Reason: platform mismatch",
        "Required system: 'riscv64-none'",
        "Current system: 'aarch64-linux, x86_64-linux'",
        "Reason: missing system features",
        "Required features: {parity-missing}",
        "Available features: {big-parallel, kvm}",
    ]


@pytest.mark.anyio
async def test_a_foreign_build_with_features_a_store_has_names_the_platform_alone() -> None:
    lines = await _refusal("riscv64-none", {"kvm"})

    assert [line for line in lines if line.startswith("Reason:")] == ["Reason: platform mismatch"]


@pytest.mark.anyio
async def test_a_native_build_that_lacks_features_names_the_features_of_its_system() -> None:
    lines = await _refusal("x86_64-linux", {"big-parallel"})

    assert lines[1:] == [
        "Reason: missing system features",
        "Required features: {big-parallel}",
        "Available features: {kvm}",
    ]
