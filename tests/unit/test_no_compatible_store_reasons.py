"""The scheduler refuses a build no store can run with each reason that applies.

`reject` (`derivation-building-goal.cc:289`) adds a block for a platform mismatch and another for missing
system features, so a derivation with both prints both. Measured with Nix 2.35.3 on a `riscv64-none`
derivation that requires `parity-missing`. nixidae/nixidae#1216.
"""

from __future__ import annotations

import re

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode, BuildResultStatus
from pynixd_store.store_path import StorePath
from tests._conftest.placement import FakeStore, FixedRanking, Spawned, fake_placer

ANSI = re.compile(r"\x1b\[[0-9;]*m")
DRV = StorePath("/nix/store/00000000000000000000000000000001-probe.drv")


def _store(name: str, feature_matrix: dict[str, set[str]]) -> FakeStore:
    store = FakeStore(StoreId(name))
    store.feature_matrix = feature_matrix
    return store


async def _refusal(platform: str, features: set[str]) -> list[str]:
    queue = BuildQueue()
    build_id, answer = await queue.enqueue(
        BuildDerivationRequest(
            drv_path=DRV, derivation=BasicDerivation(platform=platform, builder="/bin/sh"), build_mode=BuildMode.NORMAL
        )
    )
    a = _store("a", {"x86_64-linux": {"kvm"}})
    b = _store("b", {"aarch64-linux": {"big-parallel"}})
    placer = fake_placer(queue, FixedRanking.of(), Spawned(), stores={a.store_id: a, b.store_id: b})

    await placer._fail_no_compatible_store(queue.by_id[build_id], features)  # noqa: SLF001 -- the refusal is the unit

    result = answer.result().result
    assert result.status == BuildResultStatus.INPUT_REJECTED
    return ANSI.sub("", str(result.error_msg)).splitlines()


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
