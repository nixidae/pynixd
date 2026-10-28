"""A `builtin:` derivation builds on any store, and an input of that kind is built first.

`Machine::systemSupported` at `machines.cc:42` takes `builtin` on every
machine, and `derivation-building-goal.cc:421` does the same locally.
pynixd answered "No compatible store for builtin", and an input made by
`<nix/fetchurl.nix>` got no goal at all, so its parent failed with "1
dependency failed" where nix-daemon built both. Measured on dynhetz
against the stock daemon. nixidae/nixidae#1003.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.store import DaemonStore
from pynixd.store_path import StorePath

MISSING = StorePath("/nix/store/00000000000000000000000000000001-probe")


def _supports(features: set[str]) -> bool:
    store = SimpleNamespace(_feature_matrix={"x86_64-linux": {"kvm", "big-parallel"}})
    return DaemonStore.supports_derivation(cast("Any", store), "builtin", features)


class TestTheBuiltinSystem:
    def test_every_store_takes_it(self) -> None:
        assert _supports(set())

    def test_features_still_count(self) -> None:
        assert _supports({"kvm"})
        assert not _supports({"nixos-test"})


class LocalStore:
    async def read_derivation(self, _path: str) -> Any:
        return SimpleNamespace(builder="builtin:fetchurl", output_paths=lambda: {"out": MISSING})

    async def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        return SimpleNamespace(valid=False)


@pytest.mark.anyio
async def test_a_builtin_input_whose_output_is_missing_gets_a_goal() -> None:
    goal = SimpleNamespace(engine=SimpleNamespace(ctx=SimpleNamespace(local_store=LocalStore())))

    needed = await EnsureDerivedPathGoal._input_outputs_requiring_goals(  # noqa: SLF001 -- the decision is the unit
        cast("Any", goal), StorePath("/nix/store/00000000000000000000000000000002-probe.drv"), ["out"]
    )

    assert needed == {"out"}


class FloatingStore(LocalStore):
    def __init__(self) -> None:
        self.asked: list[Any] = []

    async def read_derivation(self, _path: str) -> Any:
        return SimpleNamespace(builder="/bin/sh", output_paths=lambda: {"out": StorePath("")})

    async def execute(self, request: Any, *_args: Any, **_kwargs: Any) -> Any:
        self.asked.append(request)
        return SimpleNamespace(valid=False)


@pytest.mark.anyio
async def test_a_floating_output_is_needed_without_asking_about_an_empty_path() -> None:
    store = FloatingStore()
    goal = SimpleNamespace(engine=SimpleNamespace(ctx=SimpleNamespace(local_store=store)))

    needed = await EnsureDerivedPathGoal._input_outputs_requiring_goals(  # noqa: SLF001 -- the decision is the unit
        cast("Any", goal), StorePath("/nix/store/00000000000000000000000000000003-ca.drv"), ["out"]
    )

    assert needed == {"out"}
    assert store.asked == []
