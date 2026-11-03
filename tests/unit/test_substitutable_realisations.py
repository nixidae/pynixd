"""A realisation comes from the first substituter, by priority, that holds it.

`DrvOutputSubstitutionGoal::init` (`drv-output-substitution-goal.cc:30`) and
`queryMissing` (`misc.cc:250`) ask the substituters in turn and take the
first answer. A substituter that fails is skipped: Nix prints the error and
goes on, and the goal fails when no other one holds the realisation.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.config import LocalBinaryCacheSpec
from pynixd.goals import realisations
from pynixd.goals.realisations import RealisationSearch, substitutable_realisations
from pynixd_store.store_path import StorePath

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.store import Store
    from pynixd_store.drv_parser import Derivation

DIGEST = "ab" * 32
DRV = StorePath("/nix/store/00000000000000000000000000000003-thing.drv")
FIRST = "00000000000000000000000000000001-first"
SECOND = "00000000000000000000000000000002-second"


async def _hashes(_parsed: Any, _read: Any) -> dict[str, str]:
    return {"out": DIGEST}


@pytest.fixture(autouse=True)
def _fixed_hashes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(realisations, "output_hashes", _hashes)


async def _cache(root: Path, priority: float, out_path: str | None, *, broken: bool = False) -> Store:
    (root / "realisations").mkdir(parents=True)
    (root / "nix-cache-info").write_text("StoreDir: /nix/store\n")
    if out_path is not None or broken:
        body = "{broken" if broken else json.dumps({"outPath": out_path, "signatures": []})
        (root / f"realisations/sha256:{DIGEST}!out.doi").write_text(body)
    store = LocalBinaryCacheSpec(path=root, priority=priority).to_store(root.name)
    await store.start()
    return store


async def _search(stores: list[Store]) -> RealisationSearch:
    return await substitutable_realisations(
        cast("Derivation", None),
        ["out"],
        stores,
        cast("Store", SimpleNamespace(read_derivation=None)),
        DRV,
        by_path=False,
    )


async def _first(stores: list[Store]) -> str | None:
    found = (await _search(stores)).found
    if found is None:
        return None
    (realisation,) = found.values()
    return StorePath(str(realisation.out_path)).name


async def test_the_highest_priority_holder_wins(tmp_path: Path) -> None:
    low = await _cache(tmp_path / "low", 50, SECOND)
    high = await _cache(tmp_path / "high", 10, FIRST)
    assert await _first([low, high]) == FIRST


async def test_a_tie_keeps_the_listed_order(tmp_path: Path) -> None:
    one = await _cache(tmp_path / "one", 30, FIRST)
    two = await _cache(tmp_path / "two", 30, SECOND)
    assert await _first([one, two]) == FIRST
    assert await _first([two, one]) == SECOND


async def test_a_miss_and_a_fault_fall_through(tmp_path: Path) -> None:
    empty = await _cache(tmp_path / "empty", 1, None)
    broken = await _cache(tmp_path / "broken", 2, None, broken=True)
    holder = await _cache(tmp_path / "holder", 3, SECOND)
    assert await _first([empty, broken, holder]) == SECOND


async def test_no_holder_is_none(tmp_path: Path) -> None:
    assert await _search([await _cache(tmp_path / "empty", 1, None)]) == RealisationSearch(None)


async def test_a_fault_with_no_holder_says_a_substituter_failed(tmp_path: Path) -> None:
    """`drv-output-substitution-goal.cc:61`: `ecFailed`, not `ecNoSubstituters`. nixidae/nixidae#1171."""
    empty = await _cache(tmp_path / "empty", 1, None)
    broken = await _cache(tmp_path / "broken", 2, None, broken=True)
    assert await _search([empty, broken]) == RealisationSearch(None, substituter_failed=True)


async def test_a_holder_after_a_fault_is_no_failure(tmp_path: Path) -> None:
    broken = await _cache(tmp_path / "broken", 1, None, broken=True)
    holder = await _cache(tmp_path / "holder", 2, SECOND)
    assert not (await _search([broken, holder])).substituter_failed
