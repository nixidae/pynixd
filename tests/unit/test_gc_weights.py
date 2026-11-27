"""The collector blends size and age by disk pressure, and pressure bounds the pass.

`by_weight` and `take_until_below_target` are pure on purpose: the order
a pass deletes in, and where a bounded pass stops, are asserted here,
without a daemon, a store, or a disk. What they order -- sizes from the
path infos, ages from the access table -- is read elsewhere; these two
only decide.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pynixd_store.gc import FREED_RATIO_FLOOR, by_weight, freed_ratio, sync_filesystem, take_until_below_target

if TYPE_CHECKING:
    from pathlib import Path

# A small old path against a big young one: size norms (0.6, 1.0), age
# norms (1.0, 0.1). Empty disk scores B 1.0 over A 0.1; full disk scores A
# 1.0 over B 0.7. The same two paths flip with pressure, which is the
# whole point of the blend.
OLD_SMALL = {"old": (60, 1000), "young": (100, 10)}


def test_empty_disk_collects_oldest_first() -> None:
    assert by_weight(OLD_SMALL, pressure=0.0) == ["old", "young"]


def test_full_disk_collects_biggest_first() -> None:
    assert by_weight(OLD_SMALL, pressure=1.0) == ["young", "old"]


def test_half_pressure_scores_both_axes() -> None:
    """0.5 scores 0.8 against 0.505: the old path still leads, by less."""
    assert by_weight(OLD_SMALL, pressure=0.5) == ["old", "young"]


def test_equal_weights_break_ties_by_path() -> None:
    """One candidate normalises to (1.0, 1.0) at any pressure; two identical
    ones need a stable order that is not input order."""
    assert by_weight({"b": (100, 100), "a": (100, 100)}, pressure=0.7) == ["a", "b"]


def test_empty_weights_delete_nothing() -> None:
    assert by_weight({}, pressure=0.9) == []


def test_a_target_already_met_takes_nothing() -> None:
    """Usage under the target is a pass that stays home."""
    assert take_until_below_target([("a", 100)], used=10, total=100, target=0.5) == []


def test_the_walk_stops_at_the_first_prefix_at_or_under_target() -> None:
    """Pressure bounds the pass: two big deletes relieve what the plan would
    have emptied at once."""
    ordered = [("big", 40), ("mid", 30), ("small", 20)]
    assert take_until_below_target(ordered, used=95, total=100, target=0.5) == ["big", "mid"]


def test_a_ratio_under_one_takes_a_longer_prefix() -> None:
    """Half of each NAR byte reaches the disk: 40 and 30 free 35, not 70, and 20 more reach the target."""
    ordered = [("big", 40), ("mid", 30), ("small", 20), ("tiny", 10)]
    assert take_until_below_target(ordered, used=95, total=100, target=0.5) == ["big", "mid"]
    assert take_until_below_target(ordered, used=95, total=100, target=0.5, ratio=0.5) == ["big", "mid", "small"]


def test_freed_ratio_is_the_share_reaching_the_disk() -> None:
    assert freed_ratio(25, 100) == 0.25
    assert freed_ratio(0, 100) is None
    assert freed_ratio(-5, 100) is None
    assert freed_ratio(10, 0) is None
    # More than the NAR bytes is someone else's free in the same window.
    assert freed_ratio(500, 100) == 1.0
    assert freed_ratio(1, 10**9) == FREED_RATIO_FLOOR


def test_sync_filesystem_commits_a_directory(tmp_path: Path) -> None:
    sync_filesystem(tmp_path)
    with pytest.raises(FileNotFoundError):
        sync_filesystem(tmp_path / "gone")


def test_a_target_no_prefix_meets_takes_everything() -> None:
    assert take_until_below_target([("a", 10)], used=100, total=100, target=0.0) == ["a"]


def test_exactly_at_target_takes_nothing_more() -> None:
    """At the line the disk is relieved already; one more delete is churn."""
    assert take_until_below_target([("a", 50)], used=50, total=100, target=0.5) == []
