"""The collector deletes by bytes times idle seconds, and pressure bounds the pass.

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

GB = 10**9
DAY = 86_400


def test_a_big_path_used_a_minute_ago_goes_after_an_idle_one() -> None:
    """nixidae/nixidae#1650: the size-led blend deleted vllm on sg1 first at 97% full.

    20 GB idle 60 s costs 1.2e12 byte-seconds; 1 GB idle 90 days costs 7.8e15.
    """
    weights = {"vllm": (20 * GB, 60), "stale": (1 * GB, 90 * DAY)}
    assert by_weight(weights) == ["stale", "vllm"]


def test_a_big_idle_path_goes_before_a_small_idle_one() -> None:
    weights = {"small": (1 * GB, 30 * DAY), "big": (20 * GB, 30 * DAY)}
    assert by_weight(weights) == ["big", "small"]


def test_equal_scores_go_bigger_first_then_by_path() -> None:
    """Age 0 (registered this second) and size 0 both score 0: the bigger path still goes first."""
    weights = {"b": (100, 100), "a": (100, 100), "fresh-big": (500, 0), "fresh-small": (400, 0), "empty": (0, 100)}
    assert by_weight(weights) == ["a", "b", "fresh-big", "fresh-small", "empty"]


def test_empty_weights_delete_nothing() -> None:
    assert by_weight({}) == []


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
