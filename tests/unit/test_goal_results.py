"""Unit tests for goal result metadata helpers."""

from __future__ import annotations

from pynixd.goals.results import GoalResult, goal_success
from pynixd_store.store_path import StorePath


def test_with_dynamic_outputs_does_not_mutate_source_result() -> None:
    output_path = StorePath("/nix/store/00000000000000000000000000000000-out")
    drv_path = StorePath("/nix/store/11111111111111111111111111111111-example.drv")
    source = GoalResult(
        result=goal_success().result,
        resolved_outputs={"out": output_path},
    )

    derived = source.with_dynamic_outputs(drv_path)

    assert source.dynamic_paths == {}
    assert derived.dynamic_paths == {(drv_path, "out"): output_path}


def test_with_single_output_does_not_mutate_source_result() -> None:
    path = StorePath("/nix/store/00000000000000000000000000000000-out")
    source = GoalResult(
        result=goal_success().result,
        resolved_outputs={"out": path},
    )

    derived = source.with_single_output("dev", path)

    assert source.resolved_outputs == {"out": path}
    assert source.produced_paths == set()
    assert derived.resolved_outputs == {"dev": path}
    assert derived.produced_paths == {path}
