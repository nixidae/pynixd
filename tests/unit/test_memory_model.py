"""A build's peak memory as a function of its cores (nixidae/nixidae#1558)."""

from __future__ import annotations

import pytest

from pynixd_store.memory_model import MemoryModel, MemoryPoint, fit

GIB = 1 << 30


def test_no_measured_attempt_gives_no_model() -> None:
    assert fit([]) is None
    assert fit([MemoryPoint(cores=8, peak_bytes=0)]) is None


def test_rows_that_name_no_cores_are_their_median_at_any_cores() -> None:
    model = fit([MemoryPoint(cores=0, peak_bytes=4 * GIB), MemoryPoint(cores=0, peak_bytes=6 * GIB)])
    assert model == MemoryModel(5.0 * GIB, 0.0)
    counted = fit([MemoryPoint(cores=0, peak_bytes=40 * GIB), MemoryPoint(cores=8, peak_bytes=8 * GIB)])
    assert counted == MemoryModel(0.0, 1.0 * GIB)


def test_one_core_count_is_a_line_through_zero() -> None:
    model = fit([MemoryPoint(cores=8, peak_bytes=16 * GIB)])
    assert model == MemoryModel(0.0, 2 * GIB)
    assert model.peak(64) == 128 * GIB


def test_two_core_counts_give_the_base_and_the_slope() -> None:
    model = fit([MemoryPoint(cores=4, peak_bytes=6 * GIB), MemoryPoint(cores=8, peak_bytes=10 * GIB)])
    assert model == MemoryModel(2.0 * GIB, 1.0 * GIB)


def test_the_line_holds_every_point_under_it() -> None:
    points = [
        MemoryPoint(cores=2, peak_bytes=5 * GIB),
        MemoryPoint(cores=4, peak_bytes=5 * GIB),
        MemoryPoint(cores=8, peak_bytes=13 * GIB),
    ]
    model = fit(points)
    assert model is not None
    assert all(model.peak(p.cores) >= p.peak_bytes for p in points)


def test_a_line_that_slopes_down_is_the_highest_peak() -> None:
    model = fit([MemoryPoint(cores=4, peak_bytes=9 * GIB), MemoryPoint(cores=8, peak_bytes=5 * GIB)])
    assert model == MemoryModel(9.0 * GIB, 0.0)


def test_a_kill_raises_the_line_above_its_peak() -> None:
    model = fit([MemoryPoint(cores=8, peak_bytes=8 * GIB), MemoryPoint(cores=16, peak_bytes=12 * GIB, killed=True)])
    assert model is not None
    assert model.peak(16) > 12 * GIB
    cores = model.most_cores_within(12 * GIB, 64)
    assert cores is not None
    assert cores < 16


def test_a_kill_alone_is_a_lower_bound() -> None:
    model = fit([MemoryPoint(cores=64, peak_bytes=32 * GIB, killed=True)])
    assert model is not None
    assert model.most_cores_within(32 * GIB, 64) == 63


@pytest.mark.parametrize(
    ("room", "ceiling", "cores"),
    [
        pytest.param(64 * GIB, 64, 31, id="a-roomy-host"),
        pytest.param(18 * GIB, 64, 8, id="a-starved-host"),
        pytest.param(1000 * GIB, 16, 16, id="the-ceiling"),
        pytest.param(4 * GIB, 64, 1, id="just-one-core"),
        pytest.param(3 * GIB, 64, None, id="not-even-one"),
    ],
)
def test_the_most_cores_that_fit_a_builder(room: int, ceiling: int, cores: int | None) -> None:
    assert MemoryModel(2.0 * GIB, 2.0 * GIB).most_cores_within(room, ceiling) == cores
