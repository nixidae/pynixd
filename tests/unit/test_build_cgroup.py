"""The readers of a build's cgroup files (`builder/cgroup.py`, nixidae/nixidae#1187)."""

from __future__ import annotations

import pytest

from pynixd_store.builder.cgroup import io_bytes, own_cgroup, stall_us


def test_the_v2_line_names_the_cgroup() -> None:
    assert own_cgroup("12:pids:/old\n0::/system.slice/pynixd.service\n") == "/system.slice/pynixd.service"
    assert own_cgroup("12:pids:/old\n") is None


def test_io_bytes_sum_over_devices() -> None:
    text = "8:0 rbytes=100 wbytes=10 rios=1 wios=1 dbytes=0 dios=0\n253:1 rbytes=5 wbytes=7 rios=1 wios=1\n"
    assert io_bytes(text) == (105, 17)


def test_a_stall_is_the_some_total() -> None:
    text = "some avg10=0.00 avg60=0.00 avg300=0.00 total=1234\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=99\n"
    assert stall_us(text) == 1234


def test_a_pressure_file_without_some_is_refused() -> None:
    with pytest.raises(ValueError, match="no 'some' line"):
        stall_us("full avg10=0.00 avg60=0.00 avg300=0.00 total=99\n")
