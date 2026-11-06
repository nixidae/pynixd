"""The readers of a build's cgroup files (`builder/cgroup.py`, nixidae/nixidae#1187)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest

from pynixd_store.builder.cgroup import io_bytes, own_cgroup, sample_every, stall_us

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd_store.builder.usage import UsageSample


@pytest.mark.parametrize(("populated", "sampled"), [(0, False), (1, True)])
async def test_an_empty_cgroup_gives_no_sample(tmp_path: Path, populated: int, sampled: bool) -> None:
    """After the build's last process exits, `memory.current` reads 0: a sample then would say nothing true."""
    (tmp_path / "cgroup.events").write_text(f"populated {populated}\nfrozen 0\n")
    (tmp_path / "cpu.stat").write_text("usage_usec 10\nuser_usec 6\nsystem_usec 4\n")
    (tmp_path / "memory.current").write_text("4096\n")
    samples: list[UsageSample] = []
    with anyio.move_on_after(0.05):
        await sample_every(tmp_path, 0.01, samples, in_thread=False)

    assert bool(samples) is sampled


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
