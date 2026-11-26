"""The readers of a build's cgroup files (`builder/cgroup.py`, nixidae/nixidae#1187)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest

from pynixd_store.builder import sibling
from pynixd_store.builder.cgroup import (
    cgroup_cores,
    io_bytes,
    own_cgroup,
    quota_cores,
    sample_every,
    since,
    stall_us,
)
from pynixd_store.builder.usage import BuildUsage, UsageSource, read_usage, write_usage

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


def test_the_tightest_quota_of_the_ancestors_gives_the_cores() -> None:
    """A pod's limit sits on an ancestor; pynixd's own leaf says `max` (`getMaxCPU`, `current-process.cc:27`)."""
    assert quota_cores(["max 100000\n", "250000 100000\n", "800000 100000\n"]) == 3
    assert quota_cores(["max 100000\n", "max 100000\n"]) is None
    assert quota_cores([]) is None


def test_the_quota_of_an_ancestor_limits_a_leaf_without_one(tmp_path: Path) -> None:
    """pynixd runs in its `pynixd` leaf, whose `cpu.max` is `max`; the pod's quota is two levels up."""
    leaf = tmp_path / "kubepods" / "pod" / "pynixd"
    leaf.mkdir(parents=True)
    (leaf / "cpu.max").write_text("max 100000\n")
    (tmp_path / "kubepods" / "pod" / "cpu.max").write_text("max 100000\n")
    (tmp_path / "kubepods" / "cpu.max").write_text("400000 100000\n")
    (tmp_path / "cpu.max").write_text("max 100000\n")
    assert cgroup_cores(tmp_path, "/kubepods/pod/pynixd") == 4
    assert cgroup_cores(tmp_path / "kubepods" / "pod", "/pynixd") is None


def test_io_bytes_sum_over_devices() -> None:
    text = "8:0 rbytes=100 wbytes=10 rios=1 wios=1 dbytes=0 dios=0\n253:1 rbytes=5 wbytes=7 rios=1 wios=1\n"
    assert io_bytes(text) == (105, 17)


def test_a_stall_is_the_some_total() -> None:
    text = "some avg10=0.00 avg60=0.00 avg300=0.00 total=1234\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=99\n"
    assert stall_us(text) == 1234


def test_a_pressure_file_without_some_is_refused() -> None:
    with pytest.raises(ValueError, match="no 'some' line"):
        stall_us("full avg10=0.00 avg60=0.00 avg300=0.00 total=99\n")


def _cgroup(path: Path, *, user: int, peak: int, oom: int = 0) -> Path:
    path.mkdir(exist_ok=True)
    (path / "cpu.stat").write_text(f"usage_usec {user + 1}\nuser_usec {user}\nsystem_usec 1\n")
    (path / "memory.peak").write_text(f"{peak}\n")
    (path / "memory.events").write_text(f"oom 0\noom_kill {oom}\n")
    return path


def test_a_cgroup_s_use_between_two_reads_is_its_counters_growth() -> None:
    before = BuildUsage(UsageSource.CGROUP, 100, 10, 50, 1, 2, cpu_stall_us=5, oom_kills=1)
    after = BuildUsage(UsageSource.CGROUP, 400, 30, 70, 11, 22, cpu_stall_us=None, oom_kills=3)

    used = since(before, after)

    assert (used.cpu_user_us, used.cpu_system_us, used.io_read_bytes, used.io_write_bytes) == (300, 20, 10, 20)
    assert (used.peak_memory_bytes, used.oom_kills, used.cpu_stall_us) == (70, 2, None)


def test_a_sibling_pod_records_its_container_s_cgroup_not_wait4(tmp_path: Path) -> None:
    """`wait4` gives the largest process's memory, which does not grow with the cores a pod gets."""
    cgroup = _cgroup(tmp_path / "container", user=1_000, peak=10)
    before = sibling.read_cgroup(cgroup)
    usage_file = tmp_path / "usage.json"
    write_usage(usage_file, BuildUsage(UsageSource.RUSAGE, 7, 7, 7, 0, 0))
    _cgroup(cgroup, user=9_000, peak=4 * 2**30, oom=1)

    sibling.measured_by_the_container(usage_file, before, cgroup)

    used = read_usage(usage_file)
    assert used is not None
    assert (used.source, used.cpu_user_us, used.peak_memory_bytes, used.oom_kills) == (
        UsageSource.CGROUP,
        8_000,
        4 * 2**30,
        1,
    )


def test_a_sibling_pod_keeps_wait4_without_a_readable_cgroup(tmp_path: Path) -> None:
    usage_file = tmp_path / "usage.json"
    write_usage(usage_file, BuildUsage(UsageSource.RUSAGE, 7, 7, 7, 0, 0))

    assert sibling.read_cgroup(tmp_path / "none") is None
    sibling.measured_by_the_container(usage_file, None, tmp_path / "none")

    used = read_usage(usage_file)
    assert used is not None and used.source == UsageSource.RUSAGE
