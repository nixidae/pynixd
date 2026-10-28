"""Unit tests for PSI and cgroupv2 CPU utilization parsing.

All tests in this file are parsing unit tests that don't trigger Store operations.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from pynixd.psi import (
    CgroupCpuStat,
    compute_cpu_util,
    count_cpus_from_proc_stat,
    parse_cpu_max,
    parse_cpu_stat,
    parse_meminfo,
    parse_psi_line,
    parse_psi_output,
)


class TestParseCpuStat:
    def test_full_cpu_stat(self) -> None:
        text = """usage_usec 23302029
user_usec 18824371
system_usec 4477658
nr_periods 1654
nr_throttled 56
throttled_usec 789012"""
        stat = parse_cpu_stat(text)
        assert stat.usage_usec == 23_302_029
        assert stat.user_usec == 18_824_371
        assert stat.system_usec == 4_477_658
        assert stat.nr_periods == 1654
        assert stat.nr_throttled == 56
        assert stat.throttled_usec == 789_012
        assert stat.timestamp > 0

    def test_partial_cpu_stat(self) -> None:
        text = "usage_usec 1000\nuser_usec 800"
        stat = parse_cpu_stat(text)
        assert stat.usage_usec == 1000
        assert stat.user_usec == 800
        assert stat.system_usec == 0
        assert stat.nr_periods == 0
        assert stat.nr_throttled == 0

    def test_empty_cpu_stat(self) -> None:
        stat = parse_cpu_stat("")
        assert stat.usage_usec == 0

    def test_cpu_stat_ignores_unknown_fields(self) -> None:
        text = "usage_usec 5000\nunknown_field 42"
        stat = parse_cpu_stat(text)
        assert stat.usage_usec == 5000


class TestParseCpuMax:
    def test_limited_quota(self) -> None:
        assert parse_cpu_max("40000 100000") == 0.4

    def test_full_core(self) -> None:
        assert parse_cpu_max("100000 100000") == 1.0

    def test_two_cores(self) -> None:
        assert parse_cpu_max("200000 100000") == 2.0

    def test_max_unlimited(self) -> None:
        assert parse_cpu_max("max 100000") is None

    def test_partial_quarter_core(self) -> None:
        assert parse_cpu_max("25000 100000") == 0.25

    def test_malformed_empty(self) -> None:
        assert parse_cpu_max("") is None

    def test_malformed_single_field(self) -> None:
        assert parse_cpu_max("100") is None

    def test_malformed_non_numeric(self) -> None:
        assert parse_cpu_max("abc def") is None

    def test_zero_period(self) -> None:
        assert parse_cpu_max("100000 0") is None


class TestComputeCpuUtil:
    def test_50_percent_utilization_single_core(self) -> None:
        now = time.monotonic()
        prev = CgroupCpuStat(usage_usec=0, timestamp=now - 1.0)
        curr = CgroupCpuStat(usage_usec=500_000, timestamp=now)
        result = compute_cpu_util(prev, curr, cores=1.0)
        assert result is not None
        assert abs(result.utilization - 50.0) < 1.0
        assert result.cores == 1.0
        assert result.throttled_pct == 0.0

    def test_100_percent_utilization(self) -> None:
        now = time.monotonic()
        prev = CgroupCpuStat(usage_usec=0, timestamp=now - 1.0)
        curr = CgroupCpuStat(usage_usec=1_000_000, timestamp=now)
        result = compute_cpu_util(prev, curr, cores=1.0)
        assert result is not None
        assert abs(result.utilization - 100.0) < 1.0

    def test_saturates_at_100(self) -> None:
        now = time.monotonic()
        prev = CgroupCpuStat(usage_usec=0, timestamp=now - 1.0)
        curr = CgroupCpuStat(usage_usec=2_000_000, timestamp=now)
        result = compute_cpu_util(prev, curr, cores=1.0)
        assert result is not None
        assert result.utilization == 100.0

    def test_two_cores_half_utilized(self) -> None:
        now = time.monotonic()
        prev = CgroupCpuStat(usage_usec=0, timestamp=now - 1.0)
        curr = CgroupCpuStat(usage_usec=1_000_000, timestamp=now)
        result = compute_cpu_util(prev, curr, cores=2.0)
        assert result is not None
        assert abs(result.utilization - 50.0) < 1.0

    def test_throttled_pct(self) -> None:
        now = time.monotonic()
        prev = CgroupCpuStat(
            usage_usec=0,
            nr_periods=100,
            nr_throttled=20,
            timestamp=now - 1.0,
        )
        curr = CgroupCpuStat(
            usage_usec=500_000,
            nr_periods=200,
            nr_throttled=40,
            timestamp=now,
        )
        result = compute_cpu_util(prev, curr, cores=1.0)
        assert result is not None
        assert result.throttled_pct == 20.0

    def test_none_cores_uses_1(self) -> None:
        now = time.monotonic()
        prev = CgroupCpuStat(usage_usec=0, timestamp=now - 1.0)
        curr = CgroupCpuStat(usage_usec=500_000, timestamp=now)
        result = compute_cpu_util(prev, curr, cores=None)
        assert result is not None
        assert abs(result.utilization - 50.0) < 1.0
        assert result.cores == 1.0

    def test_zero_delta_returns_none(self) -> None:
        now = time.monotonic()
        prev = CgroupCpuStat(usage_usec=500_000, timestamp=now - 1.0)
        curr = CgroupCpuStat(usage_usec=500_000, timestamp=now)
        result = compute_cpu_util(prev, curr, cores=1.0)
        assert result is None

    def test_negative_delta_returns_none(self) -> None:
        now = time.monotonic()
        prev = CgroupCpuStat(usage_usec=500_000, timestamp=now - 1.0)
        curr = CgroupCpuStat(usage_usec=100_000, timestamp=now)
        result = compute_cpu_util(prev, curr, cores=1.0)
        assert result is None

    def test_zero_elapsed_returns_none(self) -> None:
        now = time.monotonic()
        prev = CgroupCpuStat(usage_usec=0, timestamp=now)
        curr = CgroupCpuStat(usage_usec=100_000, timestamp=now)
        result = compute_cpu_util(prev, curr, cores=1.0)
        assert result is None


class TestParsePsiLine:
    def test_some_line(self) -> None:
        kind, vals = parse_psi_line(
            "some avg10=1.50 avg60=2.30 avg300=3.10 total=12345",
        )
        assert kind == "some"
        assert vals["avg10"] == 1.50
        assert vals["avg60"] == 2.30
        assert vals["avg300"] == 3.10
        assert "total" not in vals

    def test_full_line(self) -> None:
        kind, vals = parse_psi_line("full avg10=0.00 avg60=0.00 avg300=0.00 total=0")
        assert kind == "full"
        assert vals["avg10"] == 0.0


class TestParsePsiOutput:
    def test_cgroupv2_format(self) -> None:
        text = """some avg10=3.27 avg60=6.86 avg300=10.09 total=98765
full avg10=0.10 avg60=0.20 avg300=0.30 total=1234
some avg10=0.83 avg60=1.81 avg300=3.87 total=45678
full avg10=0.50 avg60=1.00 avg300=1.50 total=5678
some avg10=0.24 avg60=0.76 avg300=1.25 total=34567
full avg10=0.00 avg60=0.00 avg300=0.00 total=0"""
        snap = parse_psi_output(text)
        assert snap.cpu.some_avg10 == 3.27
        assert snap.cpu.full_avg10 == 0.10
        assert snap.memory.some_avg10 == 0.83
        assert snap.memory.full_avg10 == 0.50
        assert snap.io.some_avg10 == 0.24
        assert snap.io.full_avg10 == 0.00

    def test_no_cpu_full(self) -> None:
        text = """some avg10=5.0 avg60=5.0 avg300=5.0 total=100
some avg10=1.0 avg60=1.0 avg300=1.0 total=200
full avg10=0.0 avg60=0.0 avg300=0.0 total=300
some avg10=2.0 avg60=2.0 avg300=2.0 total=400
full avg10=0.0 avg60=0.0 avg300=0.0 total=500"""
        snap = parse_psi_output(text)
        assert snap.cpu.some_avg10 == 5.0
        assert snap.cpu.full_avg10 == 0.0


class TestParseMeminfoLegacy:
    def test_standard_meminfo(self) -> None:
        text = """MemTotal:       16384000 kB
MemAvailable:    8192000 kB
SwapTotal:       4096000 kB
SwapFree:        2048000 kB"""
        info = parse_meminfo(text)
        assert info.mem_total == 16_384_000
        assert info.mem_available == 8_192_000
        assert info.swap_total == 4_096_000
        assert info.swap_free == 2_048_000
        assert info.available_mb == 8000
        assert info.total_mb == 16000


class TestCountCpusFromProcStat:
    def test_four_cores(self) -> None:
        text = """cpu  155067853 29619 62202224 3065978997 6821103 0 0 0 0 0
cpu0 38734292 7347 15614904 766372227 1701802 0 0 0 0 0
cpu1 38870891 7346 15495720 766602600 1712987 0 0 0 0 0
cpu2 38800132 7763 15583722 766695292 1712372 0 0 0 0 0
cpu3 38662537 7161 15507877 766308876 1693941 0 0 0 0 0
intr 14951174469
ctxt 53253630315"""
        assert count_cpus_from_proc_stat(text) == 4

    def test_single_core(self) -> None:
        text = "cpu  100 0 50 1000 0 0 0 0 0 0\ncpu0 100 0 50 1000 0 0 0 0 0 0\n"
        assert count_cpus_from_proc_stat(text) == 1

    def test_no_cpu_lines(self) -> None:
        assert count_cpus_from_proc_stat("intr 42\nctxt 100\n") == 1

    def test_empty(self) -> None:
        assert count_cpus_from_proc_stat("") == 1


class TestCgroupv2Integration:
    """Integration tests that read real cgroupv2 files from the host.

    These only pass on Linux with cgroupv2. Skipped otherwise.
    """

    def _read(self, path: str) -> str | None:
        try:
            with Path(path).open() as f:
                return f.read()
        except FileNotFoundError:
            return None

    def test_parse_host_cpu_stat(self) -> None:
        text = self._read("/sys/fs/cgroup/cpu.stat")
        if text is None:
            pytest.skip("cgroupv2 cpu.stat not available")
        assert text is not None
        stat = parse_cpu_stat(text)
        assert stat.usage_usec > 0

    def test_parse_host_cpu_max(self) -> None:
        text = self._read("/sys/fs/cgroup/cpu.max")
        if text is None:
            pytest.skip("cgroupv2 cpu.max not available (root cgroup)")
        assert text is not None
        result = parse_cpu_max(text)
        if "max" in text:
            assert result is None
        else:
            assert result is not None
            assert result > 0

    def test_parse_host_psi(self) -> None:
        paths = [
            "/sys/fs/cgroup/cpu.pressure",
            "/sys/fs/cgroup/memory.pressure",
            "/sys/fs/cgroup/io.pressure",
        ]
        parts = []
        for p in paths:
            text = self._read(p)
            if text is None:
                pytest.skip(f"{p} not available")
            parts.append(text)
        snap = parse_psi_output("".join(parts))
        assert snap.cpu.some_avg10 >= 0.0
        assert snap.memory.some_avg10 >= 0.0
        assert snap.io.some_avg10 >= 0.0

    def test_parse_host_proc_stat(self) -> None:
        text = self._read("/proc/stat")
        if text is None:
            pytest.skip("/proc/stat not available")
        assert text is not None
        n = count_cpus_from_proc_stat(text)
        assert n >= 1

    def test_compute_utilization_from_host(self) -> None:
        text1 = self._read("/sys/fs/cgroup/cpu.stat")
        if text1 is None:
            pytest.skip("cgroupv2 cpu.stat not available")
        assert text1 is not None
        stat1 = parse_cpu_stat(text1)
        cpu_max_text = self._read("/sys/fs/cgroup/cpu.max")
        cores = parse_cpu_max(cpu_max_text) if cpu_max_text else None
        if cores is None:
            proc_stat = self._read("/proc/stat")
            if proc_stat:
                cores = float(count_cpus_from_proc_stat(proc_stat))
        time.sleep(0.5)
        text2 = self._read("/sys/fs/cgroup/cpu.stat")
        assert text2 is not None
        stat2 = parse_cpu_stat(text2)
        result = compute_cpu_util(stat1, stat2, cores)
        assert result is not None
        assert 0.0 <= result.utilization <= 100.0
        assert result.cores >= 1.0
