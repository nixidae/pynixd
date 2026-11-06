"""The helper's usage record (`pynixd_store/builder/usage.py`, nixidae/nixidae#1187)."""

from __future__ import annotations

import resource
from typing import TYPE_CHECKING

from pynixd_store.builder.usage import BuildUsage, UsageSource, from_rusage, read_usage, write_usage

if TYPE_CHECKING:
    from pathlib import Path


def test_rusage_becomes_microseconds_and_bytes() -> None:
    fields = [0.0] * 16
    fields[0], fields[1], fields[2] = 1.5, 0.25, 2048
    fields[9], fields[10] = 3, 4
    usage = from_rusage(resource.struct_rusage(fields))

    assert usage == BuildUsage(
        source=UsageSource.RUSAGE,
        cpu_user_us=1_500_000,
        cpu_system_us=250_000,
        peak_memory_bytes=2048 * 1024,
        io_read_bytes=3 * 512,
        io_write_bytes=4 * 512,
    )


def test_a_record_reads_back_and_a_missing_one_is_none(tmp_path: Path) -> None:
    usage = BuildUsage(UsageSource.RUSAGE, 1, 2, 3, 4, 5)
    write_usage(tmp_path / "usage.json", usage)

    assert read_usage(tmp_path / "usage.json") == usage
    assert read_usage(tmp_path / "missing.json") is None
