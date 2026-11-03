"""The watermark drive runs passes back to back until relief. Issue #71."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from pynixd.gc import Collector
from pynixd.instance import Server
from pynixd_store.gc import pressure_usage


class FakeDrive:
    """A loop reduced to pressure readings and pass outcomes."""

    def __init__(self, *, pressured: list[bool], freed: list[int], cap: int = 20) -> None:
        self.ctx = SimpleNamespace(settings=SimpleNamespace(gc_watermark_max_passes=cap))
        self._pressured = pressured
        self._freed = freed
        self.passes = 0

    def _over_gc_target(self) -> bool:
        return self._pressured.pop(0)

    async def _gc_pass(self, reason: str) -> Any | None:
        assert reason == "watermark"
        self.passes += 1
        count = self._freed.pop(0)
        if count < 0:
            return None
        return SimpleNamespace(store_paths={f"path-{self.passes}-{i}" for i in range(count)})


async def _drive(fake: FakeDrive) -> None:
    await Server._drive_to_target(cast(Server, fake))


@pytest.mark.anyio
async def test_drive_stops_when_pressure_drops() -> None:
    """Two freeing passes, then the re-check reads at or below the target."""
    fake = FakeDrive(pressured=[True, True, False], freed=[10, 10])
    await _drive(fake)
    assert fake.passes == 2


@pytest.mark.anyio
async def test_drive_stops_when_a_pass_frees_nothing() -> None:
    """A refused pass frees nothing: more passes change nothing new."""
    fake = FakeDrive(pressured=[True, True, True], freed=[10, 0])
    await _drive(fake)
    assert fake.passes == 2


@pytest.mark.anyio
async def test_drive_stops_at_the_cap() -> None:
    """Endless pressure with endless progress still ends the drive."""
    fake = FakeDrive(pressured=[True] * 10, freed=[10] * 10, cap=3)
    await _drive(fake)
    assert fake.passes == 3


def test_pressure_counts_only_bavail_as_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """Root-reserved blocks are pressure: `bfree` says 0.6, `df` says 0.9. Issue #11."""
    monkeypatch.setattr(
        "os.statvfs",
        lambda _path: SimpleNamespace(f_blocks=100, f_bfree=40, f_bavail=10, f_frsize=1024),
    )
    used, total = pressure_usage("/store")
    assert (used, total) == (90 * 1024, 100 * 1024)


def test_disk_usage_reads_the_store_filesystem(monkeypatch: pytest.MonkeyPatch) -> None:
    """`Collector._disk_usage` gauges the store dir, `bavail`-based. Issue #11."""
    seen: list[str] = []

    def fake_statvfs(path: str) -> SimpleNamespace:
        seen.append(path)
        return SimpleNamespace(f_blocks=100, f_bfree=40, f_bavail=10, f_frsize=1)

    monkeypatch.setattr("os.statvfs", fake_statvfs)
    local = SimpleNamespace(layout=SimpleNamespace(real_store_dir="/store"), db=None)
    assert Collector._disk_usage(cast("Any", local)) == (90, 100)
    assert seen == ["/store"]


async def test_drive_continues_below_watermark_until_target(monkeypatch: pytest.MonkeyPatch) -> None:
    readings = iter([(88, 100), (74, 100), (70, 100)])
    monkeypatch.setattr(Collector, "_disk_usage", staticmethod(lambda local: next(readings)))

    class Drive(FakeDrive):
        def _over_gc_target(self) -> bool:
            return Server._over_gc_target(cast(Server, self))

        def _over_watermark(self) -> bool:
            raise AssertionError("an active drive must use the target")

    fake = Drive(pressured=[], freed=[10, 10])
    fake.ctx.local_store = SimpleNamespace(gc_target_usage=0.7)
    await _drive(fake)
    assert fake.passes == 2


def test_drive_without_a_target_uses_the_watermark() -> None:
    fake = SimpleNamespace(
        ctx=SimpleNamespace(local_store=SimpleNamespace(gc_target_usage=None)), _over_watermark=lambda: True
    )
    assert Server._over_gc_target(cast(Server, fake))
