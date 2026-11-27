"""The watermark drive: one collector run, then more only after the cold tier covers. nixidae/nixidae#1600."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.gc import Collector
from pynixd.instance import Server
from pynixd_store.gc import pressure_usage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class FakeDrive:
    """A drive reduced to pressure readings, run outcomes and cover waits."""

    def __init__(
        self, *, pressured: list[bool], freed: list[int], covers: list[bool] | None = None, cap: int = 20
    ) -> None:
        self.ctx = SimpleNamespace(settings=SimpleNamespace(gc_drive_max_runs=cap))
        self.cold_uploader = None if covers is None else object()
        self._pressured = pressured
        self._freed = freed
        self._covers = covers or []
        self.runs = 0
        self.waits = 0

    def _over_gc_target(self) -> bool:
        return self._pressured.pop(0)

    async def _gc_pass(self, reason: str) -> Any | None:
        assert reason == "watermark"
        self.runs += 1
        count = self._freed.pop(0)
        if count < 0:
            return None
        return SimpleNamespace(store_paths={f"path-{self.runs}-{i}" for i in range(count)})

    async def _wait_for_cover(self, uploader: object) -> bool:
        assert uploader is self.cold_uploader
        self.waits += 1
        return self._covers.pop(0)


async def _drive(fake: FakeDrive) -> bool:
    return await Server._drive_to_target(cast(Server, fake))


async def test_a_disk_under_the_target_runs_nothing() -> None:
    fake = FakeDrive(pressured=[False], freed=[])
    assert await _drive(fake)
    assert fake.runs == 0


async def test_one_run_that_reaches_the_target_ends_the_drive() -> None:
    fake = FakeDrive(pressured=[True, False], freed=[10])
    assert await _drive(fake)
    assert fake.runs == 1


async def test_a_run_that_ends_over_the_target_ends_the_drive_without_a_tier() -> None:
    """The run already passed until it stalled: another run at once changes nothing."""
    fake = FakeDrive(pressured=[True, True], freed=[10])
    assert not await _drive(fake)
    assert fake.runs == 1


async def test_the_next_run_waits_for_the_tier_to_cover() -> None:
    fake = FakeDrive(pressured=[True, True, False], freed=[0, 10], covers=[True])
    assert await _drive(fake)
    assert (fake.runs, fake.waits) == (2, 1)


async def test_a_tier_with_nothing_to_cover_ends_the_drive() -> None:
    fake = FakeDrive(pressured=[True, True], freed=[10], covers=[False])
    assert not await _drive(fake)
    assert (fake.runs, fake.waits) == (1, 1)


async def test_drive_stops_at_the_cap() -> None:
    """Endless pressure and endless cover still end the drive."""
    fake = FakeDrive(pressured=[True] * 10, freed=[10] * 10, covers=[True] * 10, cap=3)
    assert not await _drive(fake)
    assert fake.runs == 3


class _Polled(Exception):
    """Ends the watch loop after its last poll."""


async def test_a_stalled_drive_starts_no_other_until_the_store_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each run deletes paths and frees nothing on disk: the watch holds the state the drive left."""
    states = iter([(1, 10, 0), (1, 9, 0), (1, 9, 0), (1, 9, 0), (1, 9, 0), (2, 10, 0), (2, 9, 0)])

    async def store_state(_self: Collector) -> tuple[int, ...]:
        return next(states)

    monkeypatch.setattr(Collector, "store_state", store_state)

    class Watch:
        def __init__(self) -> None:
            self.ctx = SimpleNamespace(
                settings=SimpleNamespace(
                    gc_interval=10**9, gc_poll_interval=0, gc_high_watermark=0.75, gc_drive_max_runs=20
                )
            )
            self.polls = 5
            self.drives = 0

        async def _checkpoint_wal(self) -> bool:
            return True

        def _over_watermark(self) -> bool:
            if self.polls == 0:
                raise _Polled
            self.polls -= 1
            return True

        @asynccontextmanager
        async def _singleton_pass(self) -> AsyncIterator[None]:
            yield

        async def _drive_to_target(self) -> bool:
            self.drives += 1
            return False

    watch = Watch()
    with pytest.raises(_Polled):
        await Server._gc_tick(cast(Server, watch))
    # Polls 1 and 5 drive; 2-4 read the state drive 1 left. Poll 5 reads a new path.
    assert watch.drives == 2


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


async def test_drive_reads_the_target_not_the_watermark(monkeypatch: pytest.MonkeyPatch) -> None:
    readings = iter([(88, 100), (74, 100), (70, 100)])
    monkeypatch.setattr(Collector, "_disk_usage", staticmethod(lambda local: next(readings)))

    class Drive(FakeDrive):
        def _over_gc_target(self) -> bool:
            return Server._over_gc_target(cast(Server, self))

        def _over_watermark(self) -> bool:
            raise AssertionError("an active drive must use the target")

    fake = Drive(pressured=[], freed=[10, 10], covers=[True])
    fake.ctx.local_store = SimpleNamespace(gc_target_usage=0.7)
    await _drive(fake)
    assert (fake.runs, fake.waits) == (2, 1)


def test_drive_without_a_target_uses_the_watermark() -> None:
    fake = SimpleNamespace(
        ctx=SimpleNamespace(local_store=SimpleNamespace(gc_target_usage=None)), _over_watermark=lambda: True
    )
    assert Server._over_gc_target(cast(Server, fake))
