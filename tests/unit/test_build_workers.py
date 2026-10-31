"""Builds run in a task group the scheduler owns. Step 3 of issue #91.

Each build ran in a detached `asyncio.create_task`. Nothing waited for it,
so a scheduler that stopped left its builds running, and a crash in one
reached nobody.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import PynixdSettings
from pynixd.context import PynixdContext
from pynixd.scheduler import Scheduler
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
)
from pynixd.store_path import StorePath
from tests.conftest import serde_path
from tests.functional.mock_store import MockStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from pynixd.build_queue import QueuedBuild
    from pynixd.store import DaemonStore


def _drv(name: str) -> StorePath:
    return StorePath(f"/nix/store/{'0' * 31}{len(name)}-{name}.drv")


_BUILT = BuildDerivationResponse(result=BuildResult(status=BuildResultStatus.BUILT))


def _scheduler() -> Scheduler:
    """A local and a remote store. Each test fakes `execute_build`."""
    local = MockStore("local", feature_matrix={"x86_64-linux": set()})
    remote = MockStore("remote", feature_matrix={"x86_64-linux": set()})
    return Scheduler(
        PynixdContext(settings=PynixdSettings(), _stores={StoreId("local"): local, StoreId("remote"): remote})
    )


async def _enqueue(scheduler: Scheduler, name: str) -> QueuedBuild:
    request = BuildDerivationRequest(
        drv_path=serde_path(_drv(name)),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )
    build_id, _ = await scheduler.build_derivation(request)
    return scheduler.queue.by_id[build_id]


async def _until(condition: Callable[[], bool]) -> None:
    with anyio.fail_after(5):
        while not condition():
            await anyio.sleep(0.01)


async def test_leaving_the_group_cancels_a_running_build(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = _scheduler()
    build = await _enqueue(scheduler, "slow")
    started = anyio.Event()
    cancelled: list[QueuedBuild] = []

    async def execute(build: QueuedBuild, store: DaemonStore) -> None:
        del store
        started.set()
        try:
            await anyio.sleep_forever()
        except anyio.get_cancelled_exc_class():
            cancelled.append(build)
            raise

    monkeypatch.setattr(scheduler, "execute_build", execute)

    with anyio.fail_after(5):
        async with scheduler.workers():
            await scheduler.schedule()
            await started.wait()

    assert cancelled == [build]
    assert not build.is_building


async def test_a_crashed_worker_leaves_its_siblings_running(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = _scheduler()
    slow = await _enqueue(scheduler, "slow")
    crash = await _enqueue(scheduler, "crash")
    release = anyio.Event()

    async def execute(build: QueuedBuild, store: DaemonStore) -> None:
        del store
        if build is crash:
            raise ValueError("a bug in one build")
        await release.wait()
        await scheduler.queue.complete(build.build_id, _BUILT)

    monkeypatch.setattr(scheduler, "execute_build", execute)

    async with scheduler.workers():
        await scheduler.schedule()
        await _until(lambda: not crash.is_building)
        assert crash.is_done
        assert slow.is_building
        release.set()
        await _until(lambda: slow.is_done)

    assert slow.answer.result().result.status == BuildResultStatus.BUILT


async def test_a_build_cancelled_before_its_worker_starts_never_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = _scheduler()
    build = await _enqueue(scheduler, "early")
    ran: list[QueuedBuild] = []

    async def execute(build: QueuedBuild, store: DaemonStore) -> None:
        del store
        ran.append(build)

    monkeypatch.setattr(scheduler, "execute_build", execute)

    async with scheduler.workers():
        await scheduler.schedule()
        build.cancel_worker()
        await _until(lambda: not build.is_building)

    assert ran == []
