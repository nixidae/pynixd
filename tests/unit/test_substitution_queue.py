"""Unit tests for scheduler-owned substitution queue state."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd.config import PynixdSettings
from pynixd.serde import QueryPathInfoResponse
from pynixd.store_path import StorePath
from pynixd.substitution_queue import (
    SubstitutionHealthLog,
    SubstitutionImportResult,
    SubstitutionQueryResult,
    SubstitutionQueue,
)
from tests._conftest.all_of import all_of

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd.context import PynixdContext


class FakeSubstituter:
    no_schedule = True

    def __init__(
        self,
        store_id: str,
        *,
        valid: bool,
        delay: float = 0.0,
        nar_size: int = 10,
        priority: float = 1.0,
    ) -> None:
        self.store_id = StoreId(store_id)
        self.valid = valid
        self.delay = delay
        self.nar_size = nar_size
        self.priority = priority
        self.queries = 0

    async def execute(self, request: WireRequest, **kwargs: Any) -> QueryPathInfoResponse:
        del request, kwargs
        self.queries += 1
        await anyio.sleep(self.delay)
        if not self.valid:
            return QueryPathInfoResponse(valid=False)
        return QueryPathInfoResponse(
            valid=True,
            info=UnkeyedValidPathInfo(
                deriver=None,
                nar_hash=NARHash(hash="0" * 52),
                references=set(),
                registration_time=Time(ts=0),
                nar_size=self.nar_size,
                ultimate=False,
                sigs=set(),
                ca=ContentAddress(value=""),
            ),
        )


def test_new_substitution_health_log_is_healthy_until_minimum_fill() -> None:
    settings = PynixdSettings(
        substitution_health_window=10,
        substitution_health_min_fill_ratio=0.10,
    )
    health = SubstitutionHealthLog(settings)

    assert health.is_healthy

    health.record(query_succeeded=False)

    assert not health.is_healthy


def test_substitution_queue_records_positive_and_negative_results() -> None:
    ctx = cast("PynixdContext", SimpleNamespace(settings=PynixdSettings()))
    queue = SubstitutionQueue(ctx)
    path = StorePath("/nix/store/00000000000000000000000000000000-example")
    store_id = StoreId("cache")

    queue.record_query_result(
        path,
        SubstitutionQueryResult(store_id=store_id, path_info=None, query_succeeded=True),
    )

    assert path in queue.negative
    assert queue.negative[path][store_id].query_succeeded
    assert queue.should_wait_for(store_id)


@pytest.mark.anyio
async def test_can_substitute_returns_first_positive_and_keeps_probing() -> None:
    path = StorePath("/nix/store/00000000000000000000000000000000-example")
    slow_missing = FakeSubstituter("slow-missing", valid=False, delay=0.05)
    fast_hit = FakeSubstituter("fast-hit", valid=True, nar_size=123)
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            settings=PynixdSettings(),
            stores={
                StoreId("local"): SimpleNamespace(no_schedule=False),
                slow_missing.store_id: slow_missing,
                fast_hit.store_id: fast_hit,
            },
        ),
    )
    queue = SubstitutionQueue(ctx)

    async with queue.running():
        availability = await queue.can_substitute(path)

        assert availability.available
        assert availability.nar_size == 123
        assert fast_hit.queries == 1
        await anyio.sleep(0.06)
        assert slow_missing.queries == 1


@pytest.mark.anyio
async def test_can_substitute_does_not_wait_for_unhealthy_slow_store() -> None:
    path = StorePath("/nix/store/00000000000000000000000000000000-example")
    slow_hit = FakeSubstituter("slow-hit", valid=True, delay=0.05)
    fast_missing = FakeSubstituter("fast-missing", valid=False)
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            settings=PynixdSettings(substitution_health_window=10),
            stores={
                StoreId("local"): SimpleNamespace(no_schedule=False),
                slow_hit.store_id: slow_hit,
                fast_missing.store_id: fast_missing,
            },
        ),
    )
    queue = SubstitutionQueue(ctx)
    queue.health_for(slow_hit.store_id).record(query_succeeded=False)

    async with queue.running():
        availability = await queue.can_substitute(path)

        assert not availability.available
        assert fast_missing.queries == 1
        assert slow_hit.queries == 1


@pytest.mark.anyio
async def test_a_probe_that_raises_costs_only_its_own_answer() -> None:
    path = StorePath("/nix/store/00000000000000000000000000000000-example")
    hit = FakeSubstituter("hit", valid=True)
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            settings=PynixdSettings(),
            stores={StoreId("local"): SimpleNamespace(no_schedule=False), hit.store_id: hit},
        ),
    )
    queue = SubstitutionQueue(ctx)

    async def broken() -> None:
        raise RuntimeError("probe bug")

    async with queue.running():
        queue._spawn_probe(broken)
        await anyio.sleep(0)
        availability = await queue.can_substitute(path)

    assert availability.available


@pytest.mark.anyio
async def test_close_cancels_the_stragglers() -> None:
    path = StorePath("/nix/store/00000000000000000000000000000000-example")
    fast_hit = FakeSubstituter("fast-hit", valid=True)
    stuck = FakeSubstituter("stuck", valid=False, delay=3600)
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            settings=PynixdSettings(),
            stores={
                StoreId("local"): SimpleNamespace(no_schedule=False),
                fast_hit.store_id: fast_hit,
                stuck.store_id: stuck,
            },
        ),
    )
    queue = SubstitutionQueue(ctx)

    with anyio.fail_after(5):
        async with queue.running():
            assert (await queue.can_substitute(path)).available
            await queue.close()

    assert stuck.queries == 1
    assert path not in queue.negative


@pytest.mark.anyio
async def test_get_substituter_selects_highest_priority_positive_store() -> None:
    path = StorePath("/nix/store/00000000000000000000000000000000-example")
    low_priority = FakeSubstituter("low-priority", valid=True, priority=50)
    high_priority = FakeSubstituter("high-priority", valid=True, priority=10)
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            settings=PynixdSettings(),
            stores={
                StoreId("local"): SimpleNamespace(no_schedule=False),
                low_priority.store_id: low_priority,
                high_priority.store_id: high_priority,
            },
        ),
    )
    queue = SubstitutionQueue(ctx)

    candidate = await queue.get_substituter(path)

    assert candidate is not None
    assert candidate.store is high_priority  # type: ignore[comparison-overlap]  # the fake substituter stands in for a Store; the test checks identity only


@pytest.mark.anyio
async def test_substitute_deduplicates_active_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    path = StorePath("/nix/store/00000000000000000000000000000000-example")
    store = FakeSubstituter("cache", valid=True)
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            settings=PynixdSettings(),
            stores={
                StoreId("local"): SimpleNamespace(no_schedule=False),
                store.store_id: store,
            },
        ),
    )
    queue = SubstitutionQueue(ctx)
    imports = 0

    async def fake_import(path_arg: StorePath, candidate: object) -> None:
        nonlocal imports
        del path_arg, candidate
        imports += 1
        await anyio.sleep(0.01)

    monkeypatch.setattr(queue, "_import_nar", fake_import)

    async with queue.running():
        first, second = await all_of(queue.substitute(path), queue.substitute(path))

    assert first.substituted
    assert second.substituted
    assert imports == 1
    assert store.queries == 1


@pytest.mark.anyio
async def test_a_waiter_that_leaves_leaves_the_import_to_the_others(monkeypatch: pytest.MonkeyPatch) -> None:
    path = StorePath("/nix/store/00000000000000000000000000000000-example")
    store = FakeSubstituter("cache", valid=True)
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            settings=PynixdSettings(),
            stores={
                StoreId("local"): SimpleNamespace(no_schedule=False),
                store.store_id: store,
            },
        ),
    )
    queue = SubstitutionQueue(ctx)
    importing = anyio.Event()
    release = anyio.Event()
    imports = 0
    results: list[SubstitutionImportResult] = []

    async def fake_import(path_arg: StorePath, candidate: object) -> None:
        nonlocal imports
        del path_arg, candidate
        imports += 1
        importing.set()
        await release.wait()

    monkeypatch.setattr(queue, "_import_nar", fake_import)

    async def stay() -> None:
        results.append(await queue.substitute(path))

    with anyio.fail_after(5):
        async with queue.running(), anyio.create_task_group() as tg:
            with anyio.CancelScope() as leaver:
                tg.start_soon(stay)
                await importing.wait()
                leaver.cancel()
                await queue.substitute(path)
            await anyio.sleep(0)
            release.set()

    assert [result.substituted for result in results] == [True]
    assert imports == 1


@pytest.mark.anyio
async def test_imports_stream_max_substitution_jobs_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """`substitution-goal.cc:200`: no more imports than `max-substitution-jobs`, and one at 0."""
    store = FakeSubstituter("cache", valid=True)
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            settings=PynixdSettings(),
            stores={
                StoreId("local"): SimpleNamespace(no_schedule=False),
                store.store_id: store,
            },
        ),
    )
    queue = SubstitutionQueue(ctx)
    queue.set_import_slots(2)
    running = peak = 0

    async def fake_import(path_arg: StorePath, candidate: object) -> None:
        nonlocal running, peak
        del path_arg, candidate
        running += 1
        peak = max(peak, running)
        await anyio.sleep(0.01)
        running -= 1

    monkeypatch.setattr(queue, "_import_nar", fake_import)
    paths = [StorePath(f"/nix/store/{i:032d}-example") for i in range(6)]
    async with queue.running():
        results = await all_of(*(queue.substitute(path) for path in paths))

    assert all(result.substituted for result in results)
    assert peak == 2
    queue.set_import_slots(0)
    assert queue.import_limit.total_tokens == 1
