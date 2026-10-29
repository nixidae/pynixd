"""Daemon state collection, federation, and metrics readers. Issue #81."""

from __future__ import annotations

import json
import time
import tracemalloc
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import anyio
import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import LOCAL_STORE_ID, RequestId, StoreId
from pynixd import metrics, state as state_collector
from pynixd.build_queue import BuildQueue
from pynixd.config import PynixdSettings
from pynixd.context import PynixdContext
from pynixd.daemon_extensions.pynixd_state import PynixdStateRequest
from pynixd.exceptions import OpNotImplementedError
from pynixd.handlers.pynixd_state import PynixdStateHandler, _federated_sections, _query_one
from pynixd.scheduler import Scheduler
from pynixd.serde.auth import Role
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.sessions import ClientSessions
from pynixd.state_models import FederatedError, QueueCounts, StateReport, StoreRow
from pynixd.wire import BytesReader, BytesWriter
from tests.functional.mock_store import MockStore
from tests.unit.test_local_build_slots import (
    _options as _slot_options,
    _request as _slot_request,
)

VERSION = 0x126


def test_metrics_readers_report_recorded_totals() -> None:
    before_bytes = metrics.nar_bytes_received_total()
    before_paths = metrics.nar_paths_received_total()
    metrics.NAR_FORWARD_BYTES.inc(100)
    metrics.NAR_ADD_BYTES.inc(50)
    metrics.NAR_FORWARD_PATHS.inc(3)
    assert metrics.nar_bytes_received_total() - before_bytes == 150
    assert metrics.nar_paths_received_total() - before_paths == 3

    before = metrics.builds_completed_by_status()
    metrics.BUILDS_COMPLETED.labels(status="success").inc(2)
    after = metrics.builds_completed_by_status()
    assert after["success"] - before["success"] == 2
    assert set(after) == {"success", "failure"}

    before_served = metrics.nar_bytes_served_total()
    before_accepted = metrics.sessions_accepted_by_transport()
    metrics.NAR_SERVE_BYTES.inc(7)
    metrics.DAEMON_SESSIONS_TOTAL.labels(transport="unix").inc()
    assert metrics.nar_bytes_served_total() - before_served == 7
    assert metrics.sessions_accepted_by_transport()["unix"] - before_accepted.get("unix", 0) == 1


def _build(done: bool = False, building: bool = False) -> SimpleNamespace:
    pending = not done and not building
    return SimpleNamespace(is_done=done, is_building=building, is_pending=pending)


def _ctx(
    builds: list[SimpleNamespace] | None = None,
    stores: dict[object, object] | None = None,
) -> SimpleNamespace:
    queue = SimpleNamespace(queue=list(builds or []), _queue=list(builds or []))
    queue.counts = lambda: BuildQueue.counts(cast("Any", queue))
    scheduler = SimpleNamespace(queue=queue)
    return SimpleNamespace(scheduler=scheduler, stores=stores or {}, sessions=ClientSessions())


def test_benchmark_names_the_largest_allocation_sites() -> None:
    """Tracing on: the section lists sites, the allocating line first."""
    tracemalloc.start(5)
    try:
        held = [bytearray(1 << 20) for _ in range(4)]
        section = state_collector._benchmark_section()
    finally:
        tracemalloc.stop()
    assert section.tracing
    assert held
    top = section.top[0]
    assert top.size >= 4 << 20
    assert top.site.startswith(f"{__file__}:")


def test_benchmark_off_lists_nothing() -> None:
    assert state_collector._benchmark_section().top == []


def test_collect_counts_queue_by_state() -> None:
    ctx = _ctx(builds=[_build(done=True), _build(building=True), _build(), _build()])
    report = state_collector.collect(cast("PynixdContext", ctx), ["queue"])
    assert report == StateReport(queue=QueueCounts(pending=2, building=1, done=1))


def test_collect_without_scheduler_reports_none() -> None:
    ctx = SimpleNamespace(scheduler=None, stores={}, sessions=ClientSessions())
    report = state_collector.collect(cast("PynixdContext", ctx), ["queue"])
    assert report.queue == QueueCounts(scheduler=False)


def test_collect_names_stores_and_sessions() -> None:
    store = SimpleNamespace(is_healthy=True, feature_matrix={"x86_64-linux": {"kvm"}})
    ctx = _ctx(stores={StoreId("builder-a"): store})
    report = state_collector.collect(cast("PynixdContext", ctx), ["stores", "sessions", "no-such-section"])
    assert report.stores == {"builder-a": StoreRow(healthy=True, systems=["x86_64-linux"])}
    assert report.sessions == {}
    assert report.queue is None


def test_collect_empty_wants_answers_everything() -> None:
    report = state_collector.collect(cast("PynixdContext", _ctx()), [])
    answered = {name for name in StateReport.model_fields if getattr(report, name) is not None}
    assert answered == set(state_collector.KNOWN_SECTIONS)


@pytest.mark.anyio
async def test_query_one_marks_a_hanging_store_unreachable() -> None:
    async def hang(request: Any) -> Any:
        await anyio.sleep(60.0)
        raise AssertionError("unreachable")

    store = SimpleNamespace(execute=hang, features={"PynixdState"})
    started = time.monotonic()
    answer = await _query_one(cast(Any, store), [], timeout=0.1)
    assert time.monotonic() - started < 10.0
    assert isinstance(answer, FederatedError)
    assert "no answer" in answer.error


@pytest.mark.anyio
async def test_query_one_marks_a_stock_daemon_unimplemented() -> None:
    async def refuse(request: Any) -> Any:
        raise OpNotImplementedError("nope")

    store = SimpleNamespace(execute=refuse, features={"PynixdState"})
    answer = await _query_one(cast(Any, store), [])
    assert answer == FederatedError(error="state op not implemented by this store")


class FakeProxy:
    def __init__(self, body: bytes, ctx: SimpleNamespace, stores: dict[object, object]) -> None:
        self.r = BytesReader(body, identifier="test:state")
        self.version = VERSION
        self.standard_features: frozenset[str] = frozenset()
        self.ctx = ctx
        self.stores = stores
        self.client: Any = None
        self.errors: list[str] = []

    async def send_error(self, message: str) -> None:
        self.errors.append(message)


@dataclass
class FakeContext:
    proxy: FakeProxy
    role: Role
    version: int = VERSION
    username: str = "test"


async def _body(wants: list[str], federated: bool) -> bytes:
    writer = BytesWriter("test")
    req = PynixdStateRequest(version=1, wants=wants, federated=federated)
    await req.to_writer(WriteContext(writer=writer, version=VERSION))
    return writer.get_bytes()[8:]


@pytest.mark.anyio
async def test_wire_round_trip_carries_wants_and_federated() -> None:
    body = await _body(["queue", "stores"], True)
    req = await PynixdStateRequest.from_reader(
        ReadContext(
            reader=BytesReader(body, identifier="test:state"),
            version=VERSION,
            features=frozenset(),
        )
    )
    assert req.wants == ["queue", "stores"]
    assert req.federated is True
    assert req.op == 111


@pytest.mark.anyio
async def test_handler_refuses_an_untrusted_client() -> None:
    proxy = FakeProxy(await _body([], False), _ctx(), {})
    resp = await PynixdStateHandler().handle(FakeContext(proxy=proxy, role=Role.USER))  # type: ignore[arg-type]  # fakes
    assert resp is None
    assert len(proxy.errors) == 1 and "administrative privileges" in proxy.errors[0]


@pytest.mark.anyio
async def test_handler_collects_local_sections() -> None:
    store = SimpleNamespace(is_healthy=False, feature_matrix={})
    ctx = _ctx(builds=[_build()], stores={StoreId("builder-a"): store})
    proxy = FakeProxy(await _body(["queue", "stores"], False), ctx, {})
    resp = await PynixdStateHandler().handle(FakeContext(proxy=proxy, role=Role.ADMIN))  # type: ignore[arg-type]  # fakes
    assert resp is not None
    payload = json.loads(cast(Any, resp).payload)
    assert payload["queue"]["pending"] == 1
    assert payload["stores"]["builder-a"] == {"healthy": False, "systems": [], "pools": {}}
    assert "federated" not in payload


@pytest.mark.anyio
async def test_federated_merge_marks_failures() -> None:
    async def answer(request: Any) -> Any:
        return SimpleNamespace(payload=json.dumps({"queue": {"pending": 3}}))

    async def refuse(request: Any) -> Any:
        raise OpNotImplementedError("stock daemon")

    def advertised(execute: Any) -> SimpleNamespace:
        return SimpleNamespace(execute=execute, features={"PynixdState"})

    def silent(execute: Any) -> SimpleNamespace:
        calls: list[Any] = []

        async def tracked(request: Any) -> Any:
            calls.append(request)
            return await execute(request)

        store = SimpleNamespace(execute=tracked, features=set())
        store.calls = calls
        return store

    ctx = _ctx()
    quiet = silent(answer)
    proxy = FakeProxy(
        await _body([], False),
        ctx,
        {
            LOCAL_STORE_ID: SimpleNamespace(),
            StoreId("good"): advertised(answer),
            StoreId("old"): advertised(refuse),
            StoreId("quiet"): quiet,
        },
    )
    merged = await _federated_sections(cast(Any, FakeContext(proxy=proxy, role=Role.ADMIN)), ["queue"])
    assert merged["good"] == StateReport(queue=QueueCounts(pending=3))
    assert merged["old"] == FederatedError(error="state op not implemented by this store")
    assert merged["quiet"] == FederatedError(error="state op not advertised by this store")
    assert quiet.calls == []
    assert LOCAL_STORE_ID not in merged and "local" not in merged


@pytest.mark.anyio
async def test_builds_say_why_a_pending_build_waits() -> None:
    """Issue #92: the counts said "one pending" for the build that held an
    IFD for hours in #89. This section names it and the slot it waits for."""
    ctx = PynixdContext(
        settings=PynixdSettings(),
        _stores={LOCAL_STORE_ID: MockStore("local", feature_matrix={"x86_64-linux": set()})},
    )
    ctx.scheduler = Scheduler(ctx)
    queue = ctx.scheduler.queue
    running_id, _ = await queue.enqueue(
        _slot_request("1"), from_goal_path=True, goal_request_id=RequestId(7), options=_slot_options(1)
    )
    running = queue.by_id[running_id]
    running.assigned_store_id = LOCAL_STORE_ID
    running.worker = anyio.CancelScope()
    await queue.enqueue(_slot_request("2"), from_goal_path=True, goal_request_id=RequestId(7), options=_slot_options(1))

    builds = state_collector.collect(ctx, ["builds"]).builds
    assert builds is not None
    rows = {row.state: row for row in builds}

    assert rows["building"].store == "local"
    assert rows["building"].waits_for is None
    assert rows["pending"].requests == {7: 1}
    assert rows["pending"].waits_for == "local_slot"
    # The same reason, as `pynixd_builds_waiting` scrapes it.
    assert REGISTRY.get_sample_value("pynixd_builds_waiting", {"reason": "local_slot"}) == 1


@dataclass(eq=False)
class _FakeSession:
    transport: str
    username: str
    connected_at: float
    current_op: tuple[str, float] | None
    pid: int | None = None
    request_id: int | None = None


def test_clients_name_the_operation_each_session_runs() -> None:
    ctx = _ctx()
    now = time.monotonic()
    busy = _FakeSession("unix", "alice", now - 60, ("BuildPaths", now - 30))
    idle = _FakeSession("ssh", "bob", now - 5, None)
    ctx.sessions.track(cast("Any", busy), None)
    ctx.sessions.track(cast("Any", idle), None)

    rows = state_collector.collect(cast("PynixdContext", ctx), ["clients"]).clients
    assert rows is not None

    assert [(row.user, row.op) for row in rows] == [("alice", "BuildPaths"), ("bob", None)]
    assert (rows[0].op_s or 0) >= 30
