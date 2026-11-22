"""Each build's life as one trace of spans. nixidae/nixidae#1570.

The spans go through the SDK's batch processor, as in a daemon, into an exporter that keeps them.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, cast

import anyio
import pytest
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd.build_queue import BuildQueue, QueuedBuild
from pynixd.config import TracingSettings
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    StorePath,
)
from pynixd.tracing import NO_OP_TRACE, NO_TRACE, NO_TRACING, Ending, Phase, SessionIdentity
from pynixd.tracing_otel import OtelTracing
from tests._conftest.placement import FakeStore, FixedRanking, Spawned, fake_placer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from opentelemetry.sdk.trace import ReadableSpan

    from pynixd.connection import ClientConn

FULL = {LOCAL_STORE_ID: 10**6}
"""More running builds than any build limit."""


def _request(name: str) -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path=f"/nix/store/0000000000000000000000000000000{name}-{name}.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh"),
        build_mode=BuildMode.NORMAL,
    )


def _built() -> BuildDerivationResponse:
    return BuildDerivationResponse(
        result=BuildResult(
            status=BuildResultStatus.BUILT,
            error_msg="",
            times_built=1,
            is_non_deterministic=0,
            start_time=0,
            stop_time=0,
            built_outputs={},
        )
    )


class Traced:
    def __init__(self, tracing: OtelTracing, exporter: InMemorySpanExporter) -> None:
        self.tracing = tracing
        self.exporter = exporter
        self.queue = BuildQueue()
        self.queue.tracing = tracing

    async def enqueue(self, name: str = "1") -> QueuedBuild:
        build_id, _ = await self.queue.enqueue(_request(name))
        return self.queue.by_id[build_id]

    async def spans(self) -> dict[str, list[ReadableSpan]]:
        await anyio.to_thread.run_sync(lambda: self.tracing.provider.force_flush())
        by_name: dict[str, list[ReadableSpan]] = {}
        for span in self.exporter.get_finished_spans():
            by_name.setdefault(span.name, []).append(span)
        return by_name


@pytest.fixture
async def traced() -> AsyncIterator[Traced]:
    exporter = InMemorySpanExporter()
    tracing = await anyio.to_thread.run_sync(lambda: OtelTracing.start(TracingSettings(), exporter))
    yield Traced(tracing, exporter)
    await anyio.to_thread.run_sync(tracing.close)


def _parent(span: ReadableSpan) -> int | None:
    return int(span.parent.span_id) if span.parent is not None else None


def _id(span: ReadableSpan) -> int:
    if span.context is None:
        raise AssertionError(f"{span.name} has no context")
    return int(span.context.span_id)


def _one(spans: dict[str, list[ReadableSpan]], name: str) -> ReadableSpan:
    [span] = spans[name]
    return span


async def test_placement_traces_each_wait_and_the_attempt(traced: Traced) -> None:
    build = await traced.enqueue()
    local = FakeStore(LOCAL_STORE_ID)
    placer = fake_placer(traced.queue, FixedRanking.of(local), Spawned(), local=local)

    await placer.assign([build], FULL)
    await placer.assign([build], FULL)
    await placer.assign([build], {})
    with build.trace.phase(Phase.BUILD):
        pass
    await traced.queue.complete(build.build_id, _built())

    spans = await traced.spans()
    root, wait, attempt, phase = (_one(spans, n) for n in ("build 1", "wait", "attempt", "build"))
    assert wait.attributes == {"pynixd.reason": "every candidate is at its build limit"}, (
        "a repeated reason is one wait"
    )
    assert attempt.attributes is not None
    assert attempt.attributes["pynixd.store"] == LOCAL_STORE_ID
    assert (_parent(wait), _parent(attempt), _parent(phase)) == (_id(root), _id(root), _id(attempt))
    assert root.attributes is not None
    assert root.attributes["pynixd.ending"] == Ending.BUILT
    assert len({span.context.trace_id for span in (root, wait, attempt, phase) if span.context}) == 1


async def test_a_retry_ends_its_attempt_and_the_next_attempt_is_a_sibling(traced: Traced) -> None:
    build = await traced.enqueue()
    for attempt in range(2):
        build.trace.assigned("sg1-n1", attempt=attempt, cores=8, expected_peak_bytes=None, reason=None)
        with build.trace.phase(Phase.INPUTS):
            pass
        if attempt == 0:
            build.reset_for_retry(LOCAL_STORE_ID, "the builder went away")
    await traced.queue.fail(build.build_id, "it failed")

    spans = await traced.spans()
    root = _one(spans, "build 1")
    first, second = sorted(spans["attempt"], key=lambda span: span.start_time or 0)
    assert first.attributes is not None
    assert first.attributes["pynixd.ending"] == Ending.RETRIED
    assert (first.status.status_code, first.status.description) == (StatusCode.ERROR, "the builder went away")
    assert {_parent(first), _parent(second)} == {_id(root)}
    assert {_parent(span) for span in spans["inputs"]} == {_id(first), _id(second)}
    assert root.status.description == "it failed"


async def test_close_ends_an_open_build_as_stopped(traced: Traced) -> None:
    await traced.enqueue()
    await anyio.to_thread.run_sync(traced.tracing.close)

    [root] = traced.exporter.get_finished_spans()
    assert root.attributes is not None
    assert root.attributes["pynixd.ending"] == Ending.STOPPED


CLIENT_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
"""What a client sends under `open-telemetry`: Determinate's Nix, or the pynixd that delegates the build."""


class Asker:
    """A client as `enqueue` reads it: hashable, with the trace context of its operation."""

    traceparent = CLIENT_TRACEPARENT


async def test_a_build_joins_the_trace_of_the_client_that_asked() -> None:
    exporter = InMemorySpanExporter()
    tracing = await anyio.to_thread.run_sync(lambda: OtelTracing.start(TracingSettings(), exporter))
    traced = Traced(tracing, exporter)
    asker = cast("ClientConn", Asker())
    build_id, _ = await traced.queue.enqueue(_request("1"), askers=[asker])
    await traced.queue.fail(build_id, "x")

    root = _one(await traced.spans(), "build 1")
    await anyio.to_thread.run_sync(tracing.close)
    assert root.context is not None
    assert f"{root.context.trace_id:032x}" == "0af7651916cd43dd8448eb211c80319c"
    assert root.parent is not None
    assert f"{root.parent.span_id:016x}" == "b7ad6b7169203331"


async def test_the_builder_gets_the_span_of_the_build_phase(traced: Traced) -> None:
    """`_OutputClock` sends it on the build connection, so a pynixd builder's trace nests under this phase."""
    build = await traced.enqueue()
    build.trace.assigned("sg1-n1", attempt=0, cores=8, expected_peak_bytes=None, reason=None)
    with build.trace.phase(Phase.BUILD):
        sent = build.trace.traceparent()
    await traced.queue.fail(build.build_id, "x")

    phase = _one(await traced.spans(), "build")
    assert phase.context is not None
    context = phase.context
    assert sent == f"00-{context.trace_id:032x}-{context.span_id:016x}-{context.trace_flags:02x}"
    assert NO_TRACE.traceparent() == ""


class Recording(SpanExporter):
    """An exporter that notes the thread of each export, and can hang like an endpoint that never answers."""

    def __init__(self) -> None:
        self.threads: set[int] = set()
        self.exported = threading.Event()
        self.answer = threading.Event()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        del spans
        self.threads.add(threading.get_ident())
        self.exported.set()
        self.answer.wait()
        return SpanExportResult.SUCCESS


async def test_the_loop_never_exports_and_a_dead_endpoint_holds_exit_for_the_timeout_only() -> None:
    exporter = Recording()
    settings = TracingSettings(shutdown_timeout=0.2, schedule_delay=0.05)
    tracing = await anyio.to_thread.run_sync(lambda: OtelTracing.start(settings, exporter))
    queue = BuildQueue()
    queue.tracing = tracing
    build_id, _ = await queue.enqueue(_request("1"))
    await queue.fail(build_id, "x")
    try:
        await anyio.to_thread.run_sync(lambda: exporter.exported.wait(5))
        assert exporter.threads, "the processor exported nothing"
        assert threading.get_ident() not in exporter.threads

        started = time.monotonic()
        await anyio.to_thread.run_sync(tracing.close)
        assert time.monotonic() - started < 2
    finally:
        exporter.answer.set()


async def test_tracing_is_off_by_default() -> None:
    assert TracingSettings().endpoint is None
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request("1"))
    assert queue.tracing is NO_TRACING
    assert queue.by_id[build_id].trace is NO_TRACE
    assert NO_TRACING.op("BuildPaths", IDENTITY, always=True) is NO_OP_TRACE


IDENTITY = SessionIdentity(session="5e5510n", user="alice", role="user", transport="ssh", pid=None)


async def _ops(settings: TracingSettings) -> tuple[OtelTracing, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    tracing = await anyio.to_thread.run_sync(lambda: OtelTracing.start(settings, exporter))
    return tracing, exporter


async def test_a_build_operation_is_a_span_of_the_session_under_the_client_s_trace() -> None:
    tracing, exporter = await _ops(TracingSettings())
    op = tracing.op("BuildPathsWithResults", IDENTITY, CLIENT_TRACEPARENT, always=True)
    sent = op.traceparent()
    op.ended()
    await anyio.to_thread.run_sync(tracing.close)

    [span] = exporter.get_finished_spans()
    assert span.name == "BuildPathsWithResults"
    assert span.attributes == {
        "pynixd.session": "5e5510n",
        "pynixd.user": "alice",
        "pynixd.role": "user",
        "pynixd.transport": "ssh",
    }
    assert span.parent is not None
    assert f"{span.parent.span_id:016x}" == "b7ad6b7169203331"
    assert span.context is not None
    assert sent.split("-")[2] == f"{span.context.span_id:016x}", "what the operation starts nests under its span"


async def test_another_operation_is_a_span_only_when_slow_or_failed() -> None:
    tracing, exporter = await _ops(TracingSettings(slow_op_seconds=0.05))
    fast = tracing.op("IsValidPath", IDENTITY, CLIENT_TRACEPARENT)
    assert fast.traceparent() == "", "the builds it starts nest under the client's own span"
    fast.ended()
    tracing.op("QueryPathInfo", IDENTITY).ended("path is not valid")
    slow = tracing.op("QueryMissing", IDENTITY, CLIENT_TRACEPARENT)
    started = time.time_ns()
    await anyio.sleep(0.06)
    slow.ended()
    await anyio.to_thread.run_sync(tracing.close)

    by_name = {span.name: span for span in exporter.get_finished_spans()}
    assert set(by_name) == {"QueryPathInfo", "QueryMissing"}
    assert by_name["QueryPathInfo"].status.status_code == StatusCode.ERROR
    missing = by_name["QueryMissing"]
    assert missing.start_time is not None
    assert missing.start_time <= started, "the span starts with the operation, not when it turned slow"
    assert missing.parent is not None
    assert f"{missing.parent.span_id:016x}" == "b7ad6b7169203331"


async def test_a_request_that_joins_a_build_links_to_it(traced: Traced) -> None:
    joiner = cast("ClientConn", Asker())
    build_id, _ = await traced.queue.enqueue(_request("1"))
    await traced.queue.enqueue(_request("1"), askers=[joiner])
    await traced.queue.fail(build_id, "x")

    root = _one(await traced.spans(), "build 1")
    [link] = root.links
    assert f"{link.context.span_id:016x}" == "b7ad6b7169203331"
