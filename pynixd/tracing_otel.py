"""`Tracing` over the OpenTelemetry SDK, sent as OTLP/HTTP. nixidae/nixidae#1570.

The loop only starts and ends spans. An ended span goes into the batch
processor's queue, and the processor's own thread encodes and sends it, so no
export work runs on the loop. The queue is bounded: past `queue_size` the
processor drops spans.

Each span gets its parent as an explicit context, never through
`contextvars`: a build crosses tasks, and a context attached in one task cannot
be detached in another.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from typing import TYPE_CHECKING

import structlog
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from .tracing import BuildTrace, Ending, OpTrace, Phase, Tracing

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from opentelemetry.context import Context
    from opentelemetry.trace import Span, Tracer

    from pynixd_store.build_stats import BuildObservation

    from .build_queue import QueuedBuild
    from .config import TracingSettings
    from .tracing import AttributeValue, SessionIdentity

log = structlog.get_logger(__name__)

_PROPAGATOR = TraceContextTextMapPropagator()
"""W3C trace context: what `open-telemetry` carries on the daemon protocol, as Determinate's Nix sends it."""

# The processor logs a warning for each span it drops, on the thread that ends
# the span: the event loop. A full queue would write one line per span there.
logging.getLogger("opentelemetry.sdk._shared_internal").setLevel(logging.ERROR)


def _context(traceparent: str) -> Context | None:
    return _PROPAGATOR.extract({"traceparent": traceparent}) if traceparent else None


def _traceparent(context: Context) -> str:
    carrier: dict[str, str] = {}
    _PROPAGATOR.inject(carrier, context=context)
    return carrier.get("traceparent", "")


class OtelOpTrace(OpTrace):
    """An operation traced from its start."""

    def __init__(self, tracer: Tracer, name: str, identity: SessionIdentity, parent: str) -> None:
        self._span = tracer.start_span(name, _context(parent), attributes=identity.attributes())
        self._traceparent = _traceparent(trace.set_span_in_context(self._span))

    def traceparent(self) -> str:
        return self._traceparent

    def annotate(self, key: str, value: int | float | str) -> None:
        self._span.set_attribute(key, value)

    def ended(self, error: str | None = None) -> None:
        if error is not None:
            self._span.set_status(Status(StatusCode.ERROR, error))
        self._span.end()


class SlowOpTrace(OpTrace):
    """An operation that becomes a span only when it ends slow or failed, with the time it started."""

    __slots__ = ("_annotations", "_identity", "_monotonic", "_name", "_parent", "_started_ns", "_tracing")

    def __init__(self, tracing: OtelTracing, name: str, identity: SessionIdentity, parent: str) -> None:
        self._tracing = tracing
        self._name = name
        self._identity = identity
        self._parent = parent
        self._started_ns = time.time_ns()
        self._monotonic = time.monotonic()
        self._annotations: dict[str, int | float | str] = {}

    def annotate(self, key: str, value: int | float | str) -> None:
        self._annotations[key] = value

    def ended(self, error: str | None = None) -> None:
        if error is None and time.monotonic() - self._monotonic < self._tracing.slow_op_seconds:
            return
        span = self._tracing.tracer.start_span(
            self._name,
            _context(self._parent),
            attributes={**self._identity.attributes(), **self._annotations},
            start_time=self._started_ns,
        )
        if error is not None:
            span.set_status(Status(StatusCode.ERROR, error))
        span.end()


class OtelBuildTrace(BuildTrace):
    def __init__(self, tracing: OtelTracing, build: QueuedBuild, parent: str = "") -> None:
        self._tracing = tracing
        self._tracer = tracing.tracer
        self._root: Span = self._tracer.start_span(
            f"build {build.request.drv_path.base_name().removesuffix('.drv')}",
            attributes={
                "pynixd.build.id": int(build.build_id),
                "pynixd.drv_path": str(build.request.drv_path),
                "pynixd.system": build.request.derivation.platform,
            },
            context=_context(parent),
        )
        self._root_context = trace.set_span_in_context(self._root)
        self._phase_context: Context | None = None
        self._wait: Span | None = None
        self._wait_reason: str | None = None
        self._attempt: Span | None = None
        self._attempt_context = self._root_context

    def waiting(self, reason: str) -> None:
        if reason == self._wait_reason:
            return
        self._end_wait()
        self._wait_reason = reason
        self._wait = self._tracer.start_span("wait", self._root_context, attributes={"pynixd.reason": reason})

    def assigned(
        self,
        store_id: str,
        *,
        attempt: int,
        cores: int | None,
        expected_peak_bytes: int | None,
        reason: str | None,
    ) -> None:
        self._end_wait()
        self.attempt_ended(Ending.RETRIED, "placed again before the last attempt ended")
        attributes: dict[str, AttributeValue] = {"pynixd.store": store_id, "pynixd.attempt": attempt}
        if cores is not None:
            attributes["pynixd.cores"] = cores
        if expected_peak_bytes is not None:
            attributes["pynixd.expected_peak_bytes"] = expected_peak_bytes
        if reason is not None:
            attributes["pynixd.reason"] = reason
        self._attempt = self._tracer.start_span("attempt", self._root_context, attributes=attributes)
        self._attempt_context = trace.set_span_in_context(self._attempt)

    @contextlib.contextmanager
    def phase(self, phase: Phase) -> Iterator[None]:
        span = self._tracer.start_span(str(phase), self._attempt_context)
        self._phase_context = trace.set_span_in_context(span)
        try:
            yield
        except BaseException as e:
            span.set_status(Status(StatusCode.ERROR, type(e).__name__))
            raise
        finally:
            self._phase_context = None
            span.end()

    def event(self, name: str, attributes: Mapping[str, AttributeValue]) -> None:
        (self._attempt or self._root).add_event(name, dict(attributes))

    def annotate(self, attributes: Mapping[str, AttributeValue]) -> None:
        (self._attempt or self._root).set_attributes(dict(attributes))

    def traceparent(self) -> str:
        return _traceparent(self._phase_context or self._attempt_context)

    def joined(self, parent: str) -> None:
        context = _context(parent)
        if context is None or not self._root.is_recording():
            return
        self._root.add_link(trace.get_current_span(context).get_span_context(), {"pynixd.joined": True})

    def attempt_ended(self, ending: Ending, reason: str | None = None) -> None:
        if self._attempt is None:
            return
        self._attempt.set_attribute("pynixd.ending", str(ending))
        if ending is not Ending.BUILT:
            self._attempt.set_status(Status(StatusCode.ERROR, reason))
        self._attempt.end()
        self._attempt = None
        self._attempt_context = self._root_context

    def ended(
        self,
        ending: Ending,
        *,
        status: int | None = None,
        error: str | None = None,
        observation: BuildObservation | None = None,
        decision_id: int | None = None,
    ) -> None:
        if not self._root.is_recording():
            return
        self._end_wait()
        if self._attempt is not None and observation is not None:
            for key, value in (
                ("pynixd.inputs_bytes", observation.inputs_bytes),
                ("pynixd.pull_bytes", observation.pull_bytes),
                ("pynixd.peak_memory_bytes", observation.peak_memory_bytes),
                ("pynixd.oom_kills", observation.oom_kills),
            ):
                if value is not None:
                    self._attempt.set_attribute(key, value)
        self.attempt_ended(ending, error)
        self._root.set_attribute("pynixd.ending", str(ending))
        if status is not None:
            self._root.set_attribute("pynixd.status", status)
        if decision_id is not None:
            self._root.set_attribute("pynixd.placement_decision", decision_id)
        if ending is not Ending.BUILT:
            self._root.set_status(Status(StatusCode.ERROR, error))
        self._root.end()
        self._tracing.open.discard(self)

    def _end_wait(self) -> None:
        if self._wait is not None:
            self._wait.end()
        self._wait = None
        self._wait_reason = None


class OtelTracing(Tracing):
    def __init__(self, provider: TracerProvider, shutdown_timeout: float, slow_op_seconds: float) -> None:
        self.provider = provider
        self.tracer: Tracer = provider.get_tracer("pynixd")
        self.shutdown_timeout = shutdown_timeout
        self.slow_op_seconds = slow_op_seconds
        self.open: set[OtelBuildTrace] = set()

    @classmethod
    def start(cls, settings: TracingSettings, exporter: SpanExporter | None = None) -> OtelTracing:
        """A provider that sends to `settings.endpoint`, or to *exporter*. Reads files: run it in a thread."""
        if exporter is None:
            headers = {}
            if settings.bearer_token_file is not None:
                headers["Authorization"] = f"Bearer {settings.bearer_token_file.read_text().strip()}"
            exporter = OTLPSpanExporter(endpoint=settings.endpoint, headers=headers, timeout=settings.export_timeout)
        provider = TracerProvider(
            resource=Resource.create({"service.name": settings.service_name}),
            shutdown_on_exit=False,
        )
        provider.add_span_processor(
            BatchSpanProcessor(
                exporter,
                max_queue_size=settings.queue_size,
                schedule_delay_millis=settings.schedule_delay * 1000,
                export_timeout_millis=settings.export_timeout * 1000,
            )
        )
        tracing = cls(provider, settings.shutdown_timeout, settings.slow_op_seconds)
        # The SDK's first span imports and compiles what the loop must not. A provider with no processor sends
        # nothing.
        warmup = TracerProvider(shutdown_on_exit=False).get_tracer("pynixd").start_span("warmup")
        _PROPAGATOR.inject({}, context=_PROPAGATOR.extract({"traceparent": ""}))
        warmup.end()
        log.info("tracing_started", endpoint=settings.endpoint, service=settings.service_name)
        return tracing

    def build(self, build: QueuedBuild, parent: str = "") -> BuildTrace:
        trace_ = OtelBuildTrace(self, build, parent)
        self.open.add(trace_)
        return trace_

    def op(self, name: str, identity: SessionIdentity, parent: str = "", *, always: bool = False) -> OpTrace:
        if always:
            return OtelOpTrace(self.tracer, name, identity, parent)
        return SlowOpTrace(self, name, identity, parent)

    def close(self) -> None:
        for open_trace in list(self.open):
            open_trace.ended(Ending.STOPPED, error="pynixd stopped")
        # The processor's shutdown takes no timeout and waits up to 30 s on an endpoint that does not answer. A
        # daemon thread runs it, so an exit waits `shutdown_timeout` at most and leaves it behind.
        shutdown = threading.Thread(target=self.provider.shutdown, name="pynixd-tracing-shutdown", daemon=True)
        shutdown.start()
        shutdown.join(self.shutdown_timeout)
        if shutdown.is_alive():
            log.warning("tracing_shutdown_timed_out", seconds=self.shutdown_timeout)
