"""The life of each build as trace spans. nixidae/nixidae#1570.

`QueuedBuild.trace` is a `BuildTrace`, and the scheduler, the placer and the
executor tell it what happens to the build. This module holds the interface and
the default, which records nothing. `tracing_otel` sends the spans over OTLP when
`tracing.endpoint` is set; `Server.start` loads it through `_optional`.

One build is one trace: a root span from enqueue to its answer, a `wait` span for
each reason it waits, an `attempt` span for each store it runs on, and the phases
of an attempt under it. OpenTelemetry sends a span only when it ends, so a build
that hangs shows its last finished wait or phase, not the root.

A client operation is a span too, with the session's `SessionIdentity`. A build or
transfer operation always gets one, started with the operation, and the builds it
enqueues are its children. Any other operation gets one only when it ends slower
than `tracing.slow_op_seconds` or with an error: a session sends thousands of
queries, and each span costs the loop about 35 us.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from pynixd_store.build_stats import BuildObservation

    from .build_queue import QueuedBuild

type AttributeValue = str | int | float | bool


class Phase(StrEnum):
    INPUTS = "inputs"
    """The inputs go to the store that builds."""
    BUILD = "build"
    OUTPUTS = "outputs"
    """The outputs come back to the local store."""


class Ending(StrEnum):
    BUILT = "built"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRIED = "retried"
    """An attempt that gave the build back to placement."""
    STOPPED = "stopped"
    """pynixd stopped with the build still open."""


@dataclass(frozen=True, slots=True)
class SessionIdentity:
    """Who a client session is. Each operation span of the session carries it."""

    session: str
    """Unique to the session, across restarts and hosts."""
    user: str
    role: str
    transport: str
    pid: int | None
    """The client's process, where the transport knows it."""

    def attributes(self) -> dict[str, AttributeValue]:
        attributes: dict[str, AttributeValue] = {
            "pynixd.session": self.session,
            "pynixd.user": self.user,
            "pynixd.role": self.role,
            "pynixd.transport": self.transport,
        }
        if self.pid is not None:
            attributes["pynixd.client_pid"] = self.pid
        return attributes


class OpTrace:
    """One client operation. This one records nothing."""

    __slots__ = ()

    def traceparent(self) -> str:
        """The W3C `traceparent` of the operation's span, for what it starts; empty when it has none yet."""
        return ""

    def annotate(self, key: str, value: int | float | str) -> None:
        """Record *key* on the operation's span: what it moved, to read beside its time."""

    def ended(self, error: str | None = None) -> None:
        """The operation answered, with *error* when it failed."""


NO_OP_TRACE = OpTrace()


class BuildTrace:
    """What happens to one build. This one records nothing."""

    def waiting(self, reason: str) -> None:
        """The build waits for *reason*. A repeat of the same reason changes nothing."""

    def assigned(
        self,
        store_id: str,
        *,
        attempt: int,
        cores: int | None,
        expected_peak_bytes: int | None,
        reason: str | None,
    ) -> None:
        """Placement started an attempt on *store_id*."""

    @contextlib.contextmanager
    def phase(self, phase: Phase) -> Iterator[None]:
        """One phase of the running attempt."""
        yield

    def event(self, name: str, attributes: Mapping[str, AttributeValue]) -> None:
        """A point in time on the running attempt, or on the build when none runs."""

    def annotate(self, attributes: Mapping[str, AttributeValue]) -> None:
        """Attributes of the running attempt, or of the build when none runs."""

    def traceparent(self) -> str:
        """The W3C `traceparent` of the innermost open span, for the store that builds; empty when none."""
        return ""

    def joined(self, parent: str) -> None:
        """Another request joined the build; *parent* is the `traceparent` of the client that sent it."""

    def attempt_ended(self, ending: Ending, reason: str | None = None) -> None:
        """The running attempt ended; the build itself goes on or ends next."""

    def ended(
        self,
        ending: Ending,
        *,
        status: int | None = None,
        error: str | None = None,
        observation: BuildObservation | None = None,
        decision_id: int | None = None,
    ) -> None:
        """The build has its answer. Ends every span still open."""


NO_TRACE = BuildTrace()


class Tracing:
    """Makes the trace of each build. This one makes `NO_TRACE`."""

    def build(self, build: QueuedBuild, parent: str = "") -> BuildTrace:
        """The trace of *build*, under *parent*, the `traceparent` of the client that asked for it."""
        del build, parent
        return NO_TRACE

    def op(self, name: str, identity: SessionIdentity, parent: str = "", *, always: bool = False) -> OpTrace:
        """The trace of operation *name* of *identity*'s session, under the client's *parent*.

        *always* traces it from its start; otherwise only an operation that ends slow or failed is traced.
        """
        del name, identity, parent, always
        return NO_OP_TRACE

    def close(self) -> None:
        """End the open traces and send what is left. Blocks: run it in a thread."""


NO_TRACING = Tracing()
