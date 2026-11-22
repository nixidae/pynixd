"""The life of each build as trace spans. nixidae/nixidae#1570.

`QueuedBuild.trace` is a `BuildTrace`, and the scheduler, the placer and the
executor tell it what happens to the build. This module holds the interface and
the default, which records nothing. `tracing_otel` sends the spans over OTLP when
`tracing.endpoint` is set; `Server.start` loads it through `_optional`.

One build is one trace: a root span from enqueue to its answer, a `wait` span for
each reason it waits, an `attempt` span for each store it runs on, and the phases
of an attempt under it. OpenTelemetry sends a span only when it ends, so a build
that hangs shows its last finished wait or phase, not the root.
"""

from __future__ import annotations

import contextlib
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

    def traceparent(self) -> str:
        """The W3C `traceparent` of the innermost open span, for the store that builds; empty when none."""
        return ""

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

    def close(self) -> None:
        """End the open traces and send what is left. Blocks: run it in a thread."""


NO_TRACING = Tracing()
