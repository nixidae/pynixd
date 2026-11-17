"""What a provisioner asks of a cloud. nixidae/nixidae#1198.

A provider starts, lists and ends machines; it knows nothing of builds.
Every machine carries the tags it was launched with, so a provisioner that
restarts finds its machines by tag, even one whose launch it never saw
answered.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Mapping


class MachineState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    ENDED = "ended"
    """Terminated, or failed to start: it costs nothing and builds nothing."""


@dataclass(frozen=True, slots=True)
class Machine:
    provider_id: str
    state: MachineState
    tags: Mapping[str, str] = field(default_factory=dict)
    notice: float | None = None
    """When an interruption ends it, in seconds since the epoch."""
    error: str | None = None


class ProviderError(RuntimeError):
    """The provider failed a call; the provisioner tries again later."""


class NoCapacity(ProviderError):
    """The provider has no machine of this kind to give now."""


class NotFound(ProviderError):
    """The provider knows no such machine or template."""


class Provider(Protocol):
    name: str
    """Recorded with each instance, so a row names who to ask about it."""

    async def launch(self, template: str, tags: Mapping[str, str], user_data: str) -> Machine:
        """Start one machine of *template*; `NoCapacity` when none is free."""
        ...

    async def terminate(self, provider_id: str) -> None:
        """End *provider_id*; ending one already ended is no error."""
        ...

    async def machines(self, tags: Mapping[str, str]) -> list[Machine]:
        """Every machine that carries all of *tags*, ended ones included."""
        ...

    async def close(self) -> None: ...
