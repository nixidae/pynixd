"""Transport-neutral contexts used by daemon message codecs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol

from .exceptions import DaemonProtocolError

if TYPE_CHECKING:
    from collections.abc import Callable

    from .io import NixReader, NixWriter
    from .logging import ProtocolLogger
    from .logs import LogMessage


class LogSink(Protocol):
    """Optional real-time receiver for decoded daemon log records.

    **`LogMessage` and not `object`.** A parameter is contravariant, so a
    sink declared to take `object` can only be satisfied by something that
    accepts anything. `ClientConn.send` takes a `WireModel`, which is
    narrower, so it did not satisfy this and every construction of a
    `ReadContext` from a client read as a type error.

    `LogMessage` is what is actually passed: `read_stream` yields one and
    `logs.py:198` is the only caller. A `WireModel` is wider than that, so
    `ClientConn` satisfies it.
    """

    async def send(self, message: LogMessage, /) -> None: ...  # type: ignore[valid-type, unused-ignore]  # zuban-bug: about 1 run in 25 reads the TYPE_CHECKING import as invalid; mypy accepts it


_NO_FEATURES: Final[frozenset[str]] = frozenset()
"""The feature set of a peer that named none, which is every peer below 1.38.

It is also what Nix 2.34 names at 1.38, so it is the ordinary case and not a
fallback. `WireField` reads it, and `nix_daemon_protocol.constants` gives the
whole rule.
"""


@dataclass(frozen=True)
class ReadContext:
    """Bundles the arguments needed to deserialize a response from the wire."""

    reader: NixReader
    version: int
    log_sink: LogSink | None = None
    buffer_logs: bool = True
    raise_on_error: bool = True
    error_factory: Callable[[str], Exception] = DaemonProtocolError
    logger: ProtocolLogger | None = None
    features: frozenset[str] = _NO_FEATURES
    """The features that the two peers negotiated, and not the ones one named.

    `intersectFeatures` at `worker-protocol-connection.cc:148` of Nix builds
    it. A field with `needs_features` or `unless_features` reads this set.
    Issue #14.
    """


@dataclass(frozen=True)
class WriteContext:
    """Bundles the arguments needed to serialize a request/response to the wire."""

    writer: NixWriter
    version: int
    features: frozenset[str] = _NO_FEATURES
    """The features that the two peers negotiated. See `ReadContext.features`."""
    traceparent: str = ""
    """What a request carries after its op code under `open-telemetry`: the client's, or empty."""
