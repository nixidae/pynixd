"""PynixdHostHealth operations: the answering pynixd's host, for a controller.

A pynixd peer answers them, so a controller learns a reverse builder's
pressure, memory, load and free disk with nothing deployed beside pynixd.
`PynixdHostHealth` answers one sample (nixidae/nixidae#1013).
`PynixdHostHealthStream` keeps its session and sends each sample of the
peer's own monitor as it is taken, PSI triggers included
(nixidae/nixidae#1016).
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import TypeAdapter

from nix_daemon_protocol.wire_ops import WireRequest, WireResponse

from ..psi import SystemHealth


class PynixdHostHealthResponse(WireResponse):
    """One sample as JSON: `files` (the text of `SAMPLED_FILES`), `cores`, and `disk`."""

    version: int = 1
    payload: str = ""


class PynixdHostHealthRequest(WireRequest):
    """One sample of the host, and of the filesystem that holds its store."""

    op: ClassVar[int] = 112
    is_extension: ClassVar[bool] = True
    response_type = PynixdHostHealthResponse

    version: int = 1


class PynixdHostHealthStreamRequest(WireRequest):
    """Every sample of the peer's monitor, each as one `STDERR_NEXT` line of JSON.

    The operation does not end: the controller closes the session to stop it.
    A line is a `SystemHealth` (`HEALTH_JSON`).
    """

    op: ClassVar[int] = 113
    is_extension: ClassVar[bool] = True
    response_type = PynixdHostHealthResponse

    version: int = 1
    interval_ms: int = 2000
    """The longest gap the controller accepts between two samples."""


HEALTH_JSON: TypeAdapter[SystemHealth] = TypeAdapter(SystemHealth)
"""The encoding of one streamed sample."""
