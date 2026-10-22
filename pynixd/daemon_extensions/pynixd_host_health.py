"""PynixdHostHealth operation: one sample of the answering pynixd's host.

A pynixd peer answers it, so a controller learns a reverse builder's
pressure, memory, load and free disk with nothing deployed beside pynixd.
nixidae/nixidae#1013.
"""

from __future__ import annotations

from typing import ClassVar

from nix_daemon_protocol.wire_ops import WireRequest, WireResponse


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
