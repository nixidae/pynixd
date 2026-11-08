"""AddMultipleToStore operation — WireRequest/WireResponse types.

The request has a structured header (repair + dont_check_sigs)
followed by framed path data. Only the header fields are modeled
here — the framed data is handled by the transport layer.
"""

from __future__ import annotations

from typing import ClassVar

from .wire_ops import WireRequest, WireResponse


class AddMultipleToStoreResponse(WireResponse):
    """AddMultipleToStore response — empty body, just stderr/WireLogs."""


class AddMultipleToStoreRequest(WireRequest):
    """AddMultipleToStore request header — framed path data follows."""

    op: ClassVar[int] = 44
    response_type = AddMultipleToStoreResponse
    repair: int
    dont_check_sigs: int
