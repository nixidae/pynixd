"""AddBuildLog operation — WireRequest/WireResponse types."""

from __future__ import annotations

from typing import ClassVar

from .constants import proto
from .wire_ops import WireRequest, WireResponse


class AddBuildLogResponse(WireResponse):
    """AddBuildLog response — single uint64 value."""

    value: int


class AddBuildLogRequest(WireRequest):
    """AddBuildLog request: the derivation's base name, then a framed log.

    Both ends spell the path as `StorePath::to_string`, the base name
    (`remote-store.cc:787`, `daemon.cc:1013`), not the whole path a
    `StorePath` field writes. The log follows in frames, outside the model.
    """

    op: ClassVar[int] = 45
    min_protocol: ClassVar[int] = proto(1, 32)
    response_type = AddBuildLogResponse
    path: str
