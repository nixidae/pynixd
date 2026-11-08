"""QueryValidPaths operation — WireRequest/WireResponse types."""

from __future__ import annotations

from typing import ClassVar

from .store_path import StorePath
from .wire_message import WireField
from .wire_ops import WireRequest, WireResponse


class QueryValidPathsResponse(WireResponse):
    """QueryValidPaths response — set of valid StorePaths."""

    paths: set[StorePath] = WireField(default_factory=set)


class QueryValidPathsRequest(WireRequest):
    """QueryValidPaths request — set of paths + version-gated substitute flag."""

    op: ClassVar[int] = 31
    response_type = QueryValidPathsResponse
    paths: set[StorePath] = WireField(default_factory=set)
    substitute: int | None = WireField(default=None)
