"""QueryActiveBuilds operation, code 48: the builds in progress, of Determinate Nix."""

from __future__ import annotations

from typing import ClassVar

from .constants import proto
from .wire_ops import WireRequest, WireResponse


class QueryActiveBuildsResponse(WireResponse):
    """A JSON list of `ActiveBuildInfo` (`active-builds.cc:160` of Determinate v3.24.0), as one string."""

    builds: str


class QueryActiveBuildsRequest(WireRequest):
    """No body. A client sends it only when the `queryActiveBuilds` feature was negotiated (`remote-store.cc:909`)."""

    op: ClassVar[int] = 48
    min_protocol: ClassVar[int] = proto(1, 38)
    response_type = QueryActiveBuildsResponse
