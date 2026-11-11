"""QueryPathInfos operation, code 50: the batch `QueryPathInfo` of Determinate Nix."""

from __future__ import annotations

from typing import ClassVar

from .constants import proto
from .store_path import StorePath
from .valid_path_info import ValidPathInfo
from .wire_message import WireField
from .wire_ops import WireRequest, WireResponse


class QueryPathInfosResponse(WireResponse):
    """A `ValidPathInfo` for each valid path. An invalid path has no entry (`daemon.cc:958` of Determinate v3.24.0)."""

    infos: list[ValidPathInfo] = WireField(default_factory=list)


class QueryPathInfosRequest(WireRequest):
    """A set of store paths. A client sends it only when the `queryPathInfos` feature was negotiated."""

    op: ClassVar[int] = 50
    min_protocol: ClassVar[int] = proto(1, 38)
    response_type = QueryPathInfosResponse
    paths: set[StorePath] = WireField(default_factory=set)
