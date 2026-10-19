"""BuildPaths operation — WireRequest/WireResponse types."""

from __future__ import annotations

from typing import ClassVar

from .derived_path import DerivedPath
from .wire_message import WireField
from .wire_ops import WireRequest, WireResponse


class BuildPathsResponse(WireResponse):
    """BuildPaths response — single uint64 value."""

    value: int


class BuildPathsRequest(WireRequest):
    """BuildPaths request — the derived paths in order, and the build mode.

    **A list, and not a set.** `DerivedPaths` of Nix is a
    `std::vector<DerivedPath>`, and `Store::buildPaths` keeps the order of it.
    `BuildPathsWithResults` then answers one result for each request, in the
    same order, and a client reads the answers by position. A set also drops a
    repeated path, and Nix answers one result for each one. Issue nixidae/nixidae#219.
    """

    op: ClassVar[int] = 9
    response_type = BuildPathsResponse
    forward: ClassVar[bool] = False
    derived_paths: list[DerivedPath] = WireField(default_factory=list)
    build_mode: int
