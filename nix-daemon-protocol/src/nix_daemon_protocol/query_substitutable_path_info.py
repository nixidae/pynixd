"""QuerySubstitutablePathInfo operation — WireRequest/WireResponse types.

This is the singular form of `QuerySubstitutablePathInfos`. A store that uses
a daemon as one of its substituters asks it, at `daemon.cc:851`. The response
carries no path, because the request named one.

pynixd knew no codec for it, and an unknown operation desynced the wire.
Issue nixidae/nixidae#232 holds that fault and the rule that replaced it.
"""

from __future__ import annotations

from typing import ClassVar

from .store_path import StorePath
from .wire_message import WireField, WireModel
from .wire_ops import WireRequest, WireResponse


class UnkeyedSubstitutablePathInfo(WireModel):
    """What one substituter knows about the path that the request named.

    Wire order follows `daemon.cc:862`: the deriver, the references, the
    download size and the NAR size.
    """

    deriver: StorePath | None = WireField(default=None)
    references: set[StorePath] = WireField(default_factory=set)
    download_size: int = WireField(default=0)
    nar_size: int = WireField(default=0)


class QuerySubstitutablePathInfoResponse(WireResponse):
    """QuerySubstitutablePathInfo response — the info follows the found flag."""

    found: bool = WireField(default=False)
    info: UnkeyedSubstitutablePathInfo | None = WireField(
        default=None,
        wire_depends_on=lambda self: self.found,
    )


class QuerySubstitutablePathInfoRequest(WireRequest):
    """QuerySubstitutablePathInfo request — one StorePath."""

    op: ClassVar[int] = 21
    response_type = QuerySubstitutablePathInfoResponse
    path: StorePath
