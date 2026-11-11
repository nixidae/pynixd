"""AddToStore operation — WireRequest/WireResponse types.

The request has a structured header followed by framed NAR data.
Only the header fields are modeled here — the NAR streaming is
handled by the transport layer, not the model.
"""

from __future__ import annotations

from typing import ClassVar

from .constants import FEATURE_PROVENANCE
from .content_address import ContentAddress
from .store_path import StorePath
from .valid_path_info import ValidPathInfo
from .wire_message import WireField
from .wire_ops import WireRequest, WireResponse


class AddToStoreResponse(WireResponse):
    """AddToStore response — ValidPathInfo on success, stderr error on failure."""

    info: ValidPathInfo


class AddToStoreRequest(WireRequest):
    """AddToStore request header — NAR data follows but is not part of the model."""

    op: ClassVar[int] = 7
    response_type = AddToStoreResponse
    path_name: str
    cam: ContentAddress
    references: set[StorePath]
    repair: int
    provenance: str = WireField(default="", needs_features=[FEATURE_PROVENANCE])
