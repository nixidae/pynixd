"""AddToStoreScanning (op 1001): AddToStore whose references the daemon finds by scanning (`daemon.cc:1081`).

A framed dump follows the header, as for AddToStore; the transport reads it.
"""

from __future__ import annotations

from typing import ClassVar

from .constants import FEATURE_EXCHANGE_PROTOCOL
from .content_address import ContentAddress
from .valid_path_info import ValidPathInfo
from .wire_ops import WireRequest, WireResponse


class AddToStoreScanningResponse(WireResponse):
    info: ValidPathInfo


class AddToStoreScanningRequest(WireRequest):
    op: ClassVar[int] = 1001
    response_type = AddToStoreScanningResponse
    min_protocol: ClassVar[int] = FEATURE_EXCHANGE_PROTOCOL
    path_name: str
    cam: ContentAddress
