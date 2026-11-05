"""SubmitOutput (op 1000): a `builder-rpc-v0` builder names a store object as one of its outputs (`daemon.cc:1113`)."""

from __future__ import annotations

from typing import ClassVar

from .constants import FEATURE_EXCHANGE_PROTOCOL
from .single_derived_path import SingleDerivedPath
from .wire_ops import WireRequest, WireResponse


class SubmitOutputResponse(WireResponse):
    """A single uint64, always 1."""

    value: int


class SubmitOutputRequest(WireRequest):
    op: ClassVar[int] = 1000
    response_type = SubmitOutputResponse
    min_protocol: ClassVar[int] = FEATURE_EXCHANGE_PROTOCOL
    path: SingleDerivedPath
    output: str
