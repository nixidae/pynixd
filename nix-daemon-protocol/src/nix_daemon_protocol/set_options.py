"""SetOptions operation — WireRequest/WireResponse types."""

from __future__ import annotations

from typing import ClassVar

from .protocol import Verbosity
from .wire_message import WireField
from .wire_ops import WireRequest, WireResponse
from .wire_time import TimeSpan


class SetOptionsResponse(WireResponse):
    """SetOptions response — no body fields, just stderr/WireLogs."""


class SetOptionsRequest(WireRequest):
    """SetOptions request — 12 uint64 fields + version-gated overrides dict."""

    op: ClassVar[int] = 19
    response_type = SetOptionsResponse
    keep_failed: int
    keep_going: int
    try_fallback: int
    verbosity: Verbosity
    max_build_jobs: int
    max_silent_time: TimeSpan
    obsolete_use_build_hook: int
    build_verbosity: Verbosity
    obsolete_log_type: int
    obsolete_print_build_trace: int
    build_cores: int
    use_substitutes: int
    overrides: dict[str, str] = WireField(default_factory=dict)
