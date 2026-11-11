"""AddMultipleToStore operation — WireRequest/WireResponse types.

The request has a structured header (repair + dont_check_sigs)
followed by framed path data. Only the header fields are modeled
here — the framed data is handled by the transport layer.
"""

from __future__ import annotations

from typing import ClassVar

from .constants import FEATURE_VERSIONED_ADD_TO_STORE_MULTIPLE, proto
from .wire_ops import WireRequest, WireResponse

UNVERSIONED_PATH_INFO = proto(1, 16)
"""The version of each path info in the stream of a peer without `versionedAddToStoreMultiple` (`daemon.cc:554`)."""


def path_info_version(version: int, features: frozenset[str]) -> tuple[int, frozenset[str]]:
    """The version and features that each path info of the stream takes, for a connection of *version* and *features*."""
    if FEATURE_VERSIONED_ADD_TO_STORE_MULTIPLE in features:
        return version, features
    return UNVERSIONED_PATH_INFO, frozenset()


class AddMultipleToStoreResponse(WireResponse):
    """AddMultipleToStore response — empty body, just stderr/WireLogs."""


class AddMultipleToStoreRequest(WireRequest):
    """AddMultipleToStore request header — framed path data follows."""

    op: ClassVar[int] = 44
    response_type = AddMultipleToStoreResponse
    repair: int
    dont_check_sigs: int
