"""UnkeyedValidPathInfo — store path metadata without the path key."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .constants import FEATURE_PROVENANCE
from .content_address import ContentAddress

if TYPE_CHECKING:
    from .valid_path_info import ValidPathInfo
from .nar_hash import NARHash
from .signature import Signature
from .store_path import StorePath
from .wire_message import WireField, WireModel
from .wire_time import Time


class UnkeyedValidPathInfo(WireModel):
    """Wire mirror of UnkeyedValidPathInfo."""

    deriver: StorePath | None = WireField(default=None)
    nar_hash: NARHash
    references: set[StorePath]
    registration_time: Time
    nar_size: int
    ultimate: bool
    sigs: set[Signature]
    ca: ContentAddress
    provenance: str = WireField(default="", needs_features=[FEATURE_PROVENANCE])

    def to_valid_path_info(self, path: StorePath) -> ValidPathInfo:
        """Wrap this UnkeyedValidPathInfo with a StorePath into ValidPathInfo."""
        from .valid_path_info import ValidPathInfo  # lazy to avoid circular

        return ValidPathInfo(path=path, info=self)
