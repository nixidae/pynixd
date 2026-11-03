"""NarInfo — full narinfo metadata from a Nix binary cache.

Richer than ``SubstitutablePathInfo``: captures URL, compression, NAR hash,
signatures, and all other fields needed for NAR downloading and store import.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pynixd_store.store_path import StorePath

if TYPE_CHECKING:
    from nix_daemon_protocol.aliases import StorePathSet
    from nix_daemon_protocol.valid_path_info import ValidPathInfo


@dataclass
class NarInfo:
    """Full metadata for a substitutable store path.

    Parsed from the ``.narinfo`` files served by Nix binary caches.
    Contains everything needed to download and verify a NAR.
    """

    store_path: StorePath
    url: str
    compression: str
    nar_hash: str
    nar_size: int
    references: StorePathSet = field(default_factory=set)
    deriver: StorePath = field(default_factory=lambda: StorePath(""))
    file_hash: str = ""
    file_size: int = 0
    system: str = ""
    ca: str = ""
    sigs: set[str] = field(default_factory=set)

    def to_valid_path_info(self) -> ValidPathInfo:
        """Convert to :class:`ValidPathInfo` for ``AddToStoreNar`` wire protocol."""
        from nix_daemon_protocol.content_address import ContentAddress
        from nix_daemon_protocol.nar_hash import NARHash
        from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
        from nix_daemon_protocol.signature import Signature
        from nix_daemon_protocol.valid_path_info import ValidPathInfo
        from nix_daemon_protocol.wire_time import Time

        from .serde import StorePath

        return ValidPathInfo(
            path=StorePath(path=str(self.store_path)),
            info=UnkeyedValidPathInfo(
                deriver=StorePath(path=str(self.deriver)) if self.deriver else None,
                nar_hash=NARHash(hash=self.nar_hash.removeprefix("sha256:")),
                references={StorePath(path=str(ref)) for ref in self.references},
                registration_time=Time(ts=0),
                nar_size=self.nar_size,
                ultimate=False,
                sigs={Signature(**Signature.from_str(sig)) for sig in self.sigs},
                ca=ContentAddress(value=self.ca),
            ),
        )
