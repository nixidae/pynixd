"""A path info as the store that receives a copy registers it.

Not in `transfer.py`: that module imports `LocalDBStore` (through
`NarReader`), which imports the write router, which imports this.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nix_daemon_protocol.valid_path_info import ValidPathInfo


def as_copied(info: ValidPathInfo) -> ValidPathInfo:
    """*info* as a destination registers a copy: `ultimate` cleared (`daemon.cc:516`)."""
    if not info.info.ultimate:
        return info
    return info.model_copy(update={"info": info.info.model_copy(update={"ultimate": False})})
