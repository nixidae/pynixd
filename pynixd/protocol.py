"""
Nix daemon protocol types and operation codes.
Protocol versions 1.32+ are supported (negotiated per connection).
"""

from nix_daemon_protocol.wire_ops import WIRE_REGISTRY

from . import serde as serde

DELEGATION_FEATURE = "pynixd-delegation"
"""The feature a pynixd names when it connects to a store as a client.

The store, when it is a pynixd, then knows that the builds of the session
are delegated, which its loop guard reads: `DelegationLoopError`. Nix keeps
only the features it knows, so a plain daemon ignores the name.
"""


def get_extension_features() -> set[str]:
    """Return all pynixd extension operation names."""
    return {cls.name for cls in WIRE_REGISTRY.values() if cls.is_extension}
