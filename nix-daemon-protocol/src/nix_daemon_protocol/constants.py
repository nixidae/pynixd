"""Constants defined by the Nix daemon wire protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterable


def proto(major: int, minor: int) -> int:
    """Encode a daemon protocol version."""
    return (major << 8) | minor


def proto_str(version: int) -> str:
    """Render an encoded daemon protocol version."""
    return f"{version >> 8}.{version & 0xFF}"


WORKER_MAGIC_1: Final[int] = 0x6E697863
WORKER_MAGIC_2: Final[int] = 0x6478696F
PROTOCOL_VERSION: Final[int] = proto(1, 38)
MINIMUM_REMOTE_PROTOCOL: Final[int] = proto(1, 32)
MINIMUM_CLIENT_PROTOCOL: Final[int] = proto(1, 18)
"""`WorkerProto::minimum` (`worker-protocol.cc:35`): an older client is refused at the handshake."""
SUPPORTED_PROTOCOL_VERSIONS: Final[tuple[int, ...]] = tuple(proto(1, minor) for minor in range(32, 39))


def is_supported_protocol(version: int) -> bool:
    """Return whether *version* is in the declared daemon codec interval."""
    return version in SUPPORTED_PROTOCOL_VERSIONS


# ── Protocol features ───────────────────────────────────────────────
#
# **The version number stopped at 1.38, and each new capability is a
# feature.** `worker-protocol.hh:105` of Nix says so: "you generally shouldn't
# change the protocol version number. Define a new
# `WorkerProto::Version::Feature` instead." Nix 2.34, Nix 2.35 and the master
# branch all report 1.38, and they differ by the feature set alone.
#
# Both sides send a set of names, and the negotiated set is the intersection.
# A feature that one side does not name is off, however new that side is.

FEATURE_EXCHANGE_PROTOCOL: Final[int] = proto(1, 38)
"""The protocol version at which both sides exchange a feature set.

`WorkerProto::BasicClientConnection::handshake` at
`worker-protocol-connection.cc:177` guards the exchange on this number, and
the server half at line 200 guards it the same way.
"""

FEATURE_REALISATION_WITH_PATH: Final[str] = "realisation-with-path-not-hash"
"""`DrvOutput` and `UnkeyedRealisation` carry a store path, and not JSON.

`WorkerProto::Serialise<DrvOutput>` of the master branch writes the derivation
path and the output name. `UnkeyedRealisation` writes the output path and the
signatures, and it drops `dependentRealisations`. Both codecs raise when the
feature is off, so a peer that does not name it gets no realisation at all.
"""

FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS: Final[str] = "delete-dead-specific-referrers"
"""`CollectGarbage` may delete the referrers of a named set of paths."""

FEATURE_DISABLE_SET_OPTIONS: Final[str] = "disable-set-options"
"""`SetOptions` is a no-op.

`worker-protocol.hh:140` names recursive Nix, and `builderRpcV0` takes the
same feature: neither surface lets a builder change the settings of the
daemon that serves it.
"""

FEATURE_ADD_TO_STORE_SCANNING: Final[str] = "add-to-store-scanning"
"""The `AddToStoreScanning` operation, code 1001."""

FEATURE_SUBMIT_OUTPUT: Final[str] = "submit-output"
"""The `SubmitOutput` operation, code 1000.

A builder registers one of its own outputs with it. See
`STANDARD_FEATURES` below for the surface that uses it.
"""

FEATURE_ADD_TEMP_ROOTS: Final[str] = "addTempRoots"
"""The `AddTempRoots` operation, code 49.

A client pins a set of temporary roots with one round trip instead of one
`AddTempRoot` per path. The gated shape is a plain store-path set, which
this package already reads.
"""

STANDARD_FEATURES: Final[frozenset[str]] = frozenset(
    {
        FEATURE_REALISATION_WITH_PATH,
        FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS,
        FEATURE_DISABLE_SET_OPTIONS,
        FEATURE_ADD_TO_STORE_SCANNING,
        FEATURE_SUBMIT_OUTPUT,
        FEATURE_ADD_TEMP_ROOTS,
    }
)
"""Each feature name that Nix defines, whether or not a peer offers it.

`WorkerProto::latest` of the master branch offers realisation-with-path,
delete-dead-specific-referrers and addTempRoots. The other three belong to
`WorkerProto::builderRpcV0`. Nix 2.34 offers none of the six.

**`builderRpcV0` is not recursive Nix.** It is the `builder-rpc-v0`
derivation feature of dynamic derivations, and it is a much smaller surface.
A derivation asks for it in `requiredSystemFeatures`, and Nix then gives the
builder a restricted daemon socket and **no output path in the environment**.
The builder registers each output itself, with `SubmitOutput` and
`AddToStoreScanning`. It cannot start a build through that socket, which is
the thing recursive Nix exists for. Every output of such a build is
content-addressed. `docs/notes/reentrancy.md` holds the detail, as Fact 9.
"""

SUPPORTED_STANDARD_FEATURES: Final[frozenset[str]] = frozenset(
    {FEATURE_REALISATION_WITH_PATH, FEATURE_ADD_TEMP_ROOTS, FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS}
)
"""The standard features that this package has a codec for.

A name belongs here when the codec that the name gates is in this package,
and not before: a peer that reads the name then sends the new shape, and a
codec that cannot read it drops the connection. Issue #14.

**This says what the codecs can do, and not what a proxy may claim.** pynixd
speaks to a backend as well as to a client, and it honours a feature when
any store that a build can go to reads the same shape.
`DaemonProxy.honourable_features` unions this set with the features of
those stores, and the handshake with the client uses that wider answer.
Issue #84.

`realisation-with-path-not-hash` covers `DrvOutput`, `UnkeyedRealisation`,
`BuildResult.builtOutputs`, `QueryRealisation` and `RegisterDrvOutput`.
`BuildResult` and `RegisterDrvOutput` carry both shapes as two fields, and
`needs_features` and `unless_features` pick one. `QueryRealisationRequest`
carries one shape alone, so the store boundary fills the missing half
before the forward.
"""


def negotiate_features(peer: Iterable[str], ours: Iterable[str]) -> frozenset[str]:
    """Answer the features that both sides named.

    This is `intersectFeatures` at `worker-protocol-connection.cc:148`. The
    advertisement of one side alone decides nothing.
    """
    return frozenset(peer) & frozenset(ours)


STDERR_NEXT: Final[int] = 0x6F6C6D67
STDERR_LAST: Final[int] = 0x616C7473
STDERR_ERROR: Final[int] = 0x63787470
STDERR_START_ACTIVITY: Final[int] = 0x53545254
STDERR_STOP_ACTIVITY: Final[int] = 0x53544F50
STDERR_RESULT: Final[int] = 0x52534C54
