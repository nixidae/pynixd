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
WORKER_MAGIC_ACCESS_DENIED: Final[int] = 0xAB9A9FF0
"""Determinate Nix's daemon sends it in place of `WORKER_MAGIC_2` to a peer that `authPeer` refuses
(`src/nix/unix/daemon.cc:384` of v3.24.0). Its client then names `allowed-users`."""
PROTOCOL_VERSION: Final[int] = proto(1, 38)
MINIMUM_PROTOCOL: Final[int] = proto(1, 35)
"""The oldest protocol of a client, a store and a builder: Lix speaks 1.35, nixbuild.net 1.35 (probed 2026-11-08).

NIX-DEVIATION (#27): `WorkerProto::minimum` (`worker-protocol.cc:35`) admits
a client from 1.18, and `daemon.cc:1048` refuses an older one. pynixd refuses
a client below 1.35 there, in the same words. Below 1.25 a client sends
AddToStore and AddToStoreNar unframed (`daemon.cc:406`, `daemon.cc:931`), and
no handler here reads that. Reverse it when a supported client speaks less.
"""
SUPPORTED_PROTOCOL_VERSIONS: Final[tuple[int, ...]] = tuple(proto(1, minor) for minor in range(35, 39))


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

FEATURE_QUERY_PATH_INFOS: Final[str] = "queryPathInfos"
"""The `QueryPathInfos` operation, code 50, of Determinate Nix (`worker-protocol.hh:128` of v3.24.0).

Upstream Nix reserves the code for it. The gated shape is a store-path set in, and a list of
`ValidPathInfo` out, which this package already reads.
"""

FEATURE_QUERY_ACTIVE_BUILDS: Final[str] = "queryActiveBuilds"
"""The `QueryActiveBuilds` operation, code 48, of Determinate Nix (`worker-protocol.hh:124` of v3.24.0).

It gates no shape that a proxy forwards: the answer is one JSON string. So it is not in
`SUPPORTED_STANDARD_FEATURES`, and a proxy names it only when its own store can answer.
"""

FEATURE_NAMED_ACTIVITIES: Final[str] = "named-activities"
"""`STDERR_START_ACTIVITY_STRINGLY`: an activity named by a string, with key and field pairs (Determinate Nix,
`worker-protocol.hh:158` of v3.24.0). A peer without it gets `actStringly` and the text alone (`logging.hh:225`).
"""

FEATURE_OPEN_TELEMETRY: Final[str] = "open-telemetry"
"""A W3C `traceparent` string after the handshake and after each op code, empty when the client is not tracing
(Determinate Nix, `worker-protocol.hh:151` of v3.24.0; `daemon.cc:1175` and `:1225`).
"""

FEATURE_PROVENANCE: Final[str] = "provenance"
"""A JSON string, empty for none, after the content address of each path info, and in `AddToStore` after `repair`
(Determinate Nix, `worker-protocol.cc:390`, `daemon.cc:452` of v3.24.0). Its daemon names it only with the
`provenance` experimental feature (`daemon.cc:1165`). A proxy names it only when its own store keeps the field.
"""

FEATURE_VERSIONED_ADD_TO_STORE_MULTIPLE: Final[str] = "versionedAddToStoreMultiple"
"""Each path info of an `AddMultipleToStore` stream takes the negotiated version and features, and not protocol 1.16
with none (Determinate Nix, `daemon.cc:552` and `remote-store.cc:589` of v3.24.0). Without it the stream carries no
provenance.
"""

FEATURE_PYNIXD_BUILD_RESOURCES: Final[str] = "pynixd-build-resources-2"
"""`BuildResult.resources`: what the build used (`build_resources.py`), after Nix's fields.

pynixd's own feature, not Nix's, so it is not in `STANDARD_FEATURES`. Two pynixd peers name it. The number
changes with the record's fields: a peer that reads another field set misreads every byte after it.
"""

PYNIXD_FEATURES: Final[frozenset[str]] = frozenset({FEATURE_PYNIXD_BUILD_RESOURCES})
"""pynixd's own features that this package has a codec for. A pynixd names them in both directions, and Nix never does."""

STANDARD_FEATURES: Final[frozenset[str]] = frozenset(
    {
        FEATURE_REALISATION_WITH_PATH,
        FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS,
        FEATURE_DISABLE_SET_OPTIONS,
        FEATURE_ADD_TO_STORE_SCANNING,
        FEATURE_SUBMIT_OUTPUT,
        FEATURE_ADD_TEMP_ROOTS,
        FEATURE_QUERY_PATH_INFOS,
        FEATURE_QUERY_ACTIVE_BUILDS,
        FEATURE_NAMED_ACTIVITIES,
        FEATURE_OPEN_TELEMETRY,
        FEATURE_PROVENANCE,
        FEATURE_VERSIONED_ADD_TO_STORE_MULTIPLE,
    }
)
"""Each feature name that Nix or Determinate Nix defines, whether or not a peer offers it.

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
    {
        FEATURE_REALISATION_WITH_PATH,
        FEATURE_ADD_TEMP_ROOTS,
        FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS,
        FEATURE_QUERY_PATH_INFOS,
        FEATURE_NAMED_ACTIVITIES,
        FEATURE_OPEN_TELEMETRY,
        FEATURE_PROVENANCE,
        FEATURE_VERSIONED_ADD_TO_STORE_MULTIPLE,
    }
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
STDERR_START_ACTIVITY_STRINGLY: Final[int] = 0x5354524E
"""Determinate Nix's named activity (`worker-protocol.hh:31` of v3.24.0), behind `named-activities`."""
STDERR_STOP_ACTIVITY: Final[int] = 0x53544F50
STDERR_RESULT: Final[int] = 0x52534C54
