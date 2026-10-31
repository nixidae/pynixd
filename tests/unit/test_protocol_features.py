"""The feature set of the handshake, and the gap that is left in it.

**The protocol number stopped at 1.38.** Nix 2.34, Nix 2.35 and the master
branch all report 1.38, and each new capability is a feature name instead.
`worker-protocol.hh:105` states the rule. So "which version of the protocol
does pynixd speak" is answered by the feature set, and not by the number.

The negotiated set is the intersection of the two sides,
`intersectFeatures` at `worker-protocol-connection.cc:148`. A peer that names
a feature gets it only when pynixd names it back. Issue #14.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import nix_daemon_protocol as ndp
from pynixd import wire
from pynixd.proxy import DaemonProxy

FEATURES_OF_NIX_LATEST = {
    ndp.FEATURE_REALISATION_WITH_PATH,
    ndp.FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS,
    ndp.FEATURE_ADD_TEMP_ROOTS,
}
"""What `WorkerProto::latest` of the master branch offers.

`worker-protocol.cc` builds it from these three names. A `nix` client and a
`nix-daemon` of that branch each send this set.
"""

FEATURES_OF_BUILDER_RPC_V0 = {
    ndp.FEATURE_REALISATION_WITH_PATH,
    ndp.FEATURE_DISABLE_SET_OPTIONS,
    ndp.FEATURE_ADD_TO_STORE_SCANNING,
    ndp.FEATURE_SUBMIT_OUTPUT,
}
"""What `WorkerProto::builderRpcV0` offers.

That set is frozen, because a change to it would be visible to a derivation.

**It is not recursive Nix.** `builder-rpc-v0` is a derivation feature of
dynamic derivations, and it is a much smaller surface. A derivation names it
in `requiredSystemFeatures`, and Nix then gives the builder a restricted
daemon socket and no output path in the environment. The builder registers
each output itself, with `SubmitOutput` and `AddToStoreScanning`, and it
cannot start a build through that socket. `docs/notes/reentrancy.md` holds
the detail, as Fact 9.
"""

MISSING_CODECS = {
    ndp.FEATURE_DISABLE_SET_OPTIONS: (
        "`SetOptions` must become a no-op. A builder cannot change the settings of the daemon that serves it."
    ),
    ndp.FEATURE_ADD_TO_STORE_SCANNING: (
        "The `AddToStoreScanning` operation, code 1001, has no codec. A builder adds a store object with it, and "
        "Nix scans that object for references."
    ),
    ndp.FEATURE_SUBMIT_OUTPUT: (
        "The `SubmitOutput` operation, code 1000, has no codec. A builder registers one of its own outputs with it."
    ),
}
"""Each standard feature that pynixd does not claim, and the codec it needs.

**This is the whole of the wire gap above Nix 2.34.** Move a name out of here
when its codec lands, and add it to `SUPPORTED_STANDARD_FEATURES` in the same
change. The two must move together: a peer that reads the name then sends the
new shape, and a codec that cannot read that shape drops the connection.
"""


def test_the_number_is_the_one_that_nix_froze() -> None:
    """Nix 2.34 and the master branch both report 1.38."""
    assert wire.proto(1, 38) == wire.PROTOCOL_VERSION
    assert wire.proto(1, 38) == wire.FEATURE_EXCHANGE_PROTOCOL


def test_the_names_of_nix_are_the_names_of_this_package() -> None:
    """A typo in one name turns a feature off, with no error anywhere."""
    assert ndp.FEATURE_REALISATION_WITH_PATH == "realisation-with-path-not-hash"
    assert ndp.FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS == "delete-dead-specific-referrers"
    assert ndp.FEATURE_DISABLE_SET_OPTIONS == "disable-set-options"
    assert ndp.FEATURE_ADD_TO_STORE_SCANNING == "add-to-store-scanning"
    assert ndp.FEATURE_SUBMIT_OUTPUT == "submit-output"
    assert ndp.STANDARD_FEATURES >= FEATURES_OF_NIX_LATEST | FEATURES_OF_BUILDER_RPC_V0


def test_the_ledger_names_every_feature_that_pynixd_does_not_claim() -> None:
    """The ledger and the claim are two halves of one statement."""
    assert set(MISSING_CODECS) == set(ndp.STANDARD_FEATURES) - set(ndp.SUPPORTED_STANDARD_FEATURES)
    for feature, reason in MISSING_CODECS.items():
        assert reason, feature


def test_pynixd_claims_every_feature_of_nix_latest() -> None:
    """Three codecs landed: realisations, batch temp roots and the GC paths.

    `realisation-with-path-not-hash` covers `DrvOutput`,
    `UnkeyedRealisation`, `BuildResult.builtOutputs`, `QueryRealisation` and
    `RegisterDrvOutput`. Measured before it: the `ca` suite against Nix 2.35
    answered 2 of 24 through pynixd, and 19 of the 24 raised "the daemon is
    missing the 'realisation-with-path-not-hash' protocol feature".

    `addTempRoots` covers op 49, which the proxy serves itself the way it
    serves op 11. Issue #66.

    `delete-dead-specific-referrers` covers the paths of `CollectGarbage`.
    Without it, the parity suite against Nix 2.35 could not decode a GC
    request between two Nix peers.
    """
    assert frozenset(FEATURES_OF_NIX_LATEST) == ndp.SUPPORTED_STANDARD_FEATURES


def test_the_negotiation_is_the_intersection() -> None:
    """What one side names alone decides nothing."""
    assert wire.negotiate_features({"a", "b"}, {"b", "c"}) == {"b"}
    assert wire.negotiate_features({"a"}, set()) == frozenset()
    assert wire.negotiate_features(set(), {"a"}) == frozenset()


def test_a_client_of_the_master_branch_negotiates_every_feature() -> None:
    """A `nix` client of the master branch names three features of `latest`.

    pynixd names all three back, so a realisation survives the proxy, batch
    roots arrive as op 49, and GC paths arrive as a `GCPaths`. Issue #14.

    **This is what the codecs can do, and not what a proxy will claim.**
    `DaemonProxy.honourable_features` widens it again to what any store
    that a build can go to offers, because every hop negotiates apart and
    a build request carries no realisation shape. Issue #84.
    """
    negotiated = wire.negotiate_features(FEATURES_OF_NIX_LATEST, ndp.SUPPORTED_STANDARD_FEATURES)

    assert negotiated == frozenset(FEATURES_OF_NIX_LATEST)


class _Store:
    """A store, reduced to the two things `honourable_features` reads."""

    def __init__(self, *, no_schedule: bool, features: set[str]) -> None:
        self.no_schedule = no_schedule
        self.features = features


def _honourable(stores: dict[str, _Store]) -> frozenset[str]:
    """`DaemonProxy.honourable_features`, called on a proxy that is only stores."""
    proxy = SimpleNamespace(stores=stores)
    return DaemonProxy.honourable_features(cast("DaemonProxy", proxy))


FEATURE = ndp.FEATURE_REALISATION_WITH_PATH


def test_a_backend_that_offers_the_feature_lets_pynixd_claim_it() -> None:
    assert _honourable({"local": _Store(no_schedule=False, features={FEATURE})}) == frozenset({FEATURE})


def test_one_backend_without_it_leaves_the_claim() -> None:
    """One store with the feature is enough to name it.

    Every hop negotiates apart -- the client with pynixd, pynixd with each
    backend -- and a build request carries no realisation shape, so the old
    backend never meets the new one: results cross each hop in the shape
    that hop agreed to. An older builder, or one that is simply offline,
    no longer vetoes the feature for builds that would never touch it.
    Issue #84.
    """
    stores = {
        "new": _Store(no_schedule=False, features={FEATURE}),
        "old": _Store(no_schedule=False, features=set()),
    }

    assert _honourable(stores) == frozenset({FEATURE})


def test_only_supported_standard_features_are_claimed() -> None:
    """The union is over every feature name a store reports, and the claim is not.

    Extension names (`QueryDerivationOutputMapBatch`) and scheduling data
    (`feature_matrix:*`) ride in the same set, and neither is a shape the
    codecs can write. The handshake still intersects with
    `SUPPORTED_STANDARD_FEATURES` last.
    """
    stores = {
        "local": _Store(
            no_schedule=False,
            features={FEATURE, "QueryDerivationOutputMapBatch", "feature_matrix:x86_64-linux"},
        ),
    }

    assert _honourable(stores) == frozenset({FEATURE})


def test_a_substituter_takes_nothing_away() -> None:
    """A binary cache is not a peer of the worker protocol.

    Its feature set is empty for every configuration, so counting it would
    answer "nothing" wherever a cache is configured — which is nearly
    everywhere.
    """
    stores = {
        "local": _Store(no_schedule=False, features={FEATURE}),
        "http-cache.nixos.org": _Store(no_schedule=True, features=set()),
    }

    assert _honourable(stores) == frozenset({FEATURE})


def test_a_store_that_never_connected_contributes_nothing() -> None:
    """No veto from a store that has not said anything yet.

    `DaemonStore._features` is empty until the first handshake. Under the
    union that contributes nothing, and pynixd still names the feature when
    another store offers it. With no store offering it, both sides keep the
    shape that every version reads.
    """
    assert _honourable({"local": _Store(no_schedule=False, features=set())}) == frozenset()
    assert _honourable(
        {
            "local": _Store(no_schedule=False, features=set()),
            "new": _Store(no_schedule=False, features={FEATURE}),
        }
    ) == frozenset({FEATURE})
