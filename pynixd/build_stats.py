"""What a build costs, keyed by a derivation identity that outlives a version bump.

The store path of a derivation changes with every input, so it says nothing
about the next build of the same package. The identity here keeps three
things that stay put across a version bump and change when the build is
another kind of build: the name without its version, the system, and a hash
of the shape of the derivation, which is its sorted attribute names and its
`requiredSystemFeatures`. Values stay out of the shape, because a version or
a source hash is a value. nixidae/nixidae#1001.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.basic_derivation import structured_attrs

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .serde import BasicDerivation

SHAPE_HEX_DIGITS = 16
"""64 bits of the SHA-256: no two shapes of one store meet by chance."""


@dataclass(frozen=True)
class DerivationIdentity:
    """The key a build's observations are stored and looked up under."""

    name: str
    system: str
    shape: str
    keys: tuple[str, ...]
    features: tuple[str, ...]


@dataclass
class BuildObservation:
    """What one build cost, by phase. `None` is a phase that was not measured."""

    store_id: str
    status: int
    queued_ms: int | None = None
    inputs_ms: int | None = None
    inputs_bytes: int | None = None
    build_ms: int | None = None
    cpu_user_us: int | None = None
    cpu_system_us: int | None = None
    pull_ms: int | None = None
    pull_bytes: int | None = None


def name_without_version(name: str) -> str:
    """The name part of `DrvName::DrvName` at `src/libstore/names.cc:22`.

    The version starts at the first `-` whose next character is not a letter,
    so `hello-2.12` is `hello`, `python3.13-foo-bar-1.0` is
    `python3.13-foo-bar`, and `kubecove.drv` has no version.
    """
    for index, char in enumerate(name[:-1]):
        following = name[index + 1]
        if char == "-" and not (following.isascii() and following.isalpha()):
            return name[:index]
    return name


def _attributes(derivation: BasicDerivation) -> dict[str, Any]:
    """The derivation's attributes, from `__json` when it uses structured attrs.

    A structured-attrs derivation carries everything in that one variable, so
    its environment names nothing else and every such derivation would share
    one shape.
    """
    attrs = structured_attrs(derivation.env)
    return dict(derivation.env) if attrs is None else attrs


def identity_of(derivation: BasicDerivation) -> DerivationIdentity:
    attributes = _attributes(derivation)
    pname = attributes.get("pname")
    name = str(pname) if pname else name_without_version(str(attributes.get("name", "")))
    keys = tuple(sorted(attributes))
    features = tuple(sorted(derivation.required_system_features))
    digest = hashlib.sha256(json.dumps({"features": features, "keys": keys}).encode()).hexdigest()
    return DerivationIdentity(
        name=name,
        system=derivation.platform,
        shape=digest[:SHAPE_HEX_DIGITS],
        keys=keys,
        features=features,
    )


@dataclass(frozen=True)
class StoreLink:
    """How fast a store's recent transfers ran, and its dispatch round trip."""

    send_bytes_per_s: float | None = None
    receive_bytes_per_s: float | None = None
    dispatch_ms: int | None = None


@dataclass(frozen=True)
class ExpectedBytes:
    """The bytes a build of an identity sent to a store, and pulled back, before."""

    inputs: int | None = None
    pull: int | None = None


class BuildEstimateSource(StrEnum):
    """Which history answered a build estimate: the closest one that had rows."""

    SHAPE_ON_STORE = "shape-on-store"
    SHAPE = "shape"
    NAME = "name"


@dataclass(frozen=True)
class StoreEstimate:
    """What one build is expected to cost on one store, by phase.

    A field is `None` where nothing measured it. A rate of `None` leaves the
    phase it prices unknown, and the total sums the known phases only.
    """

    inputs_bytes: int | None = None
    """The NAR bytes of the input closure the store lacks now."""
    pull_bytes: int | None = None
    """The bytes the identity's outputs took to pull before."""
    link: StoreLink = StoreLink()
    build_ms: int | None = None
    build_source: BuildEstimateSource | None = None
    setup_seconds: float = 0.0
    """`remote_setup_seconds` for a remote store, 0 for the local store."""

    @property
    def ship_seconds(self) -> float | None:
        if self.inputs_bytes == 0:
            return 0.0
        if self.inputs_bytes is None or not self.link.send_bytes_per_s:
            return None
        return self.inputs_bytes / self.link.send_bytes_per_s

    @property
    def pull_seconds(self) -> float | None:
        if self.pull_bytes == 0:
            return 0.0
        if self.pull_bytes is None or not self.link.receive_bytes_per_s:
            return None
        return self.pull_bytes / self.link.receive_bytes_per_s

    @property
    def dispatch_seconds(self) -> float | None:
        return self.link.dispatch_ms / 1000 if self.link.dispatch_ms is not None else None

    @property
    def build_seconds(self) -> float | None:
        return self.build_ms / 1000 if self.build_ms is not None else None

    def total_seconds(self, wait_seconds: float | None = None) -> float | None:
        """The known phases summed, a slot wait included: when the outputs are expected local."""
        parts = [
            part
            for part in (
                wait_seconds,
                self.setup_seconds or None,
                self.ship_seconds,
                self.dispatch_seconds,
                self.build_seconds,
                self.pull_seconds,
            )
            if part is not None
        ]
        return sum(parts) if parts else None


def slot_wait_seconds(running: Iterable[tuple[int | None, float | None]], now: float) -> float | None:
    """Seconds until the soonest of the running builds is expected to end.

    Each build is `(expected ms, monotonic start)`. A build past its
    estimate counts as ending now. `None` when no running build is estimated.
    """
    remaining = [
        max(0.0, expected_ms / 1000 - (now - started))
        for expected_ms, started in running
        if expected_ms is not None and started is not None
    ]
    return min(remaining) if remaining else None


def transfer_seconds(expected: ExpectedBytes, link: StoreLink, setup_seconds: float = 0.0) -> float | None:
    """The seconds a delegation is expected to spend outside the build itself.

    `None` when no part is known: an unmeasured store keeps today's ranking.
    """
    parts: list[float] = [setup_seconds] if setup_seconds else []
    if expected.inputs is not None and link.send_bytes_per_s:
        parts.append(expected.inputs / link.send_bytes_per_s)
    if expected.pull is not None and link.receive_bytes_per_s:
        parts.append(expected.pull / link.receive_bytes_per_s)
    if link.dispatch_ms is not None:
        parts.append(link.dispatch_ms / 1000)
    return sum(parts) if parts else None
