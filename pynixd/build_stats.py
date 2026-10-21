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
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .serde import BasicDerivation

SHAPE_HEX_DIGITS = 16
"""64 bits of the SHA-256: no two shapes of one store meet by chance."""

STRUCTURED_ATTRS = "__json"


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
    raw = derivation.env.get(STRUCTURED_ATTRS)
    if raw is not None:
        try:
            parsed = json.loads(raw)
        except ValueError:
            return dict(derivation.env)
        if isinstance(parsed, dict):
            return parsed
    return dict(derivation.env)


def _features(attributes: dict[str, Any]) -> tuple[str, ...]:
    raw = attributes.get("requiredSystemFeatures", ())
    words = raw.split() if isinstance(raw, str) else [str(word) for word in raw]
    return tuple(sorted(set(words)))


def identity_of(derivation: BasicDerivation) -> DerivationIdentity:
    attributes = _attributes(derivation)
    pname = attributes.get("pname")
    name = str(pname) if pname else name_without_version(str(attributes.get("name", "")))
    keys = tuple(sorted(attributes))
    features = _features(attributes)
    digest = hashlib.sha256(json.dumps({"features": features, "keys": keys}).encode()).hexdigest()
    return DerivationIdentity(
        name=name,
        system=derivation.platform,
        shape=digest[:SHAPE_HEX_DIGITS],
        keys=keys,
        features=features,
    )
