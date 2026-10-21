"""BasicDerivation — wire mirror of a Nix derivation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .derivation_output import DerivationOutput
from .store_path import StorePath
from .wire_message import WireField, WireModel

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

STRUCTURED_ATTRS = "__json"
REQUIRED_SYSTEM_FEATURES = "requiredSystemFeatures"


def structured_attrs(env: Mapping[str, str]) -> dict[str, Any] | None:
    """The attributes of a `__structuredAttrs` derivation, or `None` for any other.

    Such a derivation keeps every attribute in `__json`, `preferLocalBuild`
    and `requiredSystemFeatures` included, and its environment names nothing
    else. `getBoolAttr` and `getStringSetAttr` at `derivation-options.cc:36`
    then read the JSON alone. nixidae/nixidae#1003.
    """
    raw = env.get(STRUCTURED_ATTRS)
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _flag(env: Mapping[str, str], attrs: dict[str, Any] | None, name: str) -> bool:
    if attrs is None:
        return env.get(name) == "1"
    return attrs.get(name) is True


def required_features_of(env: Mapping[str, str]) -> set[str]:
    """`requiredSystemFeatures`: words in the environment, a list of strings in `__json`."""
    attrs = structured_attrs(env)
    if attrs is None:
        return set(env.get(REQUIRED_SYSTEM_FEATURES, "").split())
    # Nix refuses a value that is not a list of strings. The build then fails
    # on the builder, with Nix's message, and not in the scheduler.
    raw = attrs.get(REQUIRED_SYSTEM_FEATURES)
    if not isinstance(raw, list):
        return set()
    return {word for word in raw if isinstance(word, str)}


class BasicDerivation(WireModel):
    """Wire mirror of BasicDerivation."""

    outputs: dict[str, DerivationOutput] = WireField(default_factory=dict)
    input_srcs: set[StorePath] = WireField(default_factory=set)
    platform: str
    builder: str
    args: list[str] = WireField(default_factory=list)
    env: dict[str, str] = WireField(default_factory=dict)
    is_dynamic: bool = WireField(default=False, serialize=False, deserialize=False)

    @property
    def build_local(self) -> bool:
        attrs = structured_attrs(self.env)
        return _flag(self.env, attrs, "pynixd_fast") or _flag(self.env, attrs, "preferLocalBuild")

    @property
    def required_system_features(self) -> set[str]:
        return required_features_of(self.env)

    def set_required_system_features(self, features: Iterable[str]) -> None:
        """Write `requiredSystemFeatures` where the builder reads it: `__json` or the environment."""
        attrs = structured_attrs(self.env)
        if attrs is None:
            self.env[REQUIRED_SYSTEM_FEATURES] = " ".join(sorted(features))
            return
        attrs[REQUIRED_SYSTEM_FEATURES] = sorted(features)
        # nlohmann's `dump()`, which wrote `__json`: sorted keys, no spaces.
        self.env[STRUCTURED_ATTRS] = json.dumps(attrs, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @property
    def has_dynamic_outputs(self) -> bool:
        return any(output.is_dynamic_output for output in self.outputs.values())

    @property
    def has_ca_floating(self) -> bool:
        return any(output.is_floating_ca and not output.is_text_hashed for output in self.outputs.values())

    @property
    def has_deferred(self) -> bool:
        return any(output.is_deferred for output in self.outputs.values())

    @property
    def has_impure(self) -> bool:
        return any(output.is_impure for output in self.outputs.values())

    @property
    def has_text_hashed(self) -> bool:
        return any(output.is_text_hashed for output in self.outputs.values())

    def output_paths(self) -> dict[str, StorePath]:
        """Each output name, and the store path it names.

        `DerivationOutput.path` is the wire string. An output with no path
        yet -- deferred, or floating content-addressed -- gives the empty
        `StorePath`, which is falsy, so a caller can drop it.
        """
        return {name: StorePath(output.path) for name, output in self.outputs.items()}
