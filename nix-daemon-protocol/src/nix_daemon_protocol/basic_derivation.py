"""BasicDerivation — wire mirror of a Nix derivation."""

from __future__ import annotations

from .derivation_output import DerivationOutput
from .store_path import StorePath
from .wire_message import WireField, WireModel


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
        return self.env.get("pynixd_fast") == "1" or self.env.get("preferLocalBuild") == "1"

    @property
    def required_system_features(self) -> set[str]:
        raw = self.env.get("requiredSystemFeatures", "")
        if not raw:
            return set()
        return set(raw.split())

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
