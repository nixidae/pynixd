"""Unit tests for pynixd.serde derivation types — OutputKind, DerivationOutput, BasicDerivation.

Tests the classification logic and property accessors for derivation types.
All tests are pure — no I/O, no wire protocol.
"""

from __future__ import annotations

import pytest

from pynixd.serde import BasicDerivation, DerivationOutput, OutputKind
from pynixd.store_path import StorePath
from pynixd.system_features import effective_required_features


class TestOutputKind:
    def test_input_addressed(self) -> None:
        out = DerivationOutput(path="/nix/store/abc-foo", method="", hash_digest="")
        assert out.kind == OutputKind.INPUT_ADDRESSED

    def test_ca_fixed(self) -> None:
        out = DerivationOutput(
            path="/nix/store/abc-foo",
            method="sha256",
            hash_digest="abc123",
        )
        assert out.kind == OutputKind.CA_FIXED

    def test_ca_floating(self) -> None:
        out = DerivationOutput(path="", method="sha256", hash_digest="")
        assert out.kind == OutputKind.CA_FLOATING

    def test_deferred(self) -> None:
        out = DerivationOutput(path="", method="", hash_digest="")
        assert out.kind == OutputKind.DEFERRED

    def test_impure(self) -> None:
        out = DerivationOutput(path="", method="sha256", hash_digest="impure")
        assert out.kind == OutputKind.IMPURE


class TestDerivationOutputProperties:
    def test_is_ca_fixed(self) -> None:
        out = DerivationOutput(path="/nix/store/abc", method="sha256", hash_digest="xyz")
        assert out.is_ca
        assert out.is_fixed_ca
        assert not out.is_floating_ca
        assert not out.is_deferred
        assert not out.is_impure

    def test_is_ca_floating(self) -> None:
        out = DerivationOutput(path="", method="sha256", hash_digest="")
        assert out.is_ca
        assert not out.is_fixed_ca
        assert out.is_floating_ca
        assert not out.is_deferred
        assert not out.is_impure

    def test_is_deferred(self) -> None:
        out = DerivationOutput(path="", method="", hash_digest="")
        assert not out.is_ca
        assert not out.is_fixed_ca
        assert not out.is_floating_ca
        assert out.is_deferred
        assert not out.is_impure

    def test_is_impure(self) -> None:
        out = DerivationOutput(path="", method="sha256", hash_digest="impure")
        assert out.is_ca
        assert not out.is_fixed_ca
        assert not out.is_floating_ca
        assert not out.is_deferred
        assert out.is_impure

    def test_is_text_hashed(self) -> None:
        out = DerivationOutput(path="/nix/store/abc", method="text:sha256", hash_digest="xyz")
        assert out.is_text_hashed

    def test_is_not_text_hashed(self) -> None:
        out = DerivationOutput(path="/nix/store/abc", method="sha256", hash_digest="xyz")
        assert not out.is_text_hashed

    def test_is_dynamic_output(self) -> None:
        out = DerivationOutput(path="", method="text:sha256", hash_digest="")
        assert out.is_dynamic_output

    def test_is_not_dynamic_output(self) -> None:
        out = DerivationOutput(path="", method="sha256", hash_digest="")
        assert not out.is_dynamic_output


class TestBasicDerivation:
    def test_build_local_true(self) -> None:
        drv = BasicDerivation(env={"preferLocalBuild": "1"}, platform="", builder="")
        assert drv.build_local

    def test_build_local_pynixd_fast(self) -> None:
        drv = BasicDerivation(env={"pynixd_fast": "1"}, platform="", builder="")
        assert drv.build_local

    def test_build_local_false(self) -> None:
        drv = BasicDerivation(env={}, platform="", builder="")
        assert not drv.build_local

    def test_required_system_features_empty(self) -> None:
        drv = BasicDerivation(env={}, platform="", builder="")
        assert drv.required_system_features == set()

    def test_required_system_features_parsed(self) -> None:
        drv = BasicDerivation(env={"requiredSystemFeatures": "kvm big-parallel"}, platform="", builder="")
        assert drv.required_system_features == {"kvm", "big-parallel"}

    def test_output_paths(self) -> None:
        drv = BasicDerivation(
            outputs={
                "out": DerivationOutput(path="/nix/store/abc-foo"),
                "lib": DerivationOutput(path="/nix/store/abc-lib"),
            },
            platform="",
            builder="",
        )
        paths = drv.output_paths()
        assert paths == {
            "out": StorePath("/nix/store/abc-foo"),
            "lib": StorePath("/nix/store/abc-lib"),
        }

    def test_has_dynamic_outputs_true(self) -> None:
        drv = BasicDerivation(
            outputs={"out": DerivationOutput(path="", method="text:sha256", hash_digest="")},
            platform="",
            builder="",
        )
        assert drv.has_dynamic_outputs

    def test_has_dynamic_outputs_false(self) -> None:
        drv = BasicDerivation(
            outputs={"out": DerivationOutput(path="/nix/store/abc-foo")},
            platform="",
            builder="",
        )
        assert not drv.has_dynamic_outputs

    def test_has_ca_floating_true(self) -> None:
        drv = BasicDerivation(
            outputs={"out": DerivationOutput(path="", method="sha256", hash_digest="")},
            platform="",
            builder="",
        )
        assert drv.has_ca_floating

    def test_has_ca_floating_false_for_text(self) -> None:
        drv = BasicDerivation(
            outputs={"out": DerivationOutput(path="", method="text:sha256", hash_digest="")},
            platform="",
            builder="",
        )
        # text:sha256 with empty hash is a dynamic output, not CA floating
        assert not drv.has_ca_floating

    def test_has_deferred_true(self) -> None:
        drv = BasicDerivation(
            outputs={"out": DerivationOutput(path="", method="", hash_digest="")},
            platform="",
            builder="",
        )
        assert drv.has_deferred

    def test_has_impure_true(self) -> None:
        drv = BasicDerivation(
            outputs={"out": DerivationOutput(path="", method="sha256", hash_digest="impure")},
            platform="",
            builder="",
        )
        assert drv.has_impure

    def test_has_text_hashed_true(self) -> None:
        drv = BasicDerivation(
            outputs={
                "out": DerivationOutput(path="/nix/store/abc", method="text:sha256", hash_digest="xyz"),
            },
            platform="",
            builder="",
        )

        assert drv.has_text_hashed

    def test_effective_required_features(self) -> None:

        drv = BasicDerivation(env={"requiredSystemFeatures": "kvm ca-derivations"}, platform="", builder="")
        effective = effective_required_features(drv)
        # Not stripped: a backend that cannot serve CA must stay excluded.
        assert "ca-derivations" in effective
        assert effective == {"kvm", "ca-derivations"}


class TestExhaustiveOutputKind:
    """Test all 5 branches of DerivationOutput.kind exhaustively."""

    @pytest.mark.parametrize(
        "name,out,expected",  # noqa: PT006
        [
            ("INPUT_ADDRESSED", DerivationOutput(path="/p", method="", hash_digest=""), OutputKind.INPUT_ADDRESSED),
            ("CA_FIXED", DerivationOutput(path="/p", method="sha256", hash_digest="h"), OutputKind.CA_FIXED),
            ("CA_FLOATING", DerivationOutput(path="", method="sha256", hash_digest=""), OutputKind.CA_FLOATING),
            ("DEFERRED", DerivationOutput(path="", method="", hash_digest=""), OutputKind.DEFERRED),
            ("IMPURE", DerivationOutput(path="", method="sha256", hash_digest="impure"), OutputKind.IMPURE),
        ],
    )
    def test_kind(self, name, out, expected) -> None:
        assert out.kind == expected, f"{name} failed"
