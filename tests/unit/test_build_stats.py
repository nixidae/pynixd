"""A derivation identity survives a version bump and changes with the kind of build.

nixidae/nixidae#1001.
"""

from __future__ import annotations

import json

import pytest

from pynixd.build_stats import identity_of, name_without_version
from pynixd.serde import BasicDerivation


def _drv(env: dict[str, str], platform: str = "x86_64-linux") -> BasicDerivation:
    return BasicDerivation(platform=platform, builder="/bin/sh", env=env)


class TestTheNameWithoutItsVersion:
    """`DrvName::DrvName` at `src/libstore/names.cc:22`."""

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("hello-2.12.1", "hello"),
            ("python3.13-foo-bar-1.0", "python3.13-foo-bar"),
            ("kubecove.drv", "kubecove.drv"),
            ("source", "source"),
            ("nix-2.35.3", "nix"),
            ("gcc-wrapper-14", "gcc-wrapper"),
            ("trailing-", "trailing-"),
        ],
    )
    def test_matches_nix(self, name: str, expected: str) -> None:
        assert name_without_version(name) == expected


class TestTheIdentity:
    def test_a_version_bump_keeps_it(self) -> None:
        old = identity_of(_drv({"name": "hello-2.12", "src": "/nix/store/a-src", "version": "2.12"}))
        new = identity_of(_drv({"name": "hello-2.13", "src": "/nix/store/b-src", "version": "2.13"}))

        assert old == new

    def test_pname_wins_over_the_name(self) -> None:
        assert identity_of(_drv({"name": "hello-2.12", "pname": "greeter"})).name == "greeter"

    def test_a_new_attribute_is_a_new_shape(self) -> None:
        plain = identity_of(_drv({"name": "hello-2.12"}))
        checked = identity_of(_drv({"name": "hello-2.12", "doCheck": "1"}))

        assert plain.name == checked.name
        assert plain.shape != checked.shape

    def test_a_new_feature_is_a_new_shape(self) -> None:
        """A `big-parallel` build of the same name is another kind of build."""
        plain = identity_of(_drv({"name": "llvm-19", "requiredSystemFeatures": ""}))
        big = identity_of(_drv({"name": "llvm-19", "requiredSystemFeatures": "big-parallel"}))

        assert big.features == ("big-parallel",)
        assert plain.shape != big.shape

    def test_feature_order_does_not_matter(self) -> None:
        one = identity_of(_drv({"name": "x", "requiredSystemFeatures": "kvm big-parallel"}))
        other = identity_of(_drv({"name": "x", "requiredSystemFeatures": "big-parallel kvm"}))

        assert one.shape == other.shape

    def test_the_system_is_part_of_it(self) -> None:
        assert identity_of(_drv({"name": "x"})) != identity_of(_drv({"name": "x"}, platform="aarch64-linux"))

    def test_structured_attrs_read_the_json(self) -> None:
        """With structured attrs the environment holds `__json` alone, and
        every such derivation would share one shape."""
        attrs = {"name": "hello-2.12", "pname": "hello", "requiredSystemFeatures": ["kvm"], "doCheck": True}
        identity = identity_of(_drv({"__json": json.dumps(attrs)}))

        assert identity.name == "hello"
        assert identity.keys == ("doCheck", "name", "pname", "requiredSystemFeatures")
        assert identity.features == ("kvm",)
