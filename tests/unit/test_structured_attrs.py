"""A `__structuredAttrs` derivation keeps its scheduling attributes in `__json`.

`getBoolAttr` and `getStringSetAttr` at `derivation-options.cc:36` read the
JSON alone for such a derivation. Measured with nixpkgs' `runCommand` and
`__structuredAttrs = true`: the environment holds `__json` and `out`, and
`preferLocalBuild` is the JSON `true`. nixidae/nixidae#1003.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

from pynixd.allocator import BuildAllocator
from pynixd.build_stats import identity_of
from pynixd.serde import BasicDerivation
from pynixd.system_features import effective_required_features


def _structured(**attrs: Any) -> BasicDerivation:
    return BasicDerivation(
        env={"__json": json.dumps({"name": "probe", **attrs}), "out": "/nix/store/x-probe"},
        platform="x86_64-linux",
        builder="/bin/sh",
    )


class TestPreferLocalBuild:
    def test_is_read_from_the_json(self):
        assert _structured(preferLocalBuild=True).build_local

    def test_false_in_the_json_is_false(self):
        assert not _structured(preferLocalBuild=False).build_local

    def test_the_string_one_is_not_a_boolean(self):
        """Nix refuses `"1"` there; it is no request for a local build."""
        assert not _structured(preferLocalBuild="1").build_local


class TestRequiredSystemFeatures:
    def test_are_read_from_the_json(self):
        """A `kvm` build otherwise matched every store, kvm or not."""
        drv = _structured(requiredSystemFeatures=["kvm", "big-parallel"])

        assert effective_required_features(drv) == {"kvm", "big-parallel"}

    def test_a_value_that_is_not_a_list_reads_as_none(self):
        """The builder refuses it with Nix's message; the scheduler must not raise."""
        drv = _structured(requiredSystemFeatures=None)

        assert drv.required_system_features == set()
        assert identity_of(drv).features == ()

    def test_stripping_writes_the_json(self):
        drv = _structured(requiredSystemFeatures=["dynamic-derivations", "kvm"])
        build = SimpleNamespace(request=SimpleNamespace(derivation=drv), build_id=1)

        BuildAllocator.strip_handled_features(cast("Any", build))

        assert "requiredSystemFeatures" not in drv.env
        assert json.loads(drv.env["__json"])["requiredSystemFeatures"] == ["kvm"]
        assert drv.required_system_features == {"kvm"}
