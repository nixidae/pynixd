"""Unit tests for pynixd.serde build types — BuildResult, BuiltOutput, enums.

Tests BuildResultStatus/BuildMode enum values, BuiltOutput JSON serialization,
and BuildResult wire format helpers. All tests are pure — no I/O.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pynixd.serde import BuildMode, BuildResult, BuildResultStatus, BuiltOutput, StorePath

if TYPE_CHECKING:
    import pytest

# This repository holds two classes named `StorePath`. `pynixd.store_path`
# holds the domain one, which strips the `/nix/store/` prefix and keeps an
# `extrainfo` field. `pynixd.serde` re-exports the wire one, which is the
# string that goes on the daemon socket. Neither compares equal to the other.
#
# `BuiltOutput.out_path` held the domain one, and the move of the protocol
# into `nix-daemon-protocol` made it the wire one. That package cannot import
# `pynixd`, so the wire class is the only class it can name, and the new type
# is the correct type. This module therefore names the wire one as well.
#
# `tests/_conftest/helpers.py` uses the same `StorePath` spelling, and it
# is the spelling to copy wherever the two meet.
StorePath = StorePath


class TestBuildResultStatus:
    def test_built(self) -> None:
        assert BuildResultStatus.BUILT == 0

    def test_permanent_failure(self) -> None:
        assert BuildResultStatus.PERMANENT_FAILURE == 3

    def test_not_deterministic(self) -> None:
        assert BuildResultStatus.NOT_DETERMINISTIC == 12

    def test_hash_mismatch(self) -> None:
        assert BuildResultStatus.HASH_MISMATCH == 101


class TestBuildMode:
    def test_normal(self) -> None:
        assert BuildMode.NORMAL == 0

    def test_repair(self) -> None:
        assert BuildMode.REPAIR == 1

    def test_check(self) -> None:
        assert BuildMode.CHECK == 2


class TestBuiltOutput:
    def test_from_empty_string(self) -> None:
        out = BuiltOutput.from_string("")
        assert out.out_path == StorePath("")

    def test_from_plain_path(self) -> None:
        out = BuiltOutput.from_string("/nix/store/abc-foo")
        assert out.out_path == StorePath("/nix/store/abc-foo")
        assert out.ca == ""

    def test_to_string_plain_path(self) -> None:
        out = BuiltOutput(out_path=StorePath("/nix/store/abc-foo"))
        assert out.to_string() == "/nix/store/abc-foo"

    def test_from_json(self) -> None:
        data = {
            "outPath": "/nix/store/abc-foo",
            "ca": "text:sha256:xyz",
            "hash": "abc123",
            "hashAlgo": "sha256",
            "narHash": "sha256:xyz",
            "narSize": 42,
        }
        s = json.dumps(data)
        out = BuiltOutput.from_string(s)
        assert out.out_path == StorePath("/nix/store/abc-foo")
        assert out.ca == "text:sha256:xyz"
        assert out.hash == "abc123"
        assert out.hash_algo == "sha256"
        assert out.nar_hash == "sha256:xyz"
        assert out.nar_size == 42

    def test_to_string_json(self) -> None:
        out = BuiltOutput(
            out_path=StorePath("/nix/store/abc-foo"),
            ca="text:sha256:xyz",
            nar_hash="sha256:xyz",
            nar_size=42,
        )
        result = out.to_string()
        parsed = json.loads(result)
        assert parsed["outPath"] == "/nix/store/abc-foo"
        assert parsed["ca"] == "text:sha256:xyz"
        assert parsed["narSize"] == 42

    def test_to_string_empty(self) -> None:
        out = BuiltOutput()
        assert out.to_string() == ""

    def test_from_invalid_json(self) -> None:
        """Invalid JSON should fall back to plain path."""
        out = BuiltOutput.from_string("{not valid json}")
        assert out.out_path == StorePath("{not valid json}")

    def test_from_non_dict_json(self) -> None:
        """JSON that's not a dict should fall back to plain path."""
        out = BuiltOutput.from_string(json.dumps([1, 2, 3]))
        assert out.out_path == StorePath("[1, 2, 3]")


class TestBuildResult:
    def test_default_status(self) -> None:
        result = BuildResult()
        assert result.status == BuildResultStatus.BUILT

    def test_from_reader_minimal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test BuildResult.from_reader with pre-protocol-1.29 version."""
        result = BuildResult()
        result.status = BuildResultStatus.ALREADY_VALID
        result.error_msg = "already valid"
        assert result.status == BuildResultStatus.ALREADY_VALID
        assert result.error_msg == "already valid"

    def test_with_built_outputs(self) -> None:
        result = BuildResult()
        result.status = BuildResultStatus.BUILT
        assert result.status == BuildResultStatus.BUILT
