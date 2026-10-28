"""Unit tests for pynixd.store_path — StorePath and DrvOutput.

Tests str subclass behavior, property extraction, and wire format helpers.
All tests are pure — no I/O, no mocking.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.store_dir import reset_store_dir, set_store_dir, store_dir
from pynixd.store_path import DrvOutput, StorePath

if TYPE_CHECKING:
    from collections.abc import Iterator


class TestStorePathConstruction:
    def test_from_string(self) -> None:
        sp = StorePath("/nix/store/abc123-foo")
        assert str(sp) == "/nix/store/abc123-foo"

    def test_from_store_path(self) -> None:
        sp1 = StorePath("/nix/store/abc123-foo")
        sp2 = StorePath(sp1)
        assert str(sp2) == str(sp1)

    def test_from_store_path_preserves_extrainfo(self) -> None:
        sp1 = StorePath("/nix/store/abc123-foo", extrainfo="because")
        sp2 = StorePath(sp1)
        assert sp2.extrainfo == "because"


class TestStorePathProperties:
    def test_name(self) -> None:
        sp = StorePath("/nix/store/abc123-foo")
        assert sp.name == "abc123-foo"

    def test_hash_part(self) -> None:
        sp = StorePath("/nix/store/abc123-foo")
        assert sp.hash_part() == "abc123"

    def test_base_name(self) -> None:
        sp = StorePath("/nix/store/abc123-foo")
        assert sp.base_name() == "foo"

    def test_base_name_no_hash(self) -> None:
        sp = StorePath("/nix/store/justname")
        assert sp.base_name() == ""

    def test_is_derivation_true(self) -> None:
        sp = StorePath("/nix/store/abc123-foo.drv")
        assert sp.is_derivation()

    def test_is_derivation_false(self) -> None:
        sp = StorePath("/nix/store/abc123-foo")
        assert not sp.is_derivation()

    def test_to_path(self) -> None:

        sp = StorePath("/nix/store/abc123-foo")
        assert sp.to_path() == Path("/nix/store/abc123-foo")


class TestStorePathWithStorePrefix:
    def test_already_prefixed(self) -> None:
        sp = StorePath("/nix/store/abc123-foo")
        assert sp.with_store_prefix() is sp

    def test_bare_basename(self) -> None:
        sp = StorePath("abc123-foo")
        result = sp.with_store_prefix()
        assert str(result) == "/nix/store/abc123-foo"

    def test_bare_basename_preserves_extrainfo(self) -> None:
        sp = StorePath("abc123-foo", extrainfo="test")
        result = sp.with_store_prefix()
        assert result.extrainfo == "test"


@pytest.fixture
def other_store() -> Iterator[str]:
    """A store at a directory that is not `/nix/store`.

    Issue nixidae/nixidae#212: the store directory was a constant, so a path of another store
    got `/nix/store/` in front of it and named no file at all.
    """
    set_store_dir("/scratch/root/nix/store")
    yield "/scratch/root/nix/store"
    reset_store_dir()


class TestStorePathStoreDir:
    def test_default_is_nix_store(self) -> None:
        assert store_dir() == "/nix/store"

    def test_other_store_round_trips(self, other_store: str) -> None:
        sp = StorePath(f"{other_store}/abc123-foo")
        assert sp.base() == "abc123-foo"
        assert str(sp) == f"{other_store}/abc123-foo"

    def test_bare_name_takes_the_store_dir(self, other_store: str) -> None:
        assert str(StorePath("abc123-foo")) == f"{other_store}/abc123-foo"

    def test_path_of_another_store_is_refused(self, other_store: str) -> None:
        # Silence made this a corrupt path. An error names the mistake.
        with pytest.raises(ValueError, match="not a path of the store"):
            StorePath("/nix/store/abc123-foo")

    def test_relative_path_is_refused_nowhere(self, other_store: str) -> None:
        # A bare name is how the database and the wire both spell a path.
        assert StorePath("abc123-foo").hash_part() == "abc123"


class TestStorePathEquality:
    def test_equal(self) -> None:
        assert StorePath("/nix/store/a-foo") == StorePath("/nix/store/a-foo")

    def test_not_equal(self) -> None:
        assert StorePath("/nix/store/a-foo") != StorePath("/nix/store/b-bar")

    def test_hashable(self) -> None:
        s = {StorePath("/nix/store/a-foo"), StorePath("/nix/store/b-bar")}
        assert len(s) == 2


class TestStorePathRepr:
    def test_repr_no_extra(self) -> None:
        sp = StorePath("/nix/store/a-foo")
        assert "extrainfo" not in repr(sp)

    def test_repr_with_extra(self) -> None:
        sp = StorePath("/nix/store/a-foo", extrainfo="reason")
        assert "reason" in repr(sp)


class TestDrvOutputConstruction:
    def test_valid_format(self) -> None:
        do = DrvOutput("sha256:abc123def456!out")
        assert str(do) == "sha256:abc123def456!out"

    def test_invalid_no_bang(self) -> None:
        with pytest.raises(ValueError, match="Invalid DrvOutput"):
            DrvOutput("sha256:abc123def456")

    def test_empty_string(self) -> None:
        do = DrvOutput("")
        assert str(do) == ""

    def test_from_drv_output(self) -> None:
        do1 = DrvOutput("sha256:abc!out")
        do2 = DrvOutput(do1)  # type: ignore[arg-type]
        assert str(do2) == "sha256:abc!out"


class TestDrvOutputProperties:
    def test_id_hash(self) -> None:
        do = DrvOutput("sha256:abc!out")
        assert do.id_hash == "sha256:abc"

    def test_output_name(self) -> None:
        do = DrvOutput("sha256:abc!out")
        assert do.output_name == "out"

    def test_output_name_multiple_bangs(self) -> None:
        do = DrvOutput("sha256:a!b!c")
        assert do.output_name == "b!c"

    def test_repr(self) -> None:
        do = DrvOutput("sha256:abc!out")
        assert "DrvOutput" in repr(do)
