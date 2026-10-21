"""Derived-path edge cases as Nix 2.35.3 answers them. nixidae/nixidae#1005."""

from __future__ import annotations

import pytest

from pynixd.derived_path import DerivedPath, OutputsAll, OutputsNames, check_output_name

DRV = "/nix/store/00000000000000000000000000000001-a.drv"


@pytest.mark.parametrize(
    ("name", "reason"),
    [
        ("", "name must not be empty"),
        ("*nope", "contains illegal character '\\*'"),
        (".", "is not valid"),
        ("..-x", "first dash-separated component must not be '..'"),
    ],
)
def test_an_output_name_nix_refuses_is_refused(name: str, reason: str):
    """`checkName` at `path.cc:12-38`."""
    with pytest.raises(ValueError, match=reason):
        check_output_name(name)


def test_a_bad_name_in_a_derived_path_is_refused():
    """`build.sh:104`: `^*nope` must fail."""
    with pytest.raises(ValueError, match="illegal character"):
        DerivedPath(f"{DRV}!*nope")


def test_an_empty_output_list_is_an_empty_set():
    """`build.sh:102`: Nix's daemon reads `drv^` and plans nothing (issue #26)."""
    outputs = DerivedPath(f"{DRV}!").outputs
    assert isinstance(outputs, OutputsNames)
    assert outputs.names == frozenset()


def test_a_star_inside_a_chain_is_a_name_and_not_every_output():
    """`a.drv^*^bin`: the outer level asks for an output literally named `*`."""
    outer = DerivedPath(f"{DRV}!*!bin").outer

    assert isinstance(outer.outputs, OutputsNames)
    assert not isinstance(outer.outputs, OutputsAll)
    assert isinstance(DerivedPath(f"{DRV}!*").outputs, OutputsAll)
