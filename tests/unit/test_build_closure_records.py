"""Build-decision seeds come from the request fields. Issue #65."""

from __future__ import annotations

from pynixd.execution import build_decision_seeds

DRV = "/nix/store/00000000000000000000000000000001-x.drv"
COMPILER = "/nix/store/00000000000000000000000000000002-cc"
SOURCE = "/nix/store/00000000000000000000000000000003-src.c"


def test_decision_seeds_name_the_derivation_and_its_declared_inputs() -> None:
    """No derivation file is read: the wire request already carries every
    input path. Deeper layers resolve through the store's own references
    at flush -- if they needed building, their own decisions record them
    in turn."""
    assert build_decision_seeds(DRV, {COMPILER, SOURCE}) == {DRV, COMPILER, SOURCE}


def test_decision_seeds_survive_an_empty_input_set() -> None:
    assert build_decision_seeds(DRV, set()) == {DRV}
