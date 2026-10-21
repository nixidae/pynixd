"""An untrusted client's derivation is written the way Nix writes it.

`daemon.cc:596-653` never takes the client's output path: Nix checks the
output types (`derivations.cc:800-858`) and prints a fixed output's path
recomputed from its hash, with the hash as lowercase Base16
(`derivations.cc:680-687`). The paths below are Nix 2.35.3's own answers
for `builtins.derivation { name = "probe"; outputHash = <H>; ... }`.
nixidae/nixidae#1010.
"""

from __future__ import annotations

import pytest

from pynixd.drv_parser import _Parser  # noqa: PLC2701 -- the string reader is the unit
from pynixd.goals.resolution import canonical_derivation
from pynixd.serde import BasicDerivation, DerivationOutput
from pynixd.utils import nix32_decode, nix32_encode

NIX32 = "15sqg1j6gq6081nk0v5c6npadlswb9238l336wb2g9bmmrry779c"
HEX = "2c9de373ae75a5271637635034445a5cd3a6ae35ac6c306d40c0e06764785897"
B64 = "LJ3jc651pScWN2NQNERaXNOmrjWsbDBtQMDgZ2R4WJc="
WRONG = "/nix/store/00000000000000000000000000000000-probe"


def _fixed(method: str, digest: str) -> BasicDerivation:
    return BasicDerivation(
        outputs={"out": DerivationOutput(path=WRONG, method=method, hash_digest=digest)},
        platform="x86_64-linux",
        builder="/bin/sh",
    )


def test_nix32_round_trips():
    assert nix32_decode(NIX32).hex() == HEX
    assert nix32_encode(bytes.fromhex(HEX)) == NIX32


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("sha256", "/nix/store/8x7bscq769ychk160p3kv87b7bph3mhw-probe"),
        ("r:sha256", "/nix/store/9cmx65f7k2p3c8bibxzgnw45kaynxjy6-probe"),
        ("text:sha256", "/nix/store/a9382cj2hwa1zi145wa9bc71sahsm3zm-probe"),
    ],
)
@pytest.mark.parametrize("digest", [NIX32, HEX, HEX.upper(), B64])
def test_the_path_is_recomputed_and_the_hash_is_base16(method: str, path: str, digest: str):
    out = canonical_derivation(_fixed(method, digest), "probe").outputs["out"]

    assert out.path == path
    assert out.hash_digest == HEX


@pytest.mark.parametrize(
    ("outputs", "reason"),
    [
        ({}, "must have at least one output"),
        (
            {"dev": DerivationOutput(path=WRONG, method="sha256", hash_digest=HEX)},
            'single fixed output must be named "out"',
        ),
        (
            {"out": DerivationOutput(path="", method="sha256"), "dev": DerivationOutput(path="", method="sha512")},
            "all floating outputs must use the same hash algorithm",
        ),
        (
            {
                "out": DerivationOutput(path=WRONG, method="sha256", hash_digest=HEX),
                "dev": DerivationOutput(path="", method="sha256"),
            },
            "can't mix derivation output types",
        ),
    ],
)
def test_what_nix_refuses_is_refused(outputs: dict[str, DerivationOutput], reason: str):
    drv = BasicDerivation(outputs=outputs, platform="x86_64-linux", builder="/bin/sh")

    with pytest.raises(ValueError, match=reason):
        canonical_derivation(drv, "probe")


def test_an_unknown_escape_drops_its_backslash():
    """`derivations.cc:188-205`: only `\\n`, `\\r` and `\\t` map; `\\x` reads `x`."""
    assert _Parser('"a\\bc\\qd\\n"').parse_string() == "abcqd\n"
