"""`hashDerivationModulo` over a closure, against two real derivations.

The oracle is the derivation itself. Nix builds the path of an
input-addressed output from the hash of the derivation modulo its inputs, so
the path that `makeOutputPath` gives for that hash must be the path that the
derivation states. A wrong hash gives a wrong path, and nothing else has to
be known to see it.

`nix-instantiate` wrote both texts below. `drv-hash-top` has two outputs and
one input derivation, so the walk has to reach `drv-hash-base` and use its
hash. See `pynixd/drv_hash.py` for the two rules that the walk follows.
"""

from __future__ import annotations

import binascii
from typing import TYPE_CHECKING

import pytest

from pynixd.drv_hash import output_hashes
from pynixd.drv_parser import parse_drv
from pynixd.goals.resolution import _make_output_path

if TYPE_CHECKING:
    from pynixd.drv_parser import Derivation

BASE_PATH = "/nix/store/434i66dyibz4zshbfhsl8vvwykravahs-drv-hash-base.drv"
BASE_TEXT = (
    'Derive([("out","/nix/store/d69pwdv3lma9vw5hgd09hlcnv484j307-drv-hash-base","","")],[],[],'
    '"x86_64-linux","/bin/sh",["-c","echo base > $out"],'
    '[("builder","/bin/sh"),("name","drv-hash-base"),'
    '("out","/nix/store/d69pwdv3lma9vw5hgd09hlcnv484j307-drv-hash-base"),("system","x86_64-linux")])'
)

TOP_TEXT = (
    'Derive([("dev","/nix/store/cmrzxzizfd86ml91xqsn1wkpqlbx7bap-drv-hash-top-dev","",""),'
    '("out","/nix/store/331zkap2q342l1x5g9sd6i5cxqbwmw4a-drv-hash-top","","")],'
    '[("/nix/store/434i66dyibz4zshbfhsl8vvwykravahs-drv-hash-base.drv",["out"])],[],'
    '"x86_64-linux","/bin/sh",'
    '["-c","echo /nix/store/d69pwdv3lma9vw5hgd09hlcnv484j307-drv-hash-base > $out; echo dev > $dev"],'
    '[("builder","/bin/sh"),("dev","/nix/store/cmrzxzizfd86ml91xqsn1wkpqlbx7bap-drv-hash-top-dev"),'
    '("name","drv-hash-top"),("out","/nix/store/331zkap2q342l1x5g9sd6i5cxqbwmw4a-drv-hash-top"),'
    '("outputs","out dev"),("system","x86_64-linux")])'
)


async def _read(drv_path: str) -> Derivation | None:
    return parse_drv(BASE_TEXT) if drv_path == BASE_PATH else None


async def _read_nothing(drv_path: str) -> None:
    del drv_path
    return None


def _rebuild(name: str, digest: str, drv_name: str) -> str:
    return _make_output_path(name, binascii.unhexlify(digest), drv_name)


@pytest.mark.anyio
async def test_a_derivation_with_no_input_states_its_own_paths() -> None:
    parsed = parse_drv(BASE_TEXT)
    hashes = await output_hashes(parsed, _read_nothing, cache={})

    assert hashes is not None
    assert _rebuild("out", hashes["out"], "drv-hash-base") == str(parsed.output_paths()["out"])


@pytest.mark.anyio
async def test_the_walk_reaches_the_input_derivation() -> None:
    """Both outputs of the top derivation, and both come from the same hash."""
    parsed = parse_drv(TOP_TEXT)
    hashes = await output_hashes(parsed, _read, cache={})

    assert hashes is not None
    paths = parsed.output_paths()
    assert _rebuild("out", hashes["out"], "drv-hash-top") == str(paths["out"])
    assert _rebuild("dev", hashes["dev"], "drv-hash-top") == str(paths["dev"])


@pytest.mark.anyio
async def test_the_hash_of_the_input_is_not_the_hash_of_a_leaf() -> None:
    """The walk must use the input, and not ignore it.

    Without the input the top derivation still hashes, and it hashes to
    something else. This states that the two differ, so a walk that drops the
    input cannot pass the test above by accident.
    """
    parsed = parse_drv(TOP_TEXT)
    with_input = await output_hashes(parsed, _read, cache={})
    without = parsed.hash_derivation_modulo(mask_outputs=True, input_drv_hashes=None)

    assert with_input is not None
    assert with_input["out"] != without["out"]


@pytest.mark.anyio
async def test_a_missing_input_derivation_gives_no_answer() -> None:
    """No hash is better than a wrong one, and the caller keeps its old answer."""
    parsed = parse_drv(TOP_TEXT)

    assert await output_hashes(parsed, _read_nothing, cache={}) is None


@pytest.mark.anyio
async def test_the_cache_answers_for_an_input_read_once() -> None:
    """The walk reads each input derivation once, and the next walk reads none.

    `DaemonStore.read_derivation` asks the daemon for the path information and
    then for a whole NAR. A closure of a thousand derivations is a thousand of
    each, so the walk keeps what it reads.
    """
    reads: list[str] = []

    async def counting(drv_path: str) -> Derivation | None:
        reads.append(drv_path)
        return await _read(drv_path)

    parsed = parse_drv(TOP_TEXT)
    cache: dict[str, dict[str, str]] = {}
    first = await output_hashes(parsed, counting, cache=cache)
    second = await output_hashes(parsed, counting, cache=cache)

    assert first == second
    assert reads == [BASE_PATH], "the second walk read nothing"


TWO_FOD_A = "/nix/store/w3psnzp7b66mjjnc04q976pmmdasqa4m-same-fod.drv"
TWO_FOD_B = "/nix/store/a07yqpa0yfw4m098mcdmjdcjwakhs1l3-same-fod.drv"
_SAME_FOD = (
    'Derive([("out","/nix/store/ydywmswmq70apbfjdh22c368p9kiy9nf-same-fod","sha256",'
    '"8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4")],[],[],"x86_64-linux","/bin/sh",'
    '["-c","echo {tag} >/dev/null; printf hi > $out"],[("builder","/bin/sh"),("name","same-fod"),'
    '("out","/nix/store/ydywmswmq70apbfjdh22c368p9kiy9nf-same-fod"),'
    '("outputHash","8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4"),'
    '("outputHashAlgo","sha256"),("outputHashMode","flat"),("system","x86_64-linux")])'
)
TWO_FOD_TOP = (
    'Derive([("out","/nix/store/4x12x70rdzaandydn28ms5z2m5a7mrgm-two-fod-top","","")],'
    '[("/nix/store/a07yqpa0yfw4m098mcdmjdcjwakhs1l3-same-fod.drv",["out"]),'
    '("/nix/store/w3psnzp7b66mjjnc04q976pmmdasqa4m-same-fod.drv",["out"])],[],"x86_64-linux","/bin/sh",'
    '["-c","cat /nix/store/ydywmswmq70apbfjdh22c368p9kiy9nf-same-fod /nix/store/ydywmswmq70apbfjdh22c368p9kiy9nf-same-fod > $out"],'
    '[("builder","/bin/sh"),("name","two-fod-top"),("out","/nix/store/4x12x70rdzaandydn28ms5z2m5a7mrgm-two-fod-top"),'
    '("system","x86_64-linux")])'
)


@pytest.mark.anyio
async def test_two_inputs_with_one_fixed_output_name_it_once() -> None:
    """Two fixed-output inputs with one hash are one entry, `["out"]`.

    `hashDerivationModulo` at `derivations.cc:951` collects the names in a
    `StringSet`. A list wrote `["out","out"]`, and the hash named another
    path. `nix-instantiate` wrote all three texts. nixidae/nixidae#1004.
    """
    texts = {TWO_FOD_A: _SAME_FOD.format(tag="a"), TWO_FOD_B: _SAME_FOD.format(tag="b")}

    async def read(drv_path: str) -> Derivation | None:
        text = texts.get(drv_path)
        return None if text is None else parse_drv(text)

    hashes = await output_hashes(parse_drv(TWO_FOD_TOP), read, cache={})

    assert hashes is not None
    assert _rebuild("out", hashes["out"], "two-fod-top") == "/nix/store/4x12x70rdzaandydn28ms5z2m5a7mrgm-two-fod-top"
