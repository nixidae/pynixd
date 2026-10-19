"""`hashDerivationModulo` answers for a dynamic derivation as well.

`output_hashes` answered `None` for a derivation with a dynamic input, so
`EnsureDerivedPathGoal` could not re-key the realisation of one. The client
then asked `queryPartialDerivationOutputMap` for the derivation it had
instantiated, got no path, and `nix-build.cc:730` stopped the program on
`assert(maybeOutputPath)`. `dyn-drv:dep-built-drv` is the test of the
functional suite that reports this.

Nix does answer. `hashDerivationModulo` at `derivations.cc:931` reads
`node.value` of each input, which holds the direct outputs alone, and puts it
in a map that carries no child. `Derivation::unparse` writes that map, and it
still chooses the `DrvWithVersion("xp-dyn-drv",` form, because it asks the
derivation and not the replacement map.

**The four derivations below are a recording of one run of `nix-build
./text-hashed-output.nix -A wrapper`, and the four hashes are the ids that
that run wrote to its `Realisations` table.** So this test compares pynixd
with Nix, and not with itself.

Refs nixidae/nixidae#214.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.store_dir import reset_store_dir, set_store_dir
from pynixd.drv_hash import output_hashes
from pynixd.drv_parser import Derivation, parse_drv

STORE = "/private/tmp/dynprobe/store-root/store"

# The wrapper. Its one input is dynamic: `hello.drv.drv^out`.
WRAPPER = (
    'DrvWithVersion("xp-dyn-drv",[("out","","","")],'
    f'[("{STORE}/pd7q2pxzn8zdxqr6mlj8zjc3x5y6pd1q-hello.drv.drv",([],[("out",["out"])]))],'
    f'["{STORE}/kxac7wxyszcnqimx67ava0ayqg37qq6b-builder-use-dynamic-drv-in-non-dynamic-drv.sh"],'
    '"aarch64-darwin","/nix/store/gdbxd0zh7a4ag9b880wabjb87pzb5pgg-bash-interactive-5.3p15/bin/bash",'
    f'["-e","{STORE}/kxac7wxyszcnqimx67ava0ayqg37qq6b-builder-use-dynamic-drv-in-non-dynamic-drv.sh"],'
    '[("PATH","/nix/store/0y55d4qc8fmdhmn3c890m0zbaksprycj-coreutils-9.11/bin"),'
    '("buildCommand","cp -r /1f7qwg4rqhbb9v2g13sfxphzizy318qcf4dqk1vp7pdnhwzw2w26 $out\\n"),'
    '("builder","/nix/store/gdbxd0zh7a4ag9b880wabjb87pzb5pgg-bash-interactive-5.3p15/bin/bash"),'
    '("name","use-dynamic-drv-in-non-dynamic-drv"),("out",""),("system","aarch64-darwin")])'
)

# The producer. It writes a `.drv` file, so its output is content-addressed
# with the text method.
PRODUCER = (
    'Derive([("out","","text:sha256","")],[],'
    f'["{STORE}/0jlcpw722a4nbp37s61znzrr8hw5wxv1-builder-hello.drv.sh",'
    f'"{STORE}/3bszyl0w2ixxjfdiwd87zzlzyvr3r1ag-hello.drv"],'
    '"aarch64-darwin","/nix/store/gdbxd0zh7a4ag9b880wabjb87pzb5pgg-bash-interactive-5.3p15/bin/bash",'
    f'["-e","{STORE}/0jlcpw722a4nbp37s61znzrr8hw5wxv1-builder-hello.drv.sh"],'
    '[("PATH","/nix/store/0y55d4qc8fmdhmn3c890m0zbaksprycj-coreutils-9.11/bin"),'
    f'("buildCommand","cp {STORE}/3bszyl0w2ixxjfdiwd87zzlzyvr3r1ag-hello.drv $out\\n"),'
    '("builder","/nix/store/gdbxd0zh7a4ag9b880wabjb87pzb5pgg-bash-interactive-5.3p15/bin/bash"),'
    '("name","hello.drv"),("out","/1rz4g4znpzjwh1xymhjpm42vipw92pr73vdgl6xs1hycac8kf2n9"),'
    '("outputHashAlgo","sha256"),("outputHashMode","text"),("system","aarch64-darwin")])'
)

# The derivation that the producer writes, and that the wrapper then reads.
HELLO = (
    f'Derive([("out","{STORE}/69rnzi4ipyhmvai2k5k9gbbvcwdxsmwx-hello","","")],[],'
    f'["{STORE}/bxf13abcjvm1q487yyas203nf28aw8zg-builder-hello.sh"],'
    '"aarch64-darwin","/nix/store/gdbxd0zh7a4ag9b880wabjb87pzb5pgg-bash-interactive-5.3p15/bin/bash",'
    f'["-e","{STORE}/bxf13abcjvm1q487yyas203nf28aw8zg-builder-hello.sh"],'
    '[("PATH","/nix/store/0y55d4qc8fmdhmn3c890m0zbaksprycj-coreutils-9.11/bin"),'
    '("buildCommand","mkdir -p $out\\necho \\"Hello World\\" > $out/hello\\n"),'
    '("builder","/nix/store/gdbxd0zh7a4ag9b880wabjb87pzb5pgg-bash-interactive-5.3p15/bin/bash"),'
    f'("name","hello"),("out","{STORE}/69rnzi4ipyhmvai2k5k9gbbvcwdxsmwx-hello"),'
    '("system","aarch64-darwin")])'
)

WRAPPER_PATH = f"{STORE}/48lmdjxwq01y428hz7rnsh98c23mb206-use-dynamic-drv-in-non-dynamic-drv.drv"
PRODUCER_PATH = f"{STORE}/pd7q2pxzn8zdxqr6mlj8zjc3x5y6pd1q-hello.drv.drv"
HELLO_PATH = f"{STORE}/3bszyl0w2ixxjfdiwd87zzlzyvr3r1ag-hello.drv"

# The ids that the run wrote to `Realisations`, for the three derivations
# above.
WRAPPER_HASH = "2acb6ecdd17500acb830e4dd0162c1cba6f365be64da614e68fe0f0e7350a722"
PRODUCER_HASH = "e27d08b13a95ecea363c741e3d980be14ede6dc7fee4cb5c670d296f858ee2fa"
HELLO_HASH = "b5472c83353bd3651ec99e4dd74d07baa76dea4c4fe83835231283689b10856a"

TEXTS = {WRAPPER_PATH: WRAPPER, PRODUCER_PATH: PRODUCER, HELLO_PATH: HELLO}


@pytest.fixture(autouse=True)
def _store() -> object:
    """The recording names this store, and a store path must match it."""
    set_store_dir(STORE)
    yield
    reset_store_dir()


async def _read(drv_path: str) -> Derivation | None:
    text = TEXTS.get(str(drv_path))
    return None if text is None else parse_drv(text)


def _parse(text: str) -> Derivation:
    return parse_drv(text)


@pytest.mark.anyio
async def test_the_hash_of_a_dynamic_derivation_is_the_one_nix_wrote() -> None:
    assert await output_hashes(_parse(WRAPPER), _read, cache={}) == {"out": WRAPPER_HASH}


@pytest.mark.anyio
async def test_the_hash_of_the_producer_is_the_one_nix_wrote() -> None:
    assert await output_hashes(_parse(PRODUCER), _read, cache={}) == {"out": PRODUCER_HASH}


@pytest.mark.anyio
async def test_the_hash_of_the_produced_derivation_is_the_one_nix_wrote() -> None:
    assert await output_hashes(_parse(HELLO), _read, cache={}) == {"out": HELLO_HASH}


def test_the_replacement_inputs_do_not_change_the_form() -> None:
    """`hasDynamicDrvDep` reads the derivation, and never the replacement map."""
    parsed = _parse(WRAPPER)
    aterm = parsed.unparse(maskOutputs=True, actualInputs={})
    assert aterm.startswith('DrvWithVersion("xp-dyn-drv",')


def test_a_dynamic_input_adds_no_entry_of_its_own() -> None:
    """The one input of the wrapper names a dynamic output alone, so it goes."""
    parsed = _parse(WRAPPER)
    assert parsed.unparse(maskOutputs=True, actualInputs={}).count(PRODUCER_PATH) == 0
