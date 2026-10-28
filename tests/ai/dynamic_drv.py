"""Research script for dynamic derivation support in pynixd.

Exercises the full dynamic derivation lifecycle against a root Nix store:
1. Build a regular derivation (hello)
2. Build a CA text-hashed derivation (producingDrv) whose output IS a .drv
3. Verify the output IS the hello.drv content
4. Build through dynamic path (producingDrv^out^out)
5. Build a wrapper that depends on the dynamic drv output via builtins.outputOf
6. Inspect .drv structure (DrvWithVersion, dynamic_input_drvs, etc.)
7. Query derivation output map and realisations
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from pathlib import Path

import anyio

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pynixd.drv_parser import read_drv_file, to_basic_derivation
from pynixd.goals.resolution import _unparse_basic_derivation
from pynixd.serde import (
    QueryDerivationOutputMapRequest,
    QueryRealisationRequest,
)
from pynixd.store import LocalSocketStore
from pynixd.store_path import DrvOutput, StorePath
from tests._conftest.nix_config import for_dynamic_derivations
from tests.conftest import (
    NIX_BIN,
    STORE_PREFIX,
    make_test_spec,
    rmtree_robust,
    run_subproc,
)

DYN_NIX = Path(__file__).resolve().parent.parent / "nix"
DYN_NIX_CONFIG = for_dynamic_derivations(
    substituters=(
        "https://nixkube.cachix.org/",
        "unix:///nix/var/nix/daemon-socket/socket?root=/",
    ),
    trusted_public_keys=("nixkube.cachix.org-1:H8UE0jlI9pxHexK/NhDmEoLDarJXp1WTymQrsajlh7M=",),
)


async def main() -> None:
    root_path = STORE_PREFIX / "dyn-drv-root"
    rmtree_robust(root_path)
    root_store = LocalSocketStore(
        make_test_spec(store_id="dyn-drv-root", store_path=root_path, nix_config=DYN_NIX_CONFIG),
    )
    await root_store.ensure_daemon()

    # Step 1: Build hello
    print("=" * 70)
    print("Step 1: Build hello (regular derivation)")
    print("=" * 70)
    rc, stdout, stderr, _ = await run_subproc(
        [
            NIX_BIN,
            "build",
            "--store",
            str(root_path),
            "--impure",
            "--file",
            str(DYN_NIX),
            "dyn.hello",
            "--no-link",
            "--print-out-paths",
        ],
        nix_config=DYN_NIX_CONFIG,
    )
    assert rc == 0, f"hello build failed: {stderr}"
    hello_out = stdout.strip()
    print(f"hello output: {hello_out}")

    rc, stdout, _, _ = await run_subproc(
        [
            NIX_BIN,
            "eval",
            "--store",
            str(root_path),
            "--impure",
            "--file",
            str(DYN_NIX),
            "hello.drvPath",
            "--raw",
        ],
        nix_config=DYN_NIX_CONFIG,
    )
    assert rc == 0
    hello_drv_path = StorePath(stdout.strip())
    print(f"hello .drv path: {hello_drv_path}")

    # Step 2: Build producingDrv (CA text-hashed)
    print()
    print("=" * 70)
    print("Step 2: Build producingDrv (CA text-hashed derivation)")
    print("=" * 70)
    rc, stdout, stderr, _ = await run_subproc(
        [
            NIX_BIN,
            "build",
            "--store",
            str(root_path),
            "--impure",
            "--file",
            str(DYN_NIX),
            "dyn.producingDrv",
            "--no-link",
            "--print-out-paths",
        ],
        nix_config=DYN_NIX_CONFIG,
    )
    assert rc == 0, f"producingDrv build failed: {stderr}"
    producing_out = stdout.strip()
    print(f"producingDrv output: {producing_out}")

    # Verify it's a .drv file
    full_path = root_path / str(producing_out).lstrip("/")
    content = await anyio.Path(full_path).read_text()
    assert content.startswith("Derive("), f"Expected Derive(), got: {content[:80]}"
    print(f"Output IS a derivation file: {content[:80]}...")

    rc, stdout, _, _ = await run_subproc(
        [
            NIX_BIN,
            "eval",
            "--store",
            str(root_path),
            "--impure",
            "--file",
            str(DYN_NIX),
            "producingDrv.drvPath",
            "--raw",
        ],
        nix_config=DYN_NIX_CONFIG,
    )
    assert rc == 0
    producing_drv_path = StorePath(stdout.strip())
    print(f"producingDrv .drv path: {producing_drv_path}")

    # Step 3: Inspect producingDrv .drv
    print()
    print("=" * 70)
    print("Step 3: Inspect producingDrv .drv structure")
    print("=" * 70)
    parsed_producing = await read_drv_file(producing_drv_path, root_store.layout)
    assert parsed_producing is not None, "producingDrv .drv should exist"
    print(f"is_dynamic: {parsed_producing.is_dynamic}")
    print(
        f"outputs: {[(o.name, o.path, o.hash_algo, o.hash_value) for o in parsed_producing.outputs]}",
    )
    print(f"input_srcs: {sorted(str(p) for p in parsed_producing.input_srcs)}")
    print(f"input_drvs: {sorted(str(p) for p in parsed_producing.input_drvs)}")
    print(f"dynamic_input_drvs: {parsed_producing.dynamic_input_drvs}")

    for o in parsed_producing.outputs:
        print(
            f"  output {o.name}: path={o.path!r} hash_algo={o.hash_algo!r} hash_value={o.hash_value!r}",
        )

    # Step 4: Build then inspect wrapper .drv (has dynamic dependencies)
    print()
    print("=" * 70)
    print("Step 4: Build wrapper and inspect its .drv")
    print("=" * 70)

    # First instantiate to get the drv path
    rc, stdout, stderr, _ = await run_subproc(
        [
            NIX_BIN,
            "eval",
            "--store",
            str(root_path),
            "--impure",
            "--file",
            str(DYN_NIX),
            "wrapper.drvPath",
            "--raw",
        ],
        nix_config=DYN_NIX_CONFIG,
    )
    if rc != 0:
        print(f"wrapper drvPath eval failed: {stderr}")
    else:
        wrapper_drv_path = StorePath(stdout.strip())
        print(f"wrapper .drv path: {wrapper_drv_path}")

        # Show ATerm
        aterm_path = root_path / str(wrapper_drv_path).lstrip("/")
        if aterm_path.exists():
            aterm = await anyio.Path(aterm_path).read_text()
            print(f"wrapper .drv ATerm (first 300 chars): {aterm[:300]}...")

            # Parse with pynixd
            try:
                wrapper_parsed = await read_drv_file(wrapper_drv_path, root_store.layout)
                assert wrapper_parsed is not None
                print(f"wrapper is_dynamic: {wrapper_parsed.is_dynamic}")
                print(
                    f"wrapper outputs: {[(o.name, o.path, o.hash_algo, o.hash_value) for o in wrapper_parsed.outputs]}",
                )
                print(
                    f"wrapper input_drvs: {sorted(str(p) for p in wrapper_parsed.input_drvs)}",
                )
                print(
                    f"wrapper dynamic_input_drvs: {wrapper_parsed.dynamic_input_drvs}",
                )
                print(
                    f"wrapper input_srcs: {sorted(str(p) for p in wrapper_parsed.input_srcs)}",
                )
            except Exception as e:
                print(f"wrapper .drv parse error: {type(e).__name__}: {e}")

    # Step 5: Build through dynamic path (^out^out)
    print()
    print("=" * 70)
    print("Step 5: Build producingDrv^out^out (dynamic output path)")
    print("=" * 70)
    rc, stdout, stderr, _ = await run_subproc(
        [
            NIX_BIN,
            "build",
            "--store",
            str(root_path),
            f"{producing_drv_path}^out^out",
            "--no-link",
            "--print-out-paths",
        ],
        nix_config=DYN_NIX_CONFIG,
    )
    if rc == 0:
        dyn_out = stdout.strip()
        print(f"producingDrv^out^out = {dyn_out}")
        print(f"Matches hello output: {dyn_out == hello_out}")
    else:
        print(f"^out^out build failed: {stderr}")

    # Step 6: Build wrapper
    print()
    print("=" * 70)
    print("Step 6: Build wrapper (depends on dynamic drv via builtins.outputOf)")
    print("=" * 70)
    rc, stdout, stderr, _ = await run_subproc(
        [
            NIX_BIN,
            "build",
            "--store",
            str(root_path),
            "--impure",
            "--file",
            str(DYN_NIX),
            "dyn.wrapper",
            "--no-link",
            "--print-out-paths",
        ],
        nix_config=DYN_NIX_CONFIG,
    )
    if rc == 0:
        wrapper_out = stdout.strip()
        print(f"wrapper output: {wrapper_out}")
        wrapper_content_path = root_path / str(wrapper_out).lstrip("/")
        if wrapper_content_path.exists():
            content = (await anyio.Path(wrapper_content_path).read_text()).strip()
            print(f"wrapper content: {content!r}")
            print(f"matches hello content: {content == 'hello'}")
    else:
        print(f"wrapper build failed: {stderr}")

    # Step 7: Query derivation output map
    wrapper_drv_path_local: StorePath | None = None

    print()
    print("=" * 70)
    print("Step 7: QueryDerivationOutputMap for producingDrv and wrapper")
    print("=" * 70)

    rc2, stdout2, _stderr2, _ = await run_subproc(
        [
            NIX_BIN,
            "eval",
            "--store",
            str(root_path),
            "--impure",
            "--file",
            str(DYN_NIX),
            "wrapper.drvPath",
            "--raw",
        ],
        nix_config=DYN_NIX_CONFIG,
    )
    if rc2 == 0:
        wrapper_drv_path_local = StorePath(stdout2.strip())

    try:
        resp = await root_store.execute(
            QueryDerivationOutputMapRequest(path=producing_drv_path),
        )
        print(f"producingDrv output_map: {resp.items}")
    except Exception as e:
        print(f"producingDrv QueryDerivationOutputMap failed: {type(e).__name__}: {e}")

    if wrapper_drv_path_local is not None:
        try:
            resp = await root_store.execute(
                QueryDerivationOutputMapRequest(path=wrapper_drv_path_local),
            )
            print(f"wrapper output_map: {resp.items}")
        except Exception as e:
            print(f"wrapper QueryDerivationOutputMap failed: {type(e).__name__}: {e}")

    # Step 8: Query realisation
    print()
    print("=" * 70)
    print("Step 8: QueryRealisation for producingDrv^out")
    print("=" * 70)
    # DrvOutput format is "sha256:<hash>!<outputName>" — NOT a store path.
    # The hash is hashDerivationModulo (sha256 of the masked ATerm).
    # For a CA text-hashed drv, we compute it from the .drv content.

    try:
        basic = await to_basic_derivation(parsed_producing, root_store.layout)
        aterm = _unparse_basic_derivation(basic, mask_outputs=True)
        h = hashlib.sha256(aterm.encode()).hexdigest()
        drv_output_id = DrvOutput(f"sha256:{h}!out")
        print(f"Computed DrvOutput: {drv_output_id}")
        resp = await root_store.execute(
            QueryRealisationRequest(drv_output=drv_output_id),  # type: ignore[arg-type]  # the request takes the wire DrvOutput; the package's pynixd DrvOutput is passed (report: package type mismatch)
        )
        print(f"producingDrv realisation: {resp}")
    except Exception as e:
        print(f"QueryRealisation failed: {type(e).__name__}: {e}")

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY: What pynixd needs for dynamic derivation support")
    print("=" * 70)
    print("""
GAPS IDENTIFIED:
1. DerivedPath only supports "drv!out1,out2" — no nested "^out^out" refs
2. BuildPaths decomposition doesn't walk dynamic_input_drvs for DAG deps
3. No resolution pipeline for dynamic derivation outputs
4. DrvWithVersion wire format needs BuildDerivation support
5. After building producingDrv, its output must be parsed as a .drv
   and the inner derivation must be built (the "trampoline" pattern)
6. QueryDerivationOutputMap needs to resolve dynamic output paths via
   DownstreamPlaceholder -> real path mapping
7. Text-hashed CA outputs (outputHashMode="text") must produce
   content-addressed paths correctly
""")

    await root_store.close()


if __name__ == "__main__":
    asyncio.run(main())
