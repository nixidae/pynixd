"""Standalone test for deferred CA derivation resolution via store protocol.

Exercises the exact code path that pynixd's scheduler will use:
1. Build reference paths on a root store via nix CLI
2. Create a clean builder store
3. Transfer needed paths via stream_paths_store_to_store
4. Register CA realisation via RegisterDrvOutputRequest
5. Resolve the deferred derivation via resolve_derivation()
6. Place the resolved .drv on both stores:
   - Local store: overwrite the .drv file directly (we own the filesystem)
   - Builder store: AddToStoreNar with updated ValidPathInfo + NAR of resolved content
7. BuildDerivation with the original .drv path and resolved BasicDerivation
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pynixd.drv_parser import read_drv_file
from pynixd.goals.resolution import (
    _nix_drv_name,
    _unparse_basic_derivation,
    resolve_derivation,
)
from pynixd.serde import (
    AddToStoreRequest,
    BuildDerivationRequest,
    BuildMode,
    QueryValidPathsRequest,
    RegisterDrvOutputRequest,
)
from pynixd.store import LocalSocketStore
from pynixd.store.transfer import stream_paths_store_to_store
from pynixd.store_path import StorePath
from tests._conftest.nix_config import for_ca_derivations
from tests.conftest import (
    NIX_BIN,
    STORE_PREFIX,
    make_test_spec,
    rmtree_robust,
    run_subproc,
    serde_path_set,
)

if TYPE_CHECKING:
    from pynixd.wire import NixWriter

CA_NIX = Path(__file__).resolve().parent.parent / "nix"
CA_NIX_CONFIG = for_ca_derivations()


async def add_text_to_store(
    store: LocalSocketStore,
    name: str,
    content: bytes,
    references: set[StorePath],
) -> StorePath | None:
    """Add a text file to a store via AddToStore(text:sha256) and return its path."""

    async def provide_content(writer: NixWriter) -> None:
        fw = writer.framed()
        fw.write(content)
        await fw.finalize()

    req = AddToStoreRequest(
        path_name=name,
        cam="text:sha256",  # type: ignore[arg-type]  # stale script: cam is a ContentAddress now
        references=references,
        repair=0,
        async_provider=provide_content,  # type: ignore[call-arg]  # stale script: AddToStoreRequest has no async_provider
    )
    try:
        resp = await req.execute(store, suppress_last=True)  # type: ignore[attr-defined]  # stale script: requests no longer execute themselves
        if resp.info is not None:
            store.add_path_info(resp.info)
            return resp.info.path  # type: ignore[no-any-return]  # stale script: follows the attr-defined above
        print(f"  AddToStore returned no path info! logs={resp.logs.messages}")
    except Exception as e:
        print(f"  AddToStore FAILED: {type(e).__name__}: {e}")
        return None
    else:
        return None


async def _main(stack: contextlib.AsyncExitStack) -> None:
    # -- Step 1: Build reference on root store --
    print("=" * 70)
    print("Step 1: Build reference on root store")
    print("=" * 70)

    root_path = STORE_PREFIX / "deferred-replay-root"
    rmtree_robust(root_path)
    root_store = LocalSocketStore(
        make_test_spec(store_id="deferred-replay-root", store_path=root_path, nix_config=CA_NIX_CONFIG),
    )
    await stack.enter_async_context(root_store)

    cmd_ca = [
        NIX_BIN,
        "build",
        "--store",
        str(root_path),
        "--impure",
        "--file",
        str(CA_NIX),
        "ca.simple",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, stderr, _ = await run_subproc(cmd_ca, nix_config=CA_NIX_CONFIG)
    assert rc == 0, f"CA build failed: {stderr}"
    ca_out_path = stdout.strip()
    print(f"CA output: {ca_out_path}")

    cmd_deferred = [
        NIX_BIN,
        "build",
        "--store",
        str(root_path),
        "--impure",
        "--file",
        str(CA_NIX),
        "ca.non_ca_depends_on_ca",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, stderr, _ = await run_subproc(cmd_deferred, nix_config=CA_NIX_CONFIG)
    assert rc == 0, f"Deferred build failed: {stderr}"
    deferred_out_path = stdout.strip()
    print(f"Deferred output: {deferred_out_path}")

    # -- Step 2: Parse .drv files and get realisation --
    print()
    print("=" * 70)
    print("Step 2: Parse .drv files and get CA realisation")
    print("=" * 70)

    eval_cmd = [
        NIX_BIN,
        "eval",
        "--store",
        str(root_path),
        "--impure",
        "--file",
        str(CA_NIX),
        "ca_simple.drvPath",
        "--raw",
    ]
    rc, stdout, _, _ = await run_subproc(eval_cmd, nix_config=CA_NIX_CONFIG)
    ca_drv_path = StorePath(stdout.strip())
    print(f"CA .drv path: {ca_drv_path}")

    eval_cmd2 = [
        NIX_BIN,
        "eval",
        "--store",
        str(root_path),
        "--impure",
        "--file",
        str(CA_NIX),
        "non_ca_depends_on_ca.drvPath",
        "--raw",
    ]
    rc, stdout, _, _ = await run_subproc(eval_cmd2, nix_config=CA_NIX_CONFIG)
    deferred_drv_path = StorePath(stdout.strip())
    print(f"Deferred .drv path: {deferred_drv_path}")

    ca_parsed = await read_drv_file(ca_drv_path, root_store.layout)
    deferred_parsed = await read_drv_file(deferred_drv_path, root_store.layout)
    assert ca_parsed is not None
    assert deferred_parsed is not None

    for o in deferred_parsed.outputs:
        print(f"  output: name={o.name} path={o.path!r} hash_algo={o.hash_algo!r}")

    # Get CA realisation via nix CLI
    realisation_cmd = [
        NIX_BIN,
        "realisation",
        "info",
        "--store",
        str(root_path),
        "--json",
        f"{ca_drv_path}^out",
    ]
    rc, realisation_out, _, _ = await run_subproc(
        realisation_cmd,
        nix_config=CA_NIX_CONFIG,
    )
    realisations_raw: list[Any] = json.loads(realisation_out) if rc == 0 and realisation_out.strip() else []
    realisation_to_register = realisations_raw[0] if realisations_raw else None
    print(f"CA realisation: {realisation_to_register}")

    # -- Step 3: Create builder store and transfer paths --
    print()
    print("=" * 70)
    print("Step 3: Create builder store and transfer paths")
    print("=" * 70)

    builder_path = STORE_PREFIX / "deferred-replay-builder"
    rmtree_robust(builder_path)
    builder_store = LocalSocketStore(
        make_test_spec(store_id="deferred-replay-builder", store_path=builder_path, nix_config=CA_NIX_CONFIG),
    )
    await stack.enter_async_context(builder_store)

    paths_to_transfer: set[StorePath] = set()
    paths_to_transfer.add(ca_drv_path)
    paths_to_transfer.update(ca_parsed.input_srcs)
    paths_to_transfer.add(StorePath(ca_out_path).with_store_prefix())
    paths_to_transfer.add(deferred_drv_path)
    paths_to_transfer.update(deferred_parsed.input_srcs)
    for input_drv in deferred_parsed.input_drvs:
        paths_to_transfer.add(input_drv)

    print(f"Transferring {len(paths_to_transfer)} paths...")
    await stream_paths_store_to_store(
        root_store,
        builder_store,
        paths_to_transfer,
    )

    valid_resp = await builder_store.execute(
        QueryValidPathsRequest(paths=serde_path_set(paths_to_transfer), substitute=0),
    )
    print(
        f"Builder store has {len(valid_resp.paths)} of {len(paths_to_transfer)} paths",
    )

    # -- Step 4: Register CA realisation on builder --
    print()
    print("=" * 70)
    print("Step 4: Register CA realisation on builder store")
    print("=" * 70)

    if realisation_to_register:
        try:
            reg_req = RegisterDrvOutputRequest(realisation=realisation_to_register)
            await builder_store.call(reg_req, suppress_last=True)
            print("CA realisation registered on builder store!")
        except Exception as e:
            print(f"Registration FAILED: {e}")

    # -- Step 4b: Resolve and add resolved .drv via AddToStore --
    print()
    print("=" * 70)
    print("Step 4b: Resolve derivation and add to stores via AddToStore")
    print("=" * 70)

    resolved_output_paths_early: dict[str, StorePath] = {
        "out": StorePath(ca_out_path).with_store_prefix(),
    }
    resolved_early = resolve_derivation(
        deferred_parsed,
        deferred_drv_path,
        resolved_output_paths_early,  # type: ignore[arg-type]  # stale script: keys are output names; resolve_derivation takes tuple keys
    )
    resolved_aterm_early = _unparse_basic_derivation(resolved_early, mask_outputs=False)

    # Nix uses the derivation's "name" field (from env), not the store path name.
    # The suffix for AddToStore is: drv.name + ".drv"
    # Our resolve_derivation computes outputs using outputPathName(drv.name, id)
    # which for "out" just returns drv.name.
    drv_name = _nix_drv_name(deferred_drv_path)  # "non-ca-depends-on-ca"
    name_for_add = drv_name + ".drv"  # "non-ca-depends-on-ca.drv"
    print(f"Adding resolved .drv via AddToStore text:sha256 name={name_for_add}")
    print(f"  References: {sorted(str(p) for p in resolved_early.input_srcs)}")

    local_resolved_path = await add_text_to_store(
        root_store,
        name_for_add,
        resolved_aterm_early.encode("utf-8"),
        resolved_early.input_srcs,
    )
    print(f"Local store resolved .drv: {local_resolved_path}")

    builder_resolved_path = await add_text_to_store(
        builder_store,
        name_for_add,
        resolved_aterm_early.encode("utf-8"),
        resolved_early.input_srcs,
    )
    print(f"Local store resolved .drv: {local_resolved_path}")

    builder_resolved_path = await add_text_to_store(
        builder_store,
        name_for_add,
        resolved_aterm_early.encode("utf-8"),
        resolved_early.input_srcs,
    )
    print(f"Builder store resolved .drv: {builder_resolved_path}")

    if not local_resolved_path or not builder_resolved_path:
        print("\nAddToStore FAILED — cannot proceed with build")
        return

    if local_resolved_path != builder_resolved_path:
        print("\nWARNING: resolved .drv paths differ!")

    # -- Step 5: BuildDerivation with RESOLVED .drv path --
    print()
    print("=" * 70)
    print("Step 5: BuildDerivation with resolved .drv path")
    print("=" * 70)

    build_req = BuildDerivationRequest(
        drv_path=local_resolved_path,
        derivation=resolved_early,
        build_mode=BuildMode.NORMAL,
    )
    print(f"Sending BuildDerivation for {local_resolved_path}")
    print(f"  outputs: {[(n, o.path) for n, o in resolved_early.outputs.items()]}")

    try:
        resp = await builder_store.call(build_req)
        print(f"\nBuildDerivation result: status={resp.result.status}")
        print(f"  error_msg: {resp.result.error_msg!r}")
        print(f"  built_outputs: {resp.result.built_outputs}")
        if resp.result.status == 0:
            print("\n" + "=" * 70)
            print("SUCCESS!")
            print("=" * 70)
        else:
            print("\nFAILURE!")
    except Exception as e:
        print(f"\nBuildDerivation EXCEPTION: {type(e).__name__}: {e}")

        traceback.print_exc()

    print(f"\nExpected output: {deferred_out_path}")


async def main() -> None:
    async with contextlib.AsyncExitStack() as stack:
        await _main(stack)


if __name__ == "__main__":
    asyncio.run(main())
