"""Standalone test for derivation resolution (deferred -> resolved).

Implements the Nix derivation resolution algorithm in Python:
1. Compute DownstreamPlaceholder for CA derivation outputs
2. Build a placeholder -> actual_path rewrite map
3. Apply rewrites to env/args/builder
4. Compute hashDerivationModulo on the resolved BasicDerivation
5. Derive output paths via makeOutputPath
6. Convert Deferred outputs to InputAddressed

Validates against known Nix output paths.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import anyio

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pynixd.drv_parser import Derivation, read_drv_file
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildMode,
    DerivationOutput,
    QueryDerivationOutputMapRequest as QdomRequest,
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
)

CA_NIX = Path(__file__).resolve().parent.parent / "nix"
CA_NIX_CONFIG = for_ca_derivations(
    substituters=(
        "https://nixkube.cachix.org/",
        "unix:///nix/var/nix/daemon-socket/socket?root=/",
    ),
    trusted_public_keys=("nixkube.cachix.org-1:H8UE0jlI9pxHexK/NhDmEoLDarJXp1WTymQrsajlh7M=",),
)
STORE_DIR = "/nix/store"

NIX32_CHARS = "0123456789abcdfghijklmnpqrsvwxyz"


# ── Nix32 encoding ──────────────────────────────────────────────────


def nix32_encode(data: bytes) -> str:
    if len(data) == 0:
        return ""
    size = len(data)
    result_len = (size * 8 - 1) // 5 + 1
    result: list[str] = []
    for n in range(result_len - 1, -1, -1):
        b = n * 5
        i = b // 8
        j = b % 8
        c = (data[i] >> j) & 0x1F
        if i + 1 < size:
            c |= (data[i + 1] << (8 - j)) & 0x1F
        result.append(NIX32_CHARS[c])
    return "".join(result)


# ── Placeholder computation ──────────────────────────────────────────


def output_path_name(drv_name: str, output_name: str) -> str:
    if output_name == "out":
        return drv_name
    return f"{drv_name}-{output_name}"


def _nix_store_path_name(store_path_str: str) -> str:
    """Extract the Nix-style 'name' from a store path (after hash-)."""
    basename = store_path_str.rsplit("/", 1)[-1]
    first_dash = basename.find("-")
    if first_dash == -1:
        return basename
    return basename[first_dash + 1 :]


def downstream_placeholder_unknown_ca_output(
    drv_path_hash_part: str,
    drv_name: str,
    output_name: str,
) -> str:
    clear_text = f"nix-upstream-output:{drv_path_hash_part}:{output_path_name(drv_name, output_name)}"
    h = hashlib.sha256(clear_text.encode()).digest()
    return "/" + nix32_encode(h)


# ── Store path computation ───────────────────────────────────────────


def compress_hash(data: bytes, new_size: int) -> bytes:
    result = bytearray(new_size)
    for i in range(len(data)):
        result[i % new_size] ^= data[i]
    return bytes(result)


def make_store_path(
    type_str: str,
    hash_modulo: bytes,
    name: str,
    store_dir: str = STORE_DIR,
) -> str:
    hash_str = "sha256:" + hash_modulo.hex()
    s = f"{type_str}:{hash_str}:{store_dir}:{name}"
    digest = hashlib.sha256(s.encode()).digest()
    compressed = compress_hash(digest, 20)
    nix32_hash = nix32_encode(compressed)
    return f"{store_dir}/{nix32_hash}-{name}"


def make_output_path(
    output_id: str,
    hash_modulo: bytes,
    drv_name: str,
    store_dir: str = STORE_DIR,
) -> str:
    name = output_path_name(drv_name, output_id)
    return make_store_path(f"output:{output_id}", hash_modulo, name, store_dir)


# ── hashDerivationModulo ─────────────────────────────────────────────


def hash_derivation_modulo(
    drv: BasicDerivation,
    output_map_for_input_drvs: dict[str, dict[str, bytes]] | None = None,
    mask_outputs: bool = True,
) -> dict[str, bytes]:
    """Compute hashDerivationModulo, returning {output_name: sha256_digest_bytes}.

    For a BasicDerivation (no inputDrvs), this is straightforward:
    - Mask output paths in the ATerm
    - Hash the ATerm string
    - All outputs share the same hash

    output_map_for_input_drvs: {drv_path_str: {output_name: hash_bytes}}
      Used when resolving input derivations (not needed for BasicDerivation
      since it has no inputDrvs, but needed when we compute the hash of
      the original Derivation with inputDrvs).
    """
    aterm = _unparse_basic_derivation(drv, mask_outputs=mask_outputs)
    h = hashlib.sha256(aterm.encode()).digest()
    return dict.fromkeys(drv.outputs, h)


def _rewrite_strings(s: str, rewrites: dict[str, str]) -> str:
    for old, new in rewrites.items():
        if old == new:
            continue
        s = s.replace(old, new)
    return s


def _unparse_basic_derivation(drv: BasicDerivation, mask_outputs: bool = True) -> str:
    """Serialize a BasicDerivation to ATerm format (like Nix's drv.unparse)."""
    parts: list[str] = ["Derive("]

    out_parts: list[str] = []
    for name, o in sorted(drv.outputs.items()):
        path = "" if mask_outputs else o.path
        out_parts.append(f'("{name}","{path}","{o.method}","{o.hash_digest}")')
    parts.append(f"[{','.join(out_parts)}],")

    # inputDrvs: always empty for BasicDerivation
    parts.append("[],")

    # inputSrcs
    srcs = ",".join(f'"{p}"' for p in sorted(str(p) for p in drv.input_srcs))
    parts.append(f"[{srcs}],")

    # platform
    parts.append(f'"{drv.platform}",')

    # builder
    parts.append(f'"{drv.builder}",')

    # args
    args = ",".join(f'"{a}"' for a in drv.args)
    parts.append(f"[{args}],")

    # env
    env_parts: list[str] = []
    for k, v in sorted(drv.env.items()):
        env_parts.append(f'("{k}","{v}")')
    parts.append(f"[{','.join(env_parts)}]")

    parts.append(")")
    return "".join(parts)


# ── Derivation resolution ────────────────────────────────────────────


def resolve_derivation(
    drv: Derivation,
    drv_path: StorePath,
    resolved_output_paths: dict[str, StorePath],
) -> BasicDerivation:
    """Resolve a deferred derivation by substituting placeholders with actual paths.

    This implements the Nix `tryResolve` + `rewriteDerivation` algorithm:
    1. For each inputDrv output, compute the DownstreamPlaceholder
    2. Build a rewrite map: placeholder -> actual store path
    3. Apply rewrites to builder, args, and env
    4. Move inputDrv outputs into inputSrcs
    5. Compute hashDerivationModulo on the resolved derivation
    6. Convert Deferred outputs to InputAddressed via makeOutputPath

    Args:
        drv: The parsed derivation (with inputDrv info)
        drv_path: The .drv store path (for computing placeholders)
        resolved_output_paths: {output_name: actual_store_path} for each
            input derivation's outputs

    Returns:
        A resolved BasicDerivation with filled-in output paths
    """
    # Derive name from drv_path using Nix's StorePath::name() semantics:
    # basename after the hash- prefix, minus .drv extension
    drv_path_str = str(drv_path)
    nix_name_with_ext = _nix_store_path_name(drv_path_str)
    drv_name = nix_name_with_ext.removesuffix(".drv")

    # Compute the placeholder for each input drv output
    rewrites: dict[str, str] = {}
    new_input_srcs = {StorePath(path=str(path)) for path in drv.input_srcs}
    for input_drv_path, output_names in drv.input_drvs.items():
        input_drv_str = str(input_drv_path)
        input_basename = input_drv_str.rsplit("/", 1)[-1]
        input_hash_part = input_basename.split("-", 1)[0]
        input_nix_name = _nix_store_path_name(input_drv_str)
        input_drv_name = input_nix_name.removesuffix(".drv")

        for output_name in output_names:
            placeholder = downstream_placeholder_unknown_ca_output(
                input_hash_part,
                input_drv_name,
                output_name,
            )
            actual_path = resolved_output_paths.get(output_name)
            if actual_path is None:
                raise ValueError(f"No resolved path for {input_drv_path}!{output_name}")
            rewrites[placeholder] = str(actual_path)
            new_input_srcs.add(StorePath(path=str(actual_path)))

    # Create a BasicDerivation (copy from Derivation)
    resolved = BasicDerivation(
        outputs={
            o.name: DerivationOutput(
                path=o.path,
                method=o.hash_algo,
                hash_digest=o.hash_value,
            )
            for o in drv.outputs
        },
        input_srcs=new_input_srcs,
        platform=drv.platform,
        builder=_rewrite_strings(drv.builder, rewrites),
        args=[_rewrite_strings(a, rewrites) for a in drv.args],
        env={k: _rewrite_strings(v, rewrites) for k, v in drv.env.items()},
        is_dynamic=drv.is_dynamic,
    )

    # Compute hashDerivationModulo on the resolved derivation
    # (maskOutputs=true, which blanks output paths in the ATerm)
    hash_modulo = hash_derivation_modulo(resolved, mask_outputs=True)

    # Convert Deferred outputs to InputAddressed
    new_outputs: dict[str, DerivationOutput] = {}
    for name, o in resolved.outputs.items():
        if o.path == "" and o.method == "" and o.hash_digest == "":
            # Deferred output — compute the path
            h = hash_modulo[name]
            out_path = make_output_path(name, h, drv_name)
            new_outputs[name] = DerivationOutput(
                path=out_path,
                method="",
                hash_digest="",
            )
            resolved.env[name] = out_path
        else:
            new_outputs[name] = o

    resolved.outputs = new_outputs
    return resolved


async def main() -> None:
    # ── Verify Nix32 encoding against Nix test vectors ──
    print("=" * 70)
    print("Step 0: Verify Nix32 encoding against Nix test vectors")
    print("=" * 70)

    # Test vector from Nix: StorePath{"g1w7hy3qg1w7hy3qg1w7hy3qg1w7hy3q-foo.drv"}, output "out"
    # Expected placeholder: /0c6rn30q4frawknapgwq386zq358m8r6msvywcvc89n6m5p2dgbz
    placeholder = downstream_placeholder_unknown_ca_output(
        "g1w7hy3qg1w7hy3qg1w7hy3qg1w7hy3q",
        "foo",
        "out",
    )
    expected = "/0c6rn30q4frawknapgwq386zq358m8r6msvywcvc89n6m5p2dgbz"
    ok = "OK" if placeholder == expected else f"FAIL (got {placeholder})"
    print(f"  unknownCaOutput test vector: {ok}")

    assert placeholder == expected, f"Placeholder mismatch: {placeholder} != {expected}"

    # ── Step 1: Build against root store ──
    print()
    print("=" * 70)
    print("Step 1: Build against root store")
    print("=" * 70)

    root_path = STORE_PREFIX / "deferred-replay-root"
    rmtree_robust(root_path)

    root_store = LocalSocketStore(
        make_test_spec(store_id="deferred-replay-root", store_path=root_path, nix_config=CA_NIX_CONFIG),
    )
    await root_store.ensure_daemon()

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
    rc, stdout, _, _ = await run_subproc(cmd_ca, nix_config=CA_NIX_CONFIG)
    assert rc == 0, f"CA build failed: {stdout}"
    ca_out_path = stdout.strip()
    print(f"CA output: {ca_out_path}")

    cmd_def = [
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
    rc, stdout, _, _ = await run_subproc(cmd_def, nix_config=CA_NIX_CONFIG)
    assert rc == 0, f"Deferred build failed: {stdout}"
    deferred_out_path = stdout.strip()
    print(f"Deferred output: {deferred_out_path}")

    # ── Step 2: Get .drv paths ──
    print()
    print("=" * 70)
    print("Step 2: Get .drv paths and parse")
    print("=" * 70)

    eval_cmd = [
        NIX_BIN,
        "eval",
        "--store",
        str(root_path),
        "--impure",
        "--file",
        str(CA_NIX),
        "ca.simple.drvPath",
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
        "ca.non_ca_depends_on_ca.drvPath",
        "--raw",
    ]
    rc, stdout, _, _ = await run_subproc(eval_cmd2, nix_config=CA_NIX_CONFIG)
    deferred_drv_path = StorePath(stdout.strip())
    print(f"Deferred .drv path: {deferred_drv_path}")

    deferred_parsed = await read_drv_file(deferred_drv_path, root_store.layout)
    assert deferred_parsed is not None
    await read_drv_file(ca_drv_path, root_store.layout)

    print(f"\nDeferred .drv outputs: {deferred_parsed.output_paths()}")
    print(f"Deferred .drv input_drvs: {list(deferred_parsed.input_drvs.keys())}")
    for o in deferred_parsed.outputs:
        print(
            f"  output: name={o.name} path={o.path!r} hash_algo={o.hash_algo!r} hash_value={o.hash_value!r}",
        )

    # ── Step 3: Resolve the deferred derivation ──
    print()
    print("=" * 70)
    print("Step 3: Resolve the deferred derivation")
    print("=" * 70)

    # The resolved output paths come from the CA derivation's output
    resolved_output_paths: dict[str, StorePath] = {"out": StorePath(ca_out_path)}
    print(f"Resolved CA output paths: {resolved_output_paths}")

    # Compute the placeholder for the CA derivation's output (for debug)
    ca_basename = str(ca_drv_path).rsplit("/", 1)[-1]
    ca_hash_part = ca_basename.split("-", 1)[0]
    ca_nix_name = _nix_store_path_name(str(ca_drv_path))
    ca_drv_name_debug = ca_nix_name.removesuffix(".drv")

    placeholder_out = downstream_placeholder_unknown_ca_output(
        ca_hash_part,
        ca_drv_name_debug,
        "out",
    )
    print(f"Placeholder for CA.drv!out: {placeholder_out}")

    # Verify: the placeholder should appear in the deferred .drv's env/args
    found_in_env = placeholder_out in deferred_parsed.env.get("out", "")
    found_in_args = any(placeholder_out in a for a in deferred_parsed.args)
    print(f"Placeholder found in env['out']: {found_in_env}")
    print(f"Placeholder found in args: {found_in_args}")

    # Resolve
    resolved = resolve_derivation(
        deferred_parsed,
        deferred_drv_path,
        resolved_output_paths,
    )

    print("\nResolved BasicDerivation:")
    print(f"  input_srcs ({len(resolved.input_srcs)}):")
    for p in sorted(str(p) for p in resolved.input_srcs):
        print(f"    {p}")
    print("  outputs:")
    for name, output in resolved.outputs.items():
        print(f"    {name}: path={output.path!r} method={output.method!r} hash={output.hash_digest!r}")
    print(f"  env['out'] = {resolved.env.get('out', '!MISSING')!r}")

    # ── Step 4: Compare with Nix's result ──
    print()
    print("=" * 70)
    print("Step 4: Compare with Nix's output")
    print("=" * 70)

    resolved_out_path = resolved.outputs["out"].path
    print(f"  Our computed output: {resolved_out_path}")
    print(f"  Nix's output:        {deferred_out_path}")
    match = resolved_out_path == deferred_out_path
    print(f"  Match: {'YES' if match else 'NO'}")

    # Also compare the resolved .drv ATerm with what Nix produced
    resolved_aterm = _unparse_basic_derivation(resolved, mask_outputs=False)
    print("\n  Our resolved ATerm:")
    print(f"    {resolved_aterm}")

    # Read Nix's resolved .drv from root store
    root_db_path = root_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    root_conn = sqlite3.connect(str(root_db_path))
    resolved_rows = root_conn.execute(
        "SELECT path FROM ValidPaths WHERE path LIKE '%non-ca-depends-on-ca.drv' AND path != ?",
        (str(deferred_drv_path),),
    ).fetchall()
    root_conn.close()

    if resolved_rows:
        nix_resolved_drv_path = StorePath(resolved_rows[0][0])
        await read_drv_file(nix_resolved_drv_path, root_store.layout)
        nix_aterm_path = root_store.store_path / str(nix_resolved_drv_path).lstrip("/")
        nix_aterm = (await anyio.Path(nix_aterm_path).read_text()).strip()
        print("\n  Nix's resolved ATerm:")
        print(f"    {nix_aterm}")
        aterm_match = resolved_aterm == nix_aterm
        print(f"  ATerm match: {'YES' if aterm_match else 'NO'}")
    else:
        print("\n  No resolved .drv found in root store for comparison")

    # ── Step 5: Build via BuildDerivation with resolved derivation ──
    print()
    print("=" * 70)
    print("Step 5: Build via BuildDerivation with resolved derivation")
    print("=" * 70)

    test_path = STORE_PREFIX / "deferred-replay-resolve"
    rmtree_robust(test_path)

    test_store = LocalSocketStore(
        make_test_spec(store_id="deferred-replay-resolve", store_path=test_path, nix_config=CA_NIX_CONFIG),
    )
    await test_store.ensure_daemon()

    # Transfer needed paths: CA .drv + CA output + deferred .drv
    await stream_paths_store_to_store(
        root_store,
        test_store,
        {StorePath(ca_out_path)},
    )

    # Register CA realisation
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
        expected_retcode=None,
    )
    if rc == 0 and realisation_out.strip():
        realisations = json.loads(realisation_out)
        if realisations:
            reg_req = RegisterDrvOutputRequest(realisation=realisations[0])
            await test_store.call(reg_req, suppress_last=True)
            print("CA realisation registered!")

    # Send BuildDerivation — but we must point at the RESOLVED .drv path,
    # not the original deferred .drv path, because the daemon reads the .drv
    # from disk for hashDerivationModulo.
    #
    # The resolved .drv has a different store path (content-addressed).
    # We need to write it to the test store and then build it.
    #
    # Compute the resolved .drv store path (content-addressed by ATerm text hash)
    resolved_aterm = _unparse_basic_derivation(resolved, mask_outputs=False)
    resolved_aterm_hash = hashlib.sha256(resolved_aterm.encode()).digest()

    # The store path is computed via makeStorePath("text:sha256:<hex>", ...)
    # Actually in Nix, writeDerivation uses addToStoreFromDump which computes
    # a "text" store path. Let's just compute it:
    resolved_drv_store_path = make_store_path(
        "text:sha256:" + resolved_aterm_hash.hex(),
        resolved_aterm_hash,
        "non-ca-depends-on-ca.drv",
    )
    print(f"Computed resolved .drv store path: {resolved_drv_store_path}")

    # Write the resolved .drv to the test store's filesystem
    resolved_drv_fs_path = test_path / resolved_drv_store_path.lstrip("/")
    await anyio.Path(resolved_drv_fs_path).parent.mkdir(parents=True, exist_ok=True)
    await anyio.Path(resolved_drv_fs_path).write_text(resolved_aterm)

    # Register it with the daemon (AddToStore / valid path registration)
    # The simplest way: use nix store add-path or add the file via the daemon
    # Actually, we can use stream_paths_store_to_store to copy it from root_store
    # But we need it to be a valid path in the test store's DB too.
    # Let's copy the resolved .drv from the root store instead.
    if resolved_rows:
        nix_resolved_drv_path = StorePath(resolved_rows[0][0])
        print(f"Nix's resolved .drv path: {nix_resolved_drv_path}")
        # Transfer it to test store
        await stream_paths_store_to_store(
            root_store,
            test_store,
            {nix_resolved_drv_path},
        )

        build_req = BuildDerivationRequest(
            drv_path=nix_resolved_drv_path,
            derivation=resolved,
            build_mode=BuildMode.NORMAL,
        )
        print("\nSending BuildDerivation with RESOLVED .drv path")
        print(f"  drv_path: {nix_resolved_drv_path}")
        print(f"  output path: {resolved.outputs['out'].path}")
        try:
            resp = await test_store.call(build_req)
            print(f"\nBuildDerivation result: status={resp.result.status}")
            print(f"  error_msg: {resp.result.error_msg}")
            if resp.result.status == 0:
                print("  SUCCESS!")

                # Verify the output on disk

                outmap = await test_store.execute(QdomRequest(path=nix_resolved_drv_path))
                print(f"  Output map after build: {outmap.items}")

                # Read the output file
                out_fs = test_path / str(outmap.items.get("out", "")).lstrip("/")
                if out_fs.exists():
                    content = (await anyio.Path(out_fs).read_text()).strip()
                    print(f"  Output content: {content}")
                    expected_content = f"dep-on-{ca_out_path}"
                    print(f"  Content matches: {content == expected_content}")
                else:
                    print(f"  Output file not found at {out_fs}")
            else:
                print("  FAILURE!")
        except Exception as e:
            print(f"\nBuildDerivation EXCEPTION: {type(e).__name__}: {e}")
    else:
        print("  No resolved .drv to transfer from root store")

    await root_store.close()
    await test_store.close()


if __name__ == "__main__":
    asyncio.run(main())
