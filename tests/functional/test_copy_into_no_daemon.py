"""A copy into a store with no daemon: pynixd imports the closure itself, and Nix verifies what it wrote."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pynixd.store import LocalDBStore
from pynixd.store.transfer import stream_paths_store_to_store
from pynixd_store.store_path import StorePath
from tests._conftest.config import TEST_OP_ROUTES
from tests._conftest.import_lab import REFS, rows
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

EXPR = 'let r = builtins.toFile "r" "x"; in [ r (builtins.toFile "t" "refers to ${r}") ]'


async def _nix(*args: str) -> str:
    _rc, stdout, _stderr, _both = await run_subproc([str(NIX_BIN), *args], verbose=False)
    return stdout.strip()


async def test_the_destination_imports_the_closure_and_nix_verifies_it(tmp_path: Path) -> None:
    src_root, dst_root = tmp_path / "src", tmp_path / "dst"
    reference, text = json.loads(await _nix("eval", "--store", str(src_root), "--json", "--expr", EXPR))
    src = LocalDBStore(make_test_spec(store_id="copy-src", store_path=src_root, no_probe=True))
    dst = LocalDBStore(
        make_test_spec(
            store_id="copy-dst",
            store_path=dst_root,
            no_probe=True,
            daemon=False,
            op_routes={op: "pynixd" for op in TEST_OP_ROUTES},
        )
    )
    async with src, dst:
        await stream_paths_store_to_store(src, dst, [StorePath(text)])

    assert [ref for referrer, ref in rows(dst_root, REFS) if referrer == text] == [reference]
    # `nix store verify` hashes each NAR against the database; it fails on a path pynixd wrote wrong.
    await _nix("store", "verify", "--store", str(dst_root), "--no-trust", text, reference)
