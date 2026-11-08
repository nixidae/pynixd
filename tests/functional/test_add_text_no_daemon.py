"""`LocalDBStore.add_text_to_store` on a store with no daemon: the path `builtins.toFile` gives, in the store's database."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store import LocalDBStore, Store
from tests._conftest.config import TEST_OP_ROUTES
from tests._conftest.import_lab import REFS, rows
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

#: `toFile` puts the reference's path in the text, and that makes it a reference.
EXPR = 'let r = builtins.toFile "r" "x"; in [ r (builtins.toFile "t" "refers to ${r}") ]'


async def _nix(*args: str) -> str:
    _rc, stdout, _stderr, _both = await run_subproc([str(NIX_BIN), *args], verbose=False)
    return stdout.strip()


async def test_a_text_file_takes_the_path_nix_gives_it(tmp_path: Path) -> None:
    reference, text = json.loads(await _nix("eval", "--store", str(tmp_path / "nix"), "--json", "--expr", EXPR))
    store_path = tmp_path / "store"
    store_path.mkdir()
    spec = make_test_spec(
        store_id="local",
        store_path=store_path,
        no_probe=True,
        daemon=False,
        op_routes={op: "pynixd" for op in TEST_OP_ROUTES},
    )
    store = LocalDBStore(spec)
    stores: dict[StoreId, Store] = {StoreId("local"): store}
    async with Server(stores=stores, ssh_port=None, http_port=None):
        assert await store.add_text_to_store("r", "x", set()) == reference
        assert await store.add_text_to_store("t", f"refers to {reference}", {reference}) == text

    (ca,) = rows(store_path, f"select ca from ValidPaths where path = '{text}'")[0]
    assert ca.startswith("text:sha256:")
    assert [ref for referrer, ref in rows(store_path, REFS) if referrer == text] == [reference]
