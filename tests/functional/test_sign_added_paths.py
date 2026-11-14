"""With `sign_added_paths`, pynixd signs what an admin copies into its own store.

A store that others build in hands back what its admins pushed, and a client
that trusts only that store's key refused it: "lacks a signature by a trusted
key". `nix copy` sends `AddMultipleToStore`, which pynixd, like Nix, signed
never; `sign_added_paths` covered `AddToStore` alone, and found no key there.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import PynixdSettings
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

LEAF = (
    'derivation { name = "sign-added-leaf"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo leaf > $out" ]; }'
)
KEY_NAME = "sign-added-1"


async def _copied(tmp_path: Path, *, sign_added_paths: bool, via: str = "copy") -> list[str]:
    """Build an input-addressed path in a plain store, copy it into pynixd, and answer its signatures there."""
    key = tmp_path / "secret"
    _rc, secret, _err, _both = await run_subproc([NIX_BIN, "key", "generate-secret", "--key-name", KEY_NAME])
    key.write_text(secret)
    source = f"local?root={tmp_path / 'source'}"
    _rc, out, _err, _both = await run_subproc(
        [NIX_BIN, "build", "--store", source, "--impure", "--no-link", "--print-out-paths", "--expr", LEAF]
    )
    leaf = out.strip()

    root = tmp_path / "pynixd" / "store"
    nix_config = for_test_store(substituters=())
    nix_config.secret_key_files = [str(key)]
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=nix_config,
        op_routes={op: OpRoute.pynixd for op in ("AddMultipleToStore", "AddToStoreNar")},
    )
    spec.settings = PynixdSettings(sign_added_paths=sign_added_paths)
    socket = tmp_path / "pynixd.sock"
    url = f"unix://{socket}?root={root}"
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        if via == "add":
            # `nix store add` sends AddToStoreNar of a content-addressed path.
            text = tmp_path / "added.txt"
            text.write_text("added\n")
            _rc, added, _err, _both = await run_subproc(
                [NIX_BIN, "store", "add", "--store", url, "--mode", "flat", str(text)]
            )
            leaf = added.strip()
        elif via == "import":
            # `nix-store --import` sends AddToStoreNar, one path at a time.
            nix_store = NIX_BIN.with_name("nix-store")
            export = f"{nix_store} --store '{source}' --export {leaf} | {nix_store} --store '{url}' --import"
            await run_subproc(["/bin/sh", "-c", export])
        else:
            await run_subproc([NIX_BIN, "copy", "--no-check-sigs", "--from", source, "--to", url, leaf])
        _rc, info, _err, _both = await run_subproc([NIX_BIN, "path-info", "--json", "--store", url, leaf])
    found = json.loads(info)
    entry = found[leaf] if isinstance(found, dict) else found[0]
    return list(entry.get("signatures", []))


async def test_an_admin_s_copy_carries_the_store_s_signature(tmp_path: Path) -> None:
    sigs = await _copied(tmp_path, sign_added_paths=True)

    assert [sig.split(":", 1)[0] for sig in sigs] == [KEY_NAME], sigs


async def test_without_the_setting_a_copy_stays_unsigned(tmp_path: Path) -> None:
    """Nix's own behaviour, the default."""
    sigs = await _copied(tmp_path, sign_added_paths=False)

    assert sigs == [], sigs


async def test_an_admin_s_import_carries_the_store_s_signature(tmp_path: Path) -> None:
    sigs = await _copied(tmp_path, sign_added_paths=True, via="import")

    assert [sig.split(":", 1)[0] for sig in sigs] == [KEY_NAME], sigs


async def test_an_admin_s_add_carries_the_store_s_signature(tmp_path: Path) -> None:
    """AddToStore (op 7) is left alone: a dump is content-addressed, so its path vouches for it."""
    sigs = await _copied(tmp_path, sign_added_paths=True, via="add")

    assert [sig.split(":", 1)[0] for sig in sigs] == [KEY_NAME], sigs
