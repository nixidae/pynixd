"""With `sign_added_paths`, pynixd signs what an admin copies into its own store.

A store that others build in hands back what its admins pushed, and a client
that trusts only that store's key refused it: "lacks a signature by a trusted
key". `nix copy` sends `AddMultipleToStore`, which pynixd, like Nix, signed
never; `sign_added_paths` covered `AddToStore` alone, and found no key there.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.add_to_store_nar import AddToStoreNarRequest, AddToStoreNarResponse
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.nar_hash import NARHash
from pynixd import Server
from pynixd.config import PynixdSettings
from pynixd.exceptions import BackendError
from pynixd.op_routes import OpRoute
from pynixd.serde import StorePath
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.store import LocalDBStore
from tests._conftest.import_lab import IMPORT_OPS, NIX_STORE, client, info_of, nar_of
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc, ssh_user_uri

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from pynixd.connection import Connection

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


# A path already valid and unsigned, sent again. sg1, 2026-11-28: `nixkube-env`
# came in by an add that did not sign, `nix copy` skips a valid path, and the
# nodes refused it. `ekn kubeapply` (`storecheck.resend_unsigned`) sends it
# again with AddToStoreNar, which skips nothing on the client side.


@asynccontextmanager
async def _held_unsigned(tmp_path: Path) -> AsyncIterator[tuple[Server, str, Path, str]]:
    """pynixd holding a built leaf that an untrusted client added: the server, its unix URL, the source root, the leaf."""
    key = tmp_path / "secret"
    _rc, secret, _err, _both = await run_subproc([NIX_BIN, "key", "generate-secret", "--key-name", KEY_NAME])
    key.write_text(secret)  # noqa: ASYNC240 -- test setup
    source_root = tmp_path / "source"
    store = f"local?root={source_root}"
    _rc, out, _err, _both = await run_subproc(
        [NIX_BIN, "build", "--store", store, "--impure", "--no-link", "--print-out-paths", "--expr", LEAF]
    )
    leaf = out.strip()

    root = tmp_path / "pynixd" / "store"
    nix_config = for_test_store(substituters=())
    nix_config.secret_key_files = [str(key)]
    routes = dict.fromkeys(IMPORT_OPS, OpRoute.pynixd)
    spec = make_test_spec(store_id="local", store_path=root, no_probe=True, nix_config=nix_config, op_routes=routes)
    spec.settings = PynixdSettings(sign_added_paths=True)
    socket = tmp_path / "pynixd.sock"
    async with Server(
        stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=0, http_port=None
    ) as server:
        await _import(source_root, ssh_user_uri(server), leaf)
        yield server, f"unix://{socket}?root={root}", source_root, leaf


async def _import(source_root: Path, url: str, leaf: str) -> None:
    """`nix-store --export | nix-store --import`: AddToStoreNar (op 39) of a path, valid at *url* or not."""
    exported = (await anyio.run_process([NIX_STORE, "--store", f"local?root={source_root}", "--export", leaf])).stdout
    _rc, _out, _err, _both = await run_subproc([NIX_STORE, "--store", url, "--import"], stdin=exported)


async def _signers(url: str, leaf: str) -> list[str]:
    _rc, info, _err, _both = await run_subproc([NIX_BIN, "path-info", "--json", "--store", url, leaf])
    found = json.loads(info)
    entry = found[leaf] if isinstance(found, dict) else found[0]
    return [sig.split(":", 1)[0] for sig in entry.get("signatures", [])]


async def test_an_admin_s_import_signs_a_valid_path_an_untrusted_client_added(tmp_path: Path) -> None:
    async with _held_unsigned(tmp_path) as (server, url, source_root, leaf):
        assert await _signers(url, leaf) == [], "an untrusted client's add carries no signature (nixidae/nixidae#1009)"

        await run_subproc(
            [NIX_BIN, "copy", "--no-check-sigs", "--from", f"local?root={source_root}", "--to", url, leaf]
        )
        assert await _signers(url, leaf) == [], "`nix copy` skips a valid path, so it signs nothing"

        await _import(source_root, ssh_user_uri(server), leaf)
        assert await _signers(url, leaf) == [], "pynixd signs nothing an untrusted client sends"

        await _import(source_root, url, leaf)
        assert await _signers(url, leaf) == [KEY_NAME]


async def _send(conn: Connection, info: ValidPathInfo, nar: bytes) -> None:
    """One AddToStoreNar, raw: Nix's client always sends the info its own store holds."""
    await AddToStoreNarRequest(info=info, repair=0, dont_check_sigs=1).to_writer(WriteContext.from_conn(conn))
    framed = conn.w.framed()
    framed.write(nar)
    await framed.finalize()
    await AddToStoreNarResponse.from_reader(
        ReadContext(
            reader=conn.r,
            version=conn.version,
            buffer_logs=True,
            error_factory=BackendError,
            features=conn.standard_features,
        )
    )


@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"nar_hash": NARHash(hash="0" * 64)}, id="nar-hash"),
        pytest.param({"references": {StorePath(path=f"/nix/store/{'1' * 32}-other")}}, id="references"),
    ],
)
async def test_an_admin_s_import_with_other_info_leaves_the_valid_path_unsigned(
    tmp_path: Path, changes: dict[str, Any]
) -> None:
    """The signature covers the NAR hash and the references: another claim is not what pynixd holds."""
    async with _held_unsigned(tmp_path) as (_server, url, source_root, leaf):
        socket = Path(url.removeprefix("unix://").split("?", 1)[0])
        async with client(socket) as conn:
            await _send(conn, info_of(source_root, leaf, **changes), nar_of(source_root, leaf))
        assert await _signers(url, leaf) == []

        async with client(socket) as conn:
            await _send(conn, info_of(source_root, leaf), nar_of(source_root, leaf))
        assert await _signers(url, leaf) == [KEY_NAME], "the same session shape with the stored info signs"
