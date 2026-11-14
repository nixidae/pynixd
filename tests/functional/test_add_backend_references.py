"""An add whose reference only a backend holds: pynixd pulls the reference first. nixidae/nixidae#1351.

`ctx.output_locations` makes pynixd answer "valid" for a path a backend
built, also after the local store lost it. `nix copy` then skips that path
and sends only its referrer, which the local store refuses while the
reference is missing there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store import LocalDBStore, Store
from tests._conftest.import_lab import IA, IMPORT_OPS, REFS, VALID_PATHS, lab, rows, run
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec

if TYPE_CHECKING:
    from pathlib import Path

BUILDER = StoreId("builder")


def _store(store_id: str, root: Path, route: str) -> LocalDBStore:
    return LocalDBStore(
        make_test_spec(
            store_id=store_id,
            store_path=root,
            no_probe=True,
            nix_config=for_test_store(substituters=()),
            op_routes=dict.fromkeys(IMPORT_OPS, route),
        )
    )


@pytest.mark.parametrize("route", ["pynixd", "daemon"])
async def test_a_copy_lands_when_a_reference_lives_only_on_a_backend(tmp_path: Path, route: str) -> None:
    a, _, _ = await lab(tmp_path)
    [tree] = [ref for referrer, ref in rows(a, REFS) if referrer == IA and ref != IA]
    b = tmp_path / "b"
    socket = tmp_path / "pynixd.sock"
    stores: dict[StoreId, Store] = {StoreId("local"): _store("local", b, route), BUILDER: _store("builder", a, route)}
    async with Server(stores=stores, unix_path=socket, ssh_port=None, http_port=None) as server:
        server.ctx.output_locations[tree] = BUILDER
        await run(str(NIX_BIN), "copy", "--from", str(a), "--to", f"unix://{socket}?root={b}", "--no-check-sigs", IA)

    assert {path for path, *_ in rows(b, VALID_PATHS)} == {IA, tree}
