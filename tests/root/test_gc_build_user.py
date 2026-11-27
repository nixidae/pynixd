"""A build as an allocated build user, while a collection runs with no nix-daemon, keeps its output.

`tests/functional/test_gc_concurrent_writes.py` holds the collection the same way; only root allocates
build users (`auto-allocate-uids`, `unix/user-lock.cc:136`). nixidae/nixidae#1440.
"""

from __future__ import annotations

import os
from pathlib import Path

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store import LocalDBStore
from pynixd_store.collect import SYNC_AFTER_ROOTS
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc
from tests.functional.test_gc_concurrent_writes import _add, _collect, _held, _valid
from tests.root.test_auto_allocate_uids import IDS_PER_BUILD, START_ID

HERE = Path("/var/lib/pynixd-root-tests/gc-build-user")

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="build users need root")

BUILT = (
    'derivation { name = "gc-build-user"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "read u < /proc/self/uid_map; echo $u > $out" ]; }'
)


async def test_a_build_user_s_build_during_a_collection_keeps_its_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rmtree_robust(HERE)
    fifo = tmp_path / "gc-sync"
    os.mkfifo(fifo)
    monkeypatch.setenv(SYNC_AFTER_ROOTS, str(fifo))
    root, socket = HERE / "store", tmp_path / "pynixd.sock"
    nix_config = for_test_store(substituters=(), experimental_features=("auto-allocate-uids",))
    nix_config.auto_allocate_uids = True
    nix_config.start_id = START_ID
    nix_config.id_count = IDS_PER_BUILD
    spec = make_test_spec(store_id="local", store_path=root, no_probe=True, daemon=False, nix_config=nix_config)
    url = f"unix://{socket}?root={root}"
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        before = await _add(url, tmp_path, "before")
        async with anyio.create_task_group() as tg:
            tg.start_soon(_collect, url)
            async with _held(fifo):
                _rc, out, _err, _both = await run_subproc(
                    [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "--expr", BUILT]
                )
        valid = await _valid(url)
    built = out.strip()
    assert before not in valid, "the collection deletes a path no root keeps"
    assert built in valid
    maps = (root / built.lstrip("/")).read_text().split()
    assert maps == ["1000", str(START_ID), "1"], "the build ran as slot 0's allocated uid"
