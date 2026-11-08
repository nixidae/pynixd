"""pynixd's own NAR reads take the route of NarFromPath (op 38).

A copy out of a local store reads each NAR through `NarReader`. The daemon
can always serve a local path, so a correct copy proves nothing about who
read it: the source daemon's transfer pool tells. On `pynixd` it moves no
byte; on `shadow` it sends the NAR, and the shadow compares the dump with it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from prometheus_client import REGISTRY

from pynixd.op_routes import OpRoute
from pynixd.serde import IsValidPathRequest
from pynixd.store import LocalDBStore, LocalSocketStore
from pynixd.store.transfer import stream_paths_store_to_store
from pynixd_store.store_path import StorePath
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from pathlib import Path


def _shadow_matches(store_id: str) -> float:
    labels = {"store_id": store_id, "op": "NarFromPath", "outcome": "match"}
    return REGISTRY.get_sample_value("pynixd_op_shadow_total", labels) or 0.0


@pytest.mark.parametrize("route", [OpRoute.pynixd, OpRoute.shadow])
async def test_a_copy_reads_the_source_on_the_route_of_op_38(tmp_path: Path, route: OpRoute) -> None:
    src_root, dst_root = tmp_path / "src", tmp_path / "dst"
    text = tmp_path / "payload.txt"
    text.write_text("a NAR that pynixd reads on the route of op 38\n")  # noqa: ASYNC240 -- test setup
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "store", "add-path", "--store", str(src_root), str(text)], verbose=False
    )
    path = StorePath(stdout.strip())
    store_id = f"nar-reader-{route}"

    src = LocalDBStore(
        make_test_spec(store_id=store_id, store_path=src_root, no_probe=True, op_routes={"NarFromPath": route})
    )
    dst = LocalSocketStore(make_test_spec(daemon=True, store_id=f"{store_id}-dst", store_path=dst_root, no_probe=True))
    async with src, dst:
        _, written_before = src.daemon_client.transfer_pool.traffic_totals()
        matches_before = _shadow_matches(store_id)
        await stream_paths_store_to_store(src, dst, [path])
        _, written_after = src.daemon_client.transfer_pool.traffic_totals()
        valid = await dst.execute(IsValidPathRequest(path=StorePath(path=str(path))))

    # The destination daemon checks the NAR hash of what it imports.
    assert valid.valid
    if route is OpRoute.pynixd:
        assert written_after == written_before
        assert _shadow_matches(store_id) == matches_before
    else:
        assert written_after > written_before
        assert _shadow_matches(store_id) == matches_before + 1
