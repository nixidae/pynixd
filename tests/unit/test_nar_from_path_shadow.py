"""The NarFromPath shadow: the daemon's NAR goes to the client, pynixd's dump is compared with it.

The two are compared by SHA-256 and size and counted in
`pynixd_op_shadow_total`. A dump that fails counts as `error` and changes
nothing the client gets. The handler feeds the daemon's bytes through
`forward_raw`'s `on_chunk`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.nar_from_path import NarFromPathRequest
from pynixd.config import LocalSocketStoreSpec
from pynixd.handlers.nar_from_path import _Shadow
from pynixd.nar import NarRegular, write_nar
from pynixd.serde import StorePath
from pynixd.store.local_db import LocalDBStore

if TYPE_CHECKING:
    from pathlib import Path

NAME = "00000000000000000000000000000001-hello"
STORE = StoreId("shadow-test")


def _count(outcome: str) -> float:
    labels = {"store_id": str(STORE), "op": "NarFromPath", "outcome": outcome}
    return REGISTRY.get_sample_value("pynixd_op_shadow_total", labels) or 0.0


async def _shadow(root: Path, theirs: bytes, *, present: bool = True) -> None:
    if present:
        (root / "nix" / "store").mkdir(parents=True)
        (root / "nix" / "store" / NAME).write_bytes(b"hello")  # noqa: ASYNC240 -- test setup
    store = LocalDBStore(LocalSocketStoreSpec(store_id=STORE, store_path=root))
    shadow = _Shadow(store, NarFromPathRequest(path=StorePath(path=f"/nix/store/{NAME}")))
    await shadow.dump()
    shadow.theirs.update(theirs)
    shadow.compare(len(theirs))


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("theirs", "present", "outcome"),
    [
        (write_nar(NarRegular(b"hello")), True, "match"),
        (write_nar(NarRegular(b"hellO")), True, "mismatch"),
        (write_nar(NarRegular(b"hello", executable=True)), True, "mismatch"),
        (write_nar(NarRegular(b"hello")), False, "error"),
    ],
    ids=["same", "other-bytes", "other-size", "dump-fails"],
)
async def test_the_shadow_counts_how_the_two_compare(
    tmp_path: Path, theirs: bytes, present: bool, outcome: str
) -> None:
    before = _count(outcome)
    await _shadow(tmp_path, theirs, present=present)
    assert _count(outcome) == before + 1
