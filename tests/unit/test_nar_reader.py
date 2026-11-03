"""`NarReader` on the `pynixd` route: the dump of the store's files, held to the size its consumer promised.

An HTTP cache has sent `Content-Length` and a copy has framed the path info
before the first NAR byte, both from the path info's `narSize`. A dump of
another size must fail and name the path, not end short or run long.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec
from pynixd.exceptions import BackendError
from pynixd.nar import NarRegular, write_nar
from pynixd.op_routes import OpRoute
from pynixd.serde import StorePath
from pynixd.store.local_db import LocalDBStore
from pynixd.store.nar_reader import NarReader

if TYPE_CHECKING:
    from pathlib import Path

NAME = "00000000000000000000000000000001-hello"
PATH = StorePath(path=f"/nix/store/{NAME}")
NAR = write_nar(NarRegular(b"hello"))


def _store(root: Path) -> LocalDBStore:
    (root / "nix" / "store").mkdir(parents=True)
    (root / "nix" / "store" / NAME).write_bytes(b"hello")
    store = LocalDBStore(
        LocalSocketStoreSpec(store_id=StoreId("nar-reader"), store_path=root, op_routes={"NarFromPath": OpRoute.pynixd})
    )

    def _no_daemon() -> None:
        raise AssertionError("the pynixd route opened a daemon connection")

    store.transfer_conn = _no_daemon  # type: ignore[method-assign,assignment]  # the route must not reach it
    return store


async def _read(store: LocalDBStore, size: int) -> bytes:
    async with NarReader(store) as nars:
        return b"".join([piece async for piece in nars.read(PATH, size)])


@pytest.mark.anyio
async def test_the_dump_is_the_nar(tmp_path: Path) -> None:
    assert await _read(_store(tmp_path), len(NAR)) == NAR


@pytest.mark.anyio
@pytest.mark.parametrize("size", [len(NAR) - 1, len(NAR) + 1], ids=["shorter", "longer"])
async def test_a_dump_of_another_size_fails_naming_the_path(tmp_path: Path, size: int) -> None:
    with pytest.raises(BackendError, match=f"the NAR of '{PATH}' broke off"):
        await _read(_store(tmp_path), size)


@pytest.mark.anyio
async def test_no_size_is_refused(tmp_path: Path) -> None:
    with pytest.raises(BackendError, match="no NAR size"):
        await _read(_store(tmp_path), 0)
