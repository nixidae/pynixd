"""A store spec with `daemon = false`: pynixd answers every operation, and starts and reaches no daemon."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec
from pynixd.exceptions import NoDaemonError
from pynixd.op_routes import OpRoute
from pynixd.serde import IsValidPathRequest, StorePath
from pynixd.store.local_db import LocalDBStore


def _spec(**update: Any) -> LocalSocketStoreSpec:
    return LocalSocketStoreSpec(
        **{"store_id": StoreId("local"), "store_path": Path("/s"), "daemon": False, "pynixd_writes": True, **update}
    )


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"use_db": False}, "daemon = false needs use_db"),
        ({"pynixd_writes": False}, "daemon = false needs pynixd_writes"),
        ({"managed": False}, "daemon = false needs a store pynixd manages"),
        ({"op_routes": {"NarFromPath": "shadow"}}, "every route is pynixd, not for NarFromPath"),
    ],
)
def test_a_store_with_no_daemon_refuses_what_needs_one(update: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        _spec(**update)


def test_every_answer_takes_the_pynixd_route() -> None:
    store = LocalDBStore(_spec())
    assert set(store.routes.values()) == {OpRoute.pynixd}


async def test_the_daemon_client_refuses_by_name_and_starts_nothing() -> None:
    store = LocalDBStore(_spec())
    await store._daemon.ensure_daemon()
    assert store._daemon.daemon_proc is None
    with pytest.raises(NoDaemonError, match="no answer of its own to IsValidPathRequest"):
        await store.call(IsValidPathRequest(path=StorePath(path="/nix/store/" + "a" * 32 + "-x")))
    with pytest.raises(NoDaemonError, match="a connection to the daemon"):
        await store.create_conn()
