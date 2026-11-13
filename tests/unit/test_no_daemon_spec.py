"""A store spec with `daemon = false`: pynixd answers every operation, and starts and reaches no daemon."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from nix_daemon_protocol.constants import FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS, FEATURE_REALISATION_WITH_PATH
from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec
from pynixd.exceptions import NoDaemonError
from pynixd.op_routes import OpRoute
from pynixd.serde import IsValidPathRequest, StorePath
from pynixd.store.local_daemon import LocalStore
from pynixd.store.local_db import LocalDBStore
from pynixd.store.no_daemon import NoDaemonStore


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


def test_the_mode_picks_the_daemon_side_of_the_store() -> None:
    assert type(LocalDBStore(_spec())._daemon) is NoDaemonStore
    assert type(LocalDBStore(_spec(daemon=True))._daemon) is LocalStore


def test_each_daemon_side_refuses_the_other_mode() -> None:
    with pytest.raises(ValueError, match="LocalStore cannot serve a store whose mode is daemonless"):
        LocalStore(_spec())
    with pytest.raises(ValueError, match="NoDaemonStore cannot serve a store whose mode is shared"):
        NoDaemonStore(_spec(daemon=True))


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


def test_the_store_offers_what_nix_2_35_offers() -> None:
    """pynixd reads realisations from `BuildTraceV3`, the table of the schema `open_local_store` creates."""
    assert LocalDBStore(_spec()).features == {
        FEATURE_REALISATION_WITH_PATH,
        FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS,
    }
