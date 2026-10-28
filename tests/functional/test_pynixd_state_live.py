"""PynixdState (op 111) through real daemons. Issue #81.

Unit tests pin the collector, the merge and the wire shape; these two run
the operation through a live daemon instead. The federated one is the
macbook trial in miniature: a builder dials the controller over reverse
SSH, and the controller's merged answer carries the builder's sections.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import LocalSocketStoreSpec, PynixdSettings
from pynixd.daemon_extensions.pynixd_state import PynixdStateRequest
from pynixd.store import LocalSocketStore
from tests.conftest import STORE_PREFIX, make_test_spec, rmtree_robust
from tests.functional.test_reverse_store import (
    _builder_settings,
    _controller_settings,
    _keypair,
    _wait_for_builder,
)

if TYPE_CHECKING:
    from pathlib import Path


async def _query(
    socket_path: Path,
    wants: list[str],
    federated: bool,
) -> dict[str, Any]:
    """Ask a live daemon for state, over its unix socket like the CLI does."""
    client = LocalSocketStore(
        LocalSocketStoreSpec(
            store_id=StoreId("cli"),
            socket_path=socket_path,
            probe=False,
            monitor=False,
        ),
    )
    await client.start(sync_paths=False)
    try:
        resp = await client.execute(PynixdStateRequest(wants=wants, federated=federated))
        state: dict[str, Any] = json.loads(resp.payload)
        return state
    finally:
        await client.close()


async def test_state_local_sections_through_a_live_daemon(tmp_path: Path) -> None:
    """A live daemon answers queue, stores and sessions. Issue #81."""
    _builder_priv, builder_pub = _keypair(tmp_path, "builder")
    ctrl_settings, ctrl_local = _local_controller(tmp_path, builder_pub)

    async with Server(
        stores={StoreId("local"): ctrl_local},
        settings=ctrl_settings,
    ):
        assert ctrl_settings.unix_path is not None
        payload = await _query(ctrl_settings.unix_path, [], False)

    assert payload["queue"] == {"scheduler": True, "pending": 0, "building": 0, "done": 0}
    assert payload["stores"]["local"]["healthy"] is True
    assert "x86_64-linux" in payload["stores"]["local"]["systems"]
    assert "unix" in payload["sessions"]
    assert payload["transfers"]["bytes_received"] >= 0
    assert set(payload["totals"]) == {"builds_completed", "sessions_accepted"}


async def test_state_federated_merges_a_registered_builder(tmp_path: Path) -> None:
    """The controller's answer carries the dialled builder's sections. Issue #81."""
    builder_store_id = "state-builder"
    builder_priv, builder_pub = _keypair(tmp_path, "builder")
    ctrl_settings, ctrl_local = _local_controller(tmp_path, builder_pub)

    async with Server(
        stores={StoreId("local"): ctrl_local},
        settings=ctrl_settings,
    ) as controller:
        if controller.reverse_acceptor is None:
            raise AssertionError("Reverse acceptor did not start")
        acceptor_port = controller.reverse_acceptor.get_port()

        builder_settings, builder_local = _builder_settings(tmp_path, acceptor_port, builder_priv, builder_store_id)
        builder = Server(
            stores={StoreId("local"): builder_local},
            settings=builder_settings,
        )
        await builder.start()

        try:
            await _wait_for_builder(controller, builder_store_id)
            assert ctrl_settings.unix_path is not None
            payload = await _query(ctrl_settings.unix_path, ["queue", "stores"], True)
        finally:
            await builder.close()
            rmtree_robust(STORE_PREFIX / builder_store_id)
            rmtree_robust(STORE_PREFIX / "state-controller")

    merged = payload["federated"][builder_store_id]
    assert merged["queue"] == {"scheduler": True, "pending": 0, "building": 0, "done": 0}
    assert "x86_64-linux" in merged["stores"]["local"]["systems"]


def _local_controller(tmp_path: Path, builder_pub: Path) -> tuple[PynixdSettings, LocalSocketStore]:
    """Controller settings plus a plain local store, for state queries."""
    store_path = STORE_PREFIX / "state-controller"
    rmtree_robust(store_path)
    local = LocalSocketStore(
        make_test_spec(store_id="local", store_path=store_path, no_probe=True),
    )
    return _controller_settings(tmp_path, builder_pub), local
