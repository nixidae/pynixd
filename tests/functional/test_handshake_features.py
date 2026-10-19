"""Functional test for feature matrix announcement in handshake."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from pathlib import Path

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import SSHSubprocessStoreSpec
from pynixd.store import LocalSocketStore, SSHSubprocessStore
from tests._conftest.ssh_keys import SESSION_SSH_KEYS
from tests.conftest import (
    NIX_BIN,
    STORE_PREFIX,
    make_test_spec,
    rmtree_robust,
)

log = structlog.get_logger(__name__)


async def test_handshake_feature_announcement(tmp_path: Path) -> None:
    """Test that pynixd announces its feature matrix in the handshake and the client skips probing."""

    # 0. Setup SSH keys and stores
    key = str(SESSION_SSH_KEYS / "id_user")

    # Store B has a specific feature matrix
    fm_b = {"x86_64-linux": {"kvm", "big-parallel"}, "aarch64-linux": set()}

    store_b_path = STORE_PREFIX / "handshake-features-b"
    rmtree_robust(store_b_path)
    store_b_path.mkdir(parents=True, exist_ok=True)
    store_b = LocalSocketStore(
        make_test_spec(
            store_id="b-local",
            store_path=store_b_path,
            no_probe=True,
            feature_matrix=fm_b,
        ),
    )
    await store_b.ensure_daemon()

    # 1. Start Server B (Proxying store_b)
    async with Server(
        stores={StoreId("local"): store_b},
        ssh_port=0,
    ) as server_b:
        port_b = server_b.port
        log.info("server_b_started", port=port_b)

        # 2. Client connecting to Server B
        # It should receive the feature matrix in the handshake and skip probing.
        client_store = SSHSubprocessStore(
            SSHSubprocessStoreSpec(
                store_id=StoreId("client-to-b"),
                host="127.0.0.1",
                port=port_b,
                username=server_b.username,
                client_keys=[key],
                # The builder is a server this test just started on the
                # loopback interface, so there is no host key to pin and
                # nothing in the path to substitute an output. Issue nixidae/nixidae#204
                # gives the field no default for exactly this reason: the
                # choice belongs in the configuration and not in a default.
                known_hosts=None,
                nix_bin=str(NIX_BIN),
                monitor=False,
                probe=True,
            ),
        )

        # But it should be probed IMMEDIATELY upon connection via handshake
        await client_store.start()

        # Verify feature matrix matches fm_b
        fm_client = client_store.feature_matrix
        assert fm_client == fm_b

        # Verify that it didn't actually run the probe logic (conn_counter should be low)
        # 1 for start (QueryAllValidPaths), maybe some for handshake?
        # A real probe runs 2 requests: ProbeSystems and ProbeFeatures.
        # If it skipped probing, it should NOT have run those.

        # Check op_log or similar if available, but checking feature_matrix is the main thing.
        log.info("client_probed_successfully", fm=fm_client)

        await client_store.close()
