"""Functional test for pynixd extension delegation between servers."""

from __future__ import annotations

from pathlib import Path

import structlog

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import SSHSubprocessStoreSpec
from pynixd.daemon_extensions.query_path_infos import QueryPathInfosRequest
from pynixd.serde import QueryAllValidPathsRequest
from pynixd.store import LocalSocketStore, SSHSubprocessStore
from pynixd.store_path import StorePath
from tests._conftest.ssh_keys import SESSION_SSH_KEYS
from tests.conftest import (
    NIX_BIN,
    STORE_PREFIX,
    make_test_spec,
    rmtree_robust,
    run_subproc,
)

log = structlog.get_logger(__name__)


async def test_extension_delegation(tmp_path: Path) -> None:
    """Test that pynixd can delegate extension OPs to other pynixd instances.

    Store operations triggered:
    - QueryAllValidPaths: Queries all valid paths
    - QueryPathInfo: Queries path info
    - QueryPathInfos: Queries path infos
    """

    # 0. Setup SSH keys and stores
    key = str(SESSION_SSH_KEYS / "id_user")

    store_b_path = STORE_PREFIX / "extension-delegation-store-b"
    rmtree_robust(store_b_path)
    store_b_path.mkdir(parents=True, exist_ok=True)
    store_b = LocalSocketStore(
        make_test_spec(
            store_id="b-local",
            store_path=store_b_path,
            no_probe=True,
        ),
    )
    await store_b.ensure_daemon()

    # Add a path to store B
    cmd = [
        str(NIX_BIN),
        "store",
        "add-path",
        "--store",
        str(store_b_path),
        "README.md",
    ]
    rc, stdout, stderr, _ = await run_subproc(cmd)
    assert rc == 0
    path = StorePath(stdout.strip())

    # 1. Start Server B (Builder)
    async with Server(
        stores={StoreId("local"): store_b},
        ssh_port=0,
    ) as server_b:
        port_b = server_b.port
        log.info("server_b_started", port=port_b)

        # 2. Start Server A (Proxy)
        # It has server_b as a store.
        store_a_b = SSHSubprocessStore(
            SSHSubprocessStoreSpec(
                store_id=StoreId("builder-b"),
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
            ),
        )

        # Server A's local store doesn't have a DB and doesn't support extensions
        store_a = LocalSocketStore(
            make_test_spec(
                store_id="a-local",
                store_path=Path("/"),
                no_probe=True,
                use_db=False,
            ),
        )

        unix_path_a = tmp_path / "server-a.sock"

        async with Server(
            stores={StoreId("local"): store_a, StoreId("builder-b"): store_a_b},
            ssh_port=0,
            unix_path=unix_path_a,
        ) as server_a:
            log.info("server_a_started", port=server_a.port, unix=unix_path_a)

            # Trigger feature detection
            await store_a_b.execute(QueryAllValidPathsRequest())

            log.info("server_b_features", features=store_a_b.features)
            assert "QueryPathInfos" in store_a_b.features

            # Now try to execute QueryPathInfos on store_a_b
            req = QueryPathInfosRequest(paths={path})
            resp = await store_a_b.execute(req)

            assert any(str(info.path) == str(path) for info in resp.infos)
            log.info("query_path_infos_success", path=path)

            # 3. Verify feature gating: store_a (local daemon at /) doesn't have
            # the extension, so the executor decomposes locally into N QueryPathInfo calls.
            # store_a_b (SSH to pynixd) has the feature, so it sends a single wire message.
            assert "QueryPathInfos" not in store_a.features
            assert "QueryPathInfos" in store_a_b.features
            log.info("feature_gating_verified")
