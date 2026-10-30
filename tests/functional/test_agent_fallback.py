"""A builder that cannot take `pynixd-agent` is polled over SFTP. Issue #98.

pynixd's own SSH server is that builder: its SFTP subsystem serves only the
monitoring files, so the upload fails, and the store falls back to reading
those files.
"""

from __future__ import annotations

import platform
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import anyio

from nix_daemon_protocol.ids import StoreId
from pynixd.config import PressureSettings, SSHSubprocessStoreSpec
from pynixd.monitor import GenericResourcePoller
from pynixd.pressure import LimitReason
from pynixd.store import SSHSubprocessStore
from tests._conftest.config import NIX_BIN
from tests._conftest.ssh_keys import SESSION_SSH_KEYS

if TYPE_CHECKING:
    from pynixd import Server


async def test_a_refused_upload_falls_back_to_sftp(pynixd_server: Server) -> None:
    binary = shutil.which("pynixd-agent")
    assert binary is not None, "pynixd-agent is not on PATH"
    system = f"{platform.machine()}-linux"
    store = SSHSubprocessStore(
        SSHSubprocessStoreSpec(
            store_id=StoreId("agent-fallback"),
            host="127.0.0.1",
            port=pynixd_server.port,
            username="test",
            client_keys=[str(SESSION_SSH_KEYS / "id_user")],
            known_hosts=None,
            nix_bin=str(NIX_BIN),
            systems={system},
            probe=False,
            pressure=PressureSettings(interval=0.2, agents={system: Path(binary)}),
        ),
    )
    async with store:
        with anyio.fail_after(20):
            while store.build_limit.sampled_at is None:
                await anyio.sleep(0.1)
        assert isinstance(store.monitor, GenericResourcePoller)
        assert store.build_limit.reason is not LimitReason.unmonitored
