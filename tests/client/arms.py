"""The two daemons one example compares: `nix daemon` and pynixd, each on an empty chroot store.

Both stores are chroots (`local?root=...`), so both print paths under the
logical `/nix/store`, and the same input names the same path in both. Measured
with `nix store add-file` against a `nix daemon --store local?root=R` socket.
"""

from __future__ import annotations

import os
import signal
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import anyio

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store.local_db import LocalDBStore
from tests._conftest.config import NIX_BIN, make_test_spec
from tests._conftest.constants import STORE_PREFIX
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from anyio.abc import Process

# Short: a store root carries a daemon socket under it, and `sun_path` holds 108 bytes.
CLIENT_PREFIX = STORE_PREFIX.parent / f"{STORE_PREFIX.name}-client"
SOCKET_WAIT = 30
STOP_WAIT = 10


@dataclass(frozen=True, slots=True)
class Arms:
    control: Path
    pynixd: Path


async def _socket(path: Path) -> None:
    # `nix daemon` says nothing when it listens; the socket file is the only sign.
    with anyio.fail_after(SOCKET_WAIT):
        while not await anyio.Path(path).is_socket():
            await anyio.sleep(0.02)


async def _stop(process: Process) -> None:
    with anyio.CancelScope(shield=True), anyio.move_on_after(STOP_WAIT) as scope:
        if process.returncode is None:
            process.send_signal(signal.SIGTERM)
        await process.wait()
    if scope.cancelled_caught:
        process.kill()


@asynccontextmanager
async def control_daemon(root: Path) -> AsyncIterator[Path]:
    config = for_test_store()
    socket = root.parent / f"{root.name}.sock"
    process = await anyio.open_process(
        [str(NIX_BIN), "daemon", "--store", f"local?root={root}", *config.to_daemon_args()],
        env={**os.environ, "NIX_DAEMON_SOCKET_PATH": str(socket), "NIX_CONFIG": config.to_nix_config_env()},
        stdin=None,
        stdout=None,
        stderr=None,
    )
    try:
        await _socket(socket)
        yield socket
    finally:
        await _stop(process)


@asynccontextmanager
async def pynixd_daemon(root: Path) -> AsyncIterator[Path]:
    socket = root.parent / f"{root.name}.sock"
    store = LocalDBStore(make_test_spec(store_id=root.name, store_path=root, no_probe=True))
    async with Server(stores={StoreId("local"): store}, ssh_port=None, unix_path=socket):
        yield socket


@asynccontextmanager
async def open_arms(example: int) -> AsyncIterator[Arms]:
    """Both daemons on fresh stores; `example` keeps one example's roots apart from the last one's."""
    roots = [CLIENT_PREFIX / f"{arm}{example}" for arm in ("c", "p")]
    for root in roots:
        rmtree_robust(root)
        # A socket left by an earlier run would pass `_socket` before the daemon listens.
        root.parent.joinpath(f"{root.name}.sock").unlink(missing_ok=True)
        root.mkdir(parents=True)
    try:
        async with control_daemon(roots[0]) as control, pynixd_daemon(roots[1]) as pynixd:
            yield Arms(control=control, pynixd=pynixd)
    finally:
        for root in roots:
            rmtree_robust(root)
