"""A channel that fails on a reused SSH connection is opened once more on a fresh one (nixidae/nixidae#1641).

dynhetz's `sg1-pynixd` store held a connection sg1's pynixd had closed by restarting. Each kubeapply push
through it failed with `ChannelOpenError: SSH connection closed`, and the store reconnected a second later.
"""

from __future__ import annotations

from typing import NoReturn, cast

import asyncssh
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import SSHSubprocessStoreSpec
from pynixd.store.ssh import SSHStore


class DialingSSHStore(SSHStore):
    """Dials a new stand-in connection only when none is cached, as `SSHStore.ensure_ssh` does."""

    def __init__(self) -> None:
        spec = SSHSubprocessStoreSpec(store_id=StoreId("peer"), host="peer", monitor=False, known_hosts=None)
        super().__init__(spec)
        self.init_ssh_state(monitor_enabled=False, persistent_connection=True)
        self.dials: list[asyncssh.SSHClientConnection] = []

    async def ensure_ssh(self) -> asyncssh.SSHClientConnection:
        if self.conn is None:
            # Nothing calls the transport: the channel opener below decides by identity.
            self.conn = cast("asyncssh.SSHClientConnection", object())
            self.dials.append(self.conn)
        return self.conn

    async def create_conn(self) -> NoReturn:  # nothing to dial here
        raise AssertionError("this test opens channels through open_channel alone")


def _closed() -> asyncssh.ChannelOpenError:
    return asyncssh.ChannelOpenError(asyncssh.OPEN_CONNECT_FAILED, "SSH connection closed")


@pytest.mark.anyio
async def test_a_reused_closed_connection_is_replaced_and_the_channel_opens() -> None:
    store = DialingSSHStore()
    stale = await store.ensure_ssh()

    async def open_on(conn: asyncssh.SSHClientConnection) -> str:
        if conn is stale:
            raise _closed()
        return "channel"

    assert await store.open_channel(open_on) == "channel"
    assert len(store.dials) == 2
    assert store.conn is store.dials[1]


@pytest.mark.anyio
async def test_a_fresh_connection_that_fails_raises_without_a_retry() -> None:
    store = DialingSSHStore()
    tries: list[asyncssh.SSHClientConnection] = []

    async def open_on(conn: asyncssh.SSHClientConnection) -> NoReturn:
        tries.append(conn)
        raise _closed()

    with pytest.raises(asyncssh.ChannelOpenError):
        await store.open_channel(open_on)
    assert len(tries) == 1
    assert store.conn is None


@pytest.mark.anyio
async def test_a_retry_that_fails_too_raises() -> None:
    store = DialingSSHStore()
    await store.ensure_ssh()
    tries: list[asyncssh.SSHClientConnection] = []

    async def open_on(conn: asyncssh.SSHClientConnection) -> NoReturn:
        tries.append(conn)
        raise _closed()

    with pytest.raises(asyncssh.ChannelOpenError):
        await store.open_channel(open_on)
    assert len(tries) == 2
    assert tries[0] is not tries[1]
    assert store.conn is None
