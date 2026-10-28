"""How wide a store's pool may go depends on what a connection costs.

A connection to a Unix-socket daemon costs a process here and nothing on
anyone else's machine. A connection of `SSHSubprocessStore` is
`create_process`, which is an SSH **session** channel, and `sshd` permits
ten of those for each network connection. `sshd` does not queue the extra
ones; it refuses them, and `_send_probe` does not catch that refusal.

`SSHSocketStore` is the case that looks the same and is not.
`open_unix_connection` is a forwarded channel, and `sshd_config(5)` says
`MaxSessions` at 0 "will prevent all shell, login and subsystem sessions
while still permitting forwarding". Issue #43.
"""

from __future__ import annotations

from pathlib import Path

from nix_daemon_protocol.ids import StoreId
from pynixd.config import SSHSocketStoreSpec, SSHSubprocessStoreSpec
from pynixd.store.daemon import PROBE_CONCURRENCY, SSHD_DEFAULT_MAX_SESSIONS, DaemonStore
from pynixd.store.ssh import SSHSocketStore, SSHSubprocessStore


class TestThePoolTakesIt:
    """The class attribute is only worth stating if the pool reads it. Before
    issue #43 the pool took its own default of 64 and nothing ever passed the
    argument, so a bound written anywhere else would have changed nothing."""

    def test_the_subprocess_store_hands_its_bound_to_the_pool(self) -> None:
        store = SSHSubprocessStore(
            SSHSubprocessStoreSpec(
                store_id=StoreId("subprocess"),
                host="nowhere.invalid",
                known_hosts=None,
            ),
        )

        # `.value` and not `.max_value`: the pool builds `anyio.Semaphore(n)`
        # with no `max_value=`, which leaves that attribute `None`. On a pool
        # nobody has acquired from, `.value` is the bound it was given.
        # The two pools share the bound: sshd counts every session.
        assert [pool._slots.value for pool in store.pools] == [6, 2]
        assert sum(pool._slots.value for pool in store.pools) == SSHSubprocessStore.MAX_CONNECTIONS

    def test_the_socket_store_hands_the_wide_one(self) -> None:
        store = SSHSocketStore(
            SSHSocketStoreSpec(
                store_id=StoreId("socket"),
                host="nowhere.invalid",
                known_hosts=None,
                socket_path=Path("/nowhere"),
            ),
        )

        assert [pool._slots.value for pool in store.pools] == [48, 16]
        assert sum(pool._slots.value for pool in store.pools) == DaemonStore.MAX_CONNECTIONS


class TestTheBoundPerTransport:
    def test_a_session_channel_store_stays_under_sshd(self) -> None:
        assert SSHSubprocessStore.MAX_CONNECTIONS < SSHD_DEFAULT_MAX_SESSIONS

    def test_a_forwarded_channel_store_keeps_the_wide_bound(self) -> None:
        assert SSHSocketStore.MAX_CONNECTIONS == DaemonStore.MAX_CONNECTIONS

    def test_the_two_ssh_stores_do_not_share_a_bound(self) -> None:
        """The negative control. Both inherit `SSHStore`, so a bound put
        there rather than on the subprocess store would make these equal and
        silently re-break the session case."""
        assert SSHSubprocessStore.MAX_CONNECTIONS != SSHSocketStore.MAX_CONNECTIONS

    def test_the_pool_is_wider_than_the_probe(self) -> None:
        """A probe's builds are pool connections. A pool no wider than the
        probe lets a probe block every other caller for its whole duration."""
        assert SSHSubprocessStore.MAX_CONNECTIONS > PROBE_CONCURRENCY
