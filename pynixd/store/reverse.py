"""
Reverse store — builder-initiated reverse-SSH connection to the controller.

Each ReverseStore wraps a reverse SSH connection (SSHClientConnection from the
controller's perspective).  ``create_conn()`` opens new ``nix-daemon --stdio``
processes on the builder over this connection, providing full connection pooling
that follows the same pattern as ``SSHSubprocessStore``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import structlog

from .. import wire
from ..connection import Connection
from ..monitor import PeerHealthMonitor
from ..wire import SSHNixReader, SSHNixWriter
from .daemon import PROBE_SYSTEM_SCRIPT, ProbeState
from .ssh import SSHStore

if TYPE_CHECKING:
    import asyncssh

    from ..config import ReverseStoreSpec

log = structlog.get_logger(__name__)


class ReverseStore(SSHStore):
    """A store where the builder connected TO us via reverse SSH.

    The controller holds an ``SSHClientConnection`` (obtained via
    :func:`asyncssh.listen_reverse`) and uses it to spawn
    ``nix-daemon --stdio`` processes on the builder.
    """

    def __init__(self, spec: ReverseStoreSpec, ssh_conn: asyncssh.SSHClientConnection) -> None:
        """Wrap an existing reverse SSH connection for daemon operations on the builder."""
        super().__init__(spec)
        self._ssh_conn: asyncssh.SSHClientConnection = ssh_conn
        self.nix_bin = spec.nix_bin
        # Persistent, so `_on_pool_empty` never closes the builder's own
        # connection: this store did not open it and cannot reopen it.
        self.init_ssh_state(monitor_enabled=False, persistent_connection=True)

    async def start(self, sync_paths: bool = True) -> None:
        """Start the store over the connection the builder already opened.

        Not `SSHStore.start`: that dials `self.host`, and a reverse store
        has none. tests/functional/test_reverse_store.py registers one.
        """
        await super(SSHStore, self).start(sync_paths=sync_paths)
        # The builder's own pynixd answers its host's health, disk included;
        # SFTP and `pynixd-agent` cannot reach it. nixidae/nixidae#1013.
        if self.monitor is None:
            self.monitor = PeerHealthMonitor(self.pressure_settings, self.on_pressure_sample, self)
            self.monitor.start(self._background)

    async def create_conn(self) -> Connection:
        """Spawn nix-daemon --stdio on the builder over the reverse SSH connection."""
        conn_id = f"{self.store_id}-{self.conn_counter}"
        cmd = "nix-daemon --stdio" if self.nix_bin == "nix" else f"{self.nix_bin} daemon --stdio"
        log.debug(
            "reverse_spawning_remote_daemon",
            cmd=cmd,
            conn_id=conn_id,
            store_id=self.store_id,
        )
        proc = await self._ssh_conn.create_process(cmd, encoding=None)
        proc.channel.set_write_buffer_limits(
            high=wire._SSH_WINDOW_SIZE,
            low=wire._SSH_WINDOW_SIZE // 4,
        )
        conn = Connection(
            SSHNixReader(proc.stdout, identifier=conn_id),
            SSHNixWriter(proc.stdin, identifier=conn_id),
            conn_id,
        )
        await conn.connect()
        return conn

    async def prove_serves(self, system: str, timeout: float) -> str | None:
        """One echo build through this store; the refusal reason, or None.

        Registration calls this before the scheduler may dispatch here. Any
        failure reads as unproven: refusal, transport error, or timeout. The
        timeout matters most: a wedged serve path answers the probe never,
        and an unbounded probe wedges the registration instead. Issue #80.
        """
        try:
            with anyio.fail_after(timeout):
                _, ok, reason = await self._send_probe(
                    f"probe-system-{system}",
                    system,
                    "",
                    ["-c", PROBE_SYSTEM_SCRIPT.format(system=system)],
                )
        except TimeoutError:
            return f"registration probe timed out after {timeout} seconds"
        except Exception as exc:
            log.warning(
                "reverse_probe_error",
                store_id=self.store_id,
                system=system,
                error=str(exc),
                exc_info=True,
            )
            return f"registration probe failed: {exc}"
        return None if ok else reason

    async def probe(self) -> None:
        """Mark as probed immediately — metadata comes from the registration handshake."""
        if self.probe_state == ProbeState.PROBED:
            return
        self.probe_state = ProbeState.PROBED
        self._probe_event.set()
