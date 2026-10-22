"""
SSH Store implementations for pynixd.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import os
import shlex
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import asyncssh
import structlog

from .. import wire
from ..agent import AgentMonitor, AgentUnavailableError, agent_for
from ..config import (
    PynixdSettings,
    SSHSocketStoreSpec,
    SSHSubprocessStoreSpec,
)
from ..connection import Connection
from ..constants import SSH_ENCRYPTION_ALGS
from ..monitor import DummyResourceMonitor, GenericResourcePoller, ResourceMonitor
from ..psi import DiskSpace
from ..wire import SSHNixReader, SSHNixWriter
from .daemon import SSHD_DEFAULT_MAX_SESSIONS, DaemonStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from nix_daemon_protocol.ids import StoreId

    from ..agent import AgentStream
    from ..psi import (
        CpuUtil,
        MemInfo,
    )

log = structlog.get_logger(__name__)

AGENT_DIR = ".cache/pynixd"
"""Where `pynixd-agent` goes on a builder, relative to the login's home."""


class SSHStore(DaemonStore):
    """Shared SSH connection management with exponential backoff reconnection.

    Subclasses set host, port, username on __init__.
    """

    host: str
    port: int
    username: str | None
    client_keys: list[str | Path | asyncssh.SSHKey] | None
    conn: asyncssh.SSHClientConnection | None
    backoff: float
    max_backoff: float
    last_failure: float
    store_id: StoreId
    _bg_tasks: set[asyncio.Task[Any]]

    INITIAL_BACKOFF: float = 1.0
    MAX_BACKOFF: float = 60.0

    def init_ssh_state(
        self,
        *,
        monitor_enabled: bool = True,
        client_keys: list[str | Path | asyncssh.SSHKey] | None = None,
        settings: PynixdSettings | None = None,
        persistent_connection: bool = True,
        known_hosts: str | None = None,
    ) -> None:
        """Initialise SSH connection state, backoff, and resource monitor settings."""
        self.persistent_connection = persistent_connection
        # The spec has no default for this, so a configuration answered it.
        # `None` is `asyncssh`'s "accept any host key", and it is now written
        # rather than implied. Issue nixidae/nixidae#204.
        self.known_hosts = known_hosts
        if not persistent_connection and monitor_enabled:
            # The monitor polls over this store's own SSH connection, so it
            # would hold the builder awake by itself and undo the whole point
            # of turning the persistent connection off.
            log.warning(
                "ssh_monitor_holds_connection",
                store_id=self.store_id,
                detail="persistent_connection is off but monitor is on; set monitor = false",
            )
        self.conn = None
        self.ssh_lock = anyio.Lock()
        self._bg_tasks = set()
        self.backoff = self.INITIAL_BACKOFF
        self.max_backoff = self.MAX_BACKOFF
        self.last_failure = 0.0
        self.monitor_enabled = monitor_enabled
        self.client_keys = client_keys
        self.settings = settings or PynixdSettings()
        self.monitor: ResourceMonitor | None = None

    async def start(self, sync_paths: bool = True) -> None:
        """Connect, unless this store is configured not to hold a connection.

        Connecting here is the default and is deliberate: the connection's
        state is the health of the store. The reconnect loop, the backoff and
        the circuit breaker all read it, and the resource monitor polls over
        it, so a builder that is up looks up because the socket is there.

        `persistent_connection = false` gives that up on purpose, for a
        builder that starts on demand. `create_conn` already calls
        `ensure_ssh()`, so the connection is then made by the first thing that
        needs it, and `_on_pool_empty` drops it once the last channel closes.
        Issue nixidae/nixidae#203.
        """
        if self.persistent_connection:
            await self.ensure_ssh()
        else:
            log.info(
                "ssh_connect_deferred",
                store_id=self.store_id,
                host=self.host,
                detail="persistent_connection is off; this store connects on first use",
            )
        await super().start(sync_paths=sync_paths)

    async def _on_pool_empty(self) -> None:
        """Drop the SSH connection once no channel is left on it.

        Only when this store does not hold one on purpose. The pool reaps the
        daemon channels, and the `asyncssh` connection underneath them belongs
        to this store, so nothing else can close it -- which is why removing
        the eager connect alone was not enough. The first build would pin an
        on-demand builder awake for the lifetime of pynixd, the same defect
        and half as visible.
        """
        if self.persistent_connection or self.conn is None:
            return
        log.info("ssh_disconnecting_idle", store_id=self.store_id, host=self.host)
        await self.close_ssh()

    def start_psi_polling(self, conn: asyncssh.SSHClientConnection) -> None:
        """Sample the builder's host: with `pynixd-agent`, or over SFTP. Issue #98."""
        if not self.monitor_enabled:
            if self.monitor is None:
                self.monitor = DummyResourceMonitor(self.pressure_settings, self.on_pressure_sample)
                self.monitor.start()
            return
        if self.monitor is not None and not isinstance(self.monitor, DummyResourceMonitor):
            return
        self.stop_psi_polling()

        def blind() -> None:
            # The builder refuses exec and SFTP. It still builds, ranked last
            # (`unmonitored_penalty`). nixidae/nixidae#1016.
            log.warning("ssh_store_unmonitored", store_id=self.store_id)
            self.monitor = DummyResourceMonitor(self.pressure_settings, self.on_pressure_sample)
            self.monitor.start()

        def fall_back() -> None:
            self.monitor = self._sftp_poller(conn, blind)
            self.monitor.disk_fn = self._sftp_disk(conn)
            self.monitor.start()

        if self.pressure_settings.agent:
            self.monitor = AgentMonitor(
                self.pressure_settings,
                self.on_pressure_sample,
                functools.partial(self._run_agent, conn),
                fall_back,
            )
            self.monitor.disk_fn = self._sftp_disk(conn)
            self.monitor.start()
        else:
            fall_back()

    def _sftp_disk(self, conn: asyncssh.SSHClientConnection) -> Callable[[], Awaitable[DiskSpace | None]]:
        """The builder's store filesystem over SFTP `statvfs`. nixidae/nixidae#1013."""
        sftp: asyncssh.SFTPClient | None = None

        async def sample() -> DiskSpace | None:
            nonlocal sftp
            if sftp is None:
                sftp = await conn.start_sftp_client()
            try:
                stat = await sftp.statvfs(str(self.layout.store_dir))
            except asyncssh.SFTPError:
                return None
            return DiskSpace(free_bytes=stat.bavail * stat.frsize, total_bytes=stat.blocks * stat.frsize)

        return sample

    def _sftp_poller(self, conn: asyncssh.SSHClientConnection, fallback: Callable[[], None]) -> GenericResourcePoller:
        sftp: asyncssh.SFTPClient | None = None

        async def sftp_read(path: str) -> str | None:
            nonlocal sftp
            if sftp is None:
                sftp = await conn.start_sftp_client()
            # /proc files report size 0, so read to EOF rather than by size.
            try:
                async with sftp.open(path, "r") as f:
                    chunks = []
                    while chunk := await f.read(65536):
                        chunks.append(chunk)
                    return "".join(chunks)
            except asyncssh.SFTPError:
                return None

        return GenericResourcePoller(self.pressure_settings, self.on_pressure_sample, sftp_read, fallback)

    @contextlib.asynccontextmanager
    async def _run_agent(self, conn: asyncssh.SSHClientConnection, args: list[str]) -> AsyncIterator[AgentStream]:
        """Upload the agent for this builder's system once, and run it.

        Uploaded to the login's home and not copied into the builder's store:
        that needs no trust on the builder, and pynixd's own binaries need not
        live in the store it serves. The store name of the binary is the file
        name, so two versions never share one.
        """
        if self._feature_matrix is None:
            with anyio.move_on_after(60):
                await self._probe_event.wait()
        systems = sorted(self.systems or ())
        agent = agent_for(systems, self.pressure_settings.agents)
        if agent is None:
            raise AgentUnavailableError(f"no pynixd-agent for {systems}")
        remote = f"{AGENT_DIR}/{agent.parent.parent.name}"
        partial = f"{remote}.{os.getpid()}.part"
        try:
            async with conn.start_sftp_client() as sftp:
                if not await sftp.exists(remote):
                    await sftp.makedirs(AGENT_DIR, exist_ok=True)
                    await sftp.put(str(agent), partial)
                    await sftp.chmod(partial, 0o755)
                    await sftp.posix_rename(partial, remote)
        except asyncssh.Error as e:
            # A builder may refuse the SFTP subsystem and still allow exec.
            # nixidae/nixidae#1016.
            log.info("agent_upload_over_exec", store_id=self.store_id, sftp_error=str(e))
            if (await conn.run(f"test -x {shlex.quote(remote)}", check=False)).exit_status != 0:
                script = (
                    f"mkdir -p {shlex.quote(AGENT_DIR)} && cat > {shlex.quote(partial)}"
                    f" && chmod 755 {shlex.quote(partial)} && mv {shlex.quote(partial)} {shlex.quote(remote)}"
                )
                await conn.run(script, input=await anyio.Path(agent).read_bytes(), encoding=None, check=True)
        process = await conn.create_process(shlex.join([f"./{remote}", *args]), encoding=None)
        try:
            yield process.stdout
        finally:
            process.close()

    def stop_psi_polling(self) -> None:
        """Cancel the resource polling task."""
        if self.monitor is not None:
            task = asyncio.create_task(self.monitor.stop())
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
            self.monitor = None

    @property
    def pressure(self) -> float | None:
        """System pressure score (0-100), or None if unavailable."""
        if self.monitor is None:
            return None

        if isinstance(self.monitor, DummyResourceMonitor):
            return 0.0

        if self.monitor.health.psi is None:
            return None

        if time.monotonic() - self.monitor.health.timestamp > self.monitor.interval * 3:
            return None
        return self.monitor.health.psi.pressure_score()

    @property
    def meminfo(self) -> MemInfo | None:
        """System memory info, or None if unavailable."""
        return self.monitor.health.meminfo if self.monitor else None

    @property
    def cpu_util(self) -> CpuUtil | None:
        """CPU utilization from cgroupv2, or None if unavailable."""
        return self.monitor.health.cpu_util if self.monitor else None

    async def ensure_ssh(self) -> asyncssh.SSHClientConnection:
        """Establish or return the existing SSH connection, with backoff."""
        if self.conn is not None:
            return self.conn

        async with self.ssh_lock:
            # Re-check after acquiring lock (another task may have connected)
            if self.conn is not None:
                return self.conn

            # Respect backoff from previous failure
            now = time.monotonic()
            wait = self.last_failure + self.backoff - now
            if self.last_failure > 0 and wait > 0:
                log.info("ssh_backoff", store_id=self.store_id, backoff_seconds=wait)
                await anyio.sleep(wait)

            try:
                log.info(
                    "ssh_connecting",
                    username=self.username or "",
                    host=self.host,
                    port=self.port,
                )
                connect_kwargs: dict[str, Any] = {
                    "host": self.host,
                    "port": self.port,
                    "known_hosts": self.known_hosts,
                    "encryption_algs": SSH_ENCRYPTION_ALGS,
                }
                if self.username is not None:
                    connect_kwargs["username"] = self.username
                if self.client_keys is not None:
                    connect_kwargs["client_keys"] = self.client_keys
                self.conn = await asyncssh.connect(**connect_kwargs)
                # Reset backoff on success
                self.backoff = self.INITIAL_BACKOFF
                self.last_failure = 0.0
                self.record_success()

                if self.monitor_enabled:
                    self.start_psi_polling(self.conn)
            except (asyncssh.misc.Error, OSError):
                self.last_failure = time.monotonic()
                self.backoff = min(self.backoff * 2, self.MAX_BACKOFF)
                self.record_failure()
                log.warning(
                    "ssh_connect_failed",
                    store_id=self.store_id,
                    next_retry_seconds=self.backoff,
                )
                raise
            else:
                return self.conn

    def invalidate_ssh(self) -> None:
        """Mark SSH connection as dead, triggering reconnect on next use."""

        """Mark SSH connection as dead so next ensure_ssh reconnects."""
        if self.conn is not None:
            with contextlib.suppress(Exception):
                self.conn.close()
            self.conn = None
        self._schedule_reconnect()

    async def close_ssh(self) -> None:
        """Stop resource polling and close the SSH connection."""

        self.stop_psi_polling()
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class SSHSubprocessStore(SSHStore):
    """Persistent SSH connection, spawns nix-daemon --stdio channels.

    Used primarily for "fake Nix" stores like nixbuild.net that provide
    a nix-daemon protocol over stdin/stdout. For real Nix stores over SSH,
    SSHSocketStore (tunnelling to a Unix socket) is preferred.

    If store_path is set, runs ``nix daemon --store <path> --stdio``.
    Otherwise runs ``nix-daemon --stdio`` (default store, nixbuild.net compat).
    """

    MAX_CONNECTIONS: int = SSHD_DEFAULT_MAX_SESSIONS - 2
    """Every connection here is `create_process`, which is an SSH **session**
    channel, and `sshd` permits ten of those per network connection.

    The inherited 64 let six times that through, and `sshd` does not queue the
    extra ones -- it refuses them with `OPEN_REQUEST_SESSION_FAILED`, which
    `_send_probe` does not catch, so the store goes into a 300 second
    cooldown. Issue #43, found while bounding the probe in #15.

    Two below the ceiling: the monitor holds one session (`pynixd-agent`,
    or the SFTP poller), and one stays free for a session somebody else
    opens on the same connection. The agent's one-time upload borrows that
    free one. It stays above `PROBE_CONCURRENCY`: the probe's
    builds are pool connections too, and a pool no wider than the probe would
    let a probe block every other caller."""

    def __init__(self, spec: SSHSubprocessStoreSpec) -> None:
        """Configure SSH subprocess with host, port, and client key settings."""
        super().__init__(spec)
        self.host = spec.host
        self.port = spec.port
        self.username = spec.username
        self.nix_bin = spec.nix_bin
        self.init_ssh_state(
            monitor_enabled=spec.monitor,
            client_keys=list(spec.client_keys) if spec.client_keys else None,
            settings=spec.settings,
            persistent_connection=spec.persistent_connection,
            known_hosts=spec.known_hosts,
        )
        self.ssh_processes: list[asyncssh.SSHClientProcess] = []

    async def create_conn(self) -> Connection:
        """Spawn a nix-daemon --stdio subprocess over the SSH connection."""
        ssh_conn = await self.ensure_ssh()
        conn_id = f"{self.store_id}-{self.conn_counter}"
        if self.store_path and self.store_path != Path("/"):
            cmd = f"{self.nix_bin} daemon --store {self.store_path} --stdio"
        elif self.nix_bin != "nix":
            cmd = f"{self.nix_bin} daemon --stdio"
        else:
            cmd = "nix-daemon --stdio"
        log.debug(
            "spawning_remote_daemon",
            cmd=cmd,
            conn_id=conn_id,
        )
        try:
            proc = await ssh_conn.create_process(cmd, encoding=None)
        except (asyncssh.misc.Error, OSError):
            self.invalidate_ssh()
            raise
        self.ssh_processes.append(proc)
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

    async def close(self) -> None:
        """Close daemon store, terminate subprocess channels, and close SSH connection."""
        await super().close()
        for proc in self.ssh_processes:
            with contextlib.suppress(Exception):
                proc.terminate()
            proc.close()
        self.ssh_processes.clear()
        await self.close_ssh()


_DAEMON_SOCKET_PATH = Path("/nix/var/nix/daemon-socket/socket")


class SSHSocketStore(SSHStore):
    """Persistent SSH connection, tunnels to remote Unix socket.

    Keeps the inherited `MAX_CONNECTIONS`. `open_unix_connection` is a
    forwarded channel, not a session, and `sshd_config(5)` says `MaxSessions`
    at 0 still permits forwarding. So the ceiling that bounds
    `SSHSubprocessStore` does not apply here. Issue #43.
    """

    def __init__(self, spec: SSHSocketStoreSpec) -> None:
        """Configure SSH socket tunnel store with host, port, and remote socket path."""
        super().__init__(spec)
        self.host = spec.host
        self.port = spec.port
        self.username = spec.username
        self.socket_path = spec.socket_path
        self.init_ssh_state(
            monitor_enabled=spec.monitor,
            client_keys=list(spec.client_keys) if spec.client_keys else None,
            settings=spec.settings,
            persistent_connection=spec.persistent_connection,
            known_hosts=spec.known_hosts,
        )

    async def create_conn(self) -> Connection:
        """Open a Unix socket tunnel over the SSH connection."""
        ssh_conn = await self.ensure_ssh()
        conn_id = f"{self.store_id}-{self.conn_counter}"
        log.debug(
            "tunneling_to_socket",
            socket_path=str(self.socket_path),
            conn_id=conn_id,
        )
        try:
            r, w = await ssh_conn.open_unix_connection(str(self.socket_path))
        except (asyncssh.misc.Error, OSError):
            self.invalidate_ssh()
            raise
        conn = Connection(
            SSHNixReader(r, identifier=conn_id),
            SSHNixWriter(w, identifier=conn_id),
            conn_id,
        )
        await conn.connect()
        return conn

    async def close(self) -> None:
        """Close daemon store and close the SSH connection."""
        await super().close()
        await self.close_ssh()
