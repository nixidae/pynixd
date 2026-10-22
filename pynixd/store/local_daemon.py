"""
Local Nix daemon store implementation.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import json
import os
import shlex
import signal
import sys
import time
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from nix_daemon_protocol.store_dir import on_disk

from ..agent import local_monitor
from ..config import LocalSocketStoreSpec, PynixdSettings
from ..connection import Connection
from ..monitor import DummyResourceMonitor, local_disk
from ..store_path import StorePath
from ..trust import TrustPolicy
from ..unix_socket import _SUN_PATH_MAX, open_unix_connection
from ..wire import UnixNixReader, UnixNixWriter
from .base import get_current_system
from .daemon import DaemonStore

if TYPE_CHECKING:
    from ..drv_parser import Derivation
    from ..monitor import ResourceMonitor
    from ..store_layout import StoreLayout

log = structlog.get_logger(__name__)

# How many lines of daemon output to keep for a start-up error message. Enough
# to carry a Nix error and the lines around it, and bounded because a daemon
# that runs for a week must not grow a list for a week.
_RECENT_OUTPUT_LINES = 50

_DAEMON_START_TIMEOUT = 30.0
"""Seconds to wait for a managed `nix-daemon` to bind and then accept.

**One budget for both steps, and it is generous.** The two waits had 10 s and
5 s, and the harder of the two had the shorter one: a socket file appears as
soon as `bind` returns, and `accept` needs the daemon to have finished its own
start-up. `main:multiple-outputs-substitute-failure` of the Nix functional
suite failed on the 5 s one while the rest of the suite loaded the machine,
and the test then reported nothing about its own subject. Issue #24.

A daemon that starts in the ordinary way binds and listens in milliseconds, so
a larger number costs nothing in the case that works. The loop leaves as soon
as the process dies, so a daemon that fails to start is still reported at
once, and not after the whole budget.
"""

_DAEMON_POLL_INTERVAL = 0.05
"""Seconds between two checks. It is the granularity, and not the budget."""

# `_SUN_PATH_MAX` lives in `..unix_socket`, which owns the length policy:
# the guard below refuses what no connect can reach, and
# `open_unix_connection` reroutes the rest on Linux. `fixtures.py` and the
# length tests import the constant from here, so the name stays.
#
# **Nix binds a longer path and pynixd connects to it through
# `/proc/self/fd`.** Nix forks a helper that `chdir`s into the directory
# and binds the base name (`bindConnectProcHelper` in
# `src/libutil/unix/unix-domain-socket.cc`), so a store under a long path
# gets a working socket. Python has no such helper, and `socket.connect()`
# with the absolute path answers `OSError: AF_UNIX path too long`.
# Measured: a 129 byte path binds through `chdir` and refuses an absolute
# `connect`. Before the reroute, the socket file existed, `_probe_socket`
# could not reach it, and pynixd waited the whole budget and then reported
# `(the daemon wrote nothing)` -- which was true of the daemon and said
# nothing about the fault. Issue #44.


def feature_matrix_from_nix_config(config: dict[str, Any]) -> dict[str, set[str]]:
    """What a daemon builds, from the output of `nix config show --json`.

    The daemon decides with these settings whether it can build a
    derivation itself: `system` and `extra-platforms` for the platform,
    `system-features` for every one of them.
    """
    systems = {config["system"]["value"], *config["extra-platforms"]["value"]}
    features = set(config["system-features"]["value"])
    return {system: set(features) for system in systems}


def _refuse_a_socket_path_python_cannot_reach(socket_path: Path) -> None:
    """Fail now, with the number, rather than after the start-up budget.

    Checked before the spawn and for the system daemon as well: the limit is
    a property of the path, and both cases connect to one. On Linux a long
    path whose base name fits is reachable through `/proc/self/fd`
    (`open_unix_connection` does that), so only the rest is refused here.
    """
    measured = len(os.fsencode(str(socket_path)))
    if measured <= _SUN_PATH_MAX:
        return
    if sys.platform == "linux" and len(os.fsencode(socket_path.name)) <= _SUN_PATH_MAX:
        return
    raise RuntimeError(
        f"The socket path is {measured} bytes and a Unix socket takes {_SUN_PATH_MAX}: "
        f"{socket_path}. Nix binds such a path with a helper that chdirs, so the socket "
        "may exist and still be unreachable from here. Put the store somewhere shorter.",
    )


def _sandbox_fallback_arguments(layout: StoreLayout) -> list[str]:
    """Refuse the silent sandbox fallback for a store Nix has diverted.

    **A diverted store that falls back to no sandbox cannot build anything.**
    nix 2.34.8 `derivation-builder.cc:2111` forces the sandbox on when
    `storeDir != realStoreDir`, because only the chroot puts the real
    directory at the store path the builder writes to. Twelve lines later, at
    `:2120`, a system without mount and PID namespaces turns it back off with
    `debug()` and `sandbox-fallback` left at its default of true. The builder
    then writes to `/nix/store/<hash>-<name>`, which is the store of the
    machine, and Nix looks for the output under the real directory and finds
    nothing.

    That is how a GitHub runner answered `failed to produce output path` for
    `echo x86_64-linux > $out`. The runner blocks unprivileged user
    namespaces, so `mountAndPidNamespacesSupported()` is false there and true
    on every developer machine. The fallback is right for a store at
    `/nix/store` and wrong for every store pynixd relocates, so it goes off
    here and stays on there. Issue #47.
    """
    if layout.store_dir == layout.real_store_dir:
        return []
    return ["--option", "sandbox-fallback", "false"]


class LocalStore(DaemonStore):
    """Connects to a local nix-daemon via Unix socket.

    Manages a private daemon subprocess for isolated store paths. The root
    store instead connects to the host's system daemon: its store database is
    protected and cannot safely be served by an unprivileged private daemon.
    A managed socket is placed at
    ``<store_path>/<socket_path>`` and the daemon is told about it via
    NIX_DAEMON_SOCKET_PATH.

    If the configured socket_path is relative (the default), it is
    resolved relative to store_path. Absolute socket_paths are used as-is.
    """

    def __init__(self, spec: LocalSocketStoreSpec) -> None:
        """Configure local daemon paths, socket management, and monitor."""
        super().__init__(spec)

        socket_path = spec.socket_path or Path("pynixd-nix")
        # A relocated store is always ours to start: `NIX_STORE_DIR` names a
        # store that no daemon of the machine serves.
        #
        # `spec.managed` overrides that, because the inference reads a store at
        # `/` as the store of the machine and a container can hold its own.
        # See `LocalSocketStoreSpec.managed`.
        self.managed = (
            spec.managed if spec.managed is not None else (self.layout.relocated or self.store_path != Path("/"))
        )
        if socket_path.is_absolute():
            self.socket_path = socket_path
        elif not self.managed:
            self.socket_path = Path("/nix/var/nix/daemon-socket/socket")
        else:
            # A relative path is a name under the `daemon-socket` directory
            # of the state of this store. The old default joined
            # `nix/var/nix/daemon-socket/pynixd-nix` to the store root, which
            # gives the same path for a chroot store and the wrong one for a
            # relocated store.
            self.socket_path = self.layout.socket_path(socket_path.name)

        self.nix_bin = spec.nix_bin
        self.monitor_enabled = spec.monitor
        self.daemon_proc: asyncio.subprocess.Process | None = None
        self.daemon_ready: anyio.Event | None = None
        self._daemon_log_task: asyncio.Task | None = None
        # The last lines the daemon wrote, for the error paths of
        # `ensure_daemon`. Those used to call `daemon_proc.stderr.read()`, but
        # `_stream_daemon_output` holds a `readline()` on that same stream from
        # the moment the daemon starts, so the read raised "read() called while
        # another coroutine is already waiting for incoming data" and every
        # startup failure reported that instead of its own cause.
        self._recent_output: deque[str] = deque(maxlen=_RECENT_OUTPUT_LINES)
        self.nix_config = spec.nix_config
        self.extra_env = spec.extra_env or {}
        self.extra_args = spec.extra_args or []
        self.settings = spec.settings or PynixdSettings()
        self._probe_requested = spec.probe

        # Register atexit handler to ensure a private daemon is killed even
        # if close() is never called. The system daemon is never ours to kill.
        if self.managed:
            atexit.register(self._atexit_kill_daemon)

        self.monitor: ResourceMonitor | None = (
            local_monitor(
                self.pressure_settings, self.on_pressure_sample, [get_current_system()], self._replace_monitor
            )
            if self.monitor_enabled
            else DummyResourceMonitor(self.pressure_settings, self.on_pressure_sample)
        )
        self.monitor.disk_fn = local_disk(self.layout.real_store_dir)

    def _replace_monitor(self, monitor: ResourceMonitor) -> None:
        monitor.disk_fn = local_disk(self.layout.real_store_dir)
        self.monitor = monitor
        monitor.start()

    async def start(self, sync_paths: bool = True) -> None:
        """Spawn managed daemon and initialize the store."""
        await self.ensure_daemon()
        if not self.managed and self._feature_matrix is None and self._probe_requested is None:
            # The machine's own daemon: its nix.conf says what it builds.
            # Probing it with builds waits for free build slots before
            # READY=1, and missed systemd's start timeout on a busy machine
            # (#58).
            self._feature_matrix = feature_matrix_from_nix_config(await self.read_nix_config())
            self._probe = False
            log.info(
                "store_features_from_config",
                store_id=self.store_id,
                feature_matrix={k: sorted(v) for k, v in self._feature_matrix.items()},
            )
        try:
            await super().start(sync_paths=sync_paths)
        except BaseException:
            # **A daemon spawned here has no other owner until `start` ends.**
            # `Server.__aenter__` calls this, so a raise means `__aexit__`
            # never runs and `close()` never reaps what `ensure_daemon` left.
            # Measured in CI: `probe()` raised on a daemon that refused it,
            # 149 fixtures failed at setup, and one leftover `nix` kept pytest
            # from exiting for six hours after it had printed its summary.
            # Issue #47.
            await self._kill_daemon()
            raise

    async def ensure_daemon(self) -> None:
        """Ensure a daemon is reachable, spawning one if needed.

        Always spawns a private nix-daemon subprocess.  Uses an Event
        to coordinate concurrent callers.
        """
        if self.daemon_proc is not None:
            if self.daemon_ready is not None:
                await self.daemon_ready.wait()
            return

        _refuse_a_socket_path_python_cannot_reach(self.socket_path)

        if not self.managed:
            if not self.socket_path.exists():
                raise RuntimeError(f"System Nix daemon socket does not exist: {self.socket_path}")
            return

        self.daemon_ready = anyio.Event()

        if self.socket_path.exists():
            log.info("removing_stale_socket", socket_path=str(self.socket_path))
            self.socket_path.unlink()

        socket_dir = self.socket_path.parent
        socket_dir.mkdir(parents=True, exist_ok=True)

        cmd = [
            self.nix_bin,
            "daemon",
            *self.layout.daemon_arguments(),
            "--option",
            "build-dir",
            str(self.layout.build_dir),
            "--log-format",
            "internal-json",
            *_sandbox_fallback_arguments(self.layout),
        ]
        cmd.extend(self.extra_args)

        log.info(
            "spawning_managed_daemon",
            nix_bin=self.nix_bin,
            store_dir=str(self.layout.store_dir),
            real_store_dir=str(self.layout.real_store_dir),
            state_dir=str(self.layout.state_dir),
            relocated=self.layout.relocated,
            socket_path=str(self.socket_path),
            builder_frontend="NIX_CONFIG" in self.extra_env,
            cmd=shlex.join(cmd),
        )
        env = os.environ.copy()
        env.update(self.extra_env)
        env["NIX_DAEMON_SOCKET_PATH"] = str(self.socket_path)
        # A chroot store gets no name here, and Nix reads none. `--store
        # <root>` gives the store a rootDir, and `local-fs-store.hh:54-70`
        # then builds `<root>/nix/var/nix` and `<root>/nix/var/log/nix` from
        # that root and ignores NIX_DATA_DIR, NIX_LOG_DIR and NIX_STATE_DIR.
        # Setting the three was how issue nixidae/nixidae#210 started, and the values were
        # wrong as well: they said `<root>/var/nix`, without the `nix`
        # element that Nix puts there.
        #
        # A relocated store is the other case. It has no root, so the two
        # names below are the only way to say where its store and its state
        # are. Issue nixidae/nixidae#215.
        env.update(self.layout.daemon_environment())

        self.daemon_proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            start_new_session=True,
        )

        if self.daemon_proc.stdout:
            self._daemon_log_task = asyncio.create_task(
                self._stream_daemon_output(),
                name="daemon-log-forwarder",
            )

        # Wait for the socket file to appear.
        deadline = time.monotonic() + _DAEMON_START_TIMEOUT
        while not self.socket_path.exists():
            if self.daemon_proc.returncode is not None:
                stderr_output = self._recent_output_text()
                raise RuntimeError(
                    f"Managed daemon exited early with code {self.daemon_proc.returncode} "
                    f"(pid={self.daemon_proc.pid}): {stderr_output!r}",
                )
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Managed daemon did not create socket at {self.socket_path} "
                    f"within {_DAEMON_START_TIMEOUT:g}s (pid={self.daemon_proc.pid})",
                )
            await anyio.sleep(_DAEMON_POLL_INTERVAL)

        daemon_ready = self.daemon_ready
        if daemon_ready is None:
            raise RuntimeError("daemon_ready event was not initialized")

        # The socket file is there, and the daemon may not be listening yet.
        deadline = time.monotonic() + _DAEMON_START_TIMEOUT
        while True:
            if self.daemon_proc.returncode is not None:
                stderr_output = self._recent_output_text()
                raise RuntimeError(
                    f"Managed daemon exited with code {self.daemon_proc.returncode} "
                    f"(pid={self.daemon_proc.pid}): {stderr_output!r}",
                )
            if await self._probe_socket():
                log.info("daemon_socket_ready", socket_path=str(self.socket_path))
                await anyio.sleep(_DAEMON_POLL_INTERVAL)
                daemon_ready.set()
                return
            if time.monotonic() >= deadline:
                break
            await anyio.sleep(_DAEMON_POLL_INTERVAL)

        stderr_output = self._recent_output_text()
        raise RuntimeError(
            f"Managed daemon socket not accepting connections at {self.socket_path} "
            f"within {_DAEMON_START_TIMEOUT:g}s (pid={self.daemon_proc.pid}): {stderr_output!r}",
        )

    async def trust_policy(self) -> TrustPolicy:
        """`trusted-users`, `allowed-users` and `build-users-group`, as this store's Nix reads them."""
        return TrustPolicy.from_nix_config(await self.read_nix_config())

    async def read_nix_config(self) -> dict[str, Any]:
        """`nix config show --json` in the daemon's environment, so includes,
        `extra-` settings and `NIX_CONFIG` resolve as they do for nix-daemon."""
        env = os.environ.copy()
        env.update(self.extra_env)
        env.update(self.layout.daemon_environment())
        proc = await asyncio.create_subprocess_exec(
            self.nix_bin,
            "--extra-experimental-features",
            "nix-command",
            "config",
            "show",
            "--json",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                f"`nix config show` failed with code {proc.returncode}: {stderr.decode(errors='replace')}"
            )
        return json.loads(stdout)

    def _recent_output_text(self) -> str:
        """The last lines the daemon wrote, for a start-up error message.

        Read from the buffer that `_stream_daemon_output` fills, and never from
        the stream. That forwarder holds a `readline()` on stdout and on stderr
        for as long as the daemon lives, so a second reader of either one gets
        `RuntimeError: read() called while another coroutine is already waiting
        for incoming data`. Each error path below used to do exactly that, so
        the only failure they could report was their own.
        """
        if not self._recent_output:
            return "(the daemon wrote nothing)"
        return "\n".join(self._recent_output)

    async def _stream_daemon_output(self) -> None:
        """Forward daemon stdout/stderr to structlog indefinitely."""
        if not self.daemon_proc or not self.daemon_proc.stdout or not self.daemon_proc.stderr:
            return

        async def _forward(stream: asyncio.StreamReader | None, label: str) -> None:
            if stream is None:
                return
            while True:
                line = await stream.readline()
                if not line:
                    break
                decoded = line.decode(errors="replace").rstrip()
                self._recent_output.append(f"{label}: {decoded}")
                if decoded.startswith("@nix "):
                    try:
                        data = json.loads(decoded[5:])
                    except json.JSONDecodeError:
                        log.info(f"daemon_{label}", msg=decoded)
                        continue
                    log.info(
                        f"daemon_{data.pop('action', label)}",
                        nix_id=data.pop("id", None),
                        nix_level=data.pop("level", None),
                        nix_parent=data.pop("parent", None),
                        nix_text=data.pop("text", ""),
                        nix_type=data.pop("type", None),
                        nix_fields=data.pop("fields", None),
                        **data,
                    )
                else:
                    log.info(f"daemon_{label}", msg=decoded)

        async with anyio.create_task_group() as tg:
            tg.start_soon(_forward, self.daemon_proc.stdout, "stdout")
            tg.start_soon(_forward, self.daemon_proc.stderr, "stderr")

    async def _probe_socket(self) -> bool:
        """Perform a full daemon handshake to verify a live Nix daemon.

        Connects to socket_path, does the protocol handshake, and cleanly
        closes. Returns True only if a real Nix daemon responded.
        """
        try:
            r, w = await open_unix_connection(self.socket_path)
        except (ConnectionRefusedError, ConnectionResetError, FileNotFoundError, OSError):
            return False

        conn = Connection(
            UnixNixReader(r, identifier="probe"),
            UnixNixWriter(w, identifier="probe"),
            "probe",
            store_path=self.store_path,
        )
        try:
            await conn.connect()
        except (OSError, EOFError, ConnectionError):
            log.debug("probe_connection_failed", exc_info=True)
            with contextlib.suppress(Exception):
                await conn.close()
            return False
        await conn.close()
        return True

    async def create_conn(self) -> Connection:
        """Connect to the (managed or system) Nix daemon via Unix socket."""
        await self.ensure_daemon()
        conn_id = f"{self.store_id}-{self.conn_counter}"
        log.debug(
            "connecting_daemon_socket",
            socket_path=str(self.socket_path),
            conn_id=conn_id,
        )
        r, w = await open_unix_connection(self.socket_path)
        conn = Connection(
            UnixNixReader(r, identifier=conn_id),
            UnixNixWriter(w, identifier=conn_id),
            conn_id,
            store_path=self.store_path,
        )
        await conn.connect()
        return conn

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        """Fast-path: read .drv file directly from the filesystem."""
        from ..drv_parser import drv_text, parse_drv

        # `on_disk` and not `self.store_path / "nix" / "store" / ...`.
        # `real_store_dir()` is where the files of the store are, and a chroot
        # store puts them somewhere other than the directory a store path
        # names. See `read_drv_file` in `drv_parser.py`.
        drv_file = Path(on_disk(str(StorePath(str(drv_store_path)))))
        try:
            contents = drv_file.read_bytes()
        except (FileNotFoundError, OSError):
            return await super().read_derivation(drv_store_path)

        return parse_drv(drv_text(contents))

    async def close(self) -> None:
        """Close stores, stop monitor and terminate managed daemon if any."""
        await super().close()
        if self.monitor:
            await self.monitor.stop()
        if self._daemon_log_task and not self._daemon_log_task.done():
            self._daemon_log_task.cancel()
            with contextlib.suppress(BaseException):
                await self._daemon_log_task
        await self._kill_daemon()

    async def _kill_daemon(self) -> None:
        """Terminate the managed daemon process and the group it leads.

        The daemon is spawned with `start_new_session=True`, so its pid is
        its process group id and `os.killpg` reaches that group.

        **It does not reach the daemon's connection workers, and it cannot.**
        `nix/unix/daemon.cc` forks one worker for each accepted connection,
        and the first thing that worker does is `setsid()`. It is its own
        session leader from then on, in a group nothing here owns. The same
        call site sets `options.dieWithParent = false`, so Nix means the
        worker to outlive the daemon.

        Measured in the guest census: one `nix` worker with `ppid 1` after a
        run, holding a store under `/tmp`. Its argv[1] is the *client's* pid,
        which `daemon.cc` writes there for debugging, so it names a process
        that is already gone. Issue #36 carries the reproduction.

        A worker exits when it finishes what it is doing and reads EOF from
        its client. Nothing below makes that happen sooner.
        """
        # Unregister atexit handler so we don't double-kill
        with contextlib.suppress(ValueError):
            atexit.unregister(self._atexit_kill_daemon)

        if self.daemon_proc is None:
            return
        pid = self.daemon_proc.pid
        log.info("terminating_daemon_process_group", pid=pid)

        # The daemon's group. Its connection workers are not in it -- see
        # the docstring.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pid, signal.SIGTERM)

        try:
            with anyio.fail_after(5.0):
                await self.daemon_proc.wait()
        except TimeoutError:
            log.warning("daemon_sigterm_timeout_escalating_to_sigkill", pid=pid)
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pid, signal.SIGKILL)

            # Final wait so the process is reaped
            with contextlib.suppress(TimeoutError, ProcessLookupError):
                with anyio.fail_after(2.0):
                    await self.daemon_proc.wait()

        self.daemon_proc = None
        self.daemon_ready = None

    def _atexit_kill_daemon(self) -> None:
        """Synchronous atexit handler to kill the daemon process group.
        This is called when the Python process exits without close() having
        been invoked (e.g., unhandled exception, os._exit, or interpreter crash).
        """
        proc = self.daemon_proc
        if proc is None:
            return

        pid = proc.pid
        log.info("atexit_killing_daemon_process_group", pid=pid)

        # Best-effort SIGTERM then SIGKILL synchronously
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pid, signal.SIGTERM)

        # Very brief synchronous wait — atexit is synchronous, can't asyncio
        time.sleep(0.5)
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pid, signal.SIGKILL)
