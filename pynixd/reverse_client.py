"""
Reverse initiator — builder side that connects to a controller's reverse acceptor.

The initiator connects via :func:`asyncssh.connect_reverse`, acts as an SSH
server, and serves incoming ``nix-daemon --stdio`` sessions via
:class:`DaemonProxy`.  On disconnect it retries with exponential backoff.
"""

from __future__ import annotations

import contextlib
import json
from typing import TYPE_CHECKING

import anyio
import asyncssh
import structlog

from . import wire
from .config import ReverseInitiatorSettings
from .hangup import ssh_hangup
from .proxy import DaemonProxy
from .serde.auth import Role
from .wire import SSHNixReader, SSHNixWriter

if TYPE_CHECKING:
    from .context import PynixdContext

log = structlog.get_logger(__name__)

_REGISTRATION_COMMAND = "pynixd-register"


class _ReverseSSHServer(asyncssh.SSHServer):
    """SSH server that performs no client authentication.

    Trust runs one way on the reverse path: the acceptor verifies the
    builder's host key before registration, and the builder serves
    whatever answers its dial. The dial travels an already-authenticated
    tunnel (WireGuard), and the builder's own host key is ephemeral by
    default, so a controller pin on this side would be friction with no
    threat model behind it. Issue #75.
    """

    def begin_auth(self, username: str) -> bool:
        return True

    def public_key_auth_supported(self) -> bool:
        return True

    def validate_public_key(self, username: str, key: asyncssh.SSHKey) -> bool:
        return True


class ReverseInitiator:
    """Builder-side component that connects to a controller's reverse acceptor.

    Accepts a :class:`PynixdContext` and settings, connects to the controller,
    registers the local store, and serves daemon protocol sessions via
    :class:`DaemonProxy`.
    """

    def __init__(
        self,
        ctx: PynixdContext,
        settings: ReverseInitiatorSettings,
    ) -> None:
        self._ctx = ctx
        self._settings = settings

        store_id = settings.store_id or f"builder-{id(self):x}"
        systems = settings.systems or sorted(self._ctx.local_store.systems or [])
        system_features = settings.system_features

        self._registration_info: dict[str, object] = {
            "store_id": store_id,
            "systems": systems,
            "system_features": system_features,
            "nix_version": "pynixd-0.1.0",
            "nix_bin": settings.nix_bin,
        }

    async def run(self) -> None:
        """Connect to the controller and serve daemon sessions forever.

        Retries on disconnect with exponential backoff.  Only exits on
        cancellation or when `shutdown_on_connect_failure_seconds` is exceeded.
        """
        delay = self._settings.reconnect_min_delay
        max_delay = self._settings.reconnect_max_delay

        while True:
            try:
                await self._connect_and_serve()
                delay = self._settings.reconnect_min_delay
            except anyio.get_cancelled_exc_class():
                return
            except Exception:
                log.exception("reverse_initiator_connection_failed")
                shutdown_s = self._settings.shutdown_on_connect_failure_seconds
                if shutdown_s is not None and delay >= shutdown_s:
                    log.critical(
                        "reverse_initiator_connect_failure_threshold_reached",
                        delay=delay,
                        threshold=shutdown_s,
                    )
                    return
                await anyio.sleep(delay)
                delay = min(delay * 2, max_delay)

    async def _connect_and_serve(self) -> None:
        host_keys = self._load_host_keys()

        log.info(
            "reverse_initiator_connecting",
            host=self._settings.acceptor_host,
            port=self._settings.acceptor_port,
        )

        conn = await asyncssh.connect_reverse(
            self._settings.acceptor_host,
            self._settings.acceptor_port,
            server_host_keys=host_keys,
            server_factory=_ReverseSSHServer,
            authorized_client_keys=None,
            process_factory=self._handle_request,
            encoding=None,
        )

        log.info("reverse_initiator_connected")
        try:
            await conn.wait_closed()
        except asyncssh.Error:
            log.debug("reverse_initiator_connection_lost")
        finally:
            conn.close()
            with contextlib.suppress(asyncssh.Error):
                await conn.wait_closed()

    def _load_host_keys(self) -> list[asyncssh.SSHKey]:
        if self._settings.server_host_key_paths:
            return [asyncssh.read_private_key(str(p)) for p in self._settings.server_host_key_paths]
        key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
        log.info("reverse_initiator_ephemeral_key_generated")
        return [key]

    async def _handle_request(self, process: asyncssh.SSHServerProcess) -> None:
        cmd = process.command or ""

        if cmd == _REGISTRATION_COMMAND:
            reg_json = json.dumps(self._registration_info)
            process.stdout.write(reg_json.encode())
            process.exit(0)
            await process.wait_closed()
            return

        if "nix-daemon" not in cmd and "nix daemon" not in cmd:
            process.stderr.write(b"pynixd: unsupported command\n")
            process.exit(1)
            await process.wait_closed()
            return

        process.channel.set_write_buffer_limits(
            high=wire._SSH_WINDOW_SIZE,
            low=wire._SSH_WINDOW_SIZE // 4,
        )

        exit_code = 0
        try:
            proxy = DaemonProxy(
                SSHNixReader(process.stdin, identifier="reverse-initiator"),
                SSHNixWriter(process.stdout, identifier="reverse-initiator"),
                ctx=self._ctx,
                # The session on this channel is the controller this builder
                # dialled: no other party answers that address but the tunnel
                # makes reachable, and the acceptor pinned this builder's key
                # before it listened. A USER here refuses every
                # input-addressed build the controller delegates, so
                # delegation through a reverse builder never works (the
                # controller blacklists the builder after the first refusal).
                role=Role.ADMIN,
                username="reverse-initiator",
                schedule_mode=self._settings.schedule_mode,
                hangup=ssh_hangup(process),
            )
            await proxy.run()
        except Exception:
            log.exception("reverse_initiator_proxy_session_failed")
            exit_code = 1
        finally:
            process.exit(exit_code)
