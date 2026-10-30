"""
Reverse store server — accepts reverse-SSH builder registrations on the controller.

Builders connect via :func:`asyncssh.connect_reverse`, acting as SSH servers.
The controller uses :func:`asyncssh.listen_reverse` and receives an
``SSHClientConnection`` for each builder, which it wraps in a
:class:`ReverseStore` and registers with the scheduler for pooled build
dispatching.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import asyncssh
import structlog

from nix_daemon_protocol.ids import StoreId

from .config import ReverseAcceptorSettings, ReverseStoreSpec
from .constants import SSH_ENCRYPTION_ALGS, SSH_KEEPALIVE_COUNT_MAX, SSH_KEEPALIVE_INTERVAL
from .ssh_auth import load_pinned_keys
from .store.reverse import ReverseStore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .instance import Server
    from .store import Store

log = structlog.get_logger(__name__)

_REGISTRATION_COMMAND = "pynixd-register"


def _known_hosts_matcher(
    pins: list[asyncssh.SSHKey],
) -> Callable[[str, str, int | None], tuple[list[asyncssh.SSHKey], list[object], list[object]]]:
    """A `known_hosts` matcher that trusts exactly *pins*, no filenames.

    asyncssh matches the builder's presented host key against the
    returned trusted keys during the handshake, before `handle_builder`
    ever runs. An empty pin set must never reach here: asyncssh reads
    an empty list as "use ~/.ssh/known_hosts", not "trust nothing".
    """

    def _match(host: str, addr: str, port: int | None) -> tuple[list[asyncssh.SSHKey], list[object], list[object]]:
        return (pins, [], [])

    return _match


async def start_reverse_acceptor(
    server: Server,
    settings: ReverseAcceptorSettings,
    spawn: Callable[[Callable[[], Awaitable[None]]], object],
) -> asyncssh.SSHAcceptor | None:
    """Start the reverse store SSH listener on the controller.

    Builders connect to this listener and register themselves as build stores.
    The controller gets an ``SSHClientConnection`` per builder and can spawn
    ``nix-daemon --stdio`` processes on it via standard SSH exec channels.
    Each builder's lifecycle runs in *spawn*, the server's background.

    Returns the SSH acceptor, or ``None`` if the reverse server is not enabled.
    """
    if not settings.enabled:
        return None

    host_key: asyncssh.SSHKey
    if settings.host_key_path and settings.host_key_path.exists():
        host_key = asyncssh.read_private_key(str(settings.host_key_path))
        log.info("reverse_client_key_loaded", host_key_path=settings.host_key_path)
    else:
        host_key = asyncssh.generate_private_key("ssh-rsa", key_size=4096)
        if settings.host_key_path:
            host_key.write_private_key(str(settings.host_key_path))
            log.info("reverse_client_key_generated", host_key_path=settings.host_key_path)
        else:
            log.info("reverse_client_key_ephemeral_generated")

    # Pinned builder host keys, or empty for an explicit no-pins setup.
    # A missing file raises here, failing the acceptor at startup rather
    # than opening it: an unreadable pin must never read as no pins.
    pins = load_pinned_keys(settings.authorized_builder_keys or [])
    if pins:
        log.info("reverse_builder_keys_pinned", keys=len(pins))
    else:
        log.warning(
            "reverse_acceptor_unpinned",
            detail="any builder key registers; loopback and authenticated tunnels only",
        )

    async def handle_builder(conn: asyncssh.SSHClientConnection) -> None:
        """Manage a registered builder's lifecycle.

        Called once per builder connection. Runs the registration command,
        creates a ReverseStore, registers it with the scheduler, and waits
        for the builder to disconnect.
        """
        store_id_val: str = "unknown"

        async def _lifecycle() -> None:
            nonlocal store_id_val

            try:
                result = await conn.run(_REGISTRATION_COMMAND)
                if not result.stdout:
                    log.warning("reverse_registration_empty_response")
                    conn.abort()
                    return
                reg = json.loads(result.stdout)
            except (json.JSONDecodeError, KeyError, OSError, asyncssh.Error) as exc:
                log.warning("reverse_registration_read_failed", error=str(exc))
                conn.abort()
                return

            store_id_val = reg.get("store_id", f"builder-{id(conn):x}")
            systems = set(reg.get("systems", []))
            system_features = set(reg.get("system_features", []))
            nix_version = reg.get("nix_version", "")
            nix_bin = reg.get("nix_bin", "nix")

            log.info(
                "reverse_builder_registration",
                store_id=store_id_val,
                systems=sorted(systems),
                nix_version=nix_version,
            )

            feature_matrix: dict[str, set[str]] | None = None
            if systems and system_features:
                feature_matrix = {s: set(system_features) for s in systems}
            elif systems:
                feature_matrix = {s: set() for s in systems}

            spec = ReverseStoreSpec(
                store_id=StoreId(store_id_val),
                systems=systems or None,
                system_features=system_features,
                feature_matrix=feature_matrix,
                probe=False,
                reconnect=False,
                nix_bin=nix_bin,
            )

            store = ReverseStore(spec, conn)

            async def _proves(_: Store) -> bool:
                if not systems:
                    log.info(
                        "reverse_builder_probe_skipped",
                        store_id=store_id_val,
                        detail="no systems reported, nothing to prove",
                    )
                    return True
                reason = await store.prove_serves(sorted(systems)[0], settings.registration_probe_timeout)
                if reason is not None:
                    log.warning("reverse_builder_probe_failed", store_id=store_id_val, reason=reason)
                    return False
                return True

            if not await server.add_store(store, dynamic=True, admit=_proves):
                conn.abort()
                return

            log.info("reverse_store_added", store_id=store_id_val)

            try:
                await conn.wait_closed()
            except asyncssh.Error:
                log.debug("reverse_builder_connection_lost", store_id=store_id_val)

            log.info("reverse_builder_disconnected", store_id=store_id_val)

            try:
                await server.remove_store(StoreId(store_id_val))
            except Exception:
                log.warning(
                    "reverse_remove_store_failed",
                    store_id=store_id_val,
                    exc_info=True,
                )

        async def _guarded() -> None:
            # One builder's bug must not stop the daemon that serves the rest.
            try:
                await _lifecycle()
            except Exception:
                log.exception("reverse_builder_lifecycle_failed", store_id=store_id_val)
                conn.abort()

        spawn(_guarded)

    acceptor = await asyncssh.listen_reverse(
        host=settings.host,
        port=settings.port,
        acceptor=handle_builder,
        client_keys=[host_key],
        known_hosts=_known_hosts_matcher(pins) if pins else None,
        encoding=None,
        encryption_algs=SSH_ENCRYPTION_ALGS,
        keepalive_interval=SSH_KEEPALIVE_INTERVAL,
        keepalive_count_max=SSH_KEEPALIVE_COUNT_MAX,
    )
    bound_port = acceptor.get_port()
    log.info("reverse_server_listening", host=settings.host, port=bound_port)
    return acceptor
