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
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

import anyio
import asyncssh
import structlog

from nix_daemon_protocol.ids import StoreId

from .config import ReverseAcceptorSettings, ReverseStoreSpec
from .constants import SSH_ENCRYPTION_ALGS, SSH_KEEPALIVE_COUNT_MAX, SSH_KEEPALIVE_INTERVAL
from .provisioning.ticket import LEASE_COMMAND
from .ssh_auth import load_pinned_keys
from .store.reverse import ReverseStore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .instance import Server
    from .provisioning.ticket import Lease
    from .store import Store

log = structlog.get_logger(__name__)

_REGISTRATION_COMMAND = "pynixd-register"
LEASE_TIMEOUT = 10.0

UNDECLARED_MAX_JOBS = 8
"""The build slots of a builder that declares no `max_jobs`. The pool size
was the ceiling, 48, and dynhetz sent 48 fetches to macbook in one minute,
none of which finished in 55 min. nixidae/nixidae#1073."""
REVERSE_TRANSFER_CONNECTIONS = 16
"""The transfer pool of a reverse builder: a quarter of `DaemonStore.MAX_CONNECTIONS`."""


def build_slots(declared: object, store_id: str) -> int:
    """The build slots of a registering builder: the `max_jobs` it declares, else `UNDECLARED_MAX_JOBS`."""
    if isinstance(declared, int) and not isinstance(declared, bool) and declared >= 1:
        return declared
    if declared is not None:
        log.warning("reverse_builder_max_jobs_invalid", store_id=store_id, max_jobs=declared)
    return UNDECLARED_MAX_JOBS


@dataclass(frozen=True, slots=True)
class Admitted:
    """A builder the acceptor lets register, and what it may build."""

    store_id: StoreId
    mandatory_features: frozenset[str] = frozenset()
    """Features a build must need to go here (`Machine::mandatoryMet`); a pool sets them on its builders."""
    lease: Lease | None = None
    """What the acceptor tells the builder at registration; a builder no pool launched gets none."""
    pool: str | None = None
    """The pool that launched the builder."""


async def send_lease(conn: asyncssh.SSHClientConnection, store_id: str, lease: Lease) -> bool:
    """Tell the builder its lease. An older builder refuses the command; it then keeps its own guard's limit."""
    try:
        with anyio.fail_after(LEASE_TIMEOUT):
            result = await conn.run(LEASE_COMMAND, input=lease.model_dump_json().encode())
    except (TimeoutError, OSError, asyncssh.Error) as exc:
        log.warning("reverse_lease_failed", store_id=store_id, error=str(exc) or type(exc).__name__)
        return False
    if result.exit_status != 0:
        stderr = result.stderr.decode(errors="replace") if isinstance(result.stderr, bytes) else result.stderr
        log.warning("reverse_lease_refused", store_id=store_id, exit_status=result.exit_status, stderr=stderr)
        return False
    log.info("reverse_lease_sent", store_id=store_id, idle_seconds=lease.idle_seconds)
    return True


class Joiner(Protocol):
    """What admits a builder by the join token a provisioner gave it. nixidae/nixidae#1198."""

    async def join(self, token: str) -> Admitted | None:
        """The instance *token* belongs to, once; None for an unknown, used or ended token."""
        ...

    async def left(self, store_id: StoreId) -> None:
        """The builder *store_id* names disconnected."""
        ...


async def admit(
    reg: dict[str, object],
    presented: asyncssh.SSHKey | None,
    pins: list[asyncssh.SSHKey],
    joiner: Joiner | None,
    fallback_id: str,
) -> Admitted | None:
    """What a registering builder is admitted as, or None to refuse it.

    A builder that sends a join token is admitted by the token alone, under
    the id of its instance: a token holder cannot take another store's id.
    A wrong token is refused even from a pinned key. Without a token, the
    key must be pinned, or nothing is pinned.
    """
    token = reg.get("join_token")
    if token is not None:
        if joiner is None or not isinstance(token, str):
            log.warning("reverse_join_refused", reason="this acceptor provisions no builders")
            return None
        admitted = await joiner.join(token)
        if admitted is None:
            log.warning("reverse_join_refused", reason="unknown, used or ended join token")
        return admitted
    if pins and (presented is None or presented not in pins):
        log.warning("reverse_builder_key_not_pinned", store_id=reg.get("store_id"))
        return None
    return Admitted(StoreId(str(reg.get("store_id") or fallback_id)))


def reverse_spec(
    reg: dict[str, object],
    store_id: str,
    mandatory_features: frozenset[str] = frozenset(),
    pool: str | None = None,
) -> ReverseStoreSpec:
    """The store a builder's registration describes."""
    systems = set(cast("list[str]", reg.get("systems") or []))
    system_features = set(cast("list[str]", reg.get("system_features") or []))
    feature_matrix: dict[str, set[str]] | None = None
    if systems:
        feature_matrix = {s: set(system_features) for s in systems}
    slots = build_slots(reg.get("max_jobs"), store_id)
    cores = reg.get("cores")
    return ReverseStoreSpec(
        build_cores=cores if isinstance(cores, int) and not isinstance(cores, bool) and cores >= 1 else None,
        store_id=StoreId(store_id),
        systems=systems or None,
        system_features=system_features,
        feature_matrix=feature_matrix,
        probe=False,
        reconnect=False,
        nix_bin=str(reg.get("nix_bin", "nix")),
        max_connections=slots + REVERSE_TRANSFER_CONNECTIONS,
        transfer_connections=REVERSE_TRANSFER_CONNECTIONS,
        mandatory_features=set(mandatory_features),
        pool=pool,
    )


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


def client_key(settings: ReverseAcceptorSettings) -> asyncssh.SSHKey:
    """The key the acceptor presents to each builder: read, generated and written, or ephemeral."""
    if settings.host_key_path and settings.host_key_path.exists():
        log.info("reverse_client_key_loaded", host_key_path=settings.host_key_path)
        return asyncssh.read_private_key(str(settings.host_key_path))
    key = asyncssh.generate_private_key("ssh-rsa", key_size=4096)
    if settings.host_key_path:
        key.write_private_key(str(settings.host_key_path))
        log.info("reverse_client_key_generated", host_key_path=settings.host_key_path)
    else:
        log.info("reverse_client_key_ephemeral_generated")
    return key


async def start_reverse_acceptor(
    server: Server,
    settings: ReverseAcceptorSettings,
    spawn: Callable[[Callable[[], Awaitable[None]]], object],
    joiner: Joiner | None = None,
    host_key: asyncssh.SSHKey | None = None,
    sock: socket.socket | None = None,
) -> asyncssh.SSHAcceptor | None:
    """Start the reverse store SSH listener on the controller.

    Builders connect to this listener and register themselves as build stores.
    The controller gets an ``SSHClientConnection`` per builder and can spawn
    ``nix-daemon --stdio`` processes on it via standard SSH exec channels.
    Each builder's lifecycle runs in *spawn*, the server's background.

    Returns the SSH acceptor, or ``None`` if the reverse server is not enabled.

    With a *joiner*, a builder the provisioner launched has a host key
    nobody pinned, so the handshake accepts every key and `admit` checks
    the pins or the join token before the builder gets a build.
    """
    if not settings.enabled:
        return None

    if host_key is None:
        host_key = client_key(settings)

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

            admitted = await admit(reg, conn.get_server_host_key(), pins, joiner, f"builder-{id(conn):x}")
            if admitted is None:
                conn.abort()
                return
            store_id_val = admitted.store_id
            try:
                if admitted.lease is not None:
                    await send_lease(conn, admitted.store_id, admitted.lease)
                await _serve(admitted, reg)
            finally:
                if joiner is not None and "join_token" in reg:
                    with anyio.CancelScope(shield=True):
                        await joiner.left(admitted.store_id)

        async def _serve(admitted: Admitted, reg: dict[str, object]) -> None:
            store_id = admitted.store_id
            spec = reverse_spec(reg, store_id, admitted.mandatory_features, admitted.pool)
            systems = spec.systems or set()
            store = ReverseStore(spec, conn)

            log.info(
                "reverse_builder_registration",
                store_id=store_id,
                systems=sorted(systems),
                nix_version=reg.get("nix_version", ""),
                build_slots=store.build_pool.max_connections,
            )

            async def _proves(_: Store) -> bool:
                if not systems:
                    log.info(
                        "reverse_builder_probe_skipped",
                        store_id=store_id,
                        detail="no systems reported, nothing to prove",
                    )
                    return True
                reason = await store.prove_serves(sorted(systems)[0], settings.registration_probe_timeout)
                if reason is not None:
                    log.warning("reverse_builder_probe_failed", store_id=store_id, reason=reason)
                    return False
                return True

            # A pool's builder is not dynamic: `demand.launchable` speaks for its pool, and the union of its
            # features would hold a build that needs only a feature the pool's mandatory ones guard.
            if not await server.add_store(store, dynamic=admitted.pool is None, admit=_proves):
                conn.abort()
                return

            log.info("reverse_store_added", store_id=store_id)

            try:
                await conn.wait_closed()
            except asyncssh.Error:
                log.debug("reverse_builder_connection_lost", store_id=store_id)

            log.info("reverse_builder_disconnected", store_id=store_id)

            try:
                # The provisioner removing the store is what closes this connection on an idle end.
                await server.remove_store(store_id, missing_ok=True)
            except Exception:
                log.warning(
                    "reverse_remove_store_failed",
                    store_id=store_id,
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
        host=None if sock else settings.host,
        port=() if sock else settings.port,
        sock=sock,
        acceptor=handle_builder,
        client_keys=[host_key],
        known_hosts=_known_hosts_matcher(pins) if pins and joiner is None else None,
        encoding=None,
        encryption_algs=SSH_ENCRYPTION_ALGS,
        keepalive_interval=SSH_KEEPALIVE_INTERVAL,
        keepalive_count_max=SSH_KEEPALIVE_COUNT_MAX,
    )
    bound_port = acceptor.get_port()
    log.info("reverse_server_listening", host=settings.host, port=bound_port)
    return acceptor
