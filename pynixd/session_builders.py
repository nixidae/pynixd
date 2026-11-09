"""The stores a trusted client names in its `builders` setting.

nix-daemon reads a trusted client's `builders` in place of its own, and its
build hook sends that client's builds to those machines (`daemon.cc:234`,
`build-remote.cc`). pynixd opens a store for each machine and places the
client's builds on those stores and the local store only. A build of any
other client never goes to such a store.

NIX-DEVIATION (#27): `ClientSettings::apply` (`daemon.cc:234`) sets an empty
`builders` too, so a client that sends one gets local builds only. pynixd
places such a build on its own stores, as it does when the value names no
machine. NixOS writes `builders =` into every `nix.conf` without distributed
builds, so Nix's rule takes the whole fleet away from most clients. Measure
a client that sends an empty `builders` to mean "no remote builds" to
reverse this.
"""

from __future__ import annotations

import hashlib
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import anyio
import structlog

from nix_daemon_protocol.ids import StoreId

from .config import LocalSocketStoreSpec, SSHSubprocessStoreSpec
from .machines import Machine, MachinesError, parse_config
from .store.base import get_current_system
from .store_url import StoreReference, spec_for

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from .config import PynixdSettings, StoreSpecBase
    from .store.base import Store

log = structlog.get_logger(__name__)

_LOCAL_SCHEMES = frozenset({"local"})


class SessionBuilderError(MachinesError):
    """A machine of `builders` that pynixd opens no store for."""


def _features(machine: Machine) -> set[str]:
    return set(machine.supported_features | machine.mandatory_features)


def _slots(machine: Machine) -> dict[str, object]:
    """`maxJobs`, column 4, as the store's build slots: one connection more serves transfers."""
    return {"max_connections": max(1, machine.max_jobs) + 1, "transfer_connections": 1}


def _local_root(machine: Machine) -> str | None:
    """The store root a machine names on this host, or `None` for another host.

    `ssh-ng://localhost` is Nix's fake SSH (`ssh.cc:84`): Nix runs the far
    command on this host with no SSH, so the store is `remote-store` here.
    """
    ref = machine.reference
    if ref.scheme in _LOCAL_SCHEMES:
        return ref.path or "/"
    if ref.scheme == "ssh-ng" and ref.authority == "localhost":
        return ref.params.get("remote-store", "/")
    return None


def builder_spec(machine: Machine, settings: PynixdSettings, nix_bin: str) -> StoreSpecBase:
    """The store spec of one machine; `SessionBuilderError` for a machine pynixd cannot open."""
    systems = set(machine.systems)
    features = _features(machine)
    digest = hashlib.sha256(repr((machine.store_uri, sorted(machine.params.items()))).encode()).hexdigest()[:12]
    store_id = StoreId(f"session-{digest}")
    common: dict[str, object] = {
        "store_id": store_id,
        "systems": systems,
        "system_features": features,
        "mandatory_features": set(machine.mandatory_features),
        "monitor": False,
        "probe": False,
        "gc_enabled": False,
        **_slots(machine),
    }

    root = _local_root(machine)
    if root is not None:
        inner = StoreReference.parse(root)
        inner_features = inner.params.get("system-features")
        socket_dir = settings.unix_path.parent if settings.unix_path is not None else Path(tempfile.gettempdir())
        return LocalSocketStoreSpec.model_validate(
            {
                **common,
                "store_path": Path(inner.path),
                "managed": True,
                "use_db": False,
                "nix_bin": nix_bin,
                # A Unix socket path takes 107 bytes, and a root under a test's
                # directory passes that inside `<root>/nix/var/nix/daemon-socket`.
                "socket_path": socket_dir / f"{store_id}.socket",
                # The store URL's `system-features` (`completeStoreReference`) is the daemon's setting.
                "extra_args": [
                    "--option",
                    "system-features",
                    " ".join(sorted(set(inner_features.split()) if inner_features else features)),
                ],
            }
        )

    ref = machine.reference
    if ref.scheme == "ssh-ng":
        params = {k: v for k, v in ref.params.items() if k != "system-features"}
        url = f"ssh-ng://{ref.authority}{ref.path}"
        if params:
            url += "?" + urlencode(params, safe="/:@?=")
        spec = spec_for(url)
        if not isinstance(spec, SSHSubprocessStoreSpec):
            raise SessionBuilderError(f"pynixd cannot open the builder '{machine.store_uri}'")
        return spec.model_copy(
            update={
                **common,
                "known_hosts": spec.known_hosts,
                "scheduleable": True,
                "no_schedule": False,
                "priority": 1.0,
            }
        )

    # NIX-DEVIATION (#27): `Machine::openStore` (`machines.cc:95`) opens any
    # store Nix knows. pynixd builds on a local root and on `ssh-ng://` only;
    # `ssh://` is the legacy serve protocol, below pynixd's protocol floor of
    # 1.35. The cost is an error for such a builder. Measure a client that
    # needs one to reverse this.
    raise SessionBuilderError(
        f"pynixd cannot build on '{machine.store_uri}': it builds on a local store and on ssh-ng:// only"
    )


class SessionBuilders:
    """The stores of every `builders` value a trusted client sent, opened once each.

    `reap` closes a store that no open session names and no unfinished build
    may use, once it has been so for `session_builder_linger` seconds.
    nixidae/nixidae#1243.
    """

    def __init__(
        self,
        settings: PynixdSettings,
        nix_bin: str,
        add_store: Callable[[Store], Awaitable[bool]],
        remove_store: Callable[[StoreId], Awaitable[None]],
        live_values: Callable[[], Iterable[str]],
        pending_builders: Callable[[], Awaitable[Iterable[frozenset[StoreId] | None]]],
    ) -> None:
        self.settings = settings
        self.nix_bin = nix_bin
        self._add_store = add_store
        self._remove_store = remove_store
        self._live_values = live_values
        self._pending_builders = pending_builders
        self._by_value: dict[str, frozenset[StoreId]] = {}
        self._open: dict[StoreId, Store] = {}
        self._last_needed: dict[StoreId, float] = {}
        # Held across a close too: a store id is a digest of its machine, so a
        # reopen during the close registers a store that the close then forgets.
        self._lock = anyio.Lock()

    async def stores_for(self, builders: str) -> frozenset[StoreId] | None:
        """The stores of *builders*, or `None` when it names no machine; raises `MachinesError`."""
        async with self._lock:
            known = self._by_value.get(builders)
            if known is not None:
                now = time.monotonic()
                for store_id in known:
                    self._last_needed[store_id] = now
                return known
            # `Machine::parseConfig({settings.thisSystem}, ...)`, `worker.cc:352`.
            machines = parse_config({get_current_system()}, builders)
            if not machines:
                return None
            ids: set[StoreId] = set()
            for machine in machines:
                spec = builder_spec(machine, self.settings, self.nix_bin)
                if spec.store_id is None:
                    raise SessionBuilderError(f"no store id for '{machine.store_uri}'")
                if spec.store_id not in self._open:
                    store = spec.to_store(spec.store_id)
                    store.session_only = True
                    if not await self._add_store(store):
                        raise SessionBuilderError(f"pynixd cannot open the builder '{machine.store_uri}'")
                    log.info("session_builder_opened", store_id=spec.store_id, uri=machine.store_uri)
                    self._open[spec.store_id] = store
                self._last_needed[spec.store_id] = time.monotonic()
                ids.add(spec.store_id)
            result = frozenset(ids)
            self._by_value[builders] = result
            return result

    async def run(self) -> None:
        """Reap every half linger, so a store closes within one and a half lingers of its last need."""
        while True:
            await anyio.sleep(max(self.settings.session_builder_linger / 2, 1.0))
            await self.reap(time.monotonic())

    async def reap(self, now: float) -> list[StoreId]:
        """Close each store unneeded for the linger, and answer which ones it closed."""
        linger = self.settings.session_builder_linger
        async with self._lock:
            needed: set[StoreId] = set()
            for value in self._live_values():
                needed |= self._by_value.get(value, frozenset())
            for builders in await self._pending_builders():
                needed |= builders or frozenset()
            closed: list[StoreId] = []
            for store_id in list(self._open):
                if store_id in needed:
                    self._last_needed[store_id] = now
                elif now - self._last_needed.get(store_id, now) >= linger:
                    closed.append(store_id)
            for store_id in closed:
                del self._open[store_id]
                self._last_needed.pop(store_id, None)
                self._by_value = {v: ids for v, ids in self._by_value.items() if store_id not in ids}
                await self._remove_store(store_id)
                log.info("session_builder_closed", store_id=store_id)
            return closed
