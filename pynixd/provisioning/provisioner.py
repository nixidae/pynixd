"""The provisioner: launches builders for waiting builds and ends the ones it no longer needs. nixidae/nixidae#1198.

Each step reads the builds the last scheduling pass left without a store,
the instances in `PynixdProvisionedInstance`, and asks `decide`. A launch
writes its row before the provider is called, so a pynixd that dies in
between finds the row at its next start and ends the machine.

A launched machine gets a `JoinTicket` as its user data. Its builder dials
the reverse acceptor and registers by the ticket's token, once: the
acceptor calls `join`, and `left` when the builder goes. A builder that
left cannot come back, so its machine ends.

At start, every row still live and every machine tagged with this
pynixd's owner is an orphan: its builder's token was for the pynixd that
died. At shutdown the provisioner ends every machine it holds.
"""

from __future__ import annotations

import hashlib
import secrets
import socket
import time
from typing import TYPE_CHECKING

import anyio
import structlog

from nix_daemon_protocol.ids import StoreId
from pynixd_store.provisioning_tables import PENDING

from .decide import EndReason, Held, InstanceState, Pool, decide
from .demand import wants
from .provider import MachineState, NoCapacity, ProviderError
from .ticket import JoinTicket

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pynixd_store.provisioning_tables import InstanceRow, ProvisioningTables

    from ..config import ProvisioningPoolSettings, ProvisioningSettings
    from ..scheduler import Scheduler
    from .provider import Provider

log = structlog.get_logger(__name__)

OWNER_TAG = "pynixd-owner"
INSTANCE_TAG = "pynixd-instance"
POOL_TAG = "pynixd-pool"
STORE_PREFIX = "provisioned-"
SHUTDOWN_SECONDS = 30.0
"""How long ending every machine at shutdown may take; the next start ends what is left."""


def token_sha256(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def store_id_of(instance_id: str) -> StoreId:
    return StoreId(f"{STORE_PREFIX}{instance_id}")


def pool_of(settings: ProvisioningPoolSettings) -> Pool:
    return Pool(
        name=settings.name,
        systems=frozenset(settings.systems),
        features=frozenset(settings.features),
        max_instances=settings.max_instances,
        min_instances=settings.min_instances,
        slots=settings.slots,
        idle_seconds=settings.idle_seconds,
        launch_timeout=settings.launch_timeout,
    )


class Provisioner:
    def __init__(
        self,
        settings: ProvisioningSettings,
        provider: Provider,
        tables: ProvisioningTables,
        scheduler: Scheduler,
        remove_store: Callable[[StoreId], Awaitable[None]],
        controller_key: str | None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._settings = settings
        self._provider = provider
        self._tables = tables
        self._scheduler = scheduler
        self._remove_store = remove_store
        self._controller_key = controller_key
        self._clock = clock
        """Wall clock: the rows outlive the process."""
        self.pools = [pool_of(pool) for pool in settings.pools]
        self._templates = {pool.name: pool.template for pool in settings.pools}
        self.owner = settings.owner or socket.gethostname()
        self._wake = anyio.Event()
        self._idle_since: dict[str, float] = {}
        self._ending: set[str] = set()
        self._cooldown: dict[str, float] = {}
        """Pools the provider refused, and when to ask again."""

    def wake(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        matrix: dict[str, set[str]] = {}
        for pool in self.pools:
            for system in pool.systems:
                matrix.setdefault(system, set()).update(pool.features)
        self._scheduler.add_dynamic_features(matrix)
        self._scheduler.pass_listeners.append(self.wake)
        await self.reconcile()
        try:
            while True:
                with anyio.move_on_after(await self.step()):
                    await self._wake.wait()
                self._wake = anyio.Event()
        finally:
            self._scheduler.pass_listeners.remove(self.wake)
            with anyio.CancelScope(shield=True):
                with anyio.move_on_after(SHUTDOWN_SECONDS):
                    for row in await self._tables.live():
                        await self._end(row, EndReason.SHUTDOWN)
                await self._provider.close()

    async def reconcile(self) -> None:
        """End every machine a previous run of this pynixd left."""
        for row in await self._tables.live():
            await self._end(row, EndReason.ORPHAN)
        try:
            machines = await self._provider.machines({OWNER_TAG: self.owner})
        except ProviderError:
            log.warning("provisioning_reconcile_list_failed", exc_info=True)
            return
        for machine in machines:
            if machine.state is not MachineState.ENDED:
                log.info("provisioning_orphan_terminated", provider_id=machine.provider_id)
                await self._terminate(machine.provider_id)

    async def step(self) -> float:
        """Launch and end what `decide` says; answer how long until a timer could change the answer."""
        now = self._clock()
        rows = await self._tables.live()
        held = [self._held(row, now) for row in rows]
        queue = self._scheduler.queue
        waiting_ids = self._scheduler.no_candidate | self._scheduler.held_by_pressure
        waiting = wants(build for build in queue.queue if build.build_id in waiting_ids)
        decision = decide(self.pools, waiting, held, now)
        by_id = {row.id: row for row in rows}
        for instance_id, reason in decision.terminate.items():
            await self._end(by_id[instance_id], reason)
        for pool_name, count in decision.launch.items():
            if self._cooldown.get(pool_name, 0.0) > now:
                continue
            for _ in range(count):
                if not await self._launch(pool_name, now):
                    self._cooldown[pool_name] = now + self._settings.heartbeat
                    break
        return self._until_next(held, now)

    def _held(self, row: InstanceRow, now: float) -> Held:
        store_id = store_id_of(row.id)
        builds = sum(
            1 for build in self._scheduler.queue.queue if build.is_building and build.assigned_store_id == store_id
        )
        pending = row.state == PENDING
        if pending or builds:
            self._idle_since.pop(row.id, None)
            idle_since = None
        else:
            idle_since = self._idle_since.setdefault(row.id, now)
        return Held(
            id=row.id,
            pool=row.pool,
            state=InstanceState.PENDING if pending else InstanceState.RUNNING,
            launched_at=row.launched_at_ms / 1000,
            builds=builds,
            idle_since=idle_since,
        )

    def _until_next(self, held: list[Held], now: float) -> float:
        pools = {pool.name: pool for pool in self.pools}
        deadlines = [self._settings.heartbeat]
        for one in held:
            pool = pools.get(one.pool)
            if pool is None:
                continue
            if one.state is InstanceState.PENDING:
                deadlines.append(one.launched_at + pool.launch_timeout - now)
            elif one.idle_since is not None:
                deadlines.append(one.idle_since + pool.idle_seconds - now)
        return max(min(deadlines), 0.0) + 0.05

    async def _launch(self, pool_name: str, now: float) -> bool:
        instance_id = f"{pool_name}-{secrets.token_hex(4)}"
        token = secrets.token_urlsafe(32)
        await self._tables.record_launch(
            instance_id, pool_name, self._provider.name, token_sha256(token), at_ms=int(now * 1000)
        )
        ticket = JoinTicket(
            token=token,
            acceptor_host=self._settings.acceptor_host,
            acceptor_port=self._settings.acceptor_port,
            controller_key=self._controller_key,
        )
        tags = {OWNER_TAG: self.owner, INSTANCE_TAG: instance_id, POOL_TAG: pool_name}
        try:
            machine = await self._provider.launch(self._templates[pool_name], tags, ticket.model_dump_json())
        except NoCapacity as error:
            log.info("provisioning_no_capacity", pool=pool_name, error=str(error))
            await self._tables.ended(instance_id, EndReason.NO_CAPACITY, at_ms=self._ms())
            return False
        except ProviderError:
            log.warning("provisioning_launch_failed", pool=pool_name, exc_info=True)
            await self._tables.ended(instance_id, EndReason.LAUNCH_FAILED, at_ms=self._ms())
            return False
        await self._tables.set_provider_id(instance_id, machine.provider_id)
        log.info("provisioning_launched", pool=pool_name, instance=instance_id, provider_id=machine.provider_id)
        return True

    async def _end(self, row: InstanceRow, reason: EndReason) -> None:
        if row.id in self._ending:
            return
        self._ending.add(row.id)
        try:
            log.info("provisioning_ending", instance=row.id, pool=row.pool, reason=str(reason))
            if row.store_id is not None and reason is not EndReason.DISCONNECTED:
                await self._remove_store(StoreId(row.store_id))
            provider_ids = [row.provider_id] if row.provider_id is not None else await self._tagged(row.id)
            for provider_id in provider_ids:
                await self._terminate(provider_id)
            await self._tables.ended(row.id, reason, at_ms=self._ms())
            self._idle_since.pop(row.id, None)
        finally:
            self._ending.discard(row.id)
        self.wake()

    async def _tagged(self, instance_id: str) -> list[str]:
        """The machine of a row whose launch never answered, found by its tag."""
        try:
            machines = await self._provider.machines({OWNER_TAG: self.owner, INSTANCE_TAG: instance_id})
        except ProviderError:
            log.warning("provisioning_list_failed", instance=instance_id, exc_info=True)
            return []
        return [machine.provider_id for machine in machines if machine.state is not MachineState.ENDED]

    async def _terminate(self, provider_id: str) -> None:
        try:
            await self._provider.terminate(provider_id)
        except ProviderError:
            # The next start's reconcile finds it by its owner tag.
            log.warning("provisioning_terminate_failed", provider_id=provider_id, exc_info=True)

    def _ms(self) -> int:
        return int(self._clock() * 1000)

    async def join(self, token: str) -> StoreId | None:
        row = await self._tables.by_token(token_sha256(token))
        if row is None:
            return None
        store_id = store_id_of(row.id)
        if not await self._tables.registered(row.id, store_id, at_ms=self._ms()):
            return None
        log.info("provisioning_joined", instance=row.id, store_id=store_id)
        self.wake()
        return store_id

    async def left(self, store_id: StoreId) -> None:
        for row in await self._tables.live():
            if row.store_id == store_id:
                await self._end(row, EndReason.DISCONNECTED)
