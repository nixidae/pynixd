"""The provisioner: launches builders for waiting builds and ends the ones it no longer needs. nixidae/nixidae#1198.

Each step reads the builds the last scheduling pass left without a store,
the instances in `PynixdProvisionedInstance`, and asks `decide`. A launch
writes its row before the provider is called, so a pynixd that dies in
between finds the row at its next start and ends the machine.

A launched machine gets a `JoinTicket` as its user data. Its builder dials
the reverse acceptor and registers by the ticket's token, once: the
acceptor calls `join`, and `left` when the builder goes. A builder that
left cannot come back, so its machine ends.

Each step also lists this owner's machines. A machine under an
interruption notice gives no slot, so `decide` launches its replacement,
and its builder drains until `NOTICE_MARGIN` before the notice ends: the
builds that do not finish go back to the queue. A machine the provider
ended ends its row.

Each pool names its provider, so one pynixd can launch EC2 machines for
one pool and macOS machines for another. A row records its provider's
name, and that provider ends the row's machine.

At start, every row still live and every machine tagged with this
pynixd's owner is an orphan: its builder's token was for the pynixd that
died. At shutdown the provisioner ends every machine it holds.
"""

from __future__ import annotations

import contextlib
import functools
import hashlib
import secrets
import socket
import time
from typing import TYPE_CHECKING

import anyio
import structlog

from nix_daemon_protocol.ids import StoreId
from pynixd_store.provisioning_tables import PENDING

from ..reverse_server import Admitted
from ..state_models import ProvisionedRow
from .decide import EndReason, Held, InstanceState, Pool, decide
from .demand import launchable, wants
from .provider import MachineState, NoCapacity, ProviderError
from .ticket import JoinTicket, Lease

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Sequence

    from anyio.abc import TaskGroup

    from pynixd_store.provisioning_tables import InstanceRow, ProvisioningTables

    from ..config import ProvisioningPoolSettings, ProvisioningSettings
    from ..scheduler import Scheduler
    from .provider import Machine, Provider

log = structlog.get_logger(__name__)

OWNER_TAG = "pynixd-owner"
INSTANCE_TAG = "pynixd-instance"
POOL_TAG = "pynixd-pool"
EARLY_ENDS = frozenset({EndReason.GONE, EndReason.DISCONNECTED, EndReason.LAUNCH_TIMEOUT})
"""Ends that say the machine did not do its job: each one doubles the pool's wait before its next launch."""
BACKOFF_CEILING = 600.0
STORE_PREFIX = "provisioned-"
SHUTDOWN_SECONDS = 30.0
"""How long ending every machine at shutdown may take; the next start ends what is left."""
NOTICE_MARGIN = 10.0
"""Seconds before a notice ends that the drain stops waiting: the builds still running
go back to the queue while the builder can still answer."""


def token_sha256(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def store_id_of(instance_id: str) -> StoreId:
    return StoreId(f"{STORE_PREFIX}{instance_id}")


def pool_of(settings: ProvisioningPoolSettings) -> Pool:
    return Pool(
        name=settings.name,
        systems=frozenset(settings.systems),
        features=frozenset(settings.features),
        mandatory_features=frozenset(settings.mandatory_features),
        pressure_wait=settings.pressure_wait,
        max_instances=settings.max_instances,
        min_instances=settings.min_instances,
        slots=settings.slots,
        idle_seconds=settings.idle_seconds,
        idle_max_seconds=settings.idle_max_seconds,
        launch_timeout=settings.launch_timeout,
        single_use=settings.single_use,
    )


def boot_seconds(row: InstanceRow) -> float | None:
    """From launch to registration, or None before it registered."""
    if row.registered_at_ms is None:
        return None
    return (row.registered_at_ms - row.launched_at_ms) / 1000


class Provisioner:
    def __init__(
        self,
        settings: ProvisioningSettings,
        providers: Sequence[Provider],
        tables: ProvisioningTables,
        scheduler: Scheduler,
        remove_store: Callable[[StoreId], Awaitable[None]],
        controller_key: str | None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._settings = settings
        self._providers = {provider.name: provider for provider in providers}
        self._pool_providers = {pool.name: pool.provider or settings.provider for pool in settings.pools}
        if missing := set(self._pool_providers.values()) - set(self._providers):
            raise ValueError(f"provisioning: no provider {sorted(missing)} for the pools that name it")
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
        """Pools that may not launch yet, and when they may: the provider refused, or machines ended early."""
        self._early_ends: dict[str, int] = {}
        """Early ends by pool since its last machine ended idle (`EARLY_ENDS`)."""
        self._draining: set[str] = set()
        self._group: TaskGroup | None = None
        self._seen: list[tuple[InstanceRow, Held]] = []
        """What the last step read, for `report`."""
        self.launches_held = False
        """Set while a hand-over runs: a machine launched now would be one the successor ends as an orphan."""

    def wake(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        # Not the feature union in `add_dynamic_features`: that ignores `mandatory_features` and named pools, so a
        # build no pool takes would wait forever and a steered one would fail at once.
        self._scheduler.launchable = functools.partial(launchable, tuple(self.pools))
        single_use = frozenset(pool.name for pool in self.pools if pool.single_use)
        self._scheduler.single_use = lambda store: store.pool in single_use
        self._scheduler.trigger()
        self._scheduler.pass_listeners.append(self.wake)
        await self.reconcile()
        try:
            async with self.tasks():
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
                for provider in self._providers.values():
                    await provider.close()

    @contextlib.asynccontextmanager
    async def tasks(self) -> AsyncIterator[None]:
        """The group the drains run in; `step` starts none outside it."""
        async with anyio.create_task_group() as group:
            self._group = group
            try:
                yield
            finally:
                self._group = None
                group.cancel_scope.cancel()

    async def reconcile(self) -> None:
        """End every machine a previous run of this pynixd left."""
        for row in await self._tables.live():
            await self._end(row, EndReason.ORPHAN)
        for provider in self._providers.values():
            try:
                machines = await provider.machines({OWNER_TAG: self.owner})
            except ProviderError:
                log.warning("provisioning_reconcile_list_failed", provider=provider.name, exc_info=True)
                continue
            for machine in machines:
                if machine.state is not MachineState.ENDED:
                    log.info("provisioning_orphan_terminated", provider=provider.name, provider_id=machine.provider_id)
                    await self._terminate(provider, machine.provider_id)

    async def step(self) -> float:
        """Launch and end what `decide` says; answer how long until a timer could change the answer."""
        now = self._clock()
        rows = await self._tables.live()
        machines = await self._machines()
        for row in list(rows):
            machine = machines.get(row.id)
            if row.provider_id is not None and machine is not None and machine.state is MachineState.ENDED:
                await self._end(row, EndReason.GONE)
                rows.remove(row)
        held = [self._held(row, now, machines.get(row.id)) for row in rows]
        self._seen = list(zip(rows, held, strict=True))
        for row, one in zip(rows, held, strict=True):
            if one.notice is not None and one.state is InstanceState.RUNNING:
                self._drain(row, one.notice)
        queue = self._scheduler.queue
        no_candidate = self._scheduler.no_candidate
        busy = self._scheduler.held_by_pressure - no_candidate
        waiting = wants(
            (build for build in queue.queue if build.build_id in no_candidate or build.build_id in busy),
            busy,
            time.monotonic(),
        )
        decision = decide(self.pools, waiting, held, now)
        by_id = {row.id: row for row in rows}
        for instance_id, reason in decision.terminate.items():
            await self._end(by_id[instance_id], reason)
        for pool_name, count in decision.launch.items():
            if self.launches_held:
                break
            if self._cooldown.get(pool_name, 0.0) > now:
                continue
            for _ in range(count):
                if not await self._launch(pool_name, now):
                    self._cooldown[pool_name] = max(self._cooldown.get(pool_name, 0.0), now + self._settings.heartbeat)
                    break
        return self._until_next(held, now)

    async def live_count(self) -> int:
        """Machines whose row is live: launched and not yet ended."""
        return len(await self._tables.live())

    def report(self) -> list[ProvisionedRow]:
        """The machines the last step saw, for `pynixd state`."""
        now = self._clock()
        pools = {pool.name: pool for pool in self.pools}
        return [
            ProvisionedRow(
                id=row.id,
                pool=row.pool,
                state=str(one.state),
                store=row.store_id,
                provider_id=row.provider_id,
                age_s=round(now - one.launched_at, 1),
                builds=one.builds,
                idle_s=None if one.idle_since is None else round(now - one.idle_since, 1),
                notice_s=None if one.notice is None else round(one.notice - now, 1),
                boot_s=None if one.boot_seconds is None else round(one.boot_seconds, 1),
                idle_limit_s=None
                if one.boot_seconds is None or one.pool not in pools
                else round(pools[one.pool].idle_limit(one.boot_seconds), 1),
            )
            for row, one in self._seen
        ]

    async def _machines(self) -> dict[str, Machine]:
        """This owner's machines by instance id; none of a provider that does not answer."""
        found: dict[str, Machine] = {}
        for provider in self._providers.values():
            try:
                listed = await provider.machines({OWNER_TAG: self.owner})
            except ProviderError:
                log.warning("provisioning_list_failed", provider=provider.name, exc_info=True)
                continue
            found.update((machine.tags[INSTANCE_TAG], machine) for machine in listed if INSTANCE_TAG in machine.tags)
        return found

    def _drain(self, row: InstanceRow, notice: float) -> None:
        if row.id in self._draining or row.store_id is None or self._group is None:
            return
        self._draining.add(row.id)
        seconds = max(notice - self._clock() - NOTICE_MARGIN, 0.0)
        log.info("provisioning_draining", instance=row.id, seconds=round(seconds, 1))
        self._group.start_soon(self._scheduler.drain_store, StoreId(row.store_id), seconds)

    def _held(self, row: InstanceRow, now: float, machine: Machine | None) -> Held:
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
            notice=None if machine is None else machine.notice,
            boot_seconds=boot_seconds(row),
            used=self._used(store_id),
        )

    def _used(self, store_id: StoreId) -> bool:
        """Whether the scheduler stopped a single-use builder taking builds (`Scheduler.single_use`)."""
        held = self._scheduler.ctx.stores.get(store_id)
        store = None if held is None else held.daemon_client
        return store is not None and self._scheduler.single_use(store) and store.draining

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
                deadlines.append(one.idle_since + pool.idle_limit(one.boot_seconds) - now)
        return max(min(deadlines), 0.0) + 0.05

    async def _launch(self, pool_name: str, now: float) -> bool:
        instance_id = f"{pool_name}-{secrets.token_hex(4)}"
        token = secrets.token_urlsafe(32)
        provider = self._providers[self._pool_providers[pool_name]]
        await self._tables.record_launch(
            instance_id, pool_name, provider.name, token_sha256(token), at_ms=int(now * 1000)
        )
        pool = next(pool for pool in self.pools if pool.name == pool_name)
        ticket = JoinTicket(
            token=token,
            acceptor_host=self._settings.acceptor_host,
            acceptor_port=self._settings.acceptor_port,
            controller_key=self._controller_key,
            system_features=sorted(pool.features),
            max_jobs=pool.slots,
        )
        tags = {OWNER_TAG: self.owner, INSTANCE_TAG: instance_id, POOL_TAG: pool_name}
        try:
            machine = await provider.launch(self._templates[pool_name], tags, ticket.model_dump_json())
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
            provider = self._providers.get(row.provider)
            if provider is None:
                # The settings no longer name it: nothing here can end the machine, which its own limits bound.
                log.warning("provisioning_provider_unknown", instance=row.id, provider=row.provider)
            else:
                provider_ids = (
                    [row.provider_id] if row.provider_id is not None else await self._tagged(provider, row.id)
                )
                for provider_id in provider_ids:
                    await self._terminate(provider, provider_id)
            await self._tables.ended(row.id, reason, at_ms=self._ms())
            self._back_off(row.pool, reason)
            self._idle_since.pop(row.id, None)
            self._draining.discard(row.id)
        finally:
            self._ending.discard(row.id)
        self.wake()

    def _back_off(self, pool: str, reason: EndReason) -> None:
        """A machine ended by its provider, or lost, launches no replacement at once: on dynhetz a builder
        terminated by hand was replaced 22 s later, eleven times in five minutes (nixidae/nixidae#1445)."""
        if reason in {EndReason.IDLE, EndReason.USED}:
            self._early_ends.pop(pool, None)
        if reason not in EARLY_ENDS:
            return
        count = self._early_ends[pool] = self._early_ends.get(pool, 0) + 1
        delay = min(self._settings.heartbeat * 2 ** (count - 1), BACKOFF_CEILING)
        self._cooldown[pool] = max(self._cooldown.get(pool, 0.0), self._clock() + delay)

    async def _tagged(self, provider: Provider, instance_id: str) -> list[str]:
        """The machine of a row whose launch never answered, found by its tag."""
        try:
            machines = await provider.machines({OWNER_TAG: self.owner, INSTANCE_TAG: instance_id})
        except ProviderError:
            log.warning("provisioning_list_failed", instance=instance_id, exc_info=True)
            return []
        return [machine.provider_id for machine in machines if machine.state is not MachineState.ENDED]

    async def _terminate(self, provider: Provider, provider_id: str) -> None:
        try:
            await provider.terminate(provider_id)
        except ProviderError:
            # The next start's reconcile finds it by its owner tag.
            log.warning("provisioning_terminate_failed", provider_id=provider_id, exc_info=True)

    def _ms(self) -> int:
        return int(self._clock() * 1000)

    async def join(self, token: str) -> Admitted | None:
        row = await self._tables.by_token(token_sha256(token))
        if row is None:
            return None
        store_id = store_id_of(row.id)
        at_ms = self._ms()
        if not await self._tables.registered(row.id, store_id, at_ms=at_ms):
            return None
        boot = (at_ms - row.launched_at_ms) / 1000
        pool = next((pool for pool in self.pools if pool.name == row.pool), None)
        # A row of a pool the settings no longer name keeps no rule: it builds nothing new for long.
        idle = (pool or Pool(name=row.pool, systems=frozenset())).idle_limit(boot)
        log.info("provisioning_joined", instance=row.id, store_id=store_id, boot_s=round(boot, 1), idle_s=idle)
        self.wake()
        return Admitted(
            store_id,
            frozenset() if pool is None else pool.mandatory_features,
            Lease(idle_seconds=idle, boot_seconds=boot),
            pool=row.pool,
        )

    async def left(self, store_id: StoreId) -> None:
        for row in await self._tables.live():
            if row.store_id == store_id:
                await self._end(row, EndReason.DISCONNECTED)
