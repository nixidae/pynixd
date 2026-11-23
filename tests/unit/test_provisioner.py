"""The provisioner launches builders for waiting builds and ends the ones it holds idle. nixidae/nixidae#1198."""

from __future__ import annotations

import itertools
import os
from typing import TYPE_CHECKING

import anyio
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, RequestId, StoreId
from pynixd.config import (
    GhaSettings,
    GhaTemplateSettings,
    ProvisioningPoolSettings,
    ProvisioningSettings,
    PynixdSettings,
)
from pynixd.context import PynixdContext
from pynixd.provisioning.provider import Machine, MachineState, NoCapacity
from pynixd.provisioning.provisioner import (
    INSTANCE_TAG,
    OWNER_TAG,
    Provisioner,
    pool_of,
    store_id_of,
    token_sha256,
)
from pynixd.provisioning.ticket import JoinTicket, Lease
from pynixd.scheduler import Scheduler
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode, StorePath
from pynixd_store.create_store import open_local_store
from pynixd_store.database import DatabaseError
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout
from tests.functional.mock_store import MockStore
from tests.unit.test_local_build_slots import _options

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping
    from pathlib import Path


class _Provider:
    name = "vivarium"

    def __init__(self) -> None:
        self.machines_: dict[str, Machine] = {}
        self.user_data: dict[str, str] = {}
        self.capacity = 10
        self.launches = 0
        self.terminated: list[str] = []

    async def launch(self, template: str, tags: Mapping[str, str], user_data: str) -> Machine:
        self.launches += 1
        live = [m for m in self.machines_.values() if m.state is not MachineState.ENDED]
        if len(live) >= self.capacity:
            raise NoCapacity("full")
        provider_id = f"{template}-{len(self.machines_) + 1}"
        self.machines_[provider_id] = Machine(provider_id, MachineState.PENDING, dict(tags))
        self.user_data[provider_id] = user_data
        return self.machines_[provider_id]

    async def terminate(self, provider_id: str) -> None:
        self.terminated.append(provider_id)
        machine = self.machines_[provider_id]
        self.machines_[provider_id] = Machine(provider_id, MachineState.ENDED, machine.tags)

    async def machines(self, tags: Mapping[str, str]) -> list[Machine]:
        return [m for m in self.machines_.values() if all(m.tags.get(k) == v for k, v in tags.items())]

    async def close(self) -> None:
        pass


class _World:
    def __init__(
        self, db: LocalStoreDB, scheduler: Scheduler, provider: _Provider, mandatory_features: list[str] | None = None
    ) -> None:
        self.db = db
        self.scheduler = scheduler
        self.provider = provider
        self.now = 1000.0
        self.removed: list[StoreId] = []
        settings = ProvisioningSettings(
            owner="controller",
            acceptor_host="10.0.0.1",
            acceptor_port=2235,
            heartbeat=30.0,
            pools=[
                ProvisioningPoolSettings(
                    name="arm",
                    template="worker",
                    systems=["aarch64-linux"],
                    features=mandatory_features or [],
                    mandatory_features=mandatory_features or [],
                    pressure_wait=60.0,
                    max_instances=2,
                    idle_seconds=60.0,
                    launch_timeout=120.0,
                )
            ],
        )
        self.provisioner = Provisioner(
            settings,
            [provider],
            db.provisioned,
            scheduler,
            self._remove,
            controller_key="ssh-ed25519 AAAA controller",
            clock=lambda: self.now,
        )

    async def _remove(self, store_id: StoreId) -> None:
        self.removed.append(store_id)

    async def wait_for(self, name: str, features: str = "", request_id: int = 7) -> None:
        request = BuildDerivationRequest(
            drv_path=StorePath(path=f"/nix/store/0000000000000000000000000000000{name}-{name}.drv"),
            derivation=BasicDerivation(
                platform="aarch64-linux", builder="", env={"requiredSystemFeatures": features} if features else {}
            ),
            build_mode=BuildMode.NORMAL,
        )
        await self.scheduler.queue.enqueue(
            request, from_goal_path=True, goal_request_id=RequestId(request_id), options=_options(4)
        )
        await self.scheduler.schedule()

    async def finish_all(self) -> None:
        for build in list(self.scheduler.queue.queue):
            await self.scheduler.queue.fail(build.build_id, "the test ends it")

    def ticket(self, provider_id: str) -> JoinTicket:
        return JoinTicket.model_validate_json(self.provider.user_data[provider_id])


@pytest.fixture
async def world(tmp_path: Path) -> AsyncIterator[_World]:
    layout = StoreLayout.chroot(tmp_path)
    os.close(open_local_store(layout, {}, "2.35.0"))
    db = await LocalStoreDB.open(layout)
    store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    ctx = PynixdContext(settings=PynixdSettings(), _stores={LOCAL_STORE_ID: store})
    ctx.scheduler = scheduler = Scheduler(ctx)
    async with db:
        world = _World(db, scheduler, _Provider())
        scheduler.add_dynamic_feature("aarch64-linux")
        yield world


async def test_a_waiting_build_launches_a_builder_whose_ticket_joins_once(world: _World) -> None:
    await world.wait_for("1")

    await world.provisioner.step()

    [launched] = world.provider.machines_.values()
    ticket = world.ticket(launched.provider_id)
    assert (ticket.acceptor_host, ticket.acceptor_port, ticket.controller_key) == (
        "10.0.0.1",
        2235,
        "ssh-ed25519 AAAA controller",
    )
    assert launched.tags[OWNER_TAG] == "controller"
    [row] = await world.db.provisioned.live()
    assert (row.id, row.provider_id, row.token_sha256) == (
        launched.tags[INSTANCE_TAG],
        launched.provider_id,
        token_sha256(ticket.token),
    )

    await world.provisioner.step()
    assert world.provider.launches == 1, "a booting builder's slot counts"

    joined = await world.provisioner.join(ticket.token)
    assert joined is not None
    assert joined.store_id == store_id_of(row.id)
    assert joined.mandatory_features == frozenset(), "a pool with no mandatory features sets none"
    assert await world.provisioner.join(ticket.token) is None, "a token joined twice"
    assert await world.provisioner.join("not-a-token") is None


async def test_a_step_the_database_refuses_is_tried_again(world: _World, monkeypatch: pytest.MonkeyPatch) -> None:
    """A locked `db.sqlite` under a burst of builds must not end the daemon through this loop. nixidae/nixidae#1582."""
    failed = anyio.Event()
    stepped_again = anyio.Event()

    async def step() -> float:
        if not failed.is_set():
            failed.set()
            raise DatabaseError("database is locked")
        stepped_again.set()
        return 60.0

    monkeypatch.setattr(world.provisioner, "step", step)
    with anyio.fail_after(10):
        async with anyio.create_task_group() as group:
            group.start_soon(world.provisioner.run)
            await failed.wait()
            world.provisioner.wake()
            await stepped_again.wait()
            group.cancel_scope.cancel()


async def test_an_idle_builder_ends_after_its_idle_time(world: _World) -> None:
    await world.provisioner.step()
    assert world.provider.launches == 0, "nothing waits, so nothing launches"
    await world.wait_for("1")
    await world.provisioner.step()
    [launched] = world.provider.machines_.values()
    joined = await world.provisioner.join(world.ticket(launched.provider_id).token)
    assert joined is not None
    store_id = joined.store_id
    await world.finish_all()

    await world.provisioner.step()
    world.now += 59
    await world.provisioner.step()
    assert world.provider.terminated == []
    world.now += 2
    await world.provisioner.step()

    assert world.provider.terminated == [launched.provider_id]
    assert world.removed == [store_id]
    assert await world.db.provisioned.live() == []


async def test_a_builder_that_booted_long_is_told_and_given_its_boot_time_to_idle(world: _World) -> None:
    await world.wait_for("1")
    await world.provisioner.step()
    [launched] = world.provider.machines_.values()
    world.now += 90
    joined = await world.provisioner.join(world.ticket(launched.provider_id).token)
    assert joined is not None
    assert joined.lease == Lease(idle_seconds=90.0, boot_seconds=90.0), "60 s is the pool's minimum"
    await world.finish_all()

    await world.provisioner.step()
    [row] = world.provisioner.report()
    assert (row.boot_s, row.idle_limit_s) == (90.0, 90.0)
    world.now += 61
    await world.provisioner.step()
    assert world.provider.terminated == [], "ended at the pool's 60 s minimum, not at its 90 s boot"
    world.now += 30
    await world.provisioner.step()
    assert world.provider.terminated == [launched.provider_id]


async def test_a_builder_that_booted_fast_idles_the_pools_minimum(world: _World) -> None:
    await world.wait_for("1")
    await world.provisioner.step()
    [launched] = world.provider.machines_.values()
    world.now += 5
    joined = await world.provisioner.join(world.ticket(launched.provider_id).token)
    assert joined is not None
    assert joined.lease == Lease(idle_seconds=60.0, boot_seconds=5.0)


async def test_a_full_cloud_ends_the_row_and_waits_a_heartbeat(world: _World) -> None:
    world.provider.capacity = 0
    await world.wait_for("1")

    await world.provisioner.step()
    await world.provisioner.step()

    assert world.provider.launches == 1
    assert await world.db.provisioned.live() == []
    world.now += 31
    await world.provisioner.step()
    assert world.provider.launches == 2


async def test_a_builder_that_leaves_ends_and_is_replaced(world: _World) -> None:
    await world.wait_for("1")
    await world.provisioner.step()
    [first] = world.provider.machines_.values()
    joined = await world.provisioner.join(world.ticket(first.provider_id).token)
    assert joined is not None
    store_id = joined.store_id
    assert store_id is not None

    await world.provisioner.left(store_id)
    await world.provisioner.step()

    assert world.provider.terminated == [first.provider_id]
    assert world.removed == [], "the acceptor removed the store already"
    assert world.provider.launches == 1, "a lost builder is replaced after a wait (nixidae/nixidae#1445)"
    world.now += 30
    await world.provisioner.step()
    assert world.provider.launches == 2


async def test_a_builder_stuck_booting_is_replaced(world: _World) -> None:
    await world.wait_for("1")
    await world.provisioner.step()
    [first] = world.provider.machines_.values()

    world.now += 121
    await world.provisioner.step()

    assert world.provider.terminated == [first.provider_id]
    assert world.provider.launches == 1, "a stuck boot is replaced after a wait (nixidae/nixidae#1445)"
    world.now += 30
    await world.provisioner.step()
    assert world.provider.launches == 2


async def test_a_start_ends_what_the_last_run_left(world: _World) -> None:
    await world.db.provisioned.record_launch("arm-old", "arm", "vivarium", "00" * 32, at_ms=1)
    await world.provider.launch("worker", {OWNER_TAG: "controller", INSTANCE_TAG: "arm-old"}, "")
    await world.provider.launch("worker", {OWNER_TAG: "controller", INSTANCE_TAG: "arm-lost"}, "")
    await world.provider.launch("worker", {OWNER_TAG: "someone-else"}, "")

    await world.provisioner.reconcile()

    assert sorted(world.provider.terminated) == ["worker-1", "worker-2"]
    assert await world.db.provisioned.live() == []


async def test_a_notice_drains_the_builder_and_launches_a_replacement(world: _World) -> None:
    drained: list[tuple[StoreId, float]] = []

    async def drain(store_id: StoreId, drain_timeout: float = 300.0) -> None:
        drained.append((store_id, drain_timeout))

    world.scheduler.drain_store = drain  # type: ignore[method-assign]  # the test records the drain
    await world.wait_for("1")
    await world.provisioner.step()
    [first] = world.provider.machines_.values()
    joined = await world.provisioner.join(world.ticket(first.provider_id).token)
    assert joined is not None
    store_id = joined.store_id
    world.provider.machines_[first.provider_id] = Machine(
        first.provider_id, MachineState.RUNNING, first.tags, notice=world.now + 120
    )

    async with world.provisioner.tasks():
        await world.provisioner.step()
        await world.provisioner.step()

    assert drained == [(store_id, 110.0)], "one drain, ending before the notice does"
    [shown] = [row for row in world.provisioner.report() if row.store == store_id]
    assert (shown.state, shown.builds, shown.notice_s) == ("running", 0, 120.0)
    assert world.provider.launches == 2, "the replacement starts before the notice ends"
    assert world.provider.terminated == []


async def test_a_machine_the_provider_ended_ends_its_row(world: _World) -> None:
    await world.wait_for("1")
    await world.provisioner.step()
    [first] = world.provider.machines_.values()
    world.provider.machines_[first.provider_id] = Machine(first.provider_id, MachineState.ENDED, first.tags)

    await world.provisioner.step()

    assert [row.id for row in await world.db.provisioned.live()] != [first.tags[INSTANCE_TAG]]
    assert world.provider.launches == 1, "no replacement at once: the provider just ended one"
    world.now += 30
    await world.provisioner.step()
    assert world.provider.launches == 2, "the build still waits, so a new machine starts after the heartbeat"


async def test_machines_the_provider_keeps_ending_space_out_their_replacements(world: _World) -> None:
    """Each early end doubles the wait: 30, 60, 120 s after the heartbeat of 30 (nixidae/nixidae#1445)."""
    await world.wait_for("1")
    await world.provisioner.step()
    launched_at = [world.now]
    while len(launched_at) < 4:
        for machine in list(world.provider.machines_.values()):
            if machine.state is not MachineState.ENDED:
                world.provider.machines_[machine.provider_id] = Machine(
                    machine.provider_id, MachineState.ENDED, machine.tags
                )
        before = world.provider.launches
        await world.provisioner.step()
        while world.provider.launches == before:
            world.now += 1
            await world.provisioner.step()
        launched_at.append(world.now)
    gaps = [later - earlier for earlier, later in itertools.pairwise(launched_at)]
    assert gaps == [30, 60, 120], gaps


async def test_a_machine_that_ends_idle_resets_the_wait(world: _World) -> None:
    await world.wait_for("1")
    await world.provisioner.step()
    [first] = world.provider.machines_.values()
    world.provider.machines_[first.provider_id] = Machine(first.provider_id, MachineState.ENDED, first.tags)
    await world.provisioner.step()
    world.now += 30
    await world.provisioner.step()
    [second] = [m for m in world.provider.machines_.values() if m.state is not MachineState.ENDED]
    joined = await world.provisioner.join(world.ticket(second.provider_id).token)
    assert joined is not None
    await world.finish_all()
    await world.provisioner.step()
    world.now += 61
    await world.provisioner.step()
    assert second.provider_id in world.provider.terminated, "it ended idle"

    # A new request: failing build 1 ended request 7, and a build only it wanted is cancelled.
    await world.wait_for("2", request_id=8)
    await world.provisioner.step()
    assert world.provider.launches == 3, "an idle end left no wait"


@pytest.fixture
async def gated(tmp_path: Path) -> AsyncIterator[_World]:
    """The world with a pool whose machines take only builds that need `aws`."""
    layout = StoreLayout.chroot(tmp_path)
    os.close(open_local_store(layout, {}, "2.35.0"))
    db = await LocalStoreDB.open(layout)
    store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    ctx = PynixdContext(settings=PynixdSettings(), _stores={LOCAL_STORE_ID: store})
    ctx.scheduler = scheduler = Scheduler(ctx)
    async with db:
        world = _World(db, scheduler, _Provider(), mandatory_features=["aws"])
        # `Provisioner.run` registers its pools so; the tests drive `step` alone.
        scheduler.add_dynamic_feature("aarch64-linux", "aws")
        yield world


async def test_a_build_that_needs_no_mandatory_feature_launches_nothing(gated: _World) -> None:
    """An ordinary build fit every pool, so other agents' builds launched EC2 builders (nixidae/nixidae#1425)."""
    await gated.wait_for("1")
    await gated.provisioner.step()
    assert gated.provider.launches == 0


async def test_a_build_that_needs_the_mandatory_feature_launches_a_builder_that_takes_only_such(
    gated: _World,
) -> None:
    await gated.wait_for("1", features="aws")
    await gated.provisioner.step()
    assert gated.provider.launches == 1
    [launched] = gated.provider.machines_.values()
    joined = await gated.provisioner.join(gated.ticket(launched.provider_id).token)
    assert joined is not None
    assert joined.mandatory_features == frozenset({"aws"})


def test_a_pool_whose_idle_maximum_is_under_its_minimum_is_refused() -> None:
    with pytest.raises(ValueError, match="idle_max_seconds is less than idle_seconds"):
        ProvisioningPoolSettings(name="p", template="t", systems=["x"], idle_seconds=60.0, idle_max_seconds=30.0)
    pool = pool_of(ProvisioningPoolSettings(name="p", template="t", systems=["x"], idle_max_seconds=900.0))
    assert (pool.idle_seconds, pool.idle_max_seconds) == (300.0, 900.0)


class _GhaProvider(_Provider):
    name = "gha"


def _two_providers(tmp_path: Path) -> ProvisioningSettings:
    """`first` takes the default provider and another system, so an aarch64-linux build is `second`'s."""
    return ProvisioningSettings(
        owner="controller",
        gha=GhaSettings(
            repository="o/r",
            token_file=tmp_path / "token",
            ticket_key_file=tmp_path / "key",
            templates={"runner": GhaTemplateSettings(runs_on=["x"])},
        ),
        pools=[
            ProvisioningPoolSettings(name="first", template="worker", systems=["x86_64-linux"]),
            ProvisioningPoolSettings(
                name="second", provider="gha", template="runner", systems=["aarch64-linux"], idle_seconds=60.0
            ),
        ],
    )


async def test_each_pool_launches_and_ends_through_its_own_provider(world: _World, tmp_path: Path) -> None:
    settings = _two_providers(tmp_path)
    assert settings.providers == {"vivarium", "gha"}
    default, gha = _Provider(), _GhaProvider()
    provisioner = Provisioner(
        settings, [default, gha], world.db.provisioned, world.scheduler, world._remove, None, clock=lambda: world.now
    )
    await world.wait_for("1")

    await provisioner.step()

    assert (default.launches, gha.launches) == (0, 1)
    [row] = await world.db.provisioned.live()
    assert (row.pool, row.provider) == ("second", "gha")
    [launched] = gha.machines_.values()
    assert await provisioner.join(JoinTicket.model_validate_json(gha.user_data[launched.provider_id]).token)
    await world.finish_all()
    await provisioner.step()
    world.now += 61
    await provisioner.step()
    assert (default.terminated, gha.terminated) == ([], [launched.provider_id])


async def test_a_row_of_a_provider_no_longer_configured_ends_without_a_call(world: _World) -> None:
    await world.db.provisioned.record_launch("gone-1", "gone", "ec2", "00" * 32, at_ms=1)
    await world.provisioner.reconcile()
    assert await world.db.provisioned.live() == []
    assert world.provider.terminated == []


def test_a_pool_whose_provider_has_no_settings_is_refused() -> None:
    with pytest.raises(ValueError, match="gha settings"):
        ProvisioningSettings(pools=[ProvisioningPoolSettings(name="p", provider="gha", template="t", systems=["x"])])


async def test_a_provisioner_without_a_pool_s_provider_is_refused(world: _World, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"no provider \['gha'\]"):
        Provisioner(_two_providers(tmp_path), [_Provider()], world.db.provisioned, world.scheduler, world._remove, None)
