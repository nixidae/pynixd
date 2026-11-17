"""The provisioner launches builders for waiting builds and ends the ones it holds idle. nixidae/nixidae#1198."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, RequestId, StoreId
from pynixd.config import ProvisioningPoolSettings, ProvisioningSettings, PynixdSettings
from pynixd.context import PynixdContext
from pynixd.provisioning.provider import Machine, MachineState, NoCapacity
from pynixd.provisioning.provisioner import INSTANCE_TAG, OWNER_TAG, Provisioner, store_id_of, token_sha256
from pynixd.provisioning.ticket import JoinTicket
from pynixd.scheduler import Scheduler
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode, StorePath
from pynixd_store.create_store import open_local_store
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout
from tests.functional.mock_store import MockStore
from tests.unit.test_local_build_slots import _options

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping
    from pathlib import Path


class _Provider:
    name = "fake"

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
    def __init__(self, db: LocalStoreDB, scheduler: Scheduler, provider: _Provider) -> None:
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
                    max_instances=2,
                    idle_seconds=60.0,
                    launch_timeout=120.0,
                )
            ],
        )
        self.provisioner = Provisioner(
            settings,
            provider,
            db.provisioned,
            scheduler,
            self._remove,
            controller_key="ssh-ed25519 AAAA controller",
            clock=lambda: self.now,
        )

    async def _remove(self, store_id: StoreId) -> None:
        self.removed.append(store_id)

    async def wait_for(self, name: str) -> None:
        request = BuildDerivationRequest(
            drv_path=StorePath(path=f"/nix/store/0000000000000000000000000000000{name}-{name}.drv"),
            derivation=BasicDerivation(platform="aarch64-linux", builder=""),
            build_mode=BuildMode.NORMAL,
        )
        await self.scheduler.queue.enqueue(
            request, from_goal_path=True, goal_request_id=RequestId(7), options=_options(4)
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

    assert await world.provisioner.join(ticket.token) == store_id_of(row.id)
    assert await world.provisioner.join(ticket.token) is None, "a token joined twice"
    assert await world.provisioner.join("not-a-token") is None


async def test_an_idle_builder_ends_after_its_idle_time(world: _World) -> None:
    await world.provisioner.step()
    assert world.provider.launches == 0, "nothing waits, so nothing launches"
    await world.wait_for("1")
    await world.provisioner.step()
    [launched] = world.provider.machines_.values()
    store_id = await world.provisioner.join(world.ticket(launched.provider_id).token)
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
    store_id = await world.provisioner.join(world.ticket(first.provider_id).token)
    assert store_id is not None

    await world.provisioner.left(store_id)
    await world.provisioner.step()

    assert world.provider.terminated == [first.provider_id]
    assert world.removed == [], "the acceptor removed the store already"
    assert world.provider.launches == 2


async def test_a_builder_stuck_booting_is_replaced(world: _World) -> None:
    await world.wait_for("1")
    await world.provisioner.step()
    [first] = world.provider.machines_.values()

    world.now += 121
    await world.provisioner.step()

    assert world.provider.terminated == [first.provider_id]
    assert world.provider.launches == 2


async def test_a_start_ends_what_the_last_run_left(world: _World) -> None:
    await world.db.provisioned.record_launch("arm-old", "arm", "fake", "00" * 32, at_ms=1)
    await world.provider.launch("worker", {OWNER_TAG: "controller", INSTANCE_TAG: "arm-old"}, "")
    await world.provider.launch("worker", {OWNER_TAG: "controller", INSTANCE_TAG: "arm-lost"}, "")
    await world.provider.launch("worker", {OWNER_TAG: "someone-else"}, "")

    await world.provisioner.reconcile()

    assert sorted(world.provider.terminated) == ["worker-1", "worker-2"]
    assert await world.db.provisioned.live() == []
