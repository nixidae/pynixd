"""A reverse builder takes the builds it declares, not the pool's size.

nixidae/nixidae#1073: the build ceiling of a reverse builder was 48, from
`DaemonStore.MAX_CONNECTIONS`, and dynhetz sent 48 fetches to macbook in
one minute. None finished in 55 minutes.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from pynixd import reverse_client
from pynixd.config import PressureSettings, ReverseInitiatorSettings, ScheduleMode
from pynixd.monitor import DummyResourceMonitor
from pynixd.psi import SystemHealth
from pynixd.reverse_server import REVERSE_TRANSFER_CONNECTIONS, UNDECLARED_MAX_JOBS, reverse_spec
from pynixd.store.local_host import BuildCapacity
from pynixd.store.reverse import ReverseStore


def _slots(reg: dict[str, object]) -> tuple[int, int]:
    store = ReverseStore(reverse_spec(reg, "builder1"), object())  # type: ignore[arg-type]
    return store.build_pool.max_connections, store.transfer_pool.max_connections


async def test_the_declared_jobs_are_the_build_slots() -> None:
    assert _slots({"systems": ["aarch64-linux"], "max_jobs": 4}) == (4, REVERSE_TRANSFER_CONNECTIONS)


@pytest.mark.parametrize("declared", [None, 0, -2, "12", True], ids=["none", "zero", "negative", "string", "bool"])
async def test_a_builder_that_declares_no_jobs_gets_a_small_default(declared: object) -> None:
    reg: dict[str, object] = {"systems": ["aarch64-linux"]}
    if declared is not None:
        reg["max_jobs"] = declared
    assert _slots(reg) == (UNDECLARED_MAX_JOBS, REVERSE_TRANSFER_CONNECTIONS)


def _nix_config(max_jobs: int, cores: int) -> dict[str, Any]:  # Any: `nix config show --json`, read by key
    return {"max-jobs": {"value": max_jobs}, "cores": {"value": cores}}


class _Ctx:
    class local_store:  # noqa: N801 -- stands in for an attribute
        class daemon_client:  # noqa: N801
            systems = ("x86_64-linux",)

        @staticmethod
        async def read_nix_config() -> dict[str, Any]:
            return _nix_config(2, 8)


@pytest.mark.parametrize(("configured", "sent"), [(6, 6), (None, os.process_cpu_count() or 1)])
def test_the_builder_declares_its_jobs(configured: int | None, sent: int) -> None:
    initiator = reverse_client.ReverseInitiator(_Ctx(), ReverseInitiatorSettings(max_jobs=configured))  # type: ignore[arg-type]
    assert initiator._registration_info["max_jobs"] == sent  # noqa: SLF001 -- the registration is the unit


@pytest.mark.parametrize(
    ("settings", "declared"),
    [
        pytest.param(ReverseInitiatorSettings(), (2, 8), id="its-nix"),
        pytest.param(ReverseInitiatorSettings(max_jobs=6), (6, None), id="configured"),
        pytest.param(
            ReverseInitiatorSettings(schedule_mode=ScheduleMode.scheduler),
            (os.process_cpu_count() or 1, None),
            id="scheduler-mode",
        ),
    ],
)
async def test_a_proxying_builder_declares_its_nix_s_jobs_and_cores(
    settings: ReverseInitiatorSettings, declared: tuple[int, int | None]
) -> None:
    """sg1's daemon says `max-jobs = 2`, `cores = 8` and caps its memory for that. nixidae/nixidae#1568."""
    initiator = reverse_client.ReverseInitiator(_Ctx(), settings)  # type: ignore[arg-type]

    await initiator._declare_capacity()  # noqa: SLF001 -- the registration is the unit

    info = initiator._registration_info  # noqa: SLF001
    assert (info["max_jobs"], info["cores"]) == declared


@pytest.mark.parametrize(
    ("max_jobs", "cores", "declared"),
    [(2, 8, (2, 8)), (16, 0, (16, None)), (0, 4, (os.process_cpu_count() or 1, 4))],
    ids=["capped", "every-core", "builds-nothing"],
)
def test_the_capacity_of_a_nix_config(max_jobs: int, cores: int, declared: tuple[int, int | None]) -> None:
    assert reverse_client.declared_capacity(BuildCapacity.from_nix_config(_nix_config(max_jobs, cores))) == declared


@pytest.mark.parametrize(("sent", "cores"), [(8, 8), (None, 48), (0, 48), ("8", 48), (True, 48)])
async def test_the_declared_cores_cap_the_store_s_cores(sent: object, cores: int) -> None:
    store = ReverseStore(reverse_spec({"max_jobs": 2, "cores": sent}, "builder1"), object())  # type: ignore[arg-type]
    store.monitor = DummyResourceMonitor(PressureSettings(), lambda _health: None)
    store.monitor.health = SystemHealth(cores=48.0)

    assert store.cores == cores
