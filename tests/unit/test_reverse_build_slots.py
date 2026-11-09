"""A reverse builder takes the builds it declares, not the pool's size.

nixidae/nixidae#1073: the build ceiling of a reverse builder was 48, from
`DaemonStore.MAX_CONNECTIONS`, and dynhetz sent 48 fetches to macbook in
one minute. None finished in 55 minutes.
"""

from __future__ import annotations

import os

import pytest

from pynixd import reverse_client
from pynixd.config import ReverseInitiatorSettings
from pynixd.reverse_server import REVERSE_TRANSFER_CONNECTIONS, UNDECLARED_MAX_JOBS, reverse_spec
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


class _Ctx:
    class local_store:  # noqa: N801 -- stands in for an attribute
        class daemon_client:  # noqa: N801
            systems = ("x86_64-linux",)


@pytest.mark.parametrize(("configured", "sent"), [(6, 6), (None, os.process_cpu_count() or 1)])
def test_the_builder_declares_its_jobs(configured: int | None, sent: int) -> None:
    initiator = reverse_client.ReverseInitiator(_Ctx(), ReverseInitiatorSettings(max_jobs=configured))  # type: ignore[arg-type]
    assert initiator._registration_info["max_jobs"] == sent  # noqa: SLF001 -- the registration is the unit
