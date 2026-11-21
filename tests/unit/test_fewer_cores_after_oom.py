"""A build that ran out of memory builds again with half the cores (nixidae/nixidae#1558).

sg1's builds failed because each one ran on every core of its host. `NIX_BUILD_CORES` is not part of the
derivation's hash, so a build with fewer cores makes the same outputs. A pynixd builder sees the OOM kill in its
build's cgroup and builds again itself; for a Nix daemon's builder, pynixd reads SIGKILL and queues it again.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.build_resources import BuildResources
from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue
from pynixd.execution import builder_options, fewer_cores_on_a_daemon, ran_with
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus
from pynixd.store.local_db import fewer_cores_here
from pynixd_store.builder.cgroup import read
from pynixd_store.builder.usage import BuildUsage, UsageSource
from tests._conftest.execution import FakeSandbox, Triggers, fake_executor, queued
from tests._conftest.placement import FakeStore, options

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    from nix_daemon_protocol.wire_ops import WireRequest

BUILDER = StoreId("sg1-n1")
KILLED_MESSAGE = "builder for '/nix/store/x-llvm.drv' failed due to signal 9 (Killed)"
GCC_KILLED = (
    "builder for '/nix/store/x-llvm.drv' failed with exit code 2;\nlast 2 log lines:\n"
    "> g++: fatal error: Killed signal terminated program cc1plus\n> make: *** [Makefile:12: a.o] Error 1"
)
MAKE_KILLED = "builder for '/nix/store/x.drv' failed with exit code 2;\nlast log line:\n> make: *** [all] Error 137"


def _failed(message: str, resources: BuildResources | None = None) -> BuildDerivationResponse:
    return BuildDerivationResponse(
        result=BuildResult(
            status=int(BuildResultStatus.PERMANENT_FAILURE),
            error_msg=message,
            resources=resources or BuildResources(),
        )
    )


@pytest.mark.parametrize(
    ("resp", "cores", "fewer"),
    [
        pytest.param(_failed(KILLED_MESSAGE), 16, 8, id="killed"),
        pytest.param(_failed(GCC_KILLED), 16, 8, id="a-compiler-killed"),
        pytest.param(_failed(MAKE_KILLED), 16, 8, id="a-recipe-killed"),
        pytest.param(
            _failed(KILLED_MESSAGE, BuildResources(measured=1, source="rusage", cores=32)), 16, 16, id="rusage-pynixd"
        ),
        pytest.param(
            BuildDerivationResponse(
                result=BuildResult(status=int(BuildResultStatus.TIMED_OUT), error_msg=KILLED_MESSAGE)
            ),
            16,
            None,
            id="timed-out",
        ),
        pytest.param(_failed(KILLED_MESSAGE), 1, None, id="one-core"),
        pytest.param(_failed(KILLED_MESSAGE), None, None, id="cores-unknown"),
        pytest.param(_failed("error: expected ';'"), 16, None, id="a-build-error"),
        pytest.param(
            _failed(KILLED_MESSAGE, BuildResources(measured=1, source="cgroup", cores=16)),
            16,
            None,
            id="a-cgroup-builder",
        ),
        pytest.param(
            BuildDerivationResponse(result=BuildResult(status=int(BuildResultStatus.BUILT))), 16, None, id="built"
        ),
    ],
)
def test_a_daemon_builder_killed_by_sigkill_gets_half_the_cores(
    resp: BuildDerivationResponse, cores: int | None, fewer: int | None
) -> None:
    """A pynixd builder with a cgroup halved the cores itself already: the scheduler must not again. One without
    a cgroup counted no OOM kill, and reported its cores, which win."""
    assert fewer_cores_on_a_daemon(resp, cores) == fewer


def _usage(oom_kills: int) -> BuildUsage:
    return BuildUsage(
        source=UsageSource.CGROUP,
        cpu_user_us=0,
        cpu_system_us=0,
        peak_memory_bytes=0,
        io_read_bytes=0,
        io_write_bytes=0,
        oom_kills=oom_kills,
    )


@pytest.mark.parametrize(
    ("status", "used", "cores", "fewer"),
    [
        pytest.param(BuildResultStatus.PERMANENT_FAILURE, [_usage(1)], 64, 32, id="oom"),
        pytest.param(BuildResultStatus.PERMANENT_FAILURE, [_usage(0)], 64, None, id="no-oom"),
        pytest.param(BuildResultStatus.PERMANENT_FAILURE, [], 64, None, id="unmeasured"),
        pytest.param(BuildResultStatus.PERMANENT_FAILURE, [_usage(1)], 1, None, id="one-core"),
        pytest.param(BuildResultStatus.BUILT, [_usage(1)], 64, None, id="built-anyway"),
        pytest.param(BuildResultStatus.TIMED_OUT, [_usage(1)], 64, None, id="timed-out"),
    ],
)
def test_a_pynixd_build_that_the_oom_killer_ended_builds_again_with_half(
    status: BuildResultStatus, used: list[BuildUsage], cores: int, fewer: int | None
) -> None:
    assert fewer_cores_here(status, used, cores) == fewer


def test_the_cgroup_counts_the_oom_kills(tmp_path: Path) -> None:
    (tmp_path / "cpu.stat").write_text("usage_usec 10\nuser_usec 6\nsystem_usec 4\n")
    (tmp_path / "memory.peak").write_text("4096\n")
    (tmp_path / "io.stat").write_text("")
    (tmp_path / "memory.events").write_text("low 0\nhigh 0\nmax 3\noom 1\noom_kill 2\noom_group_kill 0\n")
    assert read(tmp_path).oom_kills == 2


async def test_the_cores_it_ran_with_come_from_the_cap_then_the_client_then_the_store() -> None:
    store = FakeStore(BUILDER)
    store.cores = 64
    queue = BuildQueue()
    build = await queued(queue)
    assert ran_with(build, store) == 64
    build.options = options(1).model_copy(update={"build_cores": 32})
    assert ran_with(build, store) == 32
    build.cores_cap = 8
    assert ran_with(build, store) == 8


def test_the_cap_reaches_the_builders_options() -> None:
    store, local = FakeStore(BUILDER), FakeStore(StoreId("local"))
    capped = builder_options(None, store, local, 8)
    assert capped is not None
    assert (capped.build_cores, capped.max_build_jobs) == (8, 1)
    client = options(4).model_copy(update={"build_cores": 4})
    narrowed = builder_options(client, store, local, 8)
    assert narrowed is not None
    assert narrowed.build_cores == 4


def _killed() -> Callable[[WireRequest], Awaitable[BuildDerivationResponse]]:
    async def answer(request: WireRequest) -> BuildDerivationResponse:
        del request
        return _failed(KILLED_MESSAGE)

    return answer


async def test_a_build_killed_on_a_nix_daemon_is_queued_again_with_half_the_cores() -> None:
    store = FakeStore(BUILDER)
    store.cores = 64
    store.conn.answer = _killed()
    queue = BuildQueue()
    build = await queued(queue)
    trigger = Triggers()
    executor = fake_executor(queue, {BUILDER: store}, here=FakeSandbox(), trigger=trigger)
    await executor.run(build, store)
    assert build.cores_cap == 32
    assert not build.answer.done()
    assert BUILDER not in build.store_failures
    assert trigger.count >= 1
