"""Test build statistics tracking and prioritization.

All tests in this file are statistics/metrics tests that don't trigger Store operations.
"""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import asynccontextmanager, closing
from typing import TYPE_CHECKING

import anyio
import pytest
import structlog

from nix_daemon_protocol.ids import BuildId, StoreId
from pynixd import Server
from pynixd.build_queue import QueuedBuild
from pynixd.build_stats import BuildObservation, identity_of
from pynixd.local_store_db import LocalStoreDB
from pynixd.psi import CpuUtil
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    DerivationOutput,
    QueryAllValidPathsRequest,
    QueryAllValidPathsResponse,
    QueryClosureWithInfoRequest,
    QueryClosureWithInfoResponse,
    SetOptionsRequest,
)
from pynixd.store import LocalDBStore
from pynixd.store_layout import StoreLayout
from pynixd.store_path import StorePath
from tests.conftest import STORE_PREFIX, make_test_spec, rmtree_robust, serde_path

if TYPE_CHECKING:
    from pathlib import Path

log = structlog.get_logger(__name__)


class StatsTestStore(LocalDBStore):
    """A store that simulates builds with specific durations."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.build_delays: dict[str, float] = {}

    @asynccontextmanager
    async def build_conn(self, options: SetOptionsRequest | None = None):  # type: ignore[override]
        # **The `options` parameter is not decoration.** Issue nixidae/nixidae#231 gave a
        # build the option set of the client that asked for it, and
        # `Scheduler.execute_build` has passed it ever since. A stand-in
        # without it made the build crash with a `TypeError` that the
        # scheduler reported as "Internal scheduler error", so this test read
        # an empty stats table and blamed the recording. Issue #29.
        async with self.build_pool.acquire("build", options):

            class MockConn:
                def __init__(self, store):
                    self.store = store
                    self.op_log: list[str] = []

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

                async def call(
                    self,
                    request,
                    client=None,
                    suppress_last=False,
                    raise_on_error=False,
                    options=None,
                ):

                    if isinstance(request, BuildDerivationRequest):
                        pname = request.derivation.env.get("pname", "unknown")
                        delay = self.store.build_delays.get(pname, 0.1)
                        await anyio.sleep(delay)
                        return BuildDerivationResponse(
                            result=BuildResult(status=BuildResultStatus.BUILT),
                        )
                    raise NotImplementedError(
                        f"MockConn doesn't support {type(request)}",
                    )

            yield MockConn(self)  # type: ignore

    async def execute(
        self,
        request,
        client=None,
        suppress_last=False,
        skip_probe=False,
        mark=True,
    ):

        if isinstance(request, QueryAllValidPathsRequest):
            return QueryAllValidPathsResponse(paths=set())

        if isinstance(request, QueryClosureWithInfoRequest):
            # **An empty closure, because this store has no NAR to give.**
            # It answered with one `ValidPathInfo` for each path asked and a
            # `nar_size` of 0. A size of zero means unknown, not empty, so
            # `stream_paths_store_to_store` wrote a frame holding no NAR and
            # the destination daemon answered `reached end of FramedSource`.
            #
            # It looked like a flake because the destination usually had the
            # path already, from the state an earlier run left under
            # `STORE_PREFIX`: a fresh store transferred, a reused one did
            # not. Issue #48.
            #
            # Nothing to pull is what this store means, and `_pull_outputs`
            # returns at once for it. The test measures the stats row.
            return QueryClosureWithInfoResponse(infos=[])
        return await super().execute(
            request,
            client,
            suppress_last,
            skip_probe=skip_probe,
        )

    async def call(
        self,
        request,
        client=None,
        suppress_last=False,
        raise_on_error=False,
        skip_probe=False,
    ):
        return await super().call(
            request,
            client,
            suppress_last,
            raise_on_error,
            skip_probe=skip_probe,
        )


async def test_build_stats_recording(tmp_path: Path) -> None:
    """Verify that build stats are recorded to the DB.

    This was `xfail`, with the reason "DB stats query returns no row". That
    is the symptom. The cause was the order in `_collect_outputs`: the pull
    of the outputs ran first, it raised `ConnectionRefusedError` against the
    fake stores of this module, and the statistics of a build that had
    already succeeded went with it.
    """
    pynixd_local_path = STORE_PREFIX / "stats-local"
    pynixd_remote_path = STORE_PREFIX / "stats-remote"
    rmtree_robust(pynixd_local_path)
    rmtree_robust(pynixd_remote_path)
    pynixd_local_path.mkdir(parents=True, exist_ok=True)
    pynixd_remote_path.mkdir(parents=True, exist_ok=True)

    pynixd_local = LocalDBStore(
        make_test_spec(store_id="local", store_path=pynixd_local_path, no_probe=True),
    )
    pynixd_remote = StatsTestStore(
        make_test_spec(store_id="remote", store_path=pynixd_remote_path, no_probe=True),
    )
    pynixd_remote.build_delays["fast-pkg"] = 0.05

    async with Server(
        stores={StoreId("local"): pynixd_local, StoreId("remote"): pynixd_remote},
        ssh_port=None,
        unix_path=pynixd_local_path / "socket",
    ) as server:
        # 1. Run a build to generate stats
        out_path = StorePath("/nix/store/00000000000000000000000000000004-fast-pkg")

        drv = BasicDerivation(
            platform="x86_64-linux",
            builder="/bin/sh",
            env={"pname": "fast-pkg", "version": "1.0"},
            outputs={
                "out": DerivationOutput(
                    path=str(out_path),
                    method="",
                    hash_digest="",
                ),
            },
        )
        req = BuildDerivationRequest(
            drv_path=serde_path(
                StorePath(
                    "/nix/store/00000000000000000000000000000001-fast-pkg.drv",
                ),
            ),
            derivation=drv,
            build_mode=BuildMode.NORMAL,
        )
        # We use the scheduler directly to avoid proxy overhead
        scheduler = server.scheduler
        assert scheduler is not None

        build_id, future = await scheduler.build_derivation(
            req,
        )
        await future

        # 2. Check the DB.
        #
        # **The future resolves before the row exists, and that is deliberate.**
        # `Scheduler.execute_build` calls `queue.complete` as soon as the
        # backend answers, and `_collect_outputs` runs after that and holds
        # `_record_observation`. Issue #12 states the same order from the
        # other side: the client is told the build succeeded before the pull
        # runs. So the row lands a moment after `await future` returns, and a
        # read at that instant found nothing and read as "the recording is
        # broken". Issue #29.
        assert pynixd_local.db is not None
        row = None
        with anyio.fail_after(10):
            while row is None:
                async with pynixd_local.db.execute(
                    "SELECT name, buildMs FROM PynixdBuildObservation WHERE name = 'fast-pkg'",
                ) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    await anyio.sleep(0.05)

        assert row[0] == "fast-pkg"
        # The 50 ms delay this store was given, recorded. The floor is 45
        # and not 50: `scheduler.py` stores `int((monotonic() - start) *
        # 1000)`, which truncates, and a 50 ms sleep that returns a few
        # hundred microseconds early is written as 49. Measured on a CI
        # runner, where the same test records 49 and fails a floor of 50.
        # 45 still separates a recorded delay from one that never ran.
        assert 45 <= row[1] <= 1000


async def test_scheduler_local_fasttrack(tmp_path: Path) -> None:
    """Verify that the scheduler fast-tracks tiny builds to the local store."""
    pynixd_local_path = STORE_PREFIX / "fasttrack-local"
    pynixd_remote_path = STORE_PREFIX / "fasttrack-remote"
    rmtree_robust(pynixd_local_path)
    rmtree_robust(pynixd_remote_path)
    pynixd_local_path.mkdir(parents=True, exist_ok=True)
    pynixd_remote_path.mkdir(parents=True, exist_ok=True)

    pynixd_local = LocalDBStore(
        make_test_spec(store_id="local", store_path=pynixd_local_path, no_probe=True),
    )
    pynixd_remote = StatsTestStore(
        make_test_spec(store_id="remote", store_path=pynixd_remote_path, no_probe=True),
    )

    async with Server(
        stores={StoreId("local"): pynixd_local, StoreId("remote"): pynixd_remote},
        ssh_port=None,
        unix_path=pynixd_local_path / "socket",
    ) as server:
        assert pynixd_local.db is not None
        # Pre-seed the DB with "tiny" stats
        await pynixd_local.db.record_build_observation(
            identity_of(BasicDerivation(platform="x86_64-linux", builder="", env={"pname": "tiny-pkg"})),
            BuildObservation(store_id="remote", status=0, build_ms=100),
        )
        scheduler = server.scheduler
        assert scheduler is not None

        # 1. Occupy the only remote slot with a build
        pynixd_remote.build_delays["blocker"] = 10.0
        blocker_drv = BasicDerivation(platform="x86_64-linux", builder="", env={"pname": "blocker"})
        blocker_req = BuildDerivationRequest(
            drv_path=serde_path(
                StorePath(
                    "/nix/store/00000000000000000000000000000000-blocker.drv",
                ),
            ),
            derivation=blocker_drv,
            build_mode=BuildMode.NORMAL,
        )
        await scheduler.build_derivation(blocker_req)

        # Wait for blocker to start on REMOTE
        while True:
            pending = await scheduler.queue.get_pending()
            if any(b.is_building for b in pending):
                break
            await anyio.sleep(0.1)

        # 2. Enqueue tiny-pkg
        # It should be fast-tracked to LOCAL because remote is full
        tiny_drv = BasicDerivation(platform="x86_64-linux", builder="", env={"pname": "tiny-pkg"})
        tiny_req = BuildDerivationRequest(
            drv_path=serde_path(StorePath("/nix/store/00000000000000000000000000000003-tiny.drv")),
            derivation=tiny_drv,
            build_mode=BuildMode.NORMAL,
        )
        id_tiny, fut_tiny = await scheduler.build_derivation(
            tiny_req,
        )

        # 3. Verify it's building on LOCAL
        while True:
            pending = await scheduler.queue.get_pending()
            tiny_build = next((b for b in pending if b.build_id == id_tiny), None)
            if tiny_build and tiny_build.is_building:
                log.info("tiny_build_started", build_id=id_tiny)
                break
            await anyio.sleep(0.1)

        assert tiny_build is not None
        assert tiny_build.is_building


async def test_expected_build_ms_is_the_median_of_the_identity(tmp_path: Path) -> None:
    """The estimate is the median of the identity's `BUILT` builds, then of its name.

    nixidae/nixidae#1001.
    """
    pynixd_local_path = STORE_PREFIX / "stats-hint-test"
    rmtree_robust(pynixd_local_path)
    (pynixd_local_path / "nix/var/nix/db").mkdir(parents=True)

    db_file = pynixd_local_path / "nix/var/nix/db/db.sqlite"
    with closing(sqlite3.connect(db_file)) as conn, conn:
        conn.execute("CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE)")

    pynixd_local = LocalDBStore(
        make_test_spec(store_id="local", store_path=pynixd_local_path, no_probe=True),
    )
    async with await LocalStoreDB.open(StoreLayout.chroot(pynixd_local_path)) as db:
        pynixd_local.db = db

        assert db.active

        testpkg = identity_of(BasicDerivation(platform="x86_64-linux", builder="", env={"pname": "testpkg"}))
        for store_id, build_ms in (("a", 500), ("b", 300), ("a", 400)):
            await db.record_build_observation(testpkg, BuildObservation(store_id=store_id, status=0, build_ms=build_ms))
        # A builder that already held the output built nothing: no estimate.
        await db.record_build_observation(
            testpkg, BuildObservation(store_id="a", status=BuildResultStatus.ALREADY_VALID, build_ms=5)
        )

        assert await db.expected_build_ms(testpkg) == 400

        # A new attribute is a new shape; the name still answers.
        reshaped = identity_of(
            BasicDerivation(platform="x86_64-linux", builder="", env={"pname": "testpkg", "doCheck": "1"})
        )
        assert reshaped.shape != testpkg.shape
        assert await db.expected_build_ms(reshaped) == 400

        other_system = identity_of(BasicDerivation(platform="aarch64-linux", builder="", env={"pname": "testpkg"}))
        assert await db.expected_build_ms(other_system) is None
        other_name = identity_of(BasicDerivation(platform="x86_64-linux", builder="", env={"pname": "otherpkg"}))
        assert await db.expected_build_ms(other_name) is None


class CpuUtilTestStore(StatsTestStore):
    """StatsTestStore with a settable cpu_util for scheduler testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cpu_util: CpuUtil | None = None

    @property
    def cpu_util(self) -> CpuUtil | None:
        return self._cpu_util

    @cpu_util.setter
    def cpu_util(self, value: CpuUtil | None) -> None:
        self._cpu_util = value


@pytest.mark.xfail(reason="pre-existing: scheduler saturation logic needs review")
async def test_scheduler_skips_saturated_store(tmp_path: Path) -> None:
    """Verify scheduler skips stores at >99% CPU utilization."""
    pynixd_local_path = STORE_PREFIX / "cpu-util-local"
    pynixd_busy_path = STORE_PREFIX / "cpu-util-busy"
    pynixd_free_path = STORE_PREFIX / "cpu-util-free"
    rmtree_robust(pynixd_local_path)
    rmtree_robust(pynixd_busy_path)
    rmtree_robust(pynixd_free_path)

    pynixd_local = LocalDBStore(
        make_test_spec(store_id="local", store_path=pynixd_local_path, no_probe=True),
    )
    pynixd_busy = CpuUtilTestStore(
        make_test_spec(store_id="busy", store_path=pynixd_busy_path, no_probe=True),
    )
    # Utilization 101% ensures score < 0 even with no penalties
    pynixd_busy._cpu_util = CpuUtil(utilization=101.0, cores=2.0, throttled_pct=10.0)
    pynixd_busy.build_delays["test-pkg"] = 0.05

    pynixd_free = CpuUtilTestStore(
        make_test_spec(store_id="free", store_path=pynixd_free_path, no_probe=True),
    )
    pynixd_free._cpu_util = CpuUtil(utilization=50.0, cores=2.0, throttled_pct=0.0)
    pynixd_free.build_delays["test-pkg"] = 0.05

    async with Server(
        stores={StoreId("local"): pynixd_local, StoreId("busy"): pynixd_busy, StoreId("free"): pynixd_free},
        ssh_port=None,
        unix_path=pynixd_local_path / "socket",
    ) as server:
        scheduler = server.scheduler
        assert scheduler is not None

        drv = BasicDerivation(platform="x86_64-linux", builder="", env={"pname": "test-pkg"})
        req = BuildDerivationRequest(
            drv_path=serde_path(
                StorePath(
                    "/nix/store/00000000000000000000000000000000-test-pkg.drv",
                ),
            ),
            derivation=drv,
            build_mode=BuildMode.NORMAL,
        )
        loop = asyncio.get_running_loop()
        build = QueuedBuild(
            build_id=BuildId(1),
            request=req,
            future=loop.create_future(),
        )

        ranked = scheduler.allocator.rank_stores(build, {})
        store_ids = [rs.store_id for rs in ranked]
        assert "busy" not in store_ids
        assert "free" in store_ids

        pynixd_busy._cpu_util = CpuUtil(utilization=98.0, cores=2.0, throttled_pct=5.0)
        ranked2 = scheduler.allocator.rank_stores(build, {})
        store_ids2 = [rs.store_id for rs in ranked2]
        assert "busy" in store_ids2
