"""pynixd's own builder builds again with half the cores after the OOM killer ended a build (nixidae/nixidae#1558).

The builder stands in for `local.build`: a real OOM kill would take the test's own processes with it. It reports an
OOM kill in its cgroup until the build runs on 2 cores, and then a failure of another kind, so the loop stops.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.serde import BuildResultStatus, LogNext
from pynixd.store import LocalDBStore, Store
from pynixd_store.builder import local as local_builder
from pynixd_store.builder.local import LocalBuildResult
from pynixd_store.builder.usage import BuildUsage, UsageSource
from tests._conftest.config import TEST_OP_ROUTES
from tests._conftest.execution import request, sandbox_settings
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

    from nix_daemon_protocol.logs import LogMessage
    from pynixd_store.builder.settings import BuildSettings


async def test_the_oom_killer_halves_the_cores_until_another_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ran_with: list[int] = []

    async def build(*args: Any, **kwargs: Any) -> LocalBuildResult:
        settings: BuildSettings = args[3]
        ran_with.append(settings.build_cores)
        oom = settings.build_cores > 2
        kwargs["on_usage"](
            BuildUsage(
                source=UsageSource.CGROUP,
                cpu_user_us=0,
                cpu_system_us=0,
                peak_memory_bytes=0,
                io_read_bytes=0,
                io_write_bytes=0,
                cores=settings.build_cores,
                oom_kills=1 if oom else 0,
            )
        )
        return LocalBuildResult(BuildResultStatus.PERMANENT_FAILURE, "killed" if oom else "error: expected ';'")

    monkeypatch.setattr(local_builder, "build", build)
    store_path = tmp_path / "store"
    store_path.mkdir()
    spec = make_test_spec(
        store_id="local",
        store_path=store_path,
        no_probe=True,
        daemon=False,
        op_routes={op: "pynixd" for op in TEST_OP_ROUTES},
    )
    store = LocalDBStore(spec)
    stores: dict[StoreId, Store] = {StoreId("local"): store}
    said: list[str] = []

    async def on_log(message: LogMessage) -> None:
        if isinstance(message, LogNext):
            said.append(message.text)

    async with Server(stores=stores, ssh_port=None, http_port=None):
        settings = dataclasses.replace(sandbox_settings(), build_cores=8)
        resp = await store.build_here(request("oom"), settings, on_log, 3)

    assert ran_with == [8, 4, 2]
    assert resp.result.status == BuildResultStatus.PERMANENT_FAILURE
    assert resp.result.resources.cores == 2
    assert resp.result.resources.killed_cores == 4
    assert sum("OOM killer" in text for text in said) == 2
