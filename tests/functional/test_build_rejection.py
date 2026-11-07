"""A build this store cannot run fails as Nix's `reject` fails it (`derivation-building-goal.cc:289`).

A reverse builder serves its sessions in `proxy` mode, so a `BuildDerivation` there reaches the local store and
no scheduler. With no `builders`, the hook declines at once (`build-remote.cc:116`) and the daemon refuses the
build with every reason that applies. With `builders`, the hook may take it, and pynixd leaves it to the daemon.
"""

from __future__ import annotations

import contextlib
import re
from typing import TYPE_CHECKING, Any

from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import ExternalUnixStoreSpec, ScheduleMode
from pynixd.op_routes import OpRoute
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildMode,
    BuildResultStatus,
    DerivationOutput,
    StorePath,
)
from pynixd.store import ExternalUnixStore, LocalDBStore
from pynixd.store.base import get_current_system
from tests._conftest.nix_config import for_test_store
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

FOREIGN = "riscv64-none"
MISSING = "rejection-missing"
NO_MACHINES = "@/nonexistent/machines"
A_MACHINE = "ssh://nobody@localhost mips64-none"
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _request(name: str, system: str, features: str) -> BuildDerivationRequest:
    out = f"/nix/store/{'1' * 32}-{name}"
    env = {"builder": "/bin/sh", "name": name, "out": out, "system": system}
    if features:
        env["requiredSystemFeatures"] = features
    return BuildDerivationRequest(
        drv_path=StorePath(path=f"/nix/store/{'0' * 32}-{name}.drv"),
        derivation=BasicDerivation(
            outputs={"out": DerivationOutput(path=out, method="", hash_digest="")},
            input_srcs=set(),
            platform=system,
            builder="/bin/sh",
            args=["-c", "echo > $out"],
            env=env,
        ),
        build_mode=BuildMode.NORMAL,
    )


def _fallthrough() -> float:
    return sum(
        sample.value
        for metric in REGISTRY.collect()
        if metric.name == "pynixd_op_fallthrough"
        for sample in metric.samples
        if sample.name == "pynixd_op_fallthrough_total" and sample.labels.get("op") == "BuildDerivation"
    )


@contextlib.asynccontextmanager
async def _client(tmp_path: Path, route: OpRoute, name: str, **settings: Any) -> AsyncIterator[ExternalUnixStore]:
    """A `proxy` session on a fresh store with BuildDerivation on *route*, and a client of it."""
    root = tmp_path / f"{route.name}-{name}" / "store"
    socket = tmp_path / f"{route.name}-{name}.sock"
    nix_config = for_test_store(substituters=())
    for key, value in settings.items():
        setattr(nix_config, key, value)
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=nix_config,
        op_routes={"BuildDerivation": route},
    )
    async with (
        Server(
            stores={StoreId("local"): LocalDBStore(spec)},
            unix_path=socket,
            ssh_port=None,
            http_port=None,
            schedule_mode=ScheduleMode.proxy,
        ),
        ExternalUnixStore(ExternalUnixStoreSpec(store_id=StoreId("client"), socket_path=socket, store_path=root)) as c,
    ):
        yield c


async def _answers(tmp_path: Path, route: OpRoute, name: str, **settings: Any) -> dict[str, tuple[int, str]]:
    native = get_current_system()
    cases = {
        "foreign": (FOREIGN, ""),
        "unfeatured": (native, MISSING),
        "neither": (FOREIGN, MISSING),
    }
    seen: dict[str, tuple[int, str]] = {}
    async with _client(tmp_path, route, name, **settings) as client:
        for case, (system, features) in cases.items():
            response = await client.call(_request(f"rejection-{case}", system, features), skip_probe=True)
            seen[case] = response.result.status, response.result.error_msg
    return seen


async def test_a_build_this_store_cannot_run_is_refused_as_the_daemon_refuses_it(tmp_path: Path) -> None:
    before = _fallthrough()
    here = await _answers(tmp_path, OpRoute.pynixd, "plain", builders=[NO_MACHINES])
    assert _fallthrough() == before, "the daemon refused a build of the pynixd route"
    there = await _answers(tmp_path, OpRoute.daemon, "plain", builders=[NO_MACHINES])

    assert here == there
    assert all(status == BuildResultStatus.INPUT_REJECTED for status, _msg in here.values()), here
    native = get_current_system()
    assert ANSI.sub("", here["foreign"][1]).splitlines()[1:] == [
        "Reason: platform mismatch",
        f"Required system: '{FOREIGN}'",
        f"Current system: '{native}'",
    ], here["foreign"]
    assert ANSI.sub("", here["unfeatured"][1]).splitlines()[1:3] == [
        "Reason: missing system features",
        f"Required features: {{{MISSING}}}",
    ], here["unfeatured"]
    reasons = [line for line in ANSI.sub("", here["neither"][1]).splitlines() if line.startswith("Reason:")]
    assert reasons == ["Reason: platform mismatch", "Reason: missing system features"], here["neither"]


async def test_max_jobs_zero_refuses_every_build_as_the_daemon_refuses_it(tmp_path: Path) -> None:
    here = await _answers(tmp_path, OpRoute.pynixd, "nojobs", builders=[NO_MACHINES], max_jobs=0)
    there = await _answers(tmp_path, OpRoute.daemon, "nojobs", builders=[NO_MACHINES], max_jobs=0)

    assert here == there
    lines = ANSI.sub("", here["foreign"][1]).splitlines()
    assert lines[1:3] == [
        "Reason: local builds are disabled (max-jobs = 0)",
        "Hint: set 'max-jobs' to a non-zero value to enable local builds, or configure remote builders via 'builders'",
    ], lines
    assert lines[3] == "Reason: platform mismatch", lines


async def test_a_build_the_daemons_builders_may_take_is_left_to_the_daemon(tmp_path: Path) -> None:
    """With a machine, the hook answers first: here it finds none of the right type and declines."""
    before = _fallthrough()
    here = await _answers(tmp_path, OpRoute.pynixd, "machine", builders=[A_MACHINE])
    assert _fallthrough() == before + 3, "pynixd refused a build the daemon's builders could take"
    there = await _answers(tmp_path, OpRoute.daemon, "machine", builders=[A_MACHINE])

    assert here == there
    assert all(status == BuildResultStatus.INPUT_REJECTED for status, _msg in here.values()), here
