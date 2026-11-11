"""Determinate Nix's `QueryActiveBuilds` (op 48): the builds of pynixd's own builder (nixidae/nixidae#1250).

`from_json` (`active-builds.cc:115` and `:150` of Determinate v3.24.0) reads each key but the usage with `.at()`,
so one missing key fails `nix ps` with no useful message.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import anyio
import pytest

from nix_daemon_protocol.query_active_builds import QueryActiveBuildsRequest, QueryActiveBuildsResponse
from pynixd import wire
from pynixd.proxy import DaemonProxy
from pynixd.recursive import RestrictedError, RestrictedProxy
from pynixd.store.daemon import DaemonStore
from pynixd.store.local_db import LocalDBStore
from pynixd_store import active_builds
from pynixd_store.active_builds import ActiveBuild, ActiveBuilds
from pynixd_store.builder.restriction import RestrictionContext
from pynixd_store.errors import PynixdError

DRV = "00000000000000000000000000000000-sleeper.drv"
BUILD_KEYS = {
    "type",
    "nixPid",
    "clientPid",
    "clientUid",
    "mainPid",
    "mainUser",
    "cgroup",
    "startTime",
    "derivation",
    "processes",
    "utime",
    "stime",
    "rss",
}
"""What `adl_serializer<ActiveBuildInfo>::to_json` (`active-builds.cc:160`) writes."""
PROCESS_KEYS = {"pid", "parentPid", "user", "argv", "utime", "stime", "cutime", "cstime", "rss"}
"""What `adl_serializer<ActiveBuildInfo::ProcessInfo>::to_json` (`active-builds.cc:100`) writes."""
GONE_PID = 2**22 + 1
"""Above the largest `pid_max` of Linux, so no process has it."""


async def test_a_running_build_has_every_key_that_from_json_reads() -> None:
    registry = ActiveBuilds()
    argv = [sys.executable, "-c", "import time; print('up', flush=True); time.sleep(60)"]
    async with await anyio.open_process(argv) as sleeper:
        try:
            # During `execve` the kernel gives an empty cmdline; a line on stdout comes after it. nixidae/nixidae#1281.
            if sleeper.stdout is None:
                raise AssertionError("open_process pipes stdout by default")
            with anyio.fail_after(30):
                await sleeper.stdout.receive()
            with registry.running(ActiveBuild(sleeper.pid, os.getuid(), None, 1700000000, DRV)):
                (build,) = json.loads(json.dumps(active_builds.query(registry.snapshot())))
            assert registry.snapshot() == ()
        finally:
            sleeper.kill()
    assert set(build) == BUILD_KEYS
    assert (build["type"], build["mainPid"], build["startTime"]) == ("build", sleeper.pid, 1700000000.0)
    assert (build["derivation"], build["cgroup"], build["clientPid"]) == (DRV, None, None)
    assert build["mainUser"]["uid"] == os.getuid()
    (process,) = build["processes"]
    assert set(process) == PROCESS_KEYS
    assert (process["pid"], process["parentPid"], process["argv"]) == (sleeper.pid, os.getpid(), argv)
    assert isinstance(process["rss"], int)


def test_a_cgroup_gives_cpu_time_and_mapped_memory(tmp_path: Path) -> None:
    """`getCgroupStats` (`cgroup.cc:52`): `anon` plus `file_mapped`, not `memory.current`."""
    (tmp_path / "cpu.stat").write_text("usage_usec 9\nuser_usec 1500000\nsystem_usec 250000\n")
    (tmp_path / "memory.stat").write_text("anon 4096\nfile 999999\nfile_mapped 1024\n")

    assert active_builds.cgroup_usage(tmp_path) == {"utime": 1.5, "stime": 0.25, "rss": 5120}


def _named(*, answers: bool) -> set[str]:
    proxy = SimpleNamespace(
        stores={}, pynixd_features=frozenset(), local_store=SimpleNamespace(answers_active_builds=answers)
    )
    return DaemonProxy.server_features(cast("DaemonProxy", proxy), frozenset())


def test_pynixd_names_query_active_builds_only_when_its_store_sees_every_build() -> None:
    """Op 48 has no fallback: a store that cannot see the builds would answer an empty list while they run."""
    assert wire.FEATURE_QUERY_ACTIVE_BUILDS in _named(answers=True)
    assert wire.FEATURE_QUERY_ACTIVE_BUILDS not in _named(answers=False)


def test_a_builder_that_names_query_active_builds_does_not_make_pynixd_name_it() -> None:
    """A build on an ssh builder is not in the local store's answer, and a recursive build names `honourable` alone."""
    builder = SimpleNamespace(no_schedule=False, features={wire.FEATURE_QUERY_ACTIVE_BUILDS})
    proxy = SimpleNamespace(stores={"builder": builder}, local_store=SimpleNamespace(keeps_provenance=False))

    assert wire.FEATURE_QUERY_ACTIVE_BUILDS not in DaemonProxy.honourable_features(cast("DaemonProxy", proxy))


class _Restricted(RestrictedProxy):
    def __init__(self, restriction: RestrictionContext) -> None:
        self.restriction = restriction


async def test_a_build_that_sends_op_48_gets_the_error_of_require() -> None:
    """`require<QueryActiveBuildsStore>` (`store-cast.hh:20`) throws after `startWork`; the session goes on."""
    restriction = RestrictionContext(set(), None, Path("/nix/store"), submitting=False)

    with pytest.raises(RestrictedError, match="Querying active builds not supported by store"):
        await _Restricted(restriction).execute(QueryActiveBuildsRequest())


def _daemon(builds: list[dict[str, Any]], *, answers: bool) -> tuple[SimpleNamespace, list[object]]:
    calls: list[object] = []

    async def call(request: object, client: object = None, suppress_last: bool = False) -> QueryActiveBuildsResponse:
        calls.append(request)
        return QueryActiveBuildsResponse(builds=json.dumps(builds))

    return SimpleNamespace(answers_active_builds=answers, store_id="local", call=call), calls


async def test_a_daemon_without_the_feature_never_gets_op_48() -> None:
    """Upstream Nix closes the session on an operation it does not know."""
    daemon, calls = _daemon([], answers=False)

    with pytest.raises(PynixdError, match="Querying active builds not supported by store"):
        await DaemonStore.query_active_builds(cast("DaemonStore", daemon), QueryActiveBuildsRequest())
    assert calls == []


async def test_the_local_store_answers_its_own_builds_and_those_of_a_daemon_that_names_the_feature() -> None:
    registry = ActiveBuilds()
    theirs = [{"type": "build", "derivation": "11111111111111111111111111111111-theirs.drv"}]
    for answers, expected in ((True, [DRV, *(build["derivation"] for build in theirs)]), (False, [DRV])):
        daemon, calls = _daemon(theirs, answers=answers)
        store = SimpleNamespace(
            answers_active_builds=True, engine=SimpleNamespace(active_builds=registry), _daemon=daemon, store_id="l"
        )
        with registry.running(ActiveBuild(GONE_PID, os.getuid(), None, 1700000000, DRV)):
            response = await LocalDBStore.query_active_builds(cast("LocalDBStore", store), QueryActiveBuildsRequest())

        assert [build["derivation"] for build in json.loads(response.builds)] == expected
        assert len(calls) == int(answers)
        assert " " not in response.builds
