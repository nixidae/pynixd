"""An unsandboxed build as root runs as the build user itself (`setUser`, `derivation-builder.cc:1037`).

No user namespace maps it, and no PID namespace ends what it leaves behind: `killUser` does
(`killSandbox`, `:156`). The guest's `root` suite runs these as root; anywhere else they skip.
"""

from __future__ import annotations

import contextlib
import grp
import os
import pwd
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_dir import real_store_dir, set_real_store_dir, set_store_dir, store_dir
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from pynixd_store.builder import cgroup as build_cgroup
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

GROUP = "nixbld"
HERE = Path("/var/lib/pynixd-root-tests/without-sandbox")
SLEEP = Path("/run/current-system/sw/bin/sleep")

pytestmark = [
    pytest.mark.skipif(os.geteuid() != 0, reason="build users need root"),
    pytest.mark.skipif(not SLEEP.exists(), reason=f"the builder leaves {SLEEP} behind"),
]

# The builder's real ids and groups, and a process it leaves running.
WHO = f"""while read k v rest; do [ $k = Uid: ] && u=$v; [ $k = Gid: ] && g=$v; [ $k = Groups: ] && s="$v $rest"; done < /proc/self/status
{SLEEP} 600 < /dev/null > /dev/null 2>&1 &
echo $u $g $s > $out"""


@pytest.fixture(autouse=True)
def _keep_store_dirs() -> Iterator[None]:
    """A server sets the process's store directories (`instance.py:113`); the session's server needs its own back."""
    saved, saved_real = store_dir(), real_store_dir()
    yield
    set_store_dir(saved)
    set_real_store_dir(saved_real)


@pytest.fixture(autouse=True)
def _clean() -> None:
    rmtree_robust(HERE)


def _fallthrough() -> float:
    return sum(
        sample.value
        for metric in REGISTRY.collect()
        if metric.name == "pynixd_op_fallthrough"
        for sample in metric.samples
        if sample.name == "pynixd_op_fallthrough_total" and sample.labels.get("op") == "BuildDerivation"
    )


def _build_uids() -> set[int]:
    return {pwd.getpwnam(member).pw_uid for member in grp.getgrnam(GROUP).gr_mem}


def _processes_of(uid: int) -> list[int]:
    found = []
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            with contextlib.suppress(OSError):
                if entry.stat().st_uid == uid:
                    found.append(int(entry.name))
    return found


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, route: OpRoute) -> AsyncIterator[tuple[list[str], dict[str, str], Path]]:
    base = HERE / route.name
    store, state, logs = base / "store", base / "var/nix", base / "var/log/nix"
    socket = tmp_path / f"{route.name}.sock"
    nix_config = for_test_store(substituters=())
    nix_config.build_users_group = GROUP
    nix_config.sandbox = "false"
    spec = make_test_spec(
        store_id="local",
        store_path=base,
        no_probe=True,
        nix_config=nix_config,
        op_routes={"BuildDerivation": route},
        store_dir=store,
        state_dir=state,
        log_dir=logs,
    )
    env = {"NIX_STORE_DIR": str(store), "NIX_STATE_DIR": str(base / "client"), "NIX_LOG_DIR": str(logs)}
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield [str(NIX_BIN), "build", "--store", f"unix://{socket}", "--impure"], env, store


async def _build_who(tmp_path: Path, route: OpRoute) -> tuple[list[int], Path]:
    expr = (
        f'derivation {{ name = "who-{route.name}"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f"args = [ (builtins.toFile \"who.sh\" ''\n{WHO}\n'') ]; }}"
    )
    async with _serve(tmp_path, route) as (build, env, store):
        _rc, out, _err, _both = await run_subproc(
            [*build, "--no-link", "--print-out-paths", "--expr", expr], env=dict(env)
        )
    real = store / out.strip().rsplit("/", 1)[-1]
    return [int(word) for word in real.read_text().split()], real


@pytest.mark.parametrize("cgroup", [True, False], ids=["cgroup", "no-cgroup"])
async def test_an_unsandboxed_build_runs_as_the_build_user_and_leaves_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cgroup: bool
) -> None:
    """A build cgroup kills what is left on its own; without one, as for Nix, `killUser` must."""
    if not cgroup:

        @contextlib.asynccontextmanager
        async def no_cgroup() -> AsyncIterator[None]:
            yield None

        monkeypatch.setattr(build_cgroup, "build_cgroup", no_cgroup)
    before = _fallthrough()
    ids, output = await _build_who(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "the daemon built the pynixd route's build"
    uid, gid, *groups = ids

    assert uid in _build_uids(), f"uid {uid} built, not a build user"
    assert gid == grp.getgrnam(GROUP).gr_gid
    assert gid not in groups, "the primary group is not a supplementary one (`SimpleUserLock`, `user-lock.cc:126`)"
    assert output.stat().st_uid == 0, "a registered output belongs to root"
    assert _processes_of(uid) == [], "killUser leaves no process of the build user"


async def test_an_unsandboxed_build_user_build_writes_what_the_daemons_writes(tmp_path: Path) -> None:
    here, here_out = await _build_who(tmp_path, OpRoute.pynixd)
    there, there_out = await _build_who(tmp_path, OpRoute.daemon)

    # Each takes a free build user, not always the same one.
    assert here[1:] == there[1:]
    assert here[0] in _build_uids()
    assert there[0] in _build_uids()
    assert here_out.stat().st_uid == there_out.stat().st_uid == 0
    assert _processes_of(here[0]) == _processes_of(there[0]) == []
