"""As root on darwin, a build runs as a `_nixbld` user, as Nix's darwin builder does (nixidae/nixidae#1367).

Each case builds twice: on Nix's own darwin builder, and with no daemon at all. The builder writes its real and
effective uid and its groups. With the minimal profile it also leaves a process behind, which `killSandbox` ends.
darwin and root only: `sudo pytest tests/root/test_build_darwin_users.py`.
"""

from __future__ import annotations

import contextlib
import grp
import os
import platform
import pwd
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_dir import real_store_dir, set_real_store_dir, set_store_dir, store_dir
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

GROUP = "nixbld"
#: A build user must pass through each parent, and none may be world-writable (`checkNotWorldWritable`):
#: pytest's directories are 0700 and root's, and `/private/var/tmp` is 1777.
ROOT_OF_TESTS = Path("/nix/var")
HERE = ROOT_OF_TESTS / "pynixd-root-tests/darwin-users"
SYSTEM = "aarch64-darwin" if platform.machine() == "arm64" else "x86_64-darwin"

pytestmark = [
    pytest.mark.skipif(sys.platform != "darwin", reason="Nix's darwin builder is the reference"),
    pytest.mark.skipif(os.geteuid() != 0, reason="build users need root"),
]

# bash's own variables: the sandbox opens no `id`. `/bin/sleep` is outside it, so only the minimal profile runs it.
#: `''${` is a literal `${` in the derivation's `''` string.
WHO = """echo $UID $EUID ''${GROUPS[*]} > $out
if [ -x /bin/sleep ]; then /bin/sleep 600 < /dev/null > /dev/null 2>&1 & fi"""


@pytest.fixture(autouse=True)
def _keep_store_dirs() -> Iterator[None]:
    """A server sets the process's store directories (`instance.py:113`); the session's server needs its own back."""
    saved, saved_real = store_dir(), real_store_dir()
    yield
    set_store_dir(saved)
    set_real_store_dir(saved_real)


def _build_uids() -> set[int]:
    return {pwd.getpwnam(member).pw_uid for member in grp.getgrnam(GROUP).gr_mem}


def _processes_of(uid: int) -> list[str]:
    ps = subprocess.run(["/bin/ps", "-U", str(uid), "-o", "pid=,command="], capture_output=True, text=True, check=False)
    return ps.stdout.splitlines()


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, daemon: bool, sandbox: str) -> AsyncIterator[tuple[list[str], dict[str, str], Path]]:
    base = HERE / sandbox
    rmtree_robust(base)
    base.mkdir(parents=True)
    for parent in (base, *base.parents):
        if parent == ROOT_OF_TESTS:
            break
        parent.chmod(0o755)
    store, state, logs = base / "store", base / "var/nix", base / "var/log/nix"
    nix_config = for_test_store(substituters=())
    nix_config.build_users_group = GROUP
    nix_config.sandbox = sandbox
    spec = make_test_spec(
        store_id="local",
        store_path=base,
        no_probe=True,
        feature_matrix={SYSTEM: set()},
        nix_config=nix_config,
        op_routes={"BuildDerivation": OpRoute.daemon if daemon else OpRoute.pynixd},
        daemon=daemon,
        store_dir=store,
        state_dir=state,
        log_dir=logs,
        socket_path=tmp_path / f"{sandbox}.nix",
    )
    socket = tmp_path / f"{'daemon' if daemon else 'pynixd'}-{sandbox}.sock"
    env = {"NIX_STORE_DIR": str(store), "NIX_STATE_DIR": str(base / "client"), "NIX_LOG_DIR": str(logs)}
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield [str(NIX_BIN), "build", "--store", f"unix://{socket}", "--impure"], env, store


async def _build_who(tmp_path: Path, daemon: bool, sandbox: str) -> tuple[list[int], os.stat_result]:
    expr = (
        f'derivation {{ name = "who"; system = builtins.currentSystem; builder = "/bin/bash"; '
        f"args = [ (builtins.toFile \"who.sh\" ''\n{WHO}\n'') ]; }}"
    )
    async with _serve(tmp_path, daemon, sandbox) as (build, env, store):
        _rc, out, _err, _both = await run_subproc(
            [*build, "--no-link", "--print-out-paths", "--expr", expr], env=dict(env), nix_config={"substituters": ""}
        )
        real = store / out.strip().rsplit("/", 1)[-1]
        return [int(word) for word in real.read_text().split()], real.lstat()


@pytest.mark.parametrize("sandbox", ["true", "false"])
async def test_a_build_runs_as_a_build_user_and_leaves_nothing(tmp_path: Path, sandbox: str) -> None:
    here, here_stat = await _build_who(tmp_path, False, sandbox)
    there, there_stat = await _build_who(tmp_path, True, sandbox)
    uid, euid, *groups = here
    assert uid == euid
    assert uid in _build_uids()
    assert grp.getgrnam(GROUP).gr_gid in groups
    # Both take the first free user, so the two builds see the same ids.
    assert here == there
    # `canonicalisePathMetaData`: the output is root's, read-only, at the store's time.
    assert (here_stat.st_uid, here_stat.st_mode & 0o7777, int(here_stat.st_mtime)) == (0, 0o444, 1)
    assert (there_stat.st_uid, there_stat.st_mode & 0o7777) == (0, 0o444)
    assert _processes_of(uid) == []
