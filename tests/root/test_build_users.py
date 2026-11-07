"""pynixd's builder as root, with build users (`build-users-group`), as Nix's is (`derivation-builder.cc:371`).

The guest's `root` suite runs these as root; anywhere else they skip.
"""

from __future__ import annotations

import contextlib
import grp
import os
import pwd
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

GROUP = "nixbld"
# Not under /tmp: with build users, Nix refuses a build directory with a world-writable parent
# (`checkNotWorldWritable`, `derivation-builder.cc:356`).
HERE = Path("/var/lib/pynixd-root-tests/build-users")

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="build users need root")

# The builder's own uid and gid in the sandbox, and a file it writes where the host can see who wrote it.
WHO = (
    'derivation { name = "who"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "while read k v rest; do [ $k = Uid: ] && u=$v; [ $k = Gid: ] && g=$v; done < /proc/self/status; '
    'echo $u $g > $out; echo seen > /shared/by-builder" ]; }'
)


@pytest.fixture(autouse=True)
def _clean() -> None:
    rmtree_robust(HERE)


def _build_uids() -> set[int]:
    return {pwd.getpwnam(member).pw_uid for member in grp.getgrnam(GROUP).gr_mem}


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, route: OpRoute) -> AsyncIterator[str]:
    root = HERE / route.name / "store"
    socket = tmp_path / f"{route.name}.sock"
    nix_config = for_test_store(substituters=())
    nix_config.build_users_group = GROUP
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=nix_config,
        op_routes={"BuildDerivation": route},
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}"


async def _build_who(tmp_path: Path, route: OpRoute) -> tuple[str, Path, os.stat_result]:
    shared = HERE / f"shared-{route.name}"
    shared.mkdir(parents=True)
    # The build user must be able to write it, as `programs.ccache`'s cache is group-writable.
    shared.chmod(0o777)
    async with _serve(tmp_path, route) as url:
        _rc, out, _err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                url,
                "--impure",
                "--no-link",
                "--print-out-paths",
                "--option",
                "extra-sandbox-paths",
                f"/shared={shared}",
                "--expr",
                WHO,
            ]
        )
    path = out.strip()
    real = HERE / route.name / "store" / path.lstrip("/")
    return real.read_text().strip(), real, (shared / "by-builder").stat()


async def test_a_build_runs_as_a_build_user_mapped_to_uid_1000(tmp_path: Path) -> None:
    """Sandbox uid 1000 is a member of `build-users-group` on the host, not root (`sandboxUid`, `:430`)."""
    ids, output, written = await _build_who(tmp_path, OpRoute.pynixd)

    assert ids == "1000 100"
    assert written.st_uid in _build_uids(), f"uid {written.st_uid} wrote the shared file, not a build user"
    assert output.stat().st_uid == 0, "a registered output belongs to root"


async def test_a_build_user_build_writes_what_the_daemons_writes(tmp_path: Path) -> None:
    """The daemon, as root, builds with the same users: the same ids inside, the same owners outside."""
    here = await _build_who(tmp_path, OpRoute.pynixd)
    there = await _build_who(tmp_path, OpRoute.daemon)

    assert here[0] == there[0]
    assert here[1].stat().st_uid == there[1].stat().st_uid == 0
    assert here[2].st_uid in _build_uids()
    assert there[2].st_uid in _build_uids()
