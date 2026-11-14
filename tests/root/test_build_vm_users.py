"""pynixd's build VM as root, with build users (nixidae/nixidae#1192).

The helper runs as the guest's root and maps the build user into its own namespace, as on the host. The VMM runs
as root, so a share's file is created as the guest process that asked: the build user. The guest's `root` suite
runs these as root, with `/dev/kvm`; anywhere else they skip.
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
from pynixd.config import BuildVmSpec
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

GROUP = "nixbld"
# Not under /tmp: with build users, Nix refuses a build directory with a world-writable parent.
HERE = Path("/var/lib/pynixd-root-tests/build-vm-users")

pytestmark = pytest.mark.skipif(
    os.geteuid() != 0 or not os.access("/dev/kvm", os.R_OK | os.W_OK) or "PYNIXD_KRUN" not in os.environ,
    reason="build users in the build VM need root, /dev/kvm and the PYNIXD_KRUN* variables",
)

# The kernel, the builder's ids and its uid map; then a write in the build directory, the XFS image, which keeps
# the build user as its owner. The second open of the output, for append, succeeds only when the share created the
# file as the build user: a root-owned 0644 file refuses it.
WHO = (
    'derivation { name = "vm-who"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "read r < /proc/sys/kernel/osrelease; '
    "while read k v rest; do [ $k = Uid: ] && u=$v; [ $k = Gid: ] && g=$v; done < /proc/self/status; "
    "read a b c < /proc/self/uid_map; echo here > $NIX_BUILD_TOP/scratch; "
    'echo $r $u $g $a $b $c > $out; echo again >> $out" ]; }'
)
RECURSIVE = """
let
  nix = builtins.storePath "@NIX@";
  inner = builtins.toFile "vm-inner.nix" ''
    derivation { name = "vm-inner"; system = builtins.currentSystem; builder = "/bin/sh";
      args = [ "-c" "echo inner > $out" ]; }
  '';
in derivation {
  name = "vm-recursive"; system = builtins.currentSystem; builder = "/bin/sh";
  requiredSystemFeatures = [ "recursive-nix" ];
  PATH = "${nix}/bin";
  inherit inner;
  args = [ (builtins.toFile "vm-recursive.sh" ''
    set -e
    read r < /proc/sys/kernel/osrelease
    built=$(nix --extra-experimental-features nix-command build --no-link --print-out-paths --file $inner)
    read b < $built
    echo "$r $b" > $out
  '') ];
}
"""


@pytest.fixture(autouse=True)
def _clean() -> None:
    rmtree_robust(HERE)


def _build_uids() -> set[int]:
    return {pwd.getpwnam(member).pw_uid for member in grp.getgrnam(GROUP).gr_mem}


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, *, experimental_features: tuple[str, ...] = ()) -> AsyncIterator[tuple[str, Path]]:
    root = HERE / "store"
    socket = tmp_path / "vm.sock"
    nix_config = for_test_store(substituters=(), experimental_features=experimental_features)
    nix_config.build_users_group = GROUP
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=nix_config,
        op_routes={"BuildDerivation": OpRoute.pynixd},
        build_vm=BuildVmSpec(memory_mib=512, cpus=1),
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}", root


async def _build(url: str, expr: str) -> tuple[str, str]:
    _rc, out, err, _both = await run_subproc(
        [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "-L", "--expr", expr]
    )
    return out.strip(), err


async def test_a_build_user_builds_in_the_vm_and_owns_what_it_writes(tmp_path: Path) -> None:
    async with _serve(tmp_path) as (url, root):
        path, log = await _build(url, WHO)
    who, again = (root / path.lstrip("/")).read_text().splitlines()
    release, uid, gid, inside, outside, count = who.split()

    assert release != os.uname().release, f"the build ran on the host's kernel: {log}"
    assert (uid, gid) == ("1000", "100"), log
    assert inside == "1000" and count == "1", log
    assert int(outside) in _build_uids(), f"the sandbox maps uid 1000 to {outside}, not a build user"
    assert again == "again"


# 24-26 s, the slowest case of the root suite: it copies Nix's closure into the store first.
@pytest.mark.extended
async def test_a_recursive_builder_as_a_build_user_reaches_the_store_in_the_vm(tmp_path: Path) -> None:
    """The guest's socket is the build user's: a root-owned 0600 socket refuses the builder."""
    nix = "/".join(os.path.realpath(NIX_BIN).split("/")[:4])
    async with _serve(tmp_path, experimental_features=("recursive-nix",)) as (url, root):
        await run_subproc([NIX_BIN, "copy", "--no-check-sigs", "--to", url, nix])
        path, log = await _build(url, RECURSIVE.replace("@NIX@", nix))
    release, inner = (root / path.lstrip("/")).read_text().split()

    assert release != os.uname().release, log
    assert inner == "inner", log
