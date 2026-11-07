"""pynixd's builder for a derivation that requires `uid-range`, as Nix's chroot builder.

The build takes a whole `auto-allocate-uids` slot, 65536 ids (`linux-derivation-builder.cc:444`), and its
builder is root in the sandbox (`sandboxUid`, `:430`) with `/sys` (`:841`) and a writable `/etc` (`:880`).
The guest's `root` suite runs these as root, in a QEMU guest: a container's read-only `/sys` refuses the
build's own. Anywhere else they skip.
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.constants import _NO_PROBE_FEATURE_MATRIX
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

HERE = Path("/var/lib/pynixd-root-tests/uid-range")
IDS_PER_BUILD = 1 << 16
START_ID = 1
"""The lowest `start-id` Nix allows (`user-lock.cc:177`), so the slot fits a guest of 65537 ids too."""

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="build users need root")

WHO = (
    'derivation { name = "range-who"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'requiredSystemFeatures = [ "uid-range" ]; '
    'args = [ "-c" "read u < /proc/self/uid_map; read g < /proc/self/gid_map; '
    "test -d /sys/kernel && s=sys || s=nosys; echo x > /etc/probe && e=etc || e=noetc; "
    'echo $u / $g / $s / $e > $out; echo seen > /shared/by-builder" ]; }'
)


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


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, route: OpRoute) -> AsyncIterator[str]:
    root = HERE / route.name / "store"
    socket = tmp_path / f"{route.name}.sock"
    nix_config = for_test_store(substituters=(), experimental_features=("auto-allocate-uids", "cgroups"))
    nix_config.auto_allocate_uids = True
    nix_config.start_id = START_ID
    nix_config.id_count = IDS_PER_BUILD
    # The guest's `nix.conf` names its features, and `uid-range` is not one (`getDefaultSystemFeatures`).
    nix_config.system_features = ["uid-range"]
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        feature_matrix={system: {*features, "uid-range"} for system, features in _NO_PROBE_FEATURE_MATRIX.items()},
        nix_config=nix_config,
        op_routes={"BuildDerivation": route},
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}"


async def _build_who(tmp_path: Path, route: OpRoute) -> tuple[str, os.stat_result, os.stat_result]:
    shared = HERE / f"shared-{route.name}"
    shared.mkdir(parents=True)
    shared.chmod(0o777)
    async with _serve(tmp_path, route) as url:
        rc, out, err, _both = await run_subproc(
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
    assert rc == 0, err
    real = HERE / route.name / "store" / out.strip().lstrip("/")
    return " ".join(real.read_text().split()), real.stat(), (shared / "by-builder").stat()


async def test_a_uid_range_build_is_root_over_a_whole_slot(tmp_path: Path) -> None:
    """Sandbox root maps to the slot's first uid, 65536 ids long, and gets `/sys` and a writable `/etc`."""
    before = _fallthrough()
    maps, output, written = await _build_who(tmp_path, OpRoute.pynixd)

    assert _fallthrough() == before, "the daemon built the pynixd route's build"
    assert maps == f"0 {START_ID} {IDS_PER_BUILD} / 0 {START_ID} {IDS_PER_BUILD} / sys / etc"
    assert (written.st_uid, written.st_gid) == (START_ID, START_ID), "the slot's first uid did not write"
    assert output.st_uid == 0, "a registered output belongs to root"


async def test_a_uid_range_build_writes_what_the_daemons_writes(tmp_path: Path) -> None:
    there = await _build_who(tmp_path, OpRoute.daemon)
    here = await _build_who(tmp_path, OpRoute.pynixd)

    assert here[0] == there[0]
    assert (here[2].st_uid, here[2].st_gid) == (there[2].st_uid, there[2].st_gid)
