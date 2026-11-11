"""pynixd's builder with `auto-allocate-uids`, as Nix's `AutoUserLock` (`unix/user-lock.cc:136`).

A build takes a slot of `<state>/userpool2` and the uid `start-id + slot * 65536`, with no account behind it.
In the sandbox the gid is that uid too (`useUserNamespace`, `:204`). The guest's `root` suite runs these as root;
anywhere else they skip.
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
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

HERE = Path("/var/lib/pynixd-root-tests/auto-allocate-uids")
IDS_PER_BUILD = 1 << 16
START_ID = 40000
"""Inside a container guest's 65536 ids: `start-id`'s default (`0x34000000`, `local-settings.hh:145`)
maps to no uid there, so a `chown` to it fails with `EINVAL`."""

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="build users need root")

# The maps the builder's namespace got, and a file it writes where the host can see who wrote it.
WHO = (
    'derivation { name = "auto-who"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "read u < /proc/self/uid_map; read g < /proc/self/gid_map; echo $u / $g > $out; '
    'echo seen > /shared/by-builder" ]; }'
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
async def _serve(
    tmp_path: Path, route: OpRoute, *, start_id: int = START_ID, id_count: int = IDS_PER_BUILD
) -> AsyncIterator[str]:
    root = HERE / route.name / "store"
    socket = tmp_path / f"{route.name}.sock"
    nix_config = for_test_store(substituters=(), experimental_features=("auto-allocate-uids",))
    nix_config.auto_allocate_uids = True
    nix_config.start_id = start_id
    nix_config.id_count = id_count
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
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
    real = HERE / route.name / "store" / out.strip().lstrip("/")
    return " ".join(real.read_text().split()), real.stat(), (shared / "by-builder").stat()


async def test_a_build_runs_as_an_allocated_uid_mapped_to_1000(tmp_path: Path) -> None:
    """Sandbox uid 1000 and gid 100 map to the slot's uid, which owns what the builder writes."""
    before = _fallthrough()
    maps, output, written = await _build_who(tmp_path, OpRoute.pynixd)

    assert _fallthrough() == before, "the daemon built the pynixd route's build"
    assert (written.st_uid, written.st_gid) == (START_ID, START_ID), "slot 0's uid did not write the shared file"
    assert maps == f"1000 {START_ID} 1 / 100 {START_ID} 1"
    assert output.st_uid == 0, "a registered output belongs to root"


async def test_an_allocated_uid_build_writes_what_the_daemons_writes(tmp_path: Path) -> None:
    """The daemon, as root, allocates from the same pool: the same maps inside, the same owners outside."""
    here = await _build_who(tmp_path, OpRoute.pynixd)
    there = await _build_who(tmp_path, OpRoute.daemon)

    assert here[0] == there[0]
    assert here[1].st_uid == there[1].st_uid == 0
    assert (here[2].st_uid, here[2].st_gid) == (there[2].st_uid, there[2].st_gid)


@pytest.mark.parametrize(
    ("start_id", "id_count", "words"),
    [
        (0, IDS_PER_BUILD, "setting 'start-id' must be greater than 0, not 0"),
        (START_ID, IDS_PER_BUILD + 1, "setting 'id-count' must be a multiple of 65536, not 65537"),
    ],
)
async def test_a_start_id_or_id_count_nix_asserts_on_fails_the_build_with_no_daemon(
    tmp_path: Path, start_id: int, id_count: int, words: str
) -> None:
    """`AutoUserLock::acquire` asserts on these (`user-lock.cc:177`), so Nix's daemon aborts (Lillecarl/nix#395).
    pynixd fails the build with an error that names the setting."""
    async with _serve(tmp_path, OpRoute.pynixd, start_id=start_id, id_count=id_count) as url:
        rc, _out, err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--expr", WHO], expected_retcode=None
        )
    assert rc == 1, err
    assert f"error: {words}" in err, err
