"""A sandboxed build in the build VM (`build_vm`, nixidae/nixidae#1186) against the same build without it.

The guest suite's functional guest has `/dev/kvm` and the `PYNIXD_KRUN*` variables; elsewhere these skip.
"""

from __future__ import annotations

import contextlib
import json
import os
from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import BuildVmSpec
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

pytestmark = pytest.mark.skipif(
    not os.access("/dev/kvm", os.R_OK | os.W_OK) or "PYNIXD_KRUN" not in os.environ,
    reason="the build VM needs /dev/kvm and the PYNIXD_KRUN* variables",
)

DEP = (
    'derivation { name = "vm-dep"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo dep > $out" ]; }'
)
TOP = (
    f"let dep = {DEP}; in "
    'derivation { name = "vm-top"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo built; echo ${dep} $out > $out" ]; }'
)
# What the builder's kernel is, and how much memory it has: the VM's, or the host's.
KERNEL = (
    'derivation { name = "vm-kernel"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "read r < /proc/sys/kernel/osrelease; echo $r > $out" ]; }'
)


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, name: str, vm: BuildVmSpec | None) -> AsyncIterator[str]:
    root = tmp_path / name / "store"
    socket = tmp_path / f"{name}.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=()),
        op_routes={"BuildDerivation": OpRoute.pynixd},
        build_vm=vm,
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}"


async def _build(url: str, expr: str, *options: str) -> tuple[str, str]:
    _rc, out, err, _both = await run_subproc(
        [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "-L", *options, "--expr", expr]
    )
    return out.strip(), err


async def _info(url: str, path: str) -> Any:
    _rc, out, _err, _both = await run_subproc([NIX_BIN, "path-info", "--store", url, "--json", "--recursive", path])
    info = json.loads(out)
    for entry in info.values() if isinstance(info, dict) else info:
        entry.pop("registrationTime", None)
    return info


async def test_a_build_in_the_vm_registers_what_the_namespaces_register(tmp_path: Path) -> None:
    async with _serve(tmp_path, "vm", BuildVmSpec(memory_mib=512, cpus=1)) as url:
        here, log = await _build(url, TOP)
        here_info = await _info(url, here)
    async with _serve(tmp_path, "ns", None) as url:
        there, _log = await _build(url, TOP)
        there_info = await _info(url, there)

    assert here == there
    assert here_info == there_info
    assert "vm-top> built" in log


async def test_the_builder_runs_on_the_guest_kernel(tmp_path: Path) -> None:
    """The proof that the VM ran the build: the namespaces share the host's kernel, and the guest has its own."""
    async with _serve(tmp_path, "vm", BuildVmSpec(memory_mib=512, cpus=1)) as url:
        path, _log = await _build(url, KERNEL)
    release = (tmp_path / "vm/store" / path.lstrip("/")).read_text().strip()

    assert release != os.uname().release


async def test_an_extra_sandbox_directory_reaches_the_builder_read_write(tmp_path: Path) -> None:
    """`extra-sandbox-paths` binds a directory read-write in the VM as on the host: `programs.ccache`'s cache."""
    cache = tmp_path / "ccache"
    cache.mkdir()
    (cache / "hit").write_text("from the host\n")
    expr = (
        'derivation { name = "vm-extra"; system = builtins.currentSystem; builder = "/bin/sh"; '
        'args = [ "-c" "read x < /cache/hit; echo $x > $out; echo stored > /cache/miss" ]; }'
    )
    async with _serve(tmp_path, "vm", BuildVmSpec(memory_mib=512, cpus=1)) as url:
        path, _log = await _build(url, expr, "--option", "extra-sandbox-paths", f"/cache={cache}")

    assert (tmp_path / "vm/store" / path.lstrip("/")).read_text() == "from the host\n"
    assert (cache / "miss").read_text() == "stored\n"
