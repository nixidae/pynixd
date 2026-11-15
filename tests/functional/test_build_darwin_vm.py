"""pynixd's darwin build VM: a sandboxed darwin build in a macOS guest from a snapshot (nixidae/nixidae#1168).

The snapshot is machine state: `darwin_builder.py save` with `PYNIXD=1` makes it
(`vivarium/docs/design/macos-guest-spike`), and `PYNIXD_DARWIN_VM_SNAPSHOT` names it. The guest's helper is the
package's venv: `PYNIXD_DARWIN_VM_PYTHON` and `PYNIXD_DARWIN_VM_CLOSURE`, from
`nix build --file . internal.darwinVmVariables`.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

from pynixd.config import DarwinBuildVmSpec
from tests.conftest import NIX_BIN, run_subproc
from tests.functional.test_build_darwin import (
    ALONE,
    PROBE,
    RECURSIVE,
    _build,
    _drv,
    _keep_store_dirs as _keep_store_dirs,  # autouse: each server sets the process's store directories
    _serve,
)

SNAPSHOT = os.environ.get("PYNIXD_DARWIN_VM_SNAPSHOT")

pytestmark = [
    pytest.mark.skipif(sys.platform != "darwin", reason="a macOS guest needs a darwin host"),
    pytest.mark.skipif(
        SNAPSHOT is None or "PYNIXD_DARWIN_VM_PYTHON" not in os.environ,
        reason="PYNIXD_DARWIN_VM_SNAPSHOT and PYNIXD_DARWIN_VM_PYTHON name the guest",
    ),
]


def _vm() -> DarwinBuildVmSpec:
    return DarwinBuildVmSpec(snapshot=Path(SNAPSHOT or ""))


async def test_a_build_in_the_vm_matches_nixs_darwin_builder(tmp_path: Path) -> None:
    """The guest applies the full profile as Nix does on the host; the output registers the same."""
    expr = _drv("darwin-probe", PROBE)
    here = await _build(tmp_path, False, "true", expr, darwin_build_vm=_vm())
    there = await _build(tmp_path, True, "true", expr)
    assert there["log"][-1] == "darwin-probe> host=refused", there["log"]
    assert here["log"] == there["log"]
    assert here["contents"] == there["contents"] == "built\n"
    assert here["info"] == there["info"]
    assert here["rows"] == there["rows"]


async def test_keep_failed_keeps_the_guests_build_directory(tmp_path: Path) -> None:
    """The build directory is a tmpfs in the guest; `keep-failed` copies a failed build's back to the host."""
    expr = _drv("darwin-kept", "echo left > here; exit 3")
    async with _serve(tmp_path, False, "true", darwin_build_vm=_vm()) as (nix, env, _state):
        _rc, _out, err, _both = await run_subproc(
            [*nix[:1], "build", *nix[1:], "--no-link", "--keep-failed", "--expr", expr],
            expected_retcode=1,
            env=dict(env),
            nix_config=ALONE,
        )
    kept = re.search(r'note: keeping build directory "([^"]+)"', err)
    assert kept is not None, err
    assert (Path(kept[1]) / "here").read_text() == "left\n"


def _recursive(name: str, body: str, *deps: str) -> str:
    bin_dir = Path(os.path.realpath(NIX_BIN)).parent
    impure = " ".join(f'"{d}"' for d in ("/nix/store", *deps))
    return _drv(
        name,
        body.replace("@BIN@", str(bin_dir)),
        f'requiredSystemFeatures = [ "recursive-nix" ]; __impureHostDeps = [ {impure} ];',
    )


async def test_a_recursive_build_in_the_vm_matches_nixs_darwin_builder(tmp_path: Path) -> None:
    """The guest relays the builder's socket to pynixd's over vsock; the added path registers as Nix's does."""
    expr = _recursive("darwin-recursive", RECURSIVE)
    here = await _build(tmp_path, False, "true", expr, recursive=True, darwin_build_vm=_vm())
    there = await _build(tmp_path, True, "true", expr, recursive=True)
    assert there["log"][-1].startswith("darwin-recursive> added "), there["log"]
    assert here["log"] == there["log"]
    assert here["contents"] == there["contents"]
    assert here["info"] == there["info"]
    assert here["rows"] == there["rows"]


async def test_a_recursive_build_in_the_vm_reaches_the_hosts_store_from_the_guests_kernel(tmp_path: Path) -> None:
    """The path the guest adds is in the host's store, and the build ran on the guest's kernel."""
    expr = _recursive("darwin-recursive-kernel", f"{RECURSIVE}\n/usr/bin/uname -r >> $out", "/usr/bin/uname")
    seen = await _build(
        tmp_path, False, "true", expr, recursive=True, impure_host_deps=("/usr/bin",), darwin_build_vm=_vm()
    )
    added, release = seen["contents"].splitlines()
    assert release != os.uname().release, seen
    assert added.rsplit("/", 1)[-1] in {Path(row[0]).name for row in seen["rows"][0]}


async def test_a_build_in_the_vm_runs_on_the_guests_kernel(tmp_path: Path) -> None:
    """The guest is the snapshot's macOS, not the host's: `uname -r` tells them apart."""
    expr = _drv("darwin-kernel", "/usr/bin/uname -r > $out", '__impureHostDeps = [ "/usr/bin/uname" ];')
    guest = await _build(tmp_path, False, "true", expr, impure_host_deps=("/usr/bin",), darwin_build_vm=_vm())
    host = await _build(tmp_path, False, "true", expr, impure_host_deps=("/usr/bin",))
    assert host["contents"] == f"{os.uname().release}\n"
    assert guest["contents"] != host["contents"], guest
