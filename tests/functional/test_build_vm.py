"""A sandboxed build in the build VM (`build_vm`, nixidae/nixidae#1186) against the same build without it.

The guest suite's functional guest has `/dev/kvm` and the `PYNIXD_KRUN*` variables; elsewhere these skip.
"""

from __future__ import annotations

import contextlib
import json
import os
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import BuildVmSpec
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from pynixd_store.db_migrations import BUILD_OBSERVATION_SAMPLE_TABLE, BUILD_OBSERVATION_TABLE
from tests._conftest.config import NIX_GIT_BIN
from tests._conftest.constants import _NO_PROBE_FEATURE_MATRIX
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

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


STRING_BYTES = 1 << 24
# Four subshells hold a 16 MiB string each at once: the guest cgroup counts them together.
PARALLEL = (
    'derivation { name = "vm-parallel"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "for n in 1 2 3 4; do (s=a; i=0; while [ $i -lt 24 ]; do s=$s$s; i=$((i+1)); done; '
    'j=0; while [ $j -lt 300000 ]; do j=$((j+1)); done) & done; wait; echo > $out" ]; }'
)


# The builder reaches what it adds and builds through the store's socket, and reads both.
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
    o="--extra-experimental-features nix-command"
    read r < /proc/sys/kernel/osrelease
    echo added > foobar
    foobar=$(nix $o store add-path ./foobar)
    built=$(nix $o build --no-link --print-out-paths --file $inner)
    read a < $foobar
    read b < $built
    echo "$r $a $b $foobar $built" > $out
  '') ];
}
"""
# `builder-rpc-v0`: the builder adds its output and submits it.
RPC = """
let nix = builtins.storePath "@NIX@"; in
derivation {
  name = "vm-rpc"; system = builtins.currentSystem; builder = "/bin/sh";
  __contentAddressed = true; outputHashMode = "recursive"; outputHashAlgo = "sha256";
  requiredSystemFeatures = [ "builder-rpc-v0" ];
  PATH = "${nix}/bin";
  args = [ (builtins.toFile "vm-rpc.sh" ''
    set -e
    o="--extra-experimental-features nix-command --extra-experimental-features dynamic-derivations"
    read r < /proc/sys/kernel/osrelease
    echo "$r" > out
    out=$(nix $o store add --name vm-rpc ./out)
    nix $o store submit-output $out out
  '') ];
}
"""


@contextlib.asynccontextmanager
async def _serve(
    tmp_path: Path,
    name: str,
    vm: BuildVmSpec | None,
    *,
    experimental_features: tuple[str, ...] = (),
    extra_platforms: tuple[str, ...] = (),
    **spec_args: Any,
) -> AsyncIterator[str]:
    root = tmp_path / name / "store"
    socket = tmp_path / f"{name}.sock"
    nix_config = for_test_store(substituters=(), experimental_features=experimental_features)
    nix_config.extra_platforms = list(extra_platforms) or None
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=nix_config,
        op_routes={"BuildDerivation": OpRoute.pynixd},
        build_vm=vm,
        **spec_args,
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}"


async def _build(url: str, expr: str, *options: str, nix_bin: Path = NIX_BIN) -> tuple[str, str]:
    _rc, out, err, _both = await run_subproc(
        [nix_bin, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "-L", *options, "--expr", expr]
    )
    return out.strip(), err


def _nix_store_path(binary: Path) -> str:
    return "/".join(os.path.realpath(binary).split("/")[:4])


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


async def test_the_build_directory_is_xfs_and_keeps_what_pynixd_wrote(tmp_path: Path) -> None:
    """The build directory is a disk image, not a share: each small-file operation stays in the guest.

    `passAsFile` puts a file in the build directory before the builder starts, so the image must hold it.
    """
    expr = (
        'derivation { name = "vm-disk"; system = builtins.currentSystem; builder = "/bin/sh"; '
        'passAsFile = [ "note" ]; note = "passed"; '
        'args = [ "-c" "while read dev dir fs rest; do [ $dir = $NIX_BUILD_TOP ] && t=$fs; done < /proc/mounts; '
        'read n < $notePath; echo $t $n > $out" ]; }'
    )
    async with _serve(tmp_path, "vm", BuildVmSpec(memory_mib=512, cpus=1)) as url:
        path, _log = await _build(url, expr)

    assert (tmp_path / "vm/store" / path.lstrip("/")).read_text() == "xfs passed\n"


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


async def test_an_extra_sandbox_file_reaches_the_builder_read_write(tmp_path: Path) -> None:
    """A file alone: the share of its parent shows only it (libkrun's allowlist on a writable share)."""
    etc = tmp_path / "etc"
    etc.mkdir()
    (etc / "counter").write_text("1\n")
    (etc / "beside").write_text("hidden\n")
    expr = (
        'derivation { name = "vm-file"; system = builtins.currentSystem; builder = "/bin/sh"; '
        'args = [ "-c" "read x < /counter; echo $x > $out; echo 2 >> /counter" ]; }'
    )
    async with _serve(tmp_path, "vm", BuildVmSpec(memory_mib=512, cpus=1)) as url:
        path, _log = await _build(url, expr, "--option", "extra-sandbox-paths", f"/counter={etc / 'counter'}")

    assert (tmp_path / "vm/store" / path.lstrip("/")).read_text() == "1\n"
    assert (etc / "counter").read_text() == "1\n2\n"


async def test_a_build_in_the_vm_reports_its_guest_cgroup(tmp_path: Path) -> None:
    """The guest's init makes a cgroup for the build, and the helper reads it. nixidae/nixidae#1187."""
    async with _serve(tmp_path, "vm", BuildVmSpec(memory_mib=512, cpus=2), usage_sample_seconds=0.1) as url:
        await _build(url, PARALLEL)
    with contextlib.closing(sqlite3.connect(tmp_path / "vm/store/nix/var/nix/db/db.sqlite")) as conn:
        source, peak, cpu_stall, observation = conn.execute(
            f"SELECT resourceSource, peakMemoryBytes, cpuStallUs, id FROM {BUILD_OBSERVATION_TABLE} WHERE name = ?",
            ("vm-parallel",),
        ).fetchone()
        (samples,) = conn.execute(
            f"SELECT count(*) FROM {BUILD_OBSERVATION_SAMPLE_TABLE} WHERE observationId = ?", (observation,)
        ).fetchone()

    assert source == "cgroup"
    assert peak > 4 * STRING_BYTES, f"{peak} bytes: the subshells held their strings at once"
    assert cpu_stall is not None, "the guest kernel has PSI"
    assert samples >= 1


async def test_a_build_for_another_system_runs_on_the_host_kernel(tmp_path: Path) -> None:
    """The guest kernel has no binfmt handler, and emulation in a VM is slow: the namespaces take the build.

    The builder is the host's own shell, so it starts on either kernel, and only the kernel tells them apart.
    """
    expr = KERNEL.replace("builtins.currentSystem", '"aarch64-linux"')
    vm = BuildVmSpec(memory_mib=512, cpus=1)
    async with _serve(tmp_path, "vm", vm, extra_platforms=("aarch64-linux",)) as url:
        path, _log = await _build(url, expr)

    assert (tmp_path / "vm/store" / path.lstrip("/")).read_text().strip() == os.uname().release


async def test_a_recursive_nix_builder_in_the_vm_reads_what_it_adds_and_builds(tmp_path: Path) -> None:
    """The store's socket reaches the guest over vsock, and each path the builder adds joins the store share's
    allowlist before the helper binds it."""
    nix = _nix_store_path(NIX_BIN)
    vm = BuildVmSpec(memory_mib=512, cpus=1)
    async with _serve(tmp_path, "vm", vm, experimental_features=("recursive-nix",)) as url:
        await run_subproc([NIX_BIN, "copy", "--no-check-sigs", "--to", url, nix])
        path, log = await _build(url, RECURSIVE.replace("@NIX@", nix))
    release, added, inner, foobar, built = (tmp_path / "vm/store" / path.lstrip("/")).read_text().split()

    assert release != os.uname().release, log
    assert (added, inner) == ("added", "inner"), log
    assert foobar.endswith("-foobar") and built.endswith("-vm-inner"), (foobar, built)


async def test_a_builder_rpc_builder_in_the_vm_submits_its_output(tmp_path: Path) -> None:
    if NIX_GIT_BIN is None:
        pytest.skip("NIX_GIT_BIN names no Nix of the master branch")
    nix = _nix_store_path(NIX_GIT_BIN)
    features = ("ca-derivations", "dynamic-derivations")
    matrix = {system: set(fs) | {"builder-rpc-v0"} for system, fs in _NO_PROBE_FEATURE_MATRIX.items()}
    vm = BuildVmSpec(memory_mib=512, cpus=1)
    async with _serve(tmp_path, "vm", vm, experimental_features=features, feature_matrix=matrix) as url:
        await run_subproc([NIX_GIT_BIN, "copy", "--no-check-sigs", "--to", url, nix])
        path, log = await _build(
            url, RPC.replace("@NIX@", nix), "--extra-experimental-features", " ".join(features), nix_bin=NIX_GIT_BIN
        )

    assert path.endswith("-vm-rpc"), log
    assert (tmp_path / "vm/store" / path.lstrip("/")).read_text().strip() != os.uname().release, log
