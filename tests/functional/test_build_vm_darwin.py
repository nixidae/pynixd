"""pynixd's build VM on a Mac: a Linux build in a libkrun microVM on Hypervisor.framework (nixidae/nixidae#1149).

The guest is aarch64-linux and runs x86_64-linux binaries through Rosetta. `nix build --file .
internal.krunDarwinEnv` writes the file to source: the launcher, the guest's kernel, init and helper, and a
static busybox of each Linux system, which each test derivation's builder is.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from pynixd.config import BuildVmSpec, DarwinBuildVmSpec
from tests.functional.test_build_darwin import (
    _build,
    _drv as darwin_drv,
    _keep_store_dirs as _keep_store_dirs,  # autouse: each server sets the process's store directories
)

BUSYBOX = {
    "aarch64-linux": os.environ.get("PYNIXD_TEST_BUSYBOX_AARCH64_LINUX"),
    "x86_64-linux": os.environ.get("PYNIXD_TEST_BUSYBOX_X86_64_LINUX"),
}
ROSETTA = Path("/Library/Apple/usr/libexec/oah/RosettaLinux/rosetta")

pytestmark = [
    pytest.mark.skipif(sys.platform != "darwin", reason="the Mac's build VM needs a darwin host"),
    pytest.mark.skipif(
        "PYNIXD_KRUN_KERNEL" not in os.environ or None in BUSYBOX.values(),
        reason="internal.krunDarwinEnv names the guest and the test builders",
    ),
]


def _drv(name: str, system: str, script: str, extra: str = "") -> str:
    """A derivation for *system* whose builder is busybox's `sh`, added to the test store as a source.

    `PATH` names nothing, so *script* runs an applet as `$builder APPLET`.
    """
    busybox = f'builtins.path {{ path = "{BUSYBOX[system]}"; name = "busybox"; }}'
    return (
        f'derivation {{ name = "{name}"; system = "{system}"; builder = {busybox}; {extra} '
        f'args = [ "sh" "-c" "{script}" ]; }}'
    )


def _vm() -> BuildVmSpec:
    return BuildVmSpec(memory_mib=512, cpus=1)


async def test_an_aarch64_linux_build_runs_in_the_vm(tmp_path: Path) -> None:
    """The build runs on the guest's Linux kernel, in the chroot, with no network."""
    script = "set -e; $builder uname -sm > $out; ! $builder wget -q -T 2 -O /dev/null http://1.1.1.1/"
    seen = await _build(
        tmp_path,
        False,
        "false",
        _drv("darwin-linux-aarch64", "aarch64-linux", script),
        build_vm=_vm(),
        systems=("aarch64-linux",),
    )
    assert seen["contents"] == "Linux aarch64\n", seen
    assert seen["info"], seen


@pytest.mark.skipif(not ROSETTA.exists(), reason="Rosetta for Linux is not installed")
async def test_an_x86_64_linux_build_runs_through_rosetta(tmp_path: Path) -> None:
    """Rosetta runs the x86_64 builder; the guest's kernel is still aarch64."""
    seen = await _build(
        tmp_path,
        False,
        "false",
        _drv("darwin-linux-x86-64", "x86_64-linux", "$builder uname -m > $out"),
        build_vm=_vm(),
        systems=("x86_64-linux",),
    )
    assert seen["contents"] == "x86_64\n", seen


@pytest.mark.skipif(
    "PYNIXD_DARWIN_VM_SNAPSHOT" not in os.environ or "PYNIXD_DARWIN_VM_PYTHON" not in os.environ,
    reason="PYNIXD_DARWIN_VM_SNAPSHOT and PYNIXD_DARWIN_VM_PYTHON name the macOS guest",
)
async def test_a_store_with_both_vms_sends_each_build_to_its_own(tmp_path: Path) -> None:
    """A Linux build goes to the Linux guest and a darwin build to the macOS guest, from one store."""
    darwin_vm = DarwinBuildVmSpec(snapshot=Path(os.environ["PYNIXD_DARWIN_VM_SNAPSHOT"]))
    linux = await _build(
        tmp_path,
        False,
        "true",
        _drv("darwin-both-linux", "aarch64-linux", "$builder uname -s > $out"),
        systems=("aarch64-linux",),
        build_vm=_vm(),
        darwin_build_vm=darwin_vm,
    )
    darwin = await _build(
        tmp_path,
        False,
        "true",
        darwin_drv("darwin-both-mac", "/usr/bin/uname -r > $out", '__impureHostDeps = [ "/usr/bin/uname" ];'),
        impure_host_deps=("/usr/bin",),
        systems=("aarch64-linux",),
        build_vm=_vm(),
        darwin_build_vm=darwin_vm,
    )
    assert linux["contents"] == "Linux\n", linux
    assert darwin["contents"] not in {"", f"{os.uname().release}\n"}, darwin


async def test_a_fixed_output_linux_build_is_left_to_the_daemon(tmp_path: Path) -> None:
    """The build VM on a Mac has no network: pynixd does not take a fetch, and with no daemon nothing builds it."""
    extra = 'outputHashMode = "flat"; outputHashAlgo = "sha256"; outputHash = "' + "0" * 52 + '";'
    seen = await _build(
        tmp_path,
        False,
        "false",
        _drv("darwin-linux-fetch", "aarch64-linux", "echo > $out", extra),
        expected=1,
        build_vm=_vm(),
        systems=("aarch64-linux",),
    )
    assert "a fixed-output or impure aarch64-linux build needs the build VM's network" in seen["error"], seen
