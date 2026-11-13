"""`choose_site`: the one place that picks where a build's sandbox helper runs (nixidae/nixidae#1318).

`tests/functional/test_build_vm.py` runs builds on both sites; this file holds each reason a build that the store
would run in its microVM stays on the host.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nix_daemon_protocol.basic_derivation import BasicDerivation
from nix_daemon_protocol.derivation_output import DerivationOutput
from nix_daemon_protocol.store_path import StorePath
from pynixd_store.builder.settings import BuildSettings
from pynixd_store.builder.site import HostSite, VmSite, choose_site
from pynixd_store.builder.users import BuildUser
from pynixd_store.builder.vm import BuildVm

PLAIN = DerivationOutput(path="/nix/store/00000000000000000000000000000000-x")
REAL = Path("/nix/store")
VISIBLE = frozenset({StorePath("/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-input")})
VM = BuildVm(
    launcher=Path("/nix/store/dddd-pynixd-krun/bin/pynixd-krun"),
    init=Path("/nix/store/eeee-init/bin/pynixd-krun-init"),
    python=Path("/nix/store/aaaa-venv/bin/python"),
    mkfs=Path("/nix/store/gggg-xfsprogs/bin/mkfs.xfs"),
    helper_closure=frozenset({"/nix/store/aaaa-venv"}),
    memory_mib=512,
    cpus=1,
    dax_window_mib=64,
    disk_gib=1,
)


def _drv(platform: str = "x86_64-linux") -> BasicDerivation:
    return BasicDerivation(outputs={"out": PLAIN}, platform=platform, builder="/bin/sh")


def _choose(
    *,
    vm: BuildVm | None = VM,
    platform: str = "x86_64-linux",
    chroot: bool = True,
    sandboxed: bool = True,
    user: BuildUser | None = None,
    config: dict[str, str] | None = None,
) -> tuple[object, str | None]:
    settings = BuildSettings.from_nix_config({}, {"system": "x86_64-linux", **(config or {})})
    return choose_site(
        _drv(platform),
        settings,
        vm,
        chroot=chroot,
        sandboxed=sandboxed,
        user=user,
        real_store_dir=REAL,
        visible=VISIBLE,
    )


def test_a_store_without_a_vm_builds_on_the_host_and_names_no_reason() -> None:
    assert _choose(vm=None) == (HostSite(chroot=True), None)
    assert _choose(vm=None, chroot=False) == (HostSite(chroot=False), None)


@pytest.mark.parametrize("platform", ["x86_64-linux", "builtin"])
def test_a_sandboxed_build_for_the_stores_system_runs_in_the_vm(platform: str) -> None:
    site, reason = _choose(platform=platform)
    assert reason is None
    assert site == VmSite(VM, REAL, VISIBLE, ())


def test_the_vm_takes_the_sandbox_paths_but_dev_and_proc() -> None:
    site, _ = _choose(config={"sandbox-paths": "/bin/sh=/nix/store/cccc-busybox/bin/sh /dev/kvm /proc/cpuinfo"})
    assert isinstance(site, VmSite)
    assert site.sources == ("/nix/store/cccc-busybox/bin/sh",)


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"chroot": False}, "the build has no sandbox"),
        ({"sandboxed": False}, "a fixed-output or impure build reaches the network"),
        ({"platform": "aarch64-linux"}, "the build is for aarch64-linux, not x86_64-linux"),
        ({"user": BuildUser(uid=30000, gid=30000, fd=-1, count=1)}, "the build runs as a build user"),
    ],
)
def test_a_build_the_vm_cannot_run_stays_on_the_host_and_says_why(kwargs: dict[str, object], reason: str) -> None:
    site, why = _choose(**kwargs)  # type: ignore[arg-type]  # each case names one keyword of `_choose`
    assert isinstance(site, HostSite)
    assert site.chroot == kwargs.get("chroot", True)
    assert why == reason
