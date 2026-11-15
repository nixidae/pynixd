"""`choose_site`: the one place that picks where a build's sandbox helper runs (nixidae/nixidae#1318).

`tests/functional/test_build_vm.py` runs builds on both sites; this file holds each reason a build that the store
would run in its microVM stays on the host.
"""

from __future__ import annotations

import dataclasses
import signal
from pathlib import Path

import pytest

from nix_daemon_protocol.basic_derivation import BasicDerivation
from nix_daemon_protocol.derivation_output import DerivationOutput
from nix_daemon_protocol.store_path import StorePath
from pynixd_store.builder.darwin_vm import DarwinBuildVm
from pynixd_store.builder.outputs import BuildFailure
from pynixd_store.builder.settings import BuildSettings
from pynixd_store.builder.site import BuilderFailed, BuildPaths, DarwinVmSite, HostSite, VmSite, choose_site
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


DRV = StorePath("/nix/store/cccccccccccccccccccccccccccccccc-x.drv")


def _ended(top: Path) -> BuildFailure | BuilderFailed | None:
    paths = BuildPaths(top=top, tmp=top / "build", chroot_parent=None, root=None)
    return VmSite(VM, REAL, VISIBLE, ()).ended(paths, DRV, 0)


def test_a_vm_build_ends_as_its_helper_did(tmp_path: Path) -> None:
    (tmp_path / "vm-exit").write_text("exit 0\n")
    assert _ended(tmp_path) is None
    (tmp_path / "vm-exit").write_text("exit 3\n")
    assert _ended(tmp_path) == BuilderFailed("failed with exit code 3")


def test_a_vm_that_stopped_on_its_init_error_names_it(tmp_path: Path) -> None:
    (tmp_path / "vm-exit").write_text("init /dev/hvc0: No such file or directory\n")
    failure = _ended(tmp_path)
    assert isinstance(failure, BuildFailure)
    assert failure.message.endswith("stopped: /dev/hvc0: No such file or directory")


def test_a_vm_that_stopped_with_no_word_says_how_far_it_got(tmp_path: Path) -> None:
    """nixidae/nixidae#1350: whether the helper started, and the VMM's CPU time, tell a boot from a build."""
    failure = _ended(tmp_path)
    assert isinstance(failure, BuildFailure)
    assert "stopped before the init started the helper" in failure.message
    assert "pynixd-krun exited with 0)" in failure.message
    (tmp_path / "vm-status").touch()
    (tmp_path / "vm-usage.json").write_text(
        '{"source": "rusage", "cpu_user_us": 1500000, "cpu_system_us": 250000, '
        '"peak_memory_bytes": 1, "io_read_bytes": 0, "io_write_bytes": 0}\n'
    )
    failure = _ended(tmp_path)
    assert isinstance(failure, BuildFailure)
    assert "stopped after the init started the helper" in failure.message
    assert "pynixd-krun exited with 0, its VMM used 1.75 s of CPU)" in failure.message


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


def test_a_vm_with_passt_runs_a_fixed_output_build_on_its_network() -> None:
    with_passt = dataclasses.replace(VM, passt=Path("/nix/store/hhhh-passt/bin/passt"))
    site, reason = _choose(vm=with_passt, sandboxed=False)
    assert reason is None
    assert site == VmSite(with_passt, REAL, VISIBLE, (), network=True)


def test_a_sandboxed_build_gets_no_network_in_a_vm_that_has_one() -> None:
    """The guard: a guest's network reaches what the host reaches, so only a fetch gets it."""
    site, _ = _choose(vm=dataclasses.replace(VM, passt=Path("/nix/store/hhhh-passt/bin/passt")))
    assert isinstance(site, VmSite)
    assert site.network is False


def test_a_build_as_one_build_user_runs_in_the_vm() -> None:
    """nixidae/nixidae#1192: the helper is the guest's root and maps the build user itself."""
    user = BuildUser(uid=30000, gid=30001, fd=-1, count=1)
    site, why = _choose(user=user)
    assert why is None
    assert site == VmSite(VM, REAL, VISIBLE, (), user=user)


def test_the_vm_takes_the_sandbox_paths_but_dev_and_proc() -> None:
    site, _ = _choose(config={"sandbox-paths": "/bin/sh=/nix/store/cccc-busybox/bin/sh /dev/kvm /proc/cpuinfo"})
    assert isinstance(site, VmSite)
    assert site.sources == ("/nix/store/cccc-busybox/bin/sh",)


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"chroot": False}, "the build has no sandbox"),
        ({"sandboxed": False}, "a fixed-output or impure build reaches the network, and the build VM has none"),
        ({"platform": "aarch64-linux"}, "the build is for aarch64-linux, not x86_64-linux"),
        ({"user": BuildUser(uid=30000, gid=30000, fd=-1, count=65536)}, "the build runs as a range of ids"),
    ],
)
def test_a_build_the_vm_cannot_run_stays_on_the_host_and_says_why(kwargs: dict[str, object], reason: str) -> None:
    site, why = _choose(**kwargs)  # type: ignore[arg-type]  # each case names one keyword of `_choose`
    assert isinstance(site, HostSite)
    assert site.chroot == kwargs.get("chroot", True)
    assert why == reason


DARWIN_VM = DarwinBuildVm(
    snapshot=Path("/var/snap"), python=Path("/nix/store/aaaa-venv/bin/python"), helper_closure=frozenset()
)
OUTPUTS = ["/nix/store/00000000000000000000000000000000-x"]


def _choose_darwin(
    platform: str = "aarch64-darwin", sandbox: bool = True, features: str = ""
) -> tuple[object, str | None]:
    settings = BuildSettings.from_nix_config({}, {"system": "aarch64-darwin"})
    drv = BasicDerivation(
        outputs={"out": PLAIN}, platform=platform, builder="/bin/sh", env={"requiredSystemFeatures": features}
    )
    return choose_site(
        drv,
        settings,
        DARWIN_VM,
        chroot=False,
        sandboxed=True,
        user=None,
        real_store_dir=REAL,
        visible=VISIBLE,
        darwin_sandbox=sandbox,
        outputs=OUTPUTS,
    )


@pytest.mark.parametrize("platform", ["aarch64-darwin", "builtin"])
def test_a_sandboxed_darwin_build_runs_in_the_darwin_vm(platform: str) -> None:
    assert _choose_darwin(platform) == (DarwinVmSite(DARWIN_VM, REAL, VISIBLE, OUTPUTS), None)


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"sandbox": False}, "the build has no sandbox"),
        ({"platform": "x86_64-darwin"}, "the build is for x86_64-darwin, not aarch64-darwin"),
        (
            {"features": "recursive-nix"},
            "a recursive build reaches the store's socket, which the darwin build VM does not carry",
        ),
    ],
)
def test_a_darwin_build_the_vm_cannot_run_stays_on_the_host_and_says_why(
    kwargs: dict[str, object], reason: str
) -> None:
    site, why = _choose_darwin(**kwargs)  # type: ignore[arg-type]  # each case names one keyword of `_choose_darwin`
    assert site == HostSite(chroot=False)
    assert why == reason


def test_a_darwin_vm_build_ends_as_its_helper_did_or_names_why_the_vm_stopped(tmp_path: Path) -> None:
    paths = BuildPaths(top=tmp_path, tmp=tmp_path / "build", chroot_parent=None, root=None)
    site = DarwinVmSite(DARWIN_VM, REAL, VISIBLE, OUTPUTS)
    failure = site.ended(paths, DRV, 1)
    assert isinstance(failure, BuildFailure)
    assert failure.message.endswith("stopped: its launcher exited with 1 and said nothing")
    (tmp_path / "vm-exit").write_text("init QEMU exited with 1 before the guest answered\n")
    failure = site.ended(paths, DRV, 1)
    assert isinstance(failure, BuildFailure)
    assert failure.message.endswith("stopped: QEMU exited with 1 before the guest answered")
    (tmp_path / "vm-exit").write_text("signal 9\n")
    assert site.ended(paths, DRV, 0) == BuilderFailed(f"failed due to signal 9 ({signal.strsignal(9)})")
