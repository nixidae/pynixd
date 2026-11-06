"""The build VM's command line and its report (`pynixd_store/builder/vm.py`, nixidae/nixidae#1186).

The guest sees a store path only through an allowlist, so what the allowlists hold decides what a builder
that escapes the helper can read. These tests pin it; a VM build itself needs `/dev/kvm`.
"""

from __future__ import annotations

from pathlib import Path

from pynixd_store.builder.vm import BuildVm, returncode, status, unshared

HELPER = "/nix/store/aaaa-venv"
INPUT = "/nix/store/bbbb-input"
SHELL = "/nix/store/cccc-busybox"


def _vm() -> BuildVm:
    return BuildVm(
        launcher=Path("/nix/store/dddd-pynixd-krun/bin/pynixd-krun"),
        init=Path("/nix/store/eeee-init/bin/pynixd-krun-init"),
        helper_closure=frozenset({HELPER}),
        memory_mib=512,
        cpus=1,
        dax_window_mib=64,
    )


def _argv(tmp_path: Path, real_store: Path, sources: tuple[str, ...] = ()) -> list[str]:
    return _vm().argv(
        top=tmp_path / "top",
        chroot_parent=real_store / "ffff-x.drv.chroot",
        real_store_dir=real_store,
        visible=[INPUT],
        sources=sources,
        helper=["python", "-m", "child", "spec.json", "3"],
    )


def _shares(argv: list[str]) -> dict[str, list[str]]:
    """Each `--ro` share's root and its allowlist's lines, in mount order."""
    shares: dict[str, list[str]] = {}
    for arg in argv[: argv.index("--")]:
        if arg.startswith("--ro="):
            root, _, allowlist = arg.removeprefix("--ro=").rpartition(":")
            shares[root] = Path(allowlist).read_text().splitlines()
    return shares


def test_a_relocated_store_and_the_host_store_each_show_their_own_paths(tmp_path: Path) -> None:
    (tmp_path / "top").mkdir()
    real = tmp_path / "root/nix/store"
    argv = _argv(tmp_path, real, sources=(f"{SHELL}/bin/busybox",))

    shares = _shares(argv)
    assert list(shares) == ["/nix/store", str(real)], "the shorter path mounts first"
    assert shares["/nix/store"] == [HELPER, SHELL]
    assert shares[str(real)] == [str(real / "bbbb-input"), str(real / "ffff-x.drv.chroot")]
    assert [a for a in argv if a.startswith("--rw=")] == [
        f"--rw={real / 'ffff-x.drv.chroot'}",
        f"--rw={tmp_path / 'top'}",
    ]
    assert argv[argv.index("--") + 1 :] == ["python", "-m", "child", "spec.json", "3"]


def test_the_host_store_is_one_share_with_every_path(tmp_path: Path) -> None:
    (tmp_path / "top").mkdir()
    shares = _shares(_argv(tmp_path, Path("/nix/store")))
    assert shares == {"/nix/store": [HELPER, INPUT, "/nix/store/ffff-x.drv.chroot"]}


def test_a_source_outside_the_stores_is_unshared() -> None:
    real = Path("/srv/store/nix/store")
    assert unshared([f"{SHELL}/bin/busybox", f"{real}/gggg-x/bin/sh"], real) is None
    assert unshared([f"{SHELL}/bin/busybox", "/run/binfmt"], real) == "/run/binfmt"
    assert unshared(["/nix/store"], real) == "/nix/store", "the store directory itself is no store path"


def test_the_guest_reports_an_exit_a_signal_or_nothing(tmp_path: Path) -> None:
    assert returncode(tmp_path) is None
    assert status(tmp_path) == ""
    (tmp_path / "vm-exit").write_text("exit 7\n")
    assert returncode(tmp_path) == 7
    (tmp_path / "vm-exit").write_text("signal 9\n")
    assert returncode(tmp_path) == -9
    (tmp_path / "vm-status").write_text("mounting: denied")
    assert status(tmp_path) == "mounting: denied"
