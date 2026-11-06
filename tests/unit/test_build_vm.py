"""The build VM's command line and its report (`pynixd_store/builder/vm.py`, nixidae/nixidae#1186).

The guest sees a store path only through an allowlist, so what the allowlists hold decides what a builder
that escapes the helper can read. These tests pin it; a VM build itself needs `/dev/kvm`.
"""

from __future__ import annotations

from pathlib import Path

from pynixd_store.builder.vm import BuildVm, returncode, status, unshareable

HELPER = "/nix/store/aaaa-venv"
INPUT = "/nix/store/bbbb-input"
SHELL = "/nix/store/cccc-busybox"


def _vm() -> BuildVm:
    return BuildVm(
        launcher=Path("/nix/store/dddd-pynixd-krun/bin/pynixd-krun"),
        init=Path("/nix/store/eeee-init/bin/pynixd-krun-init"),
        python=Path(f"{HELPER}/bin/python"),
        mkfs=Path("/nix/store/gggg-xfsprogs/bin/mkfs.xfs"),
        helper_closure=frozenset({HELPER}),
        memory_mib=512,
        cpus=1,
        dax_window_mib=64,
        disk_gib=1,
    )


def _argv(tmp_path: Path, real_store: Path, sources: tuple[str, ...] = ()) -> list[str]:
    return _vm().argv(
        top=tmp_path / "top",
        chroot_parent=real_store / "ffff-x.drv.chroot",
        real_store_dir=real_store,
        visible=[INPUT],
        sources=sources,
        spec_file=tmp_path / "top/spec.json",
        build_dir=tmp_path / "top/build",
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
        f"--rw={tmp_path / 'top'}",
        f"--rw={real / 'ffff-x.drv.chroot'}",
    ], "the shallower path mounts first"
    assert f"--cwd={tmp_path / 'top'}" in argv
    assert f"--disk={tmp_path / 'top/build.xfs'}:{tmp_path / 'top/build'}" in argv
    assert argv[argv.index("--") + 1 :] == [
        f"{HELPER}/bin/python",
        "-I",
        "-m",
        "pynixd_store.builder.child",
        str(tmp_path / "top/spec.json"),
        "3",
    ]


def test_the_host_store_is_one_share_with_every_path(tmp_path: Path) -> None:
    (tmp_path / "top").mkdir()
    shares = _shares(_argv(tmp_path, Path("/nix/store")))
    assert shares == {"/nix/store": [HELPER, INPUT, "/nix/store/ffff-x.drv.chroot"]}


def test_a_directory_outside_the_stores_is_a_writable_share_of_its_own(tmp_path: Path) -> None:
    """`programs.ccache`'s cache and `/run/binfmt`: Nix binds them read-write, and the host's permissions decide.

    The share is the directory itself, so the guest sees nothing beside it; one inside another is in its share.
    """
    (tmp_path / "top").mkdir()
    cache = tmp_path / "var/cache/ccache"
    (cache / "inner").mkdir(parents=True)
    (tmp_path / "var/cache/secret").mkdir()
    sources = (str(cache), str(cache / "inner"), str(tmp_path / "missing"))
    argv = _argv(tmp_path, tmp_path / "root/nix/store", sources=sources)

    rw = [a.removeprefix("--rw=") for a in argv if a.startswith("--rw=")]
    assert rw.count(str(cache)) == 1
    assert str(cache / "inner") not in rw
    assert str(tmp_path / "missing") not in rw, "a missing source is left to the helper to report"
    assert not any(root.startswith(str(tmp_path / "var")) for root in _shares(argv))


def _kinds(argv: list[str]) -> dict[str, tuple[str, list[str] | None]]:
    """Each share's root: its kind, and its allowlist's lines when it has one."""
    kinds: dict[str, tuple[str, list[str] | None]] = {}
    for arg in argv[: argv.index("--")]:
        kind, _, value = arg.removeprefix("--").partition("=")
        if kind == "rw":
            kinds[value] = (kind, None)
        elif kind in {"ro", "rw-allow"}:
            root, _, allowlist = value.rpartition(":")
            kinds[root] = (kind, Path(allowlist).read_text().splitlines())
    return kinds


def test_a_file_outside_the_stores_is_its_parent_showing_the_file_alone(tmp_path: Path) -> None:
    """virtio-fs mounts a directory only: the kernel fixes its root's mode at `S_IFDIR`. The share of the
    file's parent is writable and lists the file alone, as Nix binds the file read-write."""
    (tmp_path / "top").mkdir()
    etc = tmp_path / "etc"
    etc.mkdir()
    (etc / "netrc").touch()
    (etc / "machines").touch()
    (etc / "shadow").touch()
    (etc / "nix").mkdir()
    sources = (str(etc / "netrc"), str(etc / "machines"), str(etc / "nix"))
    kinds = _kinds(_argv(tmp_path, tmp_path / "root/nix/store", sources=sources))

    assert kinds[str(etc)] == ("rw-allow", [str(etc / "machines"), str(etc / "netrc"), str(etc / "nix")])
    assert str(etc / "nix") not in kinds, "a directory inside a filtered share shows through it"


def test_a_file_inside_a_shared_directory_needs_no_share_of_its_own(tmp_path: Path) -> None:
    (tmp_path / "top").mkdir()
    cache = tmp_path / "ccache"
    cache.mkdir()
    (cache / "conf").touch()
    kinds = _kinds(_argv(tmp_path, tmp_path / "root/nix/store", sources=(str(cache / "conf"), str(cache))))

    assert kinds[str(cache)] == ("rw", None)


def test_only_a_file_in_the_root_is_unshareable(tmp_path: Path) -> None:
    (tmp_path / "netrc").touch()
    real = tmp_path / "root/nix/store"
    assert unshareable([f"{SHELL}/bin/busybox", str(tmp_path), str(tmp_path / "netrc")], real) is None


def test_the_guest_reports_an_exit_a_signal_or_nothing(tmp_path: Path) -> None:
    assert returncode(tmp_path) is None
    assert status(tmp_path) == ""
    (tmp_path / "vm-exit").write_text("exit 7\n")
    assert returncode(tmp_path) == 7
    (tmp_path / "vm-exit").write_text("signal 9\n")
    assert returncode(tmp_path) == -9
    (tmp_path / "vm-status").write_text("mounting: denied")
    assert status(tmp_path) == "mounting: denied"
