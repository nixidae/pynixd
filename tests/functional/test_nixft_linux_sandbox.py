"""Nix's `linux-sandbox.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

The script skips because the sandbox runs on the builder's side. On the pynixd arm pynixd builds in its own
sandbox (`docs/design/builder.md`), so this holds that sandbox to Nix's. Each build binds the host's
`/nix/store`, where `config.nix`'s bash and coreutils are, as `linux-sandbox.sh:18` does.

Each case names its line; a group runs its cases in one store and reports every mismatch at once.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest

from tests._conftest.nix_config import for_test_store
from tests.functional._nixft import Arm, ArmStore, arm_store, copy_nix_tests
from tests.functional.test_build_without_sandbox import _keep_store_dirs
from tests.functional.test_nixft_repair import NO_CA, _ok, _run

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.nix_config import NixConfig

__all__ = ["_keep_store_dirs"]

FILES = (
    "dependencies.nix",
    "dependencies.builder0.sh",
    "check.nix",
    "linux-sandbox-cert-test.nix",
    "symlink-derivation.nix",
    "simple-failing.nix",
)


def _config() -> NixConfig:
    config = for_test_store(substituters=())
    config.sandbox = "true"
    return config


def _rc(arm: Arm, rc: int) -> int:
    """*rc* on the local arm. Through a daemon a build failure exits 1: `timeout.sh`'s NIX-DEFECT, Lillecarl/nix#427."""
    return rc if arm is Arm.local or rc < 100 else 1


async def _build(store: ArmStore, *args: str | Path) -> tuple[int, str, str]:
    """`nix-sandbox-build` (`linux-sandbox.sh:18`)."""
    return await _run(store, "nix-build", "--no-out-link", "--sandbox-paths", "/nix/store", *args)


def _sources(root: Path) -> Path:
    """Nix's files in `<root>/src`, a directory the sandbox does not see.

    The sandbox sees *root*, as the parent of the store; `symlink-derivation.nix` links to its own directory and
    needs it absent there, as `linux-sandbox.sh` keeps its store in `/my/store`.
    """
    src = root / "src"
    src.mkdir()
    copy_nix_tests(src, *FILES)
    return src


def _files(root: Path) -> dict[str, Path]:
    """`linux-sandbox.sh:63` to `:69`: a cert, symlinks to it, a symlink to the test root, and no cert."""
    cert = root / "some-cert-file.pem"
    cert.write_text("CERT_CONTENT")
    symlink = root / "symlink-cert-file.pem"
    symlink.symlink_to(cert)
    transitive = root / "transitive-symlink-cert-file.pem"
    transitive.symlink_to(symlink)
    symlink_dir = root / "symlink-dir"
    symlink_dir.symlink_to(root)
    return {
        "nocert": root / "no-cert-file.pem",
        "cert": cert,
        "symlinkcert": symlink,
        "transitivesymlinkcert": transitive,
        "symlinkDir": symlink_dir,
        "TEST_ROOT": root,
    }


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_sandboxed_build_builds_checks_and_refuses_etc(tmp_path: Path, arm: Arm) -> None:
    """`linux-sandbox.sh:23` to `:47`: a build, its queries, `--check`, a nondeterministic `--check -K`, `/etc`."""
    src = _sources(tmp_path)
    deps = src / "dependencies.nix"
    async with arm_store(tmp_path, arm, _config(), server_lines=NO_CA) as store:
        rc, out, err = await _build(store, deps)
        assert rc == 0, f"linux-sandbox.sh:23: {err}"
        out_path = out.strip()
        assert re.fullmatch(rf"{re.escape(str(tmp_path / 'store'))}/.*-dependencies-top", out_path), out_path
        assert "input-2" in await _ok(store, 27, "nix", "path-info", "-r", out_path)
        assert "foobar" in await _ok(store, 29, "nix", "store", "ls", "-R", "-l", out_path)
        assert "FOOBAR" in await _ok(store, 31, "nix", "store", "cat", f"{out_path}/foobar")
        rc, _out, err = await _build(store, deps, "--check")
        assert rc == 0, f"linux-sandbox.sh:34: {err}"
        check = src / "check.nix"
        rc, _out, err = await _build(store, check, "-A", "nondeterministic")
        assert rc == 0, f"linux-sandbox.sh:37: {err}"
        rc, _out, err = await _build(store, check, "-A", "nondeterministic", "--check", "-K")
        assert (rc, "error: renaming" in err, "may not be deterministic" in err) == (_rc(arm, 104), False, True), (
            f"linux-sandbox.sh:40: {err}"
        )
        etc = f'with import {src}/config.nix; mkDerivation {{ name = "etc-write"; buildCommand = "echo > /etc/test"; }}'
        rc, _out, err = await _build(store, "-E", etc)
        assert (rc, "/etc/test: Permission denied" in err) == (_rc(arm, 100), True), f"linux-sandbox.sh:46: {err}"


# (`linux-sandbox.sh` line, expectation, mode, cert file name)
CERTS = (
    (72, "missing", "normal", "cert"),
    (75, "missing", "fixed-output", ""),
    (78, "missing", "fixed-output", "nocert"),
    (81, "present", "fixed-output", "cert"),
    (84, "present", "fixed-output", "symlinkcert"),
    (85, "present", "fixed-output", "transitivesymlinkcert"),
)


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_fixed_output_build_alone_sees_the_ssl_cert_file(tmp_path: Path, arm: Arm) -> None:
    """`linux-sandbox.sh:52` to `:85` (`testCert`): `ssl-cert-file` reaches a fixed-output build's sandbox only."""
    src = _sources(tmp_path)
    files = _files(src)
    wrong = []
    async with arm_store(tmp_path, arm, _config(), server_lines=NO_CA) as store:
        for line, expectation, mode, name in CERTS:
            cert = str(files[name]) if name else ""
            rc, _out, err = await _build(
                store, src / "linux-sandbox-cert-test.nix", "--argstr", "mode", mode,
                "--option", "ssl-cert-file", cert,
            )  # fmt: skip
            want = (1 if mode == "fixed-output" else _rc(arm, 100), True)
            if (rc, f"CERT_{expectation}_IN_SANDBOX" in err) != want:
                wrong.append(f"linux-sandbox.sh:{line}: rc {rc}, want {want}: {err[-600:]}")
    assert wrong == []


def _sandbox_paths(files: dict[str, Path]) -> list[str]:
    """`linux-sandbox.sh:89`: a file, a directory and two symlinks as sandbox paths."""
    paths = (
        f"/file={files['cert']}",
        f"/dir={files['TEST_ROOT']}",
        f"/symlinkDir={files['symlinkDir']}",
        f"/symlink={files['symlinkcert']}",
    )
    return [arg for path in paths for arg in ("--option", "extra-sandbox-paths", path)]


@pytest.mark.parametrize("arm", list(Arm))
async def test_symlinks_and_sandbox_paths_reach_the_sandbox_as_they_are(tmp_path: Path, arm: Arm) -> None:
    """`linux-sandbox.sh:88`, `:89`, `:144`: a symlink input stays a symlink, and so does a symlink sandbox path."""
    src = _sources(tmp_path)
    files = _files(src)
    symlinks = src / "symlink-derivation.nix"
    async with arm_store(tmp_path, arm, _config(), server_lines=NO_CA) as store:
        rc, _out, err = await _build(store, symlinks, "-A", "depends_on_symlink")
        assert rc == 0, f"linux-sandbox.sh:88: {err}"
        rc, _out, err = await _build(store, symlinks, "-A", "test_sandbox_paths", *_sandbox_paths(files))
        assert rc == 0, f"linux-sandbox.sh:89: {err}"
        # `:144`: `/./file`, `//dir/=<root>/`, `///./symlinkDir`; each normalises to the path above.
        normalised = [
            "--option", "extra-sandbox-paths", f"/./file={files['cert']}",
            "--option", "extra-sandbox-paths", f"//dir/={files['TEST_ROOT']}/",
            "--option", "extra-sandbox-paths", f"///./symlinkDir={files['symlinkDir']}",
            "--option", "extra-sandbox-paths", f"/symlink={files['symlinkcert']}",
        ]  # fmt: skip
        rc, _out, err = await _build(store, symlinks, "-A", "test_sandbox_paths", *normalised)
        assert rc == 0, f"linux-sandbox.sh:144: {err}"


INVALID_LEAVES = ("/foo/..", "/foo/.", "/..", "/.")
ESCAPES = (
    "/../escape",
    "/foo/../../escape",
    "//foo///.//../../escape",
    "/./../escape",
    "///.//./../escape//",
    "/a/b/../../../escape",
)
SOURCES = ("cert", "symlinkcert", "symlinkDir", "TEST_ROOT")


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_sandbox_path_that_leaves_the_chroot_is_refused(tmp_path: Path, arm: Arm) -> None:
    """`linux-sandbox.sh:95` to `:142`: an invalid or escaping sandbox path fails the build with Nix's message."""
    src = _sources(tmp_path)
    files = _files(src)
    cases: list[tuple[int, str, int, str]] = [
        (103, f"{bad}={files['cert']}", 1, "invalid filename") for bad in INVALID_LEAVES
    ]
    cases += [(125, f"{bad}={files[s]}", 1, "escapes the chroot") for bad in ESCAPES for s in SOURCES]
    cases += [
        (131, f"/a/={files['cert']}", 1, r"error: creating regular file.*: No such file or directory"),
        (132, f"/a/={files['symlinkcert']}", 1, r"error: creating symlink.*: No such file or directory"),
        (135, f"/foo/../={files['cert']}", 1, r"error: creating regular file.*: No such file or directory"),
        (138, f"/foo/../={src}", 1, r"error: .*escapes the chroot"),
        (141, f"/a/={src}", _rc(arm, 100), ""),
        (142, f"/a/={src}/", _rc(arm, 100), ""),
    ]
    wrong = []
    async with arm_store(tmp_path, arm, _config(), server_lines=NO_CA) as store:
        for line, path, rc_wanted, pattern in cases:
            rc, _out, err = await _build(store, src / "simple-failing.nix", "--option", "extra-sandbox-paths", path)
            if (rc, bool(re.search(pattern, err))) != (rc_wanted, True):
                wrong.append(f"linux-sandbox.sh:{line} {path}: rc {rc}, want {rc_wanted} and /{pattern}/: {err[-400:]}")
    assert wrong == []
