"""The sandbox helper runs a builder as Nix's chroot builder does (`linux-derivation-builder.cc:506`).

It needs unprivileged user namespaces; the guest has them, as `nix build --store DIR` there shows.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from pynixd_store.builder.child import ChildSpec

SH = os.path.realpath("/bin/sh")
COREUTILS = os.path.dirname(os.path.realpath(shutil.which("ls") or "/bin/ls"))


def _spec(root: Path, script: str, **over: object) -> ChildSpec:
    for d in ("etc", "tmp", "nix/store", "dev"):
        (root / d).mkdir(parents=True, exist_ok=True)
    build = root.parent / "build"
    build.mkdir(exist_ok=True)
    spec: ChildSpec = {
        "builder": SH,
        "args": ["-c", script],
        "env": {"PATH": COREUTILS, "FOO": "bar"},
        "root": str(root),
        "binds": [
            ("/nix/store", "/nix/store", False),
            ("/build", str(build), False),
            ("/dev/null", "/dev/null", False),
        ],
        "private_network": True,
        "sandbox_uid": 1000,
        "sandbox_gid": 100,
        "work_dir": "/build",
        "shm_size": "50%",
        "no_new_privileges": True,
    }
    spec.update(over)  # type: ignore[typeddict-item]  # the tests vary one key at a time
    return spec


def _run(tmp_path: Path, spec: ChildSpec) -> tuple[int, str, str]:
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(json.dumps(spec))
    read, write = os.pipe()
    proc = subprocess.run(
        [sys.executable, "-m", "pynixd_store.builder.child", str(spec_file), str(write)],
        pass_fds=(write,),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    os.close(write)
    with os.fdopen(read) as status:
        return proc.returncode, proc.stdout + proc.stderr, status.read()


def test_the_builder_runs_as_the_sandbox_user_in_its_own_namespaces(tmp_path: Path) -> None:
    script = 'echo "$FOO $(id -u):$(id -g) $$ $PWD $(cat /proc/sys/kernel/hostname)"; ls /'
    code, out, status = _run(tmp_path, _spec(tmp_path / "root", script))
    assert (code, status) == (0, ""), out
    first, *listing = out.splitlines()
    assert first == "bar 1000:100 1 /build localhost"
    assert "real-root" not in listing
    assert {"build", "dev", "etc", "nix", "proc", "tmp"} <= set(listing)


def test_the_builders_exit_code_reaches_the_caller(tmp_path: Path) -> None:
    assert _run(tmp_path, _spec(tmp_path / "root", "exit 3"))[:2] == (3, "")


def test_a_missing_input_is_a_setup_error_not_a_build_failure(tmp_path: Path) -> None:
    spec = _spec(tmp_path / "root", "true")
    spec["binds"].append(("/nix/store/missing", str(tmp_path / "missing"), False))
    code, _out, status = _run(tmp_path, spec)
    assert code == 1
    assert status == f"path '{tmp_path / 'missing'}' does not exist"


def test_the_network_is_private_unless_fixed_output(tmp_path: Path) -> None:
    private = _run(tmp_path, _spec(tmp_path / "a", "cat /proc/net/dev"))[1]
    shared = _run(tmp_path, _spec(tmp_path / "b", "cat /proc/net/dev", private_network=False))[1]
    assert [line.split(":")[0].strip() for line in private.splitlines()[2:]] == ["lo"]
    assert private != shared
