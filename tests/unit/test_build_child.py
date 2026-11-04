"""The sandbox helper runs a builder as Nix's chroot builder does (`linux-derivation-builder.cc:506`).

It needs unprivileged user namespaces; the guest has them, as `nix build --store DIR` there shows.
"""

from __future__ import annotations

import ctypes
import errno
import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from pynixd_store.builder.spec import ChildSpec

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
        "filter_syscalls": True,
        "fetchurl": None,
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


_TRY_MODES = """
import errno, os
def attempt(call, *args):
    try:
        call(*args)
        print("ok")
    except OSError as e:
        print(errno.errorcode[e.errno])
open("f", "w").close()
fd = os.open("f", os.O_RDONLY)
for mode in (0o4755, 0o2755, 0o755):
    attempt(os.chmod, "f", mode)
    attempt(os.fchmod, fd, mode)
attempt(os.listxattr, "f")
"""


def _try_modes(tmp_path: Path, root: str, **over: object) -> list[str]:
    spec = _spec(tmp_path / root, "", builder=sys.executable, args=["-I", "-c", _TRY_MODES], **over)
    code, out, status = _run(tmp_path, spec)
    assert (code, status) == (0, ""), out
    return out.split()


def test_the_filter_refuses_setuid_setgid_and_xattrs(tmp_path: Path) -> None:
    """`setupSeccomp` (`linux-derivation-builder.cc:79`): EPERM for a set-id bit, ENOTSUP for an xattr."""
    refused = errno.errorcode[errno.EPERM]
    assert _try_modes(tmp_path, "a") == [refused] * 4 + ["ok", "ok", errno.errorcode[errno.ENOTSUP]]


def test_filter_syscalls_false_lets_them_through(tmp_path: Path) -> None:
    assert _try_modes(tmp_path, "a", filter_syscalls=False) == ["ok"] * 7


_CONNECT = """
import errno, socket, sys
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
try:
    s.connect(b"\\0" + sys.argv[1].encode())
    print("ok")
except OSError as e:
    print(errno.errorcode[e.errno])
"""


def _landlock_abi() -> int:
    libc = ctypes.CDLL(None, use_errno=True)
    return int(libc.syscall(444, None, 0, 1))


@pytest.mark.skipif(_landlock_abi() < 6, reason="landlock ABI 6 scopes abstract sockets")
def test_a_shared_network_build_cannot_reach_an_abstract_socket_outside(tmp_path: Path) -> None:
    """`setupLandlock` (`linux-derivation-builder.cc:201`): a fixed-output build shares the network namespace."""
    name = f"pynixd-test-{os.getpid()}"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(b"\0" + name.encode())
        listener.listen()
        outside = subprocess.run(
            [sys.executable, "-I", "-c", _CONNECT, name], capture_output=True, text=True, check=True
        )
        args = ["-I", "-c", _CONNECT, name]
        spec = _spec(tmp_path / "root", "", builder=sys.executable, args=args, private_network=False)
        code, out, status = _run(tmp_path, spec)
    assert outside.stdout.split() == ["ok"]
    assert (code, status, out.split()) == (0, "", [errno.errorcode[errno.EPERM]])
