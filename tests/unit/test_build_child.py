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
        "chroot": True,
        "root": str(root),
        "binds": [
            ("/nix/store", "/nix/store", False),
            ("/build", str(build), False),
            ("/dev/null", "/dev/null", False),
        ],
        "private_network": True,
        "sandbox_uid": 1000,
        "sandbox_gid": 100,
        "build_user": None,
        "build_groups": [],
        "map_fd": None,
        "work_dir": "/build",
        "shm_size": "50%",
        "no_new_privileges": True,
        "filter_syscalls": True,
        "fetchurl": None,
        "buildenv": None,
        "unpack_channel": None,
        "store_dir": "/nix/store",
        "usage_file": str(root.parent / "usage.json"),
        "cgroup": None,
        "cgroup_report": False,
        "sample_seconds": 0,
    }
    spec.update(over)  # type: ignore[typeddict-item]  # the tests vary one key at a time
    return spec


def _run(tmp_path: Path, spec: ChildSpec, extra_fds: tuple[int, ...] = ()) -> tuple[int, str, str]:
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(json.dumps(spec))
    read, write = os.pipe()
    proc = subprocess.run(
        [sys.executable, "-m", "pynixd_store.builder.child", str(spec_file), str(write)],
        pass_fds=(write, *extra_fds),
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


def test_without_the_chroot_the_builder_runs_on_the_host_as_its_caller(tmp_path: Path) -> None:
    """`LinuxDerivationBuilder` (`linux/build/linux-derivation-builder.hh:24`): no namespace, no chroot."""
    work = tmp_path / "work"
    work.mkdir()
    script = 'echo "$FOO $(id -u):$(id -g) $PWD $(cat /proc/sys/kernel/hostname)"; [ $$ != 1 ] && echo own-pid'
    spec = _spec(tmp_path / "root", script, chroot=False, binds=[], private_network=False, work_dir=str(work))
    spec["root"] = ""
    code, out, status = _run(tmp_path, spec)
    assert (code, status) == (0, ""), out
    assert out.splitlines() == [f"bar {os.getuid()}:{os.getgid()} {work} {socket.gethostname()}", "own-pid"]


@pytest.mark.parametrize("chroot", [True, False])
def test_the_builder_inherits_no_descriptor_but_its_standard_three(tmp_path: Path, chroot: bool) -> None:
    """`closeExtraFDs` (`unix/build/derivation-builder.cc:975`): a descriptor the helper got stays out.

    Without the chroot a leftover process that holds a copy of the terminal keeps the build's log open.
    """
    extra = os.open("/dev/null", os.O_RDONLY)
    try:
        script = f"[ -e /proc/self/fd/{extra} ] && echo leaked || echo closed"
        over: dict[str, object] = {}
        if not chroot:
            work = tmp_path / "work"
            work.mkdir()
            over = {"chroot": False, "binds": [], "private_network": False, "work_dir": str(work)}
        spec = _spec(tmp_path / "root", script, **over)
        code, out, status = _run(tmp_path, spec, (extra,))
    finally:
        os.close(extra)
    assert (code, status) == (0, ""), out
    assert out.strip() == "closed"


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


# The file, not the directory: `_bind` makes the mount point before it mounts.
_WAIT_FOR = (
    'until [ -e "$1/file" ]; do sleep 0.05; done; ls "$1"; cat "$1/file"; until [ -e "$2" ]; do sleep 0.05; done'
)


def _add(control: socket.socket, source: str, target: str) -> dict[str, str | None]:
    control.sendall(json.dumps({"source": source, "target": target}).encode() + b"\n")
    reply: dict[str, str | None] = json.loads(control.makefile("rb").readline())
    return reply


def test_a_path_added_during_the_build_appears_in_the_sandbox(tmp_path: Path) -> None:
    """`addDependency` (`linux-derivation-builder.cc:959`): a bind into the namespace before `pivot_root`.

    It reaches the builder because the sandbox's store is a shared subtree (`:745`).
    """
    source = tmp_path / "x-added"
    source.mkdir()
    (source / "file").write_text("late\n")
    args = ["-c", _WAIT_FOR, "sh", "/store2/x-added", "/store2/done"]
    spec = _spec(tmp_path / "root", "", args=args, store_dir="/store2")
    (tmp_path / "root/store2").mkdir()
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(json.dumps(spec))
    read, write = os.pipe()
    ours, theirs = socket.socketpair()
    with (
        ours,
        subprocess.Popen(
            [sys.executable, "-m", "pynixd_store.builder.child", str(spec_file), str(write), str(theirs.fileno())],
            pass_fds=(write, theirs.fileno()),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        ) as proc,
    ):
        os.close(write)
        theirs.close()
        ours.settimeout(10)
        try:
            replies = [
                _add(ours, str(source), "/store2/x-added"),
                _add(ours, str(source), "/store2/x-added"),
                _add(ours, str(source), "/elsewhere/x-added"),
            ]
        except Exception as e:
            proc.kill()
            raise AssertionError(f"the helper said:\n{proc.communicate()[0]}") from e
        finally:
            (tmp_path / "root/store2/done").touch()
        # A builder that never sees the path would hold the block open for good.
        try:
            out, _ = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
    assert replies == [
        {"error": None},
        {"error": "store path '/store2/x-added' already exists in the sandbox"},
        {"error": "'/elsewhere/x-added' is not a path in the store '/store2'"},
    ]
    with os.fdopen(read) as status:
        assert (proc.returncode, status.read()) == (0, ""), out
    assert out.split() == ["file", "late"]


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
