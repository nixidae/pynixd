"""The sandbox helper on darwin applies Nix's profile and starts the builder as `DarwinDerivationBuilder` does.

darwin only: `sandbox_init_with_parameters` and `posix_spawn`'s `POSIX_SPAWN_SETEXEC` are Apple's.
"""

from __future__ import annotations

import ctypes
import json
import os
import socket
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from pynixd_store.builder import darwin
from pynixd_store.builder.spec import ChildSpec, DarwinSpec

if TYPE_CHECKING:
    from pathlib import Path

IPC_CREAT = 0o1000

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="the darwin sandbox profile")

#: `sandbox-paths` on darwin (`globals.cc:94`), less `/private/tmp` and `/private/var/tmp`.
HOST_PATHS = ("/System/Library/Frameworks", "/System/Library/PrivateFrameworks", "/bin/sh", "/bin/bash", "/usr/lib")


def _spec(tmp_path: Path, script: str, *, sandbox: bool = True, **over: object) -> tuple[ChildSpec, Path]:
    # The kernel matches a rule against the real path, and `/tmp` is a symlink to `/private/tmp`; so does Nix's.
    tmp_path = tmp_path.resolve()
    build = tmp_path / "build"
    build.mkdir()
    store = tmp_path / "store"
    store.mkdir()
    out = store / "00000000000000000000000000000000-out"
    profile: DarwinSpec = {
        "sandbox": sandbox,
        "paths": [(p, p, False) for p in (*HOST_PATHS, str(build))],
        "inputs": [],
        "scratch": [str(out)],
        "store_dir": str(store),
        "network": False,
        "log_violations": False,
        "additional_profile": "",
        "build_top": str(build),
        "allow_local_networking": False,
        "platform": "aarch64-darwin",
    }
    profile.update(over)  # type: ignore[typeddict-item]  # the tests vary one key at a time
    spec: ChildSpec = {
        "builder": "/bin/sh",
        "args": ["-c", script],
        "env": {"PATH": "/bin", "out": str(out)},
        "chroot": False,
        "root": "",
        "binds": [],
        "private_network": False,
        "pid_namespace": True,
        "sandbox_uid": 1000,
        "sandbox_gid": 100,
        "build_user": None,
        "build_groups": [],
        "build_ids": 1,
        "id_range": False,
        "external": None,
        "work_dir": str(build),
        "shm_size": "50%",
        "no_new_privileges": False,
        "filter_syscalls": False,
        "require_drop_groups": False,
        "fetchurl": None,
        "buildenv": None,
        "unpack_channel": None,
        "store_dir": str(store),
        "usage_file": str(tmp_path / "usage.json"),
        "cgroup": None,
        "cgroup_report": False,
        "sample_seconds": 0,
        "darwin": profile,
    }
    return spec, out


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


READ_ETC = 'echo built > "$out"; if read l < /private/etc/passwd; then echo read; else echo refused; fi'


def test_the_sandbox_lets_the_builder_write_its_output_and_nothing_else_outside(tmp_path: Path) -> None:
    spec, out = _spec(tmp_path, READ_ETC)
    code, log, status = _run(tmp_path, spec)
    assert (code, status) == (0, ""), log
    # `\2` ends the set-up, as Nix's child writes it after `setUser` (`derivation-builder.cc:990`).
    assert log.startswith("\x02\n"), log
    assert log.splitlines()[-1] == "refused"
    assert out.read_text() == "built\n"


def test_without_the_sandbox_the_builder_reads_the_host(tmp_path: Path) -> None:
    spec, out = _spec(tmp_path, READ_ETC, sandbox=False)
    code, log, status = _run(tmp_path, spec)
    assert (code, status) == (0, ""), log
    assert log.splitlines()[-1] == "read"
    assert out.read_text() == "built\n"


def test_a_required_path_that_is_absent_is_a_setup_error(tmp_path: Path) -> None:
    spec, _out = _spec(tmp_path, "true")
    if spec["darwin"] is None:
        raise AssertionError("the spec is darwin's")
    missing = str(tmp_path / "missing")
    spec["darwin"]["paths"].append((missing, missing, False))
    code, log, status = _run(tmp_path, spec)
    assert code == 1
    assert "\x02" not in log
    assert status.startswith("getting attributes of required path '"), status


def test_a_profile_the_kernel_refuses_ends_the_build_on_its_log(tmp_path: Path) -> None:
    """`darwin-derivation-builder.cc:176`: written to the terminal before `\\2`, then exit 1."""
    spec, _out = _spec(tmp_path, "true", additional_profile="(not-a-rule)")
    code, log, status = _run(tmp_path, spec)
    assert (code, status) == (1, ""), log
    # libsandbox prints its own "sandbox initialization failed" first, under Nix as here.
    assert "\nfailed to configure sandbox: unbound variable: not-a-rule" in log, log
    assert "\x02" not in log


@pytest.mark.parametrize(
    ("network", "local", "reached"), [(False, False, False), (True, False, True), (False, True, True)]
)
def test_the_network_is_closed_unless_fixed_output_or_asked_for_locally(
    tmp_path: Path, network: bool, local: bool, reached: bool
) -> None:
    """A sandboxed build reaches no IP socket; `__darwinAllowLocalNetworking` opens localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        script = f"if (exec 3<>/dev/tcp/127.0.0.1/{port}) 2>/dev/null; then echo reached; else echo closed; fi"
        spec, _out = _spec(tmp_path, script, network=network, allow_local_networking=local)
        spec["builder"] = "/bin/bash"
        code, log, status = _run(tmp_path, spec)
    assert (code, status) == (0, ""), log
    assert log.splitlines()[-1] == ("reached" if reached else "closed")


def test_a_build_users_system_v_ipc_objects_are_removed() -> None:
    """`cleanupSysVIPCForUser` (`darwin-derivation-builder.cc:209`), run on this test's own uid.

    It removes every object the uid owns, so the test skips when the uid owns one already.
    """
    uid = os.getuid()
    if any(darwin.sysv_ipc_keys(uid).values()):
        pytest.skip("this uid owns System V IPC objects that the clean-up would remove")
    libc = ctypes.CDLL(None, use_errno=True)
    key = 0x5059_0000 | (os.getpid() & 0xFFFF)
    made = {
        "shm": libc.shmget(key, 4096, IPC_CREAT | 0o600),
        "msg": libc.msgget(key, IPC_CREAT | 0o600),
        "sem": libc.semget(key, 1, IPC_CREAT | 0o600),
    }
    try:
        assert all(i != -1 for i in made.values()), (made, ctypes.get_errno())
        assert darwin.sysv_ipc_keys(uid) == {"shm": [key], "msg": [key], "sem": [key]}
        darwin.cleanup_sysv_ipc(uid)
        assert darwin.sysv_ipc_keys(uid) == {"shm": [], "msg": [], "sem": []}
    finally:
        libc.shmctl(made["shm"], darwin.IPC_RMID, None)
        libc.msgctl(made["msg"], darwin.IPC_RMID, None)
        libc.semctl(made["sem"], 0, darwin.IPC_RMID)
