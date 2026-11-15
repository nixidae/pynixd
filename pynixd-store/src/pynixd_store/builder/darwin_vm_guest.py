"""The darwin build VM's side in the guest: run the helper, then hand the outputs to the host (`darwin_vm`).

    python -I -m pynixd_store.builder.darwin_vm_guest TOP/darwin-vm.json

The helper's file descriptor 3 is `top/vm-status`, as in the Linux build VM. Its output is this process's, which
the guest's agent sends to the host. The outputs are on the tmpfs over the store, which the host cannot see: each
one is copied into `top/vm-out`, a writable share. Not with macOS `cp -p`: its copyfile gave a 0644 file 0755 on
the share (measured).

The build directory is a tmpfs too: a file made on the share costs about 3 ms, and unpacking coreutils' 3786
files took 6 s there against 1 s on the host (measured). With `keep-failed`, a failed build's directory is copied
to `top/vm-build`, which the launcher puts in its place.

A recursive build's socket is in the build directory. The guest listens there and relays each connection over
vsock to the host, where pynixd listens at the same path.
"""

from __future__ import annotations

import contextlib
import ctypes
import json
import os
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

from .darwin_vm import BUILDER_VSOCK_PORT, KEPT_NAME, OUTPUTS_NAME
from .vm import EXIT_NAME, STATUS_NAME


def copy(source: Path, target: Path, *, owners: bool = False) -> None:
    """*source* to *target*: contents, the mode's permission bits and symbolic links, with *owners* the owners too.

    Any other kind of file is left out: a FIFO would block the read.
    """
    st = source.lstat()
    if stat.S_ISLNK(st.st_mode):
        os.symlink(os.readlink(source), target)
    elif stat.S_ISDIR(st.st_mode):
        target.mkdir()
        for child in sorted(source.iterdir()):
            copy(child, target / child.name, owners=owners)
    elif stat.S_ISREG(st.st_mode):
        with source.open("rb") as reader, target.open("xb") as writer:
            while chunk := reader.read(1 << 20):
                writer.write(chunk)
    else:
        return
    if owners:
        os.lchown(target, st.st_uid, st.st_gid)
    if not stat.S_ISLNK(st.st_mode):
        os.chmod(target, st.st_mode & 0o7777)


def build_on_tmpfs(build: Path) -> None:
    """A tmpfs over *build*, holding what it held, with its owner and mode."""
    stage = Path(tempfile.mkdtemp(prefix="pynixd-build-")) / "build"
    copy(build, stage, owners=True)
    subprocess.run(["/sbin/mount_tmpfs", str(build)], check=True, capture_output=True)
    st = stage.lstat()
    os.chown(build, st.st_uid, st.st_gid)
    os.chmod(build, st.st_mode & 0o7777)
    for child in stage.iterdir():
        copy(child, build / child.name, owners=True)
    shutil.rmtree(stage.parent)


class _SockaddrVm(ctypes.Structure):
    """`struct sockaddr_vm` (`sys/vsock.h`); CPython has `AF_VSOCK` on Linux only."""

    _fields_ = (
        ("svm_len", ctypes.c_uint8),
        ("svm_family", ctypes.c_uint8),
        ("svm_reserved1", ctypes.c_uint16),
        ("svm_port", ctypes.c_uint32),
        ("svm_cid", ctypes.c_uint32),
    )


AF_VSOCK = 40
VMADDR_CID_HOST = 2
_libc = ctypes.CDLL(None, use_errno=True)


def connect_host(port: int) -> socket.socket:
    sock = socket.socket(AF_VSOCK, socket.SOCK_STREAM)
    address = _SockaddrVm(ctypes.sizeof(_SockaddrVm), AF_VSOCK, 0, port, VMADDR_CID_HOST)
    if _libc.connect(sock.fileno(), ctypes.byref(address), ctypes.sizeof(address)) != 0:
        sock.close()
        error = ctypes.get_errno()
        raise OSError(error, f"vsock to the host's port {port}: {os.strerror(error)}")
    return sock


def _pump(source: socket.socket, target: socket.socket) -> None:
    with contextlib.suppress(OSError):
        while data := source.recv(1 << 16):
            target.sendall(data)
    with contextlib.suppress(OSError):
        target.shutdown(socket.SHUT_WR)


def _relay(client: socket.socket) -> None:
    with client:
        try:
            host = connect_host(BUILDER_VSOCK_PORT)
        except OSError as e:
            print(f"darwin build VM: {e}", file=sys.stderr, flush=True)
            return
        with host:
            back = threading.Thread(target=_pump, args=(host, client), daemon=True)
            back.start()
            _pump(client, host)
            back.join()


def relay_builder_socket(path: Path) -> None:
    """Listen at *path*, as pynixd does on the host, and relay each connection to it over vsock."""
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    # A build directory may be longer than `sun_path`: bind the name from its directory.
    here = os.getcwd()
    os.chdir(path.parent)
    try:
        listener.bind(path.name)
    finally:
        os.chdir(here)
    st = path.parent.stat()
    os.chown(path, st.st_uid, st.st_gid)
    os.chmod(path, 0o600)
    listener.listen()

    def accept() -> None:
        while True:
            client, _ = listener.accept()
            threading.Thread(target=_relay, args=(client,), daemon=True).start()

    threading.Thread(target=accept, daemon=True).start()


def main(job_path: Path) -> None:
    job = json.loads(job_path.read_text())
    top = Path(job["top"])
    build = Path(job["build"])
    try:
        build_on_tmpfs(build)
    except (OSError, subprocess.CalledProcessError) as e:
        detail = e.stderr.decode(errors="replace").strip() if isinstance(e, subprocess.CalledProcessError) else e
        (top / EXIT_NAME).write_text(f"init cannot put the build directory on a tmpfs: {detail}\n")
        return
    if job["builder_socket"] is not None:
        try:
            relay_builder_socket(Path(job["builder_socket"]))
        except OSError as e:
            (top / EXIT_NAME).write_text(f"init cannot relay the builder's socket: {e}\n")
            return
    status = os.open(top / STATUS_NAME, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.dup2(status, 3)
    os.close(status)
    helper = subprocess.run(
        [sys.executable, "-I", "-m", "pynixd_store.builder.child", job["spec"], "3"], pass_fds=(3,), check=False
    )
    os.close(3)
    code = helper.returncode
    end = f"signal {-code}" if code < 0 else f"exit {code}"
    outputs = top / OUTPUTS_NAME
    try:
        if code != 0 and job["keep_failed"]:
            copy(build, top / KEPT_NAME)
        outputs.mkdir()
        for path in map(Path, job["outputs"]):
            if os.path.lexists(path):
                copy(path, outputs / path.name)
    except OSError as e:
        end = f"init cannot copy the outputs to the host: {e}"
    (top / EXIT_NAME).write_text(f"{end}\n")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
