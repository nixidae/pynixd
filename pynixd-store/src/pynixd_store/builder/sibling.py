"""A build whose sandbox helper runs in a sibling container of a Kubernetes pod (nixidae/nixidae#1560, spike).

The pod is one build. Its daemon container runs `build()`; its sandbox container runs `sibling run DIR` and the
helper. Both mount the build's volume at the same path, so the spec, the build directory, the chroot and the
store mean the same in each. The sandbox container has no capability and its own pid namespace: the helper keeps
it (`pid_namespace`) and makes the user, mount, network, IPC and UTS namespaces itself.

`build()` starts `sibling wait DIR ...` as the helper's process, on the build's terminal. The waiter connects to
the runner's socket in DIR, sends the job, and copies the runner's records to the terminal: the log, then the
helper's exit status. The runner gives the helper a terminal of its own, so the builder writes to a terminal as
on the host. A waiter that goes away, as a cancelled build's does, makes the runner kill the helper.
"""

from __future__ import annotations

import contextlib
import json
import os
import pty
import selectors
import signal
import socket
import struct
import subprocess
import sys
import termios
import time
import tty
from dataclasses import dataclass
from pathlib import Path
from typing import Final

SOCKET_NAME: Final = "sandbox.sock"
STATUS_NAME: Final = "sibling-status"
CONTROL_SOCKET_NAME: Final = "sibling-control.sock"
CONTROL_FD: Final = 4
#: A record: its kind, then its length.
HEADER: Final = struct.Struct("!cI")
LOG: Final = b"L"
EXIT: Final = b"E"
#: How long the waiter tries the runner's socket: the sandbox container may start after the daemon's.
CONNECT_SECONDS: Final = 60.0


@dataclass(frozen=True)
class SiblingSandbox:
    """The rendezvous directory both containers mount, where the runner listens."""

    rendezvous: Path


def _records(conn: socket.socket) -> tuple[bytes, bytes]:
    header = _exactly(conn, HEADER.size)
    kind, size = HEADER.unpack(header)
    return kind, _exactly(conn, size)


def _exactly(conn: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        piece = conn.recv(size - len(data))
        if not piece:
            raise EOFError("the sandbox container closed the connection")
        data.extend(piece)
    return bytes(data)


def _connect(path: Path) -> socket.socket:
    deadline = time.monotonic() + CONNECT_SECONDS
    while True:
        conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            conn.connect(str(path))
        except (FileNotFoundError, ConnectionRefusedError):
            conn.close()
            if time.monotonic() > deadline:
                raise
            # No notification crosses the two containers but the socket file itself.
            time.sleep(0.1)
            continue
        return conn


def wait(rendezvous: Path, spec_file: Path, top: Path, *, recursive: bool) -> int:
    """The daemon's end: send the job, copy the log to stdout, and return the helper's exit status."""
    job = {"spec": str(spec_file), "status": str(top / STATUS_NAME), "control": None}
    if recursive:
        job["control"] = str(top / CONTROL_SOCKET_NAME)
    with _connect(rendezvous / SOCKET_NAME) as conn:
        conn.sendall(json.dumps(job).encode() + b"\n")
        while True:
            kind, payload = _records(conn)
            if kind == LOG:
                os.write(1, payload)
            elif kind == EXIT:
                return int(payload)
            else:
                raise ValueError(f"unknown record {kind!r} from the sandbox container")


def _exit_like(code: int) -> None:
    """Exit as the helper did: a signal kills the waiter with it, so `Popen.returncode` reads the same."""
    if code < 0:
        signal.signal(-code, signal.SIG_DFL)
        os.kill(os.getpid(), -code)
    sys.exit(code)


def _send(conn: socket.socket, kind: bytes, payload: bytes) -> None:
    conn.sendall(HEADER.pack(kind, len(payload)) + payload)


def _serve(conn: socket.socket) -> None:
    job = json.loads(conn.makefile("rb").readline())
    master, slave = pty.openpty()
    mode = termios.tcgetattr(slave)
    tty.cfmakeraw(mode)
    termios.tcsetattr(slave, termios.TCSANOW, mode)
    status = os.open(job["status"], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    fds = {3: status}
    control: socket.socket | None = None
    if job["control"] is not None:
        control = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        control.connect(job["control"])
        fds[CONTROL_FD] = control.fileno()
    argv = [sys.executable, "-I", "-m", "pynixd_store.builder.child", job["spec"], "3"]
    if control is not None:
        argv.append(str(CONTROL_FD))

    def place() -> None:
        for target, fd in fds.items():
            os.dup2(fd, target)

    with subprocess.Popen(
        argv, stdin=subprocess.DEVNULL, stdout=slave, stderr=slave, preexec_fn=place, pass_fds=tuple(fds)
    ) as helper:
        os.close(slave)
        os.close(status)
        if control is not None:
            control.close()
        with selectors.DefaultSelector() as selector:
            selector.register(master, selectors.EVENT_READ)
            selector.register(conn, selectors.EVENT_READ)
            open_log = True
            while open_log:
                for key, _ in selector.select():
                    if key.fileobj is conn:
                        # The waiter sends nothing after the job: readable means it went away.
                        helper.kill()
                        selector.unregister(conn)
                        continue
                    try:
                        piece = os.read(master, 64 * 1024)
                    except OSError:
                        piece = b""
                    if not piece:
                        open_log = False
                        break
                    with contextlib.suppress(BrokenPipeError):
                        _send(conn, LOG, piece)
        os.close(master)
        code = helper.wait()
    with contextlib.suppress(BrokenPipeError):
        _send(conn, EXIT, str(code).encode())


def run(rendezvous: Path) -> None:
    """The sandbox container's end: serve builds one at a time until the pod ends. A pod that registers as a
    builder runs the probe build before its own."""
    rendezvous.mkdir(parents=True, exist_ok=True)
    path = rendezvous / SOCKET_NAME
    with contextlib.suppress(FileNotFoundError):
        path.unlink()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(str(path))
        listener.listen(1)
        while True:
            conn, _ = listener.accept()
            with conn:
                _serve(conn)


def main() -> None:
    command, *rest = sys.argv[1:]
    if command == "run":
        run(Path(rest[0]))
    elif command == "wait":
        rendezvous, spec_file, top, *flags = rest
        _exit_like(wait(Path(rendezvous), Path(spec_file), Path(top), recursive="recursive" in flags))
    else:
        raise SystemExit(f"usage: sibling run DIR | sibling wait DIR SPEC TOP [recursive]; not {command!r}")


if __name__ == "__main__":
    main()
