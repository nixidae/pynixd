"""A build whose sandbox helper runs in a sibling container of a Kubernetes pod (nixidae/nixidae#1560, spike).

The pod is one build. Its daemon container runs `build()`; its sandbox container runs `sibling run DIR` and the
helper. Both mount the build's volume at the same path, so the spec, the build directory, the chroot and the
store mean the same in each. The sandbox container has no capability and its own pid namespace. The helper makes
the user, mount, network, IPC and UTS namespaces itself, and a pid namespace with a fresh `/proc` when the
container lets it: a pod whose sandbox container sets `procMount: Unmasked`, which needs `hostUsers: false`. The
runner asks the kernel once, at start (`fresh_proc_allowed`). Otherwise the helper keeps the container's pid
namespace and binds its masked `/proc`: the kernel refuses a fresh one in a user namespace while a locked mount
covers a file of the old one, and a container's masks, such as `/proc/keys`, are those.

`build()` starts `sibling wait DIR ...` as the helper's process, on the build's terminal. The waiter connects to
the runner's socket in DIR, sends the job, and copies the runner's records to the terminal: the log, then the
helper's exit status. The runner gives the helper a terminal of its own, so the builder writes to a terminal as
on the host. A waiter that goes away, as a cancelled build's does, makes the runner kill the helper.
"""

from __future__ import annotations

import contextlib
import ctypes
import json
import os
import pty
import selectors
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import termios
import time
import tty
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from . import cgroup as build_cgroup
from .usage import BuildUsage, write_usage

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
#: The namespaces `fresh_proc_allowed` tries, as the helper makes them.
PROBE_NAMESPACES: Final = os.CLONE_NEWUSER | os.CLONE_NEWNS | os.CLONE_NEWPID

_libc = ctypes.CDLL(None, use_errno=True)


def fresh_proc_allowed(scratch: Path) -> bool:
    """Whether a new user and pid namespace here may mount a `/proc` of its own, as the helper would.

    Tried, not read from the mount table: which locked mounts make the kernel refuse is `mnt_already_visible`'s
    rule, and a mount over an empty directory, as `binfmt_misc` is, does not. The mount point goes under
    *scratch*: a sandbox container may have no writable /tmp (measured).
    """
    with tempfile.TemporaryDirectory(prefix="proc-probe-", dir=scratch) as target:
        pid = os.fork()
        if pid == 0:
            code = 1
            try:
                os.unshare(PROBE_NAMESPACES)
                # The new pid namespace takes the caller's next child as its pid 1, and only that one may mount it.
                inner = os.fork()
                if inner == 0:
                    os._exit(0 if _libc.mount(b"none", target.encode(), b"proc", 0, None) == 0 else 1)
                code = os.waitstatus_to_exitcode(os.waitpid(inner, 0)[1])
            except OSError:
                code = 1
            finally:
                os._exit(code)
        return os.waitstatus_to_exitcode(os.waitpid(pid, 0)[1]) == 0


def leftovers(parents: dict[int, int], keep: int) -> set[int]:
    """The processes of *parents* (pid to parent pid) that are not *keep* or one of its ancestors."""
    spared = {0}
    pid = keep
    while pid not in spared:
        spared.add(pid)
        pid = parents.get(pid, 0)
    return set(parents) - spared


def _parents() -> dict[int, int]:
    found: dict[int, int] = {}
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            with contextlib.suppress(FileNotFoundError, ProcessLookupError):
                # The field after the command, which may hold spaces and parentheses.
                found[int(entry.name)] = int((entry / "stat").read_text().rpartition(")")[2].split()[1])
    return found


def kill_leftovers() -> None:
    """`killUser` for a build without a chroot: the container runs one build, so every other process is its.

    A process the builder left holds the terminal open, and the runner then never sees the log end. Nix's own
    sandbox gets this from its pid namespace; a build without one has a build user to kill, or hangs.
    """
    while targets := leftovers(_parents(), os.getpid()):
        for pid in targets:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
        # Orphans come back to the runner, as pid 1 of the container: reap them, or each stays as a zombie.
        with contextlib.suppress(ChildProcessError):
            while os.waitpid(-1, os.WNOHANG) != (0, 0):
                pass


def own_pid_namespace(spec_file: Path) -> None:
    """Ask the helper for a pid namespace of its own in *spec_file*, the job's `ChildSpec`."""
    spec = json.loads(spec_file.read_text())
    spec["pid_namespace"] = True
    spec_file.write_text(json.dumps(spec))


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


def container_cgroup() -> Path | None:
    """The sandbox container's own cgroup, or `None` when it names none."""
    try:
        path = build_cgroup.own_cgroup(Path("/proc/self/cgroup").read_text())
    except OSError:
        return None
    return None if path is None else build_cgroup.ROOT / path.lstrip("/")


def read_cgroup(path: Path) -> BuildUsage | None:
    """*path*'s usage, or `None` for a cgroup that cannot be read: no `memory.peak` before Linux 5.19."""
    try:
        return build_cgroup.read(path)
    except (OSError, KeyError, ValueError):
        return None


def measured_by_the_container(usage_file: Path, before: BuildUsage | None, cgroup: Path) -> None:
    """Replace the helper's `wait4` record in *usage_file* with what the container's cgroup counted since *before*.

    `wait4` gives the memory of the largest process alone, which does not grow with `NIX_BUILD_CORES`, and central
    sizes a pod's memory by that growth. A build pod runs one build, so its sandbox container's cgroup holds the
    build and the runner alone. The peak is the container's since it started. No record: the builder did not run.
    """
    if before is None or not usage_file.exists():
        return
    if (after := read_cgroup(cgroup)) is not None:
        write_usage(usage_file, build_cgroup.since(before, after))


def _serve(conn: socket.socket, *, fresh_proc: bool) -> None:
    job = json.loads(conn.makefile("rb").readline())
    usage_file = Path(json.loads(Path(job["spec"]).read_text())["usage_file"])
    cgroup = container_cgroup()
    before = None if cgroup is None else read_cgroup(cgroup)
    if fresh_proc:
        own_pid_namespace(Path(job["spec"]))
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
        exited = os.pidfd_open(helper.pid)
        with selectors.DefaultSelector() as selector:
            selector.register(master, selectors.EVENT_READ)
            selector.register(conn, selectors.EVENT_READ)
            selector.register(exited, selectors.EVENT_READ)
            open_log = True
            while open_log:
                for key, _ in selector.select():
                    if key.fileobj is conn:
                        # The waiter sends nothing after the job: readable means it went away.
                        helper.kill()
                        selector.unregister(conn)
                        continue
                    if key.fileobj == exited:
                        selector.unregister(exited)
                        code = helper.wait()
                        kill_leftovers()
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
        os.close(exited)
        code = helper.wait()
    # Before the exit record: the daemon reads the usage once the waiter exits.
    if cgroup is not None:
        measured_by_the_container(usage_file, before, cgroup)
    with contextlib.suppress(BrokenPipeError):
        _send(conn, EXIT, str(code).encode())


def run(rendezvous: Path) -> None:
    """The sandbox container's end: serve builds one at a time until the pod ends. A pod that registers as a
    builder runs the probe build before its own."""
    rendezvous.mkdir(parents=True, exist_ok=True)
    path = rendezvous / SOCKET_NAME
    with contextlib.suppress(FileNotFoundError):
        path.unlink()
    fresh_proc = fresh_proc_allowed(rendezvous)
    which = "its own" if fresh_proc else "the container's"
    sys.stdout.write(f"sibling: the helper's pid namespace is {which}\n")
    sys.stdout.flush()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(str(path))
        listener.listen(1)
        while True:
            conn, _ = listener.accept()
            with conn:
                _serve(conn, fresh_proc=fresh_proc)


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
