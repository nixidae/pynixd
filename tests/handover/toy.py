"""Toys for docs/design/upgrade-handover.md. They run inside the guest.

Standard library only, so a guest unit runs it with a bare Python. Each mode
proves one thing that systemd or nix-daemon does; none is pynixd.

- `direct`: a client of nix-daemon builds, then closes, keeps or hands on
  its connection.
- `fdstore`: a `Type=notify` service parks its nix-daemon connection in
  systemd's file descriptor store, and the next instance reads the result.
- `overlap-daemon`, `overlap-reload`, `overlap-client`: a reload starts a
  new main process beside the old one, and the old one drains.
"""

from __future__ import annotations

import argparse
import array
import contextlib
import os
import socket
import struct
import sys
import threading
import time
from pathlib import Path

UPSTREAM = "/nix/var/nix/daemon-socket/upstream"
MAGIC_1 = 0x6E697863
MAGIC_2 = 0x6478696F
VERSION = (1 << 8) | 37
"""1.37: the last version before the feature exchange, so the toy needs none."""

STDERR_NEXT = 0x6F6C6D67
STDERR_READ = 0x64617461
STDERR_WRITE = 0x64617416
STDERR_LAST = 0x616C7473
STDERR_ERROR = 0x63787470
STDERR_START_ACTIVITY = 0x53545254
STDERR_STOP_ACTIVITY = 0x53544F50
STDERR_RESULT = 0x52534C54
OP_BUILD_PATHS = 9
ACT_BUILD = 105
RES_BUILD_LOG_LINE = 101

TAG = os.environ.get("TOY_TAG", "?")
SELF = os.environ.get("TOY_SELF", sys.argv[0])


def say(*parts: object) -> None:
    print(f"[toy {TAG} pid={os.getpid()} t={time.time():.3f}]", *parts, flush=True)


class Wire:
    """The worker protocol's framing: little-endian u64s and padded strings."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock

    def exact(self, n: int) -> bytes:
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise EOFError("nix-daemon closed the connection")
            data += chunk
        return data

    def u64(self) -> int:
        return int(struct.unpack("<Q", self.exact(8))[0])

    def string(self) -> str:
        size = self.u64()
        data = self.exact(size + (-size % 8))
        return data[:size].decode(errors="replace")

    def send(self, *items: int | str) -> None:
        out = b""
        for item in items:
            if isinstance(item, int):
                out += struct.pack("<Q", item)
            else:
                raw = item.encode()
                out += struct.pack("<Q", len(raw)) + raw + b"\0" * (-len(raw) % 8)
        self.sock.sendall(out)

    def fields(self) -> list[object]:
        values: list[object] = []
        for _ in range(self.u64()):
            values.append(self.u64() if self.u64() == 0 else self.string())
        return values

    def stderr(self, *, until_build: bool = False) -> tuple[str, object]:
        """Read log messages to the end of an operation, or only until a build starts."""
        while True:
            msg = self.u64()
            if msg == STDERR_LAST:
                return "last", None
            if msg == STDERR_NEXT:
                say("next:", self.string().rstrip())
            elif msg == STDERR_ERROR:
                self.string()
                self.u64()
                self.string()
                text = self.string()
                self.u64()
                for _ in range(self.u64()):
                    self.u64()
                    self.string()
                return "error", text
            elif msg == STDERR_START_ACTIVITY:
                act, _lvl, kind, text = self.u64(), self.u64(), self.u64(), self.string()
                self.fields()
                self.u64()
                say("activity", act, kind, text)
                if until_build and kind == ACT_BUILD:
                    return "building", act
            elif msg == STDERR_STOP_ACTIVITY:
                self.u64()
            elif msg == STDERR_RESULT:
                self.u64()
                kind = self.u64()
                values = self.fields()
                if kind == RES_BUILD_LOG_LINE:
                    say("log:", values[0] if values else "")
            elif msg in (STDERR_READ, STDERR_WRITE):
                raise RuntimeError(f"unexpected stderr message {msg:#x}")
            else:
                raise RuntimeError(f"unknown stderr message {msg:#x}")


def connect_upstream() -> Wire:
    """Connect to nix-daemon and finish the handshake."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(UPSTREAM)
    wire = Wire(sock)
    wire.send(MAGIC_1, VERSION)
    if wire.u64() != MAGIC_2:
        raise RuntimeError("not a nix-daemon")
    daemon = wire.u64()
    wire.send(0, 0)
    if daemon & 0xFF >= 33:
        wire.string()
    if daemon & 0xFF >= 35:
        wire.u64()
    wire.stderr()
    return wire


def start_build(wire: Wire, drv: str) -> None:
    """Send `BuildPaths` for `drv` and read until its build starts."""
    wire.send(OP_BUILD_PATHS, 1, f"{drv}!out", 0)
    state, _ = wire.stderr(until_build=True)
    if state != "building":
        raise RuntimeError(f"build of {drv} ended before it started: {state}")
    say("build started", drv)


def finish_build(wire: Wire) -> str:
    """Read the rest of a `BuildPaths` answer: `ok` or the error text."""
    state, value = wire.stderr()
    if state == "error":
        return f"error: {value}"
    return f"ok {wire.u64()}"


def record(path: str, text: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(f"{time.time():.3f} {TAG} {os.getpid()} {text}\n")
    say("recorded", path, text)


def notify(state: str, fds: list[int] | None = None) -> None:
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        say("no NOTIFY_SOCKET for", state)
        return
    target = "\0" + address[1:] if address.startswith("@") else address
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
        ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", fds))] if fds else []
        sock.sendmsg([state.encode()], ancillary, 0, target)
    say("notified", state.replace("\n", " "), "fds", fds or [])


def inherited() -> dict[str, int]:
    """`sd_listen_fds_with_names`: each passed descriptor by name."""
    if os.environ.get("LISTEN_PID") != str(os.getpid()):
        return {}
    names = os.environ.get("LISTEN_FDNAMES", "").split(":")
    count = int(os.environ.get("LISTEN_FDS", "0"))
    return {names[i] if i < len(names) else f"fd{i}": 3 + i for i in range(count)}


def mode_direct(args: argparse.Namespace) -> None:
    wire = connect_upstream()
    start_build(wire, args.drv)
    if args.then == "close":
        say("closing the connection mid-build")
        wire.sock.close()
        return
    if args.then == "fork-hold":
        if os.fork() != 0:
            say("parent exits; the child keeps the connection")
            os._exit(0)
        say(f"child holds the connection unread for {args.hold}s")
        time.sleep(args.hold)
    record(args.out, finish_build(wire))


def mode_fdstore(args: argparse.Namespace) -> None:
    fds = inherited()
    say("inherited", fds)
    if "upstream" in fds:
        wire = Wire(socket.socket(fileno=fds["upstream"]))
        notify("READY=1")
        say("adopted the parked connection")
        result = finish_build(wire)
        record(f"/var/lib/toy/{args.name}.result", f"adopted {result}")
        notify("FDSTOREREMOVE=1\nFDNAME=upstream")
    else:
        wire = connect_upstream()
        start_build(wire, Path(f"/run/toy/{args.name}.drv").read_text().strip())
        notify("FDSTORE=1\nFDNAME=upstream", [wire.sock.fileno()])
        notify("READY=1")
        record(f"/var/lib/toy/{args.name}.result", "parked")
    while True:
        time.sleep(3600)


def ctl_path(pid: int) -> str:
    return f"/run/toy/overlap-ctl.{pid}"


def send_fd(sock: socket.socket, text: str, fd: int) -> None:
    sock.sendmsg([text.encode()], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [fd]))])


def recv_fd(sock: socket.socket) -> tuple[str, int]:
    fds: array.array[int] = array.array("i")
    msg, ancdata, _flags, _addr = sock.recvmsg(4096, socket.CMSG_LEN(fds.itemsize))
    for level, kind, data in ancdata:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            fds.frombytes(data[: len(data) - (len(data) % fds.itemsize)])
    return msg.decode(), fds[0]


def mode_overlap_daemon(args: argparse.Namespace) -> None:
    fds = inherited()
    if "listener" in fds:
        listener = socket.socket(fileno=fds["listener"])
        say("took the listener over", listener.getsockname())
    else:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(args.sock)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(args.sock)
        os.chmod(args.sock, 0o666)
        listener.listen(64)
    in_flight = [0]
    draining = threading.Event()
    lock = threading.Lock()

    def serve(conn: socket.socket) -> None:
        with conn:
            drv = conn.makefile().readline().strip()
            say("client asks for", drv)
            try:
                wire = connect_upstream()
                start_build(wire, drv)
                result = finish_build(wire)
            except Exception as error:  # noqa: BLE001 - a toy reports, never hides
                result = f"error: {error}"
            conn.sendall(f"{TAG} {os.getpid()} {result}\n".encode())
            say("answered", drv, result)
        with lock:
            in_flight[0] -= 1
        if draining.is_set() and in_flight[0] == 0:
            say("drained; exiting")
            os._exit(0)

    def accept() -> None:
        while not draining.is_set():
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            with lock:
                in_flight[0] += 1
            threading.Thread(target=serve, args=(conn,), daemon=True).start()

    def control() -> None:
        path = ctl_path(os.getpid())
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        ctl = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        ctl.bind(path)
        ctl.listen(4)
        while True:
            conn, _ = ctl.accept()
            command = conn.recv(4096).decode().split()
            say("control:", command)
            if command[0] == "listener":
                send_fd(conn, "listener", listener.fileno())
            elif command[0] == "spawn":
                conn.sendall(f"{spawn_successor(listener.fileno(), command[1])}".encode())
            elif command[0] == "drain":
                draining.set()
                listener.close()
                with lock:
                    left = in_flight[0]
                conn.sendall(f"draining {left}".encode())
                say("draining", left, "in flight")
                if left == 0:
                    say("nothing in flight; exiting")
                    os._exit(0)
            conn.close()

    threading.Thread(target=control, daemon=True).start()
    state = "READY=1"
    if "listener" in fds:
        state = f"MAINPID={os.getpid()}\nREADY=1"
    notify(state)
    record(f"/run/toy/overlap-ready.{os.getpid()}", "ready")
    accept()
    while True:
        time.sleep(3600)


def spawn_successor(listener: int, exe: str) -> int:
    """Start `exe overlap-daemon` with the listener as its fd 3."""
    pid = os.fork()
    if pid == 0:
        # The old main's listener often already is fd 3: `dup2(3, 3)` then
        # keeps CLOEXEC, and the successor got EBADF. Measured.
        if listener != 3:
            os.dup2(listener, 3)
        os.set_inheritable(3, True)
        os.environ["LISTEN_FDS"] = "1"
        os.environ["LISTEN_FDNAMES"] = "listener"
        os.environ["LISTEN_PID"] = str(os.getpid())
        os.execv(exe, [exe, "overlap-daemon"])
    say("spawned the successor", exe, pid)
    deadline = time.monotonic() + 30
    while not Path(f"/run/toy/overlap-ready.{pid}").exists():
        if time.monotonic() > deadline:
            raise RuntimeError("the successor never became ready")
        time.sleep(0.1)
    return pid


def ctl(pid: int, command: str) -> socket.socket:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(ctl_path(pid))
    sock.sendall(command.encode())
    return sock


def mode_overlap_reload(args: argparse.Namespace) -> None:
    """`ExecReload=`: start the new main process, then drain the old one."""
    old = args.mainpid
    say("reload: old main", old, "spawn by", args.spawn_by, "successor", SELF)
    if args.spawn_by == "control":
        with ctl(old, "listener") as sock:
            _, fd = recv_fd(sock)
        new = spawn_successor(fd, SELF)
    else:
        with ctl(old, f"spawn {SELF}") as sock:
            new = int(sock.recv(64).decode())
    with ctl(old, "drain") as sock:
        say("old answers", sock.recv(64).decode())
    say("reload done; new main", new)


def mode_overlap_client(args: argparse.Namespace) -> None:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(args.sock)
    sock.sendall(f"{args.drv}\n".encode())
    answer = sock.makefile().readline().strip()
    record(args.out, answer or "error: connection closed with no answer")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    direct = sub.add_parser("direct")
    direct.add_argument("--drv", required=True)
    direct.add_argument("--then", choices=["wait", "close", "fork-hold"], default="wait")
    direct.add_argument("--hold", type=float, default=0)
    direct.add_argument("--out", required=True)
    fdstore = sub.add_parser("fdstore")
    fdstore.add_argument("--name", required=True)
    daemon = sub.add_parser("overlap-daemon")
    daemon.add_argument("--sock", default="/run/toy/overlap.sock")
    reload = sub.add_parser("overlap-reload")
    reload.add_argument("--mainpid", type=int, required=True)
    reload.add_argument("--spawn-by", choices=["control", "old"], default="control")
    client = sub.add_parser("overlap-client")
    client.add_argument("--sock", default="/run/toy/overlap.sock")
    client.add_argument("--drv", required=True)
    client.add_argument("--out", required=True)
    args = parser.parse_args()
    {
        "direct": mode_direct,
        "fdstore": mode_fdstore,
        "overlap-daemon": mode_overlap_daemon,
        "overlap-reload": mode_overlap_reload,
        "overlap-client": mode_overlap_client,
    }[args.mode](args)


if __name__ == "__main__":
    main()
