"""A derivation's builder that writes what its sandbox looks like to `$out`, as JSON. nixidae/nixidae#1560.

`kubernetes_sandbox_probe.py` builds it three ways -- Nix's own sandbox, pynixd's on the node, and a build
pod's -- and compares the three. The standard library only: the build sees its input closure and nothing else.

Each fact is one key. A system call answers `ok` or its errno's name; each is chosen so that a refusal is
harmless and a success changes nothing outside the build.
"""

from __future__ import annotations

import ctypes
import errno
import json
import os
import platform
import resource
import socket
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Final

LIBC: Final = ctypes.CDLL(None, use_errno=True)

# System call numbers, by machine. Only calls Python's `os` does not wrap.
CALLS: Final = {
    "x86_64": {
        "add_key": 248,
        "bpf": 321,
        "clone3": 435,
        "fanotify_init": 300,
        "get_mempolicy": 239,
        "io_uring_setup": 425,
        "kcmp": 312,
        "keyctl": 250,
        "landlock_create_ruleset": 444,
        "mount": 165,
        "open_by_handle_at": 304,
        "perf_event_open": 298,
        "personality": 135,
        "pidfd_open": 434,
        "ptrace": 101,
        "syslog": 103,
        "userfaultfd": 323,
    },
    "aarch64": {
        "add_key": 217,
        "bpf": 280,
        "clone3": 435,
        "fanotify_init": 262,
        "get_mempolicy": 236,
        "io_uring_setup": 425,
        "kcmp": 272,
        "keyctl": 219,
        "landlock_create_ruleset": 444,
        "mount": 40,
        "open_by_handle_at": 265,
        "perf_event_open": 241,
        "personality": 92,
        "pidfd_open": 434,
        "ptrace": 117,
        "syslog": 116,
        "userfaultfd": 282,
    },
}

KEY_SPEC_PROCESS_KEYRING: Final = -2
KEY_SPEC_SESSION_KEYRING: Final = -3
KEYCTL_GET_KEYRING_ID: Final = 0
BPF_PROG_LOAD: Final = 5
SYSLOG_ACTION_SIZE_BUFFER: Final = 10
PTRACE_TRACEME: Final = 0
PERSONALITY_QUERY: Final = 0xFFFFFFFF
LANDLOCK_CREATE_RULESET_VERSION: Final = 1
CLONE_NEWUSER: Final = 0x10000000
CLONE_NEWNS: Final = 0x00020000
STATUS_FIELDS: Final = (
    "CapInh",
    "CapPrm",
    "CapEff",
    "CapBnd",
    "CapAmb",
    "NoNewPrivs",
    "Seccomp",
    "Seccomp_filters",
)


def answer(result: int) -> str:
    """A raw system call's result: `ok`, or the errno's name."""
    if result >= 0:
        return "ok"
    return errno.errorcode.get(ctypes.get_errno(), str(ctypes.get_errno()))


def raw(number: int, *args: int | bytes | None) -> str:
    syscall = LIBC.syscall
    syscall.restype = ctypes.c_long
    ctypes.set_errno(0)
    return answer(syscall(ctypes.c_long(number), *(ctypes.c_long(a) if isinstance(a, int) else a for a in args)))


def tried(action: Callable[[], object]) -> str:
    """An `os` call's outcome: `ok`, or the errno's name."""
    try:
        action()
    except OSError as e:
        return errno.errorcode.get(e.errno or 0, str(e.errno))
    return "ok"


def in_child(action: Callable[[], str]) -> str:
    """*action*'s answer from a forked child, for a call that changes the process that makes it."""
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read)
        try:
            said = action()
        except OSError as e:
            said = errno.errorcode.get(e.errno or 0, str(e.errno))
        os.write(write, said.encode())
        os._exit(0)
    os.close(write)
    said = os.read(read, 64).decode()
    os.close(read)
    os.waitpid(pid, 0)
    return said


def read(path: str) -> str:
    try:
        return Path(path).read_text().strip()
    except OSError as e:
        return f"<{errno.errorcode.get(e.errno or 0, e.errno)}>"


def listing(path: str) -> list[str] | str:
    try:
        return sorted(os.listdir(path))
    except OSError as e:
        return f"<{errno.errorcode.get(e.errno or 0, e.errno)}>"


def mounts() -> list[str]:
    """Each mount as `point fstype options`, without ids or sources, which differ for no reason."""
    seen = []
    for line in read("/proc/self/mountinfo").splitlines():
        fields = line.split()
        after = fields[fields.index("-") + 1 :]
        seen.append(f"{fields[4]} {after[0]} {fields[5]}")
    return sorted(seen)


def status() -> dict[str, str]:
    lines = dict(line.split(":\t", 1) for line in read("/proc/self/status").splitlines() if ":\t" in line)
    return {field: lines.get(field, "<absent>").strip() for field in STATUS_FIELDS}


def environment(out: str) -> dict[str, str]:
    # The output path and the nonce differ by design: each arm builds its own derivation.
    hidden = {"out": "<out>", "nonce": "<nonce>"}
    return {k: hidden.get(k, v.replace(out, "<out>")) for k, v in sorted(os.environ.items())}


def network() -> dict[str, object]:
    def connect() -> str:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            return tried(lambda: s.connect(("1.1.1.1", 80)))

    return {
        "interfaces": sorted(line.split(":")[0].strip() for line in read("/proc/net/dev").splitlines()[2:]),
        "connect 1.1.1.1:80": connect(),
        "resolve example.org": tried(lambda: socket.getaddrinfo("example.org", 80)),
    }


def syscalls(scratch: Path) -> dict[str, str]:
    calls = CALLS[platform.machine()]
    setuid = scratch / "setuid"
    setuid.write_text("")
    probes: dict[str, Callable[[], str]] = {
        "chmod setuid": lambda: tried(lambda: setuid.chmod(0o4755)),
        "chmod setgid": lambda: tried(lambda: setuid.chmod(0o2755)),
        "setxattr user": lambda: tried(lambda: os.setxattr(setuid, "user.probe", b"x")),
        "mknod char": lambda: tried(lambda: os.mknod(scratch / "null", 0o600 | 0o020000, os.makedev(1, 3))),
        "mkfifo": lambda: tried(lambda: os.mkfifo(scratch / "fifo")),
        "chroot": lambda: tried(lambda: os.chroot(str(scratch))),
        "memfd_create": lambda: tried(lambda: os.close(os.memfd_create("probe"))),
        "unshare user": lambda: in_child(lambda: tried(lambda: os.unshare(CLONE_NEWUSER))),
        "unshare mount": lambda: in_child(lambda: tried(lambda: os.unshare(CLONE_NEWNS))),
        "unshare user then mount tmpfs": lambda: in_child(lambda: _user_mount(calls["mount"], scratch)),
        "ptrace traceme": lambda: in_child(lambda: raw(calls["ptrace"], PTRACE_TRACEME, 0, 0, 0)),
        "personality query": lambda: raw(calls["personality"], PERSONALITY_QUERY),
        "keyctl session": lambda: raw(calls["keyctl"], KEYCTL_GET_KEYRING_ID, KEY_SPEC_SESSION_KEYRING, 0),
        "add_key user": lambda: raw(calls["add_key"], b"user", b"probe", b"x", 1, KEY_SPEC_PROCESS_KEYRING),
        "bpf prog_load null": lambda: raw(calls["bpf"], BPF_PROG_LOAD, None, 0),
        "perf_event_open null": lambda: raw(calls["perf_event_open"], None, 0, -1, -1, 0),
        "userfaultfd": lambda: raw(calls["userfaultfd"], 0),
        "io_uring_setup null": lambda: raw(calls["io_uring_setup"], 1, None),
        "clone3 null": lambda: raw(calls["clone3"], None, 0),
        "syslog size": lambda: raw(calls["syslog"], SYSLOG_ACTION_SIZE_BUFFER, None, 0),
        "pidfd_open self": lambda: raw(calls["pidfd_open"], os.getpid(), 0),
        "kcmp self": lambda: raw(calls["kcmp"], os.getpid(), os.getpid(), 0, 0, 0),
        "landlock version": lambda: raw(calls["landlock_create_ruleset"], None, 0, LANDLOCK_CREATE_RULESET_VERSION),
        "get_mempolicy": lambda: raw(calls["get_mempolicy"], None, None, 0, None, 0),
        "fanotify_init": lambda: raw(calls["fanotify_init"], 0, 0),
        "open_by_handle_at null": lambda: raw(calls["open_by_handle_at"], -100, None, 0),
        "socket netlink audit": lambda: tried(lambda: socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 9).close()),
        "socket packet": lambda: tried(lambda: socket.socket(socket.AF_PACKET, socket.SOCK_RAW, 0).close()),
        "socket inet6": lambda: tried(lambda: socket.socket(socket.AF_INET6, socket.SOCK_STREAM).close()),
    }
    return {name: probe() for name, probe in sorted(probes.items())}


def _user_mount(mount: int, scratch: Path) -> str:
    """A tmpfs mount in a user and mount namespace of the build's own: what a nested sandbox needs."""
    said = tried(lambda: os.unshare(CLONE_NEWUSER | CLONE_NEWNS))
    if said != "ok":
        return f"unshare {said}"
    target = scratch / "mnt"
    target.mkdir(exist_ok=True)
    return raw(mount, b"none", str(target).encode(), b"tmpfs", 0, None)


def limits() -> dict[str, str]:
    names = sorted(n for n in dir(resource) if n.startswith("RLIMIT_"))
    return {n: str(resource.getrlimit(getattr(resource, n))) for n in names}


def probe(out: str) -> dict[str, object]:
    scratch = Path(os.environ.get("TMPDIR", "/tmp")) / "probe"
    scratch.mkdir(exist_ok=True)
    umask = os.umask(0o022)
    os.umask(umask)
    uname = os.uname()
    return {
        "uname": {
            "sysname": uname.sysname,
            "release": uname.release,
            "version": uname.version,
            "machine": uname.machine,
        },
        "hostname": socket.gethostname(),
        "domainname": read("/proc/sys/kernel/domainname"),
        "uid": os.getuid(),
        "gid": os.getgid(),
        "groups": sorted(os.getgroups()),
        "uid_map": " ".join(read("/proc/self/uid_map").split()),
        "gid_map": " ".join(read("/proc/self/gid_map").split()),
        "setgroups": read("/proc/self/setgroups"),
        "status": status(),
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "pids visible": len([p for p in os.listdir("/proc") if p.isdigit()]),
        "pid 1": read("/proc/1/cmdline").replace("\0", " ").strip(),
        "cgroup": read("/proc/self/cgroup"),
        "cpu.max": read("/sys/fs/cgroup/cpu.max"),
        "memory.max": read("/sys/fs/cgroup/memory.max"),
        "cpu affinity": len(os.sched_getaffinity(0)),
        "cpu count": os.cpu_count(),
        "cwd": os.getcwd(),
        "umask": oct(umask),
        "environment": environment(out),
        "mounts": mounts(),
        "/": listing("/"),
        "/dev": listing("/dev"),
        "/etc": listing("/etc"),
        "/etc/passwd": read("/etc/passwd"),
        "/etc/group": read("/etc/group"),
        "/etc/hosts": read("/etc/hosts"),
        "/dev/shm writable": tried(lambda: (Path("/dev/shm") / "probe").write_text("x")),
        "/tmp writable": tried(lambda: (Path("/tmp") / "probe").write_text("x")),
        "network": network(),
        "limits": limits(),
        "syscalls": syscalls(scratch),
    }


def main() -> None:
    out = os.environ["out"]  # noqa: SIM112 -- Nix names the output variable in lower case
    Path(out).write_text(json.dumps(probe(out), indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
    sys.exit(0)
