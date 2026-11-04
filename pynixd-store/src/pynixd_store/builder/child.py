"""The sandbox helper: `python -m pynixd_store.builder.child SPEC STATUS_FD`.

pynixd never forks itself (`builder.md`), so this process does what Nix's
`ChrootLinuxDerivationBuilder::startChild` (`linux/build/linux-derivation-builder.cc:506`)
and `enterChroot` (`:684`) do, then `execve`s the builder. It reads its whole job
from *SPEC*, a JSON `ChildSpec`; pynixd has prepared the chroot root already.

A setup error is written to *STATUS_FD*, which is close-on-exec: a clean exec
closes it empty. The exit status of this process is the builder's: a code, or
the same signal.

Unprivileged only: the helper maps its own uid and gid to the sandbox ids, as
Nix does without build users.
"""

from __future__ import annotations

import ctypes
import errno
import fcntl
import json
import os
import platform
import resource
import signal
import socket
import stat
import struct
import sys
from pathlib import Path

import seccomp

from . import fetchurl
from .spec import ChildSpec

MS_BIND = 4096
MS_REC = 16384
MS_PRIVATE = 1 << 18
MNT_DETACH = 2
PR_SET_PDEATHSIG = 1
PR_SET_NO_NEW_PRIVS = 38
SIOCSIFFLAGS = 0x8914
IFF_UP, IFF_LOOPBACK, IFF_RUNNING = 0x1, 0x8, 0x40
SYS_PIVOT_ROOT = {"x86_64": 155, "aarch64": 41, "riscv64": 41}
#: The same number on every architecture: landlock came after the unified syscall table.
SYS_LANDLOCK_CREATE_RULESET = 444
SYS_LANDLOCK_RESTRICT_SELF = 446
LANDLOCK_CREATE_RULESET_VERSION = 1
LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET = 1
#: The calls that change a mode, and the index of their mode argument.
CHMOD_CALLS = (("chmod", 1), ("fchmod", 1), ("fchmodat", 2), ("fchmodat2", 2))
XATTR_CALLS = (
    "listxattr",
    "llistxattr",
    "flistxattr",
    "getxattr",
    "lgetxattr",
    "fgetxattr",
    "setxattr",
    "lsetxattr",
    "fsetxattr",
)

_libc = ctypes.CDLL(None, use_errno=True)


class SetupError(Exception):
    pass


def _check(result: int, what: str) -> None:
    if result == -1:
        errno = ctypes.get_errno()
        raise SetupError(f"{what}: {os.strerror(errno)}")


def _c(text: str | None) -> bytes | None:
    return None if text is None else os.fsencode(text)


def _mount(source: str | None, target: str, fstype: str | None, flags: int, data: str | None = None) -> None:
    _check(_libc.mount(_c(source), _c(target), _c(fstype), flags, _c(data)), f"mounting '{target}'")


def _prctl(option: int, value: int) -> None:
    _check(_libc.prctl(option, value, 0, 0, 0), f"prctl {option}")


def _map_ids(spec: ChildSpec, uid: int, gid: int) -> None:
    """The uid map handshake of `startChild`, done by the namespace's own creator."""
    Path("/proc/self/uid_map").write_text(f"{spec['sandbox_uid']} {uid} 1")
    Path("/proc/self/setgroups").write_text("deny")
    Path("/proc/self/gid_map").write_text(f"{spec['sandbox_gid']} {gid} 1")


def _loopback_up() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        fcntl.ioctl(sock.fileno(), SIOCSIFFLAGS, struct.pack("16sh22x", b"lo", IFF_UP | IFF_LOOPBACK | IFF_RUNNING))


def _bind(source: str, target: Path, optional: bool) -> None:
    """`doBind`: a directory is bind-mounted, a file is bind-mounted over a new one, a link is copied."""
    try:
        st = os.lstat(source)
    except FileNotFoundError:
        if optional:
            return
        raise SetupError(f"path '{source}' does not exist") from None
    target.parent.mkdir(parents=True, exist_ok=True)
    if stat.S_ISLNK(st.st_mode):
        target.symlink_to(os.readlink(source))
        return
    if stat.S_ISDIR(st.st_mode):
        target.mkdir(exist_ok=True)
    else:
        target.touch(exist_ok=True)
    _mount(source, str(target), None, MS_BIND | MS_REC)


def _enter_chroot(spec: ChildSpec) -> None:
    if spec["private_network"]:
        _loopback_up()
    socket.sethostname("localhost")
    _check(_libc.setdomainname(b"(none)", 6), "setting the domain name")

    root = Path(spec["root"])
    _mount(None, "/", None, MS_PRIVATE | MS_REC)
    _mount(str(root), str(root), None, MS_BIND)
    for in_sandbox, source, optional in spec["binds"]:
        _bind(source, root / in_sandbox.lstrip("/"), optional)

    (root / "proc").mkdir(exist_ok=True)
    _mount("none", str(root / "proc"), "proc", 0)
    if Path("/dev/shm").exists():
        (root / "dev/shm").mkdir(parents=True, exist_ok=True)
        _mount("none", str(root / "dev/shm"), "tmpfs", 0, f"size={spec['shm_size']}")
    pts = root / "dev/pts"
    if Path("/dev/pts/ptmx").exists() and not (root / "dev/ptmx").exists():
        pts.mkdir(parents=True, exist_ok=True)
        if _libc.mount(b"none", os.fsencode(pts), b"devpts", 0, b"newinstance,mode=0620") == 0:
            (root / "dev/ptmx").symlink_to("/dev/pts/ptmx")
            (pts / "ptmx").chmod(0o666)
        else:
            _bind("/dev/pts", pts, False)
            _bind("/dev/ptmx", root / "dev/ptmx", False)

    os.unshare(os.CLONE_NEWNS)
    os.chdir(root)
    os.mkdir("real-root", 0o500)
    number = SYS_PIVOT_ROOT.get(platform.machine())
    if number is None:
        raise SetupError(f"no pivot_root syscall number for '{platform.machine()}'")
    _check(_libc.syscall(number, b".", b"real-root"), "pivot_root")
    os.chroot(".")
    _check(_libc.umount2(b"real-root", MNT_DETACH), "unmounting the real root")
    os.rmdir("real-root")


def _filter_syscalls(no_new_privileges: bool) -> None:
    """`setupSeccomp` (`linux/build/linux-derivation-builder.cc:79`): no setuid or setgid file, no xattr.

    NAR cannot hold an extended attribute or an ACL, so a builder never gets to make one.
    """
    rules = seccomp.SyscallFilter(defaction=seccomp.ALLOW)
    machine = platform.machine()
    try:
        if machine == "x86_64":
            rules.add_arch(seccomp.Arch.X86)
            rules.add_arch(seccomp.Arch.X32)
        elif machine == "aarch64":
            try:
                rules.add_arch(seccomp.Arch.ARM)
            except RuntimeError:
                print(
                    "unable to add ARM seccomp architecture; this may result in spurious build failures"
                    " if running 32-bit ARM processes",
                    file=sys.stderr,
                )
        for perm in (stat.S_ISUID, stat.S_ISGID):
            for call, index in CHMOD_CALLS:
                rules.add_rule(seccomp.ERRNO(errno.EPERM), call, seccomp.Arg(index, seccomp.MASKED_EQ, perm, perm))
        for call in XATTR_CALLS:
            rules.add_rule(seccomp.ERRNO(errno.ENOTSUP), call)
        rules.set_attr(seccomp.Attr.CTL_NNP, 1 if no_new_privileges else 0)
        rules.load()
    except RuntimeError as e:
        raise SetupError(f"setting up the system call filter: {e}") from e


def _scope_abstract_sockets() -> None:
    """`setupLandlock` (`linux/build/linux-derivation-builder.cc:201`): no connect to an abstract socket made outside.

    Two fixed-output builds share the host's network namespace, so without this they can talk.
    """
    abi = _libc.syscall(SYS_LANDLOCK_CREATE_RULESET, None, 0, LANDLOCK_CREATE_RULESET_VERSION)
    if abi < 6:
        return
    # struct landlock_ruleset_attr: handled_access_fs, handled_access_net, scoped.
    attr = struct.pack("=QQQ", 0, 0, LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET)

    def refused(what: str) -> None:
        """`enterChroot` (`:412`) warns on EPERM: no CAP_SYS_ADMIN and `allow-new-privileges`."""
        err = ctypes.get_errno()
        if err != errno.EPERM:
            raise SetupError(f"{what}: {os.strerror(err)}")
        print(f"warning: setting up landlock: {what}: {os.strerror(err)}", file=sys.stderr)

    ruleset = _libc.syscall(SYS_LANDLOCK_CREATE_RULESET, attr, len(attr), 0)
    if ruleset == -1:
        refused("failed to create a landlock ruleset")
        return
    try:
        if _libc.syscall(SYS_LANDLOCK_RESTRICT_SELF, ruleset, 0) == -1:
            refused("failed to apply landlock")
    finally:
        os.close(ruleset)


def _run_child(spec: ChildSpec) -> None:
    """`runChild` (`unix/build/derivation-builder.cc:933`) after `commonChildInit` (`child.cc:10`)."""
    os.setsid()
    os.dup2(2, 1)
    null = os.open("/dev/null", os.O_RDWR | os.O_CLOEXEC)
    os.dup2(null, 0)
    os.close(null)
    if spec["fetchurl"] is not None:
        fetchurl.preload_nss()
    _enter_chroot(spec)
    if spec["no_new_privileges"]:
        _prctl(PR_SET_NO_NEW_PRIVS, 1)
    if spec["filter_syscalls"]:
        _filter_syscalls(spec["no_new_privileges"])
    _scope_abstract_sockets()
    os.chdir(spec["work_dir"])
    resource.setrlimit(resource.RLIMIT_CORE, (0, resource.RLIM_INFINITY))
    os.umask(0o022)
    if spec["fetchurl"] is not None:
        fetchurl.run(spec["fetchurl"])
    builder = spec["builder"]
    argv = [os.path.basename(builder), *spec["args"]]
    os.execve(builder, argv, spec["env"])


def _report(status_fd: int, message: str) -> None:
    os.write(status_fd, message.encode(errors="surrogateescape"))


def _exit_like(status: int) -> None:
    if os.WIFSIGNALED(status):
        sig = os.WTERMSIG(status)
        signal.signal(sig, signal.SIG_DFL)
        os.kill(os.getpid(), sig)
    os._exit(os.waitstatus_to_exitcode(status) if os.WIFEXITED(status) else 1)


def main() -> None:
    spec_path, status_arg = sys.argv[1:3]
    status_fd = int(status_arg)
    os.set_inheritable(status_fd, False)
    try:
        _prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
        spec: ChildSpec = json.loads(Path(spec_path).read_text())
        uid, gid = os.getuid(), os.getgid()
        flags = os.CLONE_NEWUSER | os.CLONE_NEWPID | os.CLONE_NEWNS | os.CLONE_NEWIPC | os.CLONE_NEWUTS
        if spec["private_network"]:
            flags |= os.CLONE_NEWNET
        os.unshare(flags)
        _map_ids(spec, uid, gid)
    except (OSError, SetupError) as e:
        _report(status_fd, f"setting up the sandbox: {e}")
        os._exit(1)

    pid = os.fork()
    if pid == 0:
        try:
            _prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
            _run_child(spec)
            raise SetupError(f"executing '{spec['builder']}' returned")
        except (OSError, SetupError) as e:
            _report(status_fd, str(e))
        os._exit(1)
    os.close(status_fd)
    _pid, status = os.waitpid(pid, 0)
    _exit_like(status)


if __name__ == "__main__":
    main()
