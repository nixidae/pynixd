"""The sandbox helper: `python -m pynixd_store.builder.child SPEC STATUS_FD`.

pynixd never forks itself (`builder.md`), so this process does what Nix's
`ChrootLinuxDerivationBuilder::startChild` (`linux/build/linux-derivation-builder.cc:506`)
and `enterChroot` (`:684`) do, then `execve`s the builder. It reads its whole job
from *SPEC*, a JSON `ChildSpec`; pynixd has prepared the chroot root already.

A setup error is written to *STATUS_FD*, which is close-on-exec: a clean exec
closes it empty. The exit status of this process is the builder's: a code, or
the same signal.

With a third argument, *CONTROL_FD*, the helper serves `_add_path` on that socket while the builder runs.

Unprivileged, the helper maps its own uid and gid to the sandbox ids, as Nix does without build users. As
root with a build user, pynixd writes the maps (`ChildSpec.map_fd`), and the helper sets up the sandbox as host
root until it becomes the sandbox uid, as Nix's child does.

Without the chroot (`ChildSpec.chroot`), the helper does what `LinuxDerivationBuilder` does: no namespace, and
the builder runs as the build user itself. No PID namespace ends what the builder leaves, so the helper kills
the build user's processes after the builder too (`killSandbox`, `unix/build/derivation-builder.cc:156`).
"""

from __future__ import annotations

import contextlib
import ctypes
import errno
import fcntl
import functools
import json
import os
import platform
import resource
import signal
import socket
import stat
import struct
import sys
from dataclasses import replace
from pathlib import Path

import anyio
import seccomp
from anyio.abc import SocketStream
from anyio.streams.buffered import BufferedByteReceiveStream

from ..hintfmt import magenta, show_error
from . import buildenv, cgroup as build_cgroup, fetchurl, unpack_channel
from .spec import ChildSpec
from .usage import UsageSample, from_rusage, write_usage

MS_BIND = 4096
MS_REC = 16384
MS_PRIVATE = 1 << 18
MS_SHARED = 1 << 20
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


def _kill_user(uid: int) -> None:
    """`killUser` (`processes.cc:160`): `kill(-1)` as *uid* reaches every process of *uid*."""
    pid = os.fork()
    if pid == 0:
        try:
            os.setuid(uid)
            while True:
                try:
                    os.kill(-1, signal.SIGKILL)
                    break
                except OSError as e:
                    if e.errno in (errno.ESRCH, errno.EPERM):
                        break
                    if e.errno != errno.EINTR:
                        raise
        except OSError:
            os._exit(1)
        os._exit(0)
    _pid, status = os.waitpid(pid, 0)
    if status != 0:
        raise SetupError(f"cannot kill processes for uid '{uid}'")


def _await_maps(map_fd: int) -> None:
    """Tell pynixd the namespace exists, and wait until it has mapped the build user into it."""
    os.write(map_fd, b"u")
    if os.read(map_fd, 1) != b"g":
        raise SetupError("pynixd did not map the build user")
    os.close(map_fd)


def _loopback_up() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        fcntl.ioctl(sock.fileno(), SIOCSIFFLAGS, struct.pack("16sh22x", b"lo", IFF_UP | IFF_LOOPBACK | IFF_RUNNING))


def _open_dirs(root_fd: int, path: str) -> tuple[int | None, bool]:
    """`createDirsAndOpen` (`linux-derivation-builder.cc:301`): *path*'s directories under the root, made as needed.

    No component is followed as a symlink, and a `..` may not leave the root. Returns the last directory's fd
    (`None` for the root itself) and whether *path* ends at the root.
    """
    fd: int | None = None
    depth = 0
    for part in path.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if depth == 0:
                raise SetupError(f'sandbox path "{path}" escapes the chroot')
            depth -= 1
        else:
            depth += 1
        parent = root_fd if fd is None else fd
        flags = os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            try:
                nxt = os.open(part, flags, dir_fd=parent)
            except FileNotFoundError:
                os.mkdir(part, 0o777, dir_fd=parent)
                nxt = os.open(part, flags, dir_fd=parent)
        except OSError as e:
            raise SetupError(f'creating directory component "{part}" in the sandbox path "{path}": {e.strerror}') from e
        if fd is not None:
            os.close(fd)
        fd = nxt
    return fd, depth == 0


def _split(target: str) -> tuple[str, str]:
    """`std::filesystem::path`'s `parent_path` and `filename`.

    A trailing `/` leaves an empty file name, and the parent loses every separator after its last name.
    """
    parent, _sep, name = target.rpartition("/")
    return parent.rstrip("/"), name


def _bind(source: str, root: Path, target: str, optional: bool) -> None:
    """`doBind` (`linux-derivation-builder.cc:246`): bind *source* at *target*, a path relative to *root*.

    A directory is bind-mounted, a file is bind-mounted over a new one, and a symlink is copied.
    """
    if not target or target.startswith("/"):
        raise SetupError(f'invalid path to bind mount in the chroot: "{target}"')
    parent, name = _split(target)
    if name in (".", ".."):
        raise SetupError(f'sandbox path to bind mount in the chroot has an invalid filename: "{target}"')
    # `PathFmt(chrootRootDirPath / target)`: a trailing `/` of *target* stays.
    shown = f"{root}/{target}"
    try:
        source_fd = os.open(source, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC)
    except (FileNotFoundError, NotADirectoryError) as e:
        if optional:
            return
        raise SetupError(f'getting attributes of path "{source}": {e.strerror}') from e
    root_fd = os.open(root, os.O_DIRECTORY | os.O_CLOEXEC)
    opened: list[int] = [source_fd, root_fd]
    try:
        st = os.fstat(source_fd)
        if stat.S_ISDIR(st.st_mode):
            dir_fd, at_root = _open_dirs(root_fd, target)
            if dir_fd is not None:
                opened.append(dir_fd)
            if at_root or dir_fd is None:
                raise SetupError(f'sandbox path "{shown}" escapes the chroot')
            _mount(f"/proc/self/fd/{source_fd}", f"/proc/self/fd/{dir_fd}", None, MS_BIND | MS_REC)
            return
        parent_fd, _at_root = _open_dirs(root_fd, parent)
        if parent_fd is not None:
            opened.append(parent_fd)
        in_dir = root_fd if parent_fd is None else parent_fd
        if stat.S_ISLNK(st.st_mode):
            try:
                os.symlink(os.readlink(source), name, dir_fd=in_dir)
                os.utime(name, ns=(st.st_atime_ns, st.st_mtime_ns), dir_fd=in_dir, follow_symlinks=False)
            except OSError as e:
                raise SetupError(f'creating symlink "{shown}": {e.strerror}') from e
            return
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            dest_fd = os.open(name, flags, 0o666, dir_fd=in_dir)
        except OSError as e:
            raise SetupError(f'creating regular file "{shown}": {e.strerror}') from e
        opened.append(dest_fd)
        _mount(f"/proc/self/fd/{source_fd}", f"/proc/self/fd/{dest_fd}", None, MS_BIND | MS_REC)
    finally:
        for fd in opened:
            os.close(fd)


def _enter_chroot(spec: ChildSpec) -> None:
    if spec["private_network"]:
        _loopback_up()
    socket.sethostname("localhost")
    _check(_libc.setdomainname(b"(none)", 6), "setting the domain name")

    root = Path(spec["root"])
    _mount(None, "/", None, MS_PRIVATE | MS_REC)
    _mount(str(root), str(root), None, MS_BIND)
    # `enterChroot` (`linux-derivation-builder.cc:742`): a shared store, so a bind that `_add_path` makes in this
    # namespace after the `unshare` below still reaches the builder. A shared root makes `pivot_root` fail.
    store = str(root / spec["store_dir"].lstrip("/"))
    _mount(store, store, None, MS_BIND)
    _mount(None, store, None, MS_SHARED)
    for in_sandbox, source, optional in spec["binds"]:
        _bind(source, root, in_sandbox.lstrip("/"), optional)

    (root / "proc").mkdir(exist_ok=True)
    _mount("none", str(root / "proc"), "proc", 0)
    if spec["id_range"]:
        (root / "sys").mkdir(exist_ok=True)
        _mount("none", str(root / "sys"), "sysfs", 0)
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
            _bind("/dev/pts", root, "dev/pts", False)
            _bind("/dev/ptmx", root, "dev/ptmx", False)

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


def _set_user(groups: list[int] | None, gid: int, uid: int) -> None:
    """`setUser` within `preserveDeathSignal`: a change of credentials clears the parent death signal.

    *groups* `None` keeps them: in a user namespace with `setgroups` denied, the call fails.
    """
    if groups is not None:
        os.setgroups(groups)
    os.setgid(gid)
    os.setuid(uid)
    _prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
    if os.getppid() == 1:
        raise SetupError("the sandbox helper died")


def _close_extra_fds(keep: int) -> None:
    """`closeExtraFDs` (`unix/build/derivation-builder.cc:975`), but *keep*: the close-on-exec status pipe."""
    limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    os.closerange(3, keep)
    os.closerange(keep + 1, limit if limit != resource.RLIM_INFINITY else 1 << 20)


def _run_child(spec: ChildSpec, status_fd: int) -> None:
    """`runChild` (`unix/build/derivation-builder.cc:933`) after `commonChildInit` (`child.cc:10`)."""
    os.setsid()
    os.dup2(2, 1)
    null = os.open("/dev/null", os.O_RDWR | os.O_CLOEXEC)
    os.dup2(null, 0)
    os.close(null)
    user = spec["build_user"]
    if (external := spec["external"]) is not None:
        _run_external(spec, external, user)
    if spec["fetchurl"] is not None:
        fetchurl.preload_nss()
    if spec["chroot"]:
        _enter_chroot(spec)
        if user is not None:
            # `setUser` (`linux-derivation-builder.cc:930`): host root until here, so the binds read what root may.
            _set_user(None, spec["sandbox_gid"], spec["sandbox_uid"])
    elif user is not None:
        # `DerivationBuilderImpl::setUser` (`derivation-builder.cc:1037`).
        _set_user(spec["build_groups"], user[1], user[0])
    if spec["no_new_privileges"]:
        _prctl(PR_SET_NO_NEW_PRIVS, 1)
    if spec["filter_syscalls"]:
        _filter_syscalls(spec["no_new_privileges"])
    _scope_abstract_sockets()
    os.chdir(spec["work_dir"])
    _close_extra_fds(status_fd)
    resource.setrlimit(resource.RLIMIT_CORE, (0, resource.RLIM_INFINITY))
    os.umask(0o022)
    if spec["fetchurl"] is not None:
        fetchurl.run(spec["fetchurl"])
    if spec["buildenv"] is not None:
        buildenv.run(spec["buildenv"])
    if spec["unpack_channel"] is not None:
        unpack_channel.run(spec["unpack_channel"])
    builder = spec["builder"]
    if builder.startswith("builtin:"):
        # `runChild` (`derivation-builder.cc:1007`): a builtin with no entry in `builtinBuilders`.
        message = show_error(f"unsupported builtin builder '{magenta(builder.removeprefix('builtin:'))}'")
        os.write(2, f"{message}\n".encode(errors="surrogateescape"))
        os._exit(1)
    argv = [os.path.basename(builder), *spec["args"]]
    try:
        os.execve(builder, argv, spec["env"])
    except OSError as e:
        # `runChild` (`derivation-builder.cc:1377`): set-up is over, so the error goes to the log, not STATUS_FD.
        os.write(2, f"error: executing '{builder}': {e.strerror}\n".encode(errors="surrogateescape"))
        os._exit(1)


def _run_external(spec: ChildSpec, argv: list[str], user: tuple[int, int] | None) -> None:
    """`ExternalDerivationBuilder::startChild` (`external-derivation-builder.cc:78`): `chdir`, `setUser`, `execv`.

    No seccomp, no `no_new_privs` and no socket scoping, as in Nix: the program may start a VM. The status pipe
    is close-on-exec, so a failed `execv` is a set-up error and a started program reports on the terminal.
    """
    os.chdir(spec["work_dir"])
    if user is not None:
        _set_user(spec["build_groups"], user[1], user[0])
    try:
        os.execv(argv[0], argv)
    except OSError as e:
        raise SetupError(f'executing "{argv[0]}": {e.strerror}') from e


def _report(status_fd: int, message: str) -> None:
    os.write(status_fd, message.encode(errors="surrogateescape"))


def _add_path(spec: ChildSpec, source: str, target: str) -> str | None:
    """`addDependency` (`linux-derivation-builder.cc:959`): bind *source* at *target* in the running sandbox.

    The helper's own process is still in the namespace from before `pivot_root`, so it needs no `setns`.
    """
    store = spec["store_dir"]
    if os.path.dirname(target) != store:
        return f"'{target}' is not a path in the store '{store}'"
    root = Path(spec["root"])
    # `addDependencyPrep` (`chroot-derivation-builder.cc:117`).
    if os.path.lexists(root / target.lstrip("/")):
        return f"store path '{target}' already exists in the sandbox"
    try:
        _bind(source, root, target.lstrip("/"), False)
    except (OSError, SetupError) as e:
        return str(e)
    return None


async def _answer_adds(spec: ChildSpec, control: socket.socket) -> None:
    """Answer each add request on *control*: a JSON line in, a JSON line out."""
    async with await SocketStream.from_socket(control) as stream:
        lines = BufferedByteReceiveStream(stream)
        while True:
            try:
                line = await lines.receive_until(b"\n", 1 << 20)
            except (anyio.EndOfStream, anyio.IncompleteRead):
                return
            request = json.loads(line)
            # Not `to_thread`: after `unshare(CLONE_NEWPID)` the kernel refuses this process a thread
            # ("can't start new thread"). The bind is a few syscalls, and the loop serves nothing else.
            error = _add_path(spec, request["source"], request["target"])
            await stream.send(json.dumps({"error": error}).encode() + b"\n")


async def _wait_serving(spec: ChildSpec, pid: int, control: socket.socket | None, samples: list[UsageSample]) -> None:
    """Return when *pid* exits, and serve `_answer_adds` and sample the cgroup until then."""
    pidfd = os.pidfd_open(pid)
    try:
        async with anyio.create_task_group() as tg:
            if control is not None:
                tg.start_soon(_answer_adds, spec, control)
            if spec["cgroup_report"] and spec["cgroup"] is not None and spec["sample_seconds"] > 0:
                tg.start_soon(
                    functools.partial(
                        build_cgroup.sample_every,
                        Path(spec["cgroup"]),
                        spec["sample_seconds"],
                        samples,
                        in_thread=False,
                    )
                )
            await anyio.wait_readable(pidfd)
            tg.cancel_scope.cancel()
    finally:
        os.close(pidfd)


def _drop_terminal() -> None:
    """Close every copy of the build's terminal here, so the log ends when the builder closes it (`timeout.sh:18`).

    Nix's parent holds no copy. Measured in the test guest: the helper held the terminal on fds 68 and 69 as well as
    1 and 2, and with only 1 and 2 redirected the log never ended.
    """
    terminal = os.fstat(1)
    for name in os.listdir("/proc/self/fd"):
        fd = int(name)
        try:
            same = os.path.samestat(os.fstat(fd), terminal)
        except OSError:
            continue
        if same and fd > 2:
            os.close(fd)
    null = os.open(os.devnull, os.O_RDWR)
    for fd in (1, 2):
        os.dup2(null, fd)
    os.close(null)


def _kill_on_term(pid: int) -> None:
    """pynixd sends SIGTERM when the log ends (`unprepareBuild`, `derivation-builder.cc:187`).

    A builder that has exited stays a zombie until `wait4`, so the kill leaves its status as it was.
    """

    def kill(_sig: int, _frame: object) -> None:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)

    signal.signal(signal.SIGTERM, kill)


def _exit_like(status: int) -> None:
    if os.WIFSIGNALED(status):
        sig = os.WTERMSIG(status)
        # SIGKILL and SIGSTOP take no handler: `signal.signal` refuses them, and the helper then exits 1.
        if sig not in (signal.SIGKILL, signal.SIGSTOP):
            signal.signal(sig, signal.SIG_DFL)
        os.kill(os.getpid(), sig)
    os._exit(os.waitstatus_to_exitcode(status) if os.WIFEXITED(status) else 1)


def main() -> None:
    spec_path, status_arg, *control_arg = sys.argv[1:4]
    status_fd = int(status_arg)
    os.set_inheritable(status_fd, False)
    control = socket.socket(fileno=int(control_arg[0])) if control_arg else None
    if control is not None:
        control.set_inheritable(False)
    try:
        _prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
        spec: ChildSpec = json.loads(Path(spec_path).read_text())
        # Before the fork, so that the builder starts in it. The interpreter's own pages stay charged to pynixd.
        if spec["cgroup"] is not None:
            Path(spec["cgroup"], "cgroup.procs").write_text(str(os.getpid()))
        uid, gid = os.getuid(), os.getgid()
        if spec["build_user"] is not None:
            _kill_user(spec["build_user"][0])
        if spec["chroot"]:
            # `linux-derivation-builder.cc:566`: before the user namespace, which cannot drop them. EPERM is an
            # unprivileged helper's answer, or that of a namespace whose `setgroups` is denied.
            try:
                os.setgroups([])
            except PermissionError:
                if spec["require_drop_groups"]:
                    raise SetupError(
                        "setgroups failed. Set the require-drop-supplementary-groups option to false to skip this step."
                    ) from None
            except OSError as e:
                raise SetupError(f"setgroups failed: {e.strerror}") from e
            flags = os.CLONE_NEWUSER | os.CLONE_NEWPID | os.CLONE_NEWNS | os.CLONE_NEWIPC | os.CLONE_NEWUTS
            if spec["private_network"]:
                flags |= os.CLONE_NEWNET
            if spec["id_range"] and spec["cgroup"] is not None:
                flags |= os.CLONE_NEWCGROUP
            os.unshare(flags)
            if spec["map_fd"] is not None:
                _await_maps(spec["map_fd"])
            else:
                _map_ids(spec, uid, gid)
    except (OSError, SetupError) as e:
        _report(status_fd, f"setting up the sandbox: {e}")
        os._exit(1)

    pid = os.fork()
    if pid == 0:
        try:
            _prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
            _run_child(spec, status_fd)
            raise SetupError(f"executing '{spec['builder']}' returned")
        except (OSError, SetupError) as e:
            _report(status_fd, str(e))
        os._exit(1)
    _drop_terminal()
    _kill_on_term(pid)
    # Only now an event loop: the `unshare` above refuses a process with a second thread, and a fork copies a loop.
    samples: list[UsageSample] = []
    anyio.run(_wait_serving, spec, pid, control, samples)
    # The sandbox's pid 1 reaps its orphans, so its usage holds theirs too.
    _pid, status, rusage = os.wait4(pid, 0)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if not spec["chroot"] and spec["build_user"] is not None:
        try:
            _kill_user(spec["build_user"][0])
        except SetupError as e:
            _report(status_fd, str(e))
    os.close(status_fd)
    if spec["cgroup_report"] and spec["cgroup"] is not None:
        usage = replace(build_cgroup.read(Path(spec["cgroup"])), samples=tuple(samples))
        write_usage(Path(spec["usage_file"]), usage)
    elif spec["cgroup"] is None:
        write_usage(Path(spec["usage_file"]), from_rusage(rusage))
    _exit_like(status)


if __name__ == "__main__":
    main()
