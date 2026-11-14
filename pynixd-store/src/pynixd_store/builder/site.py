"""Where a build's sandbox helper runs: on the host's kernel, or in a libkrun microVM.

`build()` in `local.py` prepares a build the same way for each site, and asks its `BuildSite` for each part that
differs: the socket a recursive builder reaches, the command that starts the helper, how the helper reports a
set-up error and the builder's exit, and how the build's use of the machine is measured. `choose_site` is the one
place that picks the site. A new site is one class here and one case in `choose_site`. nixidae/nixidae#1318.
"""

from __future__ import annotations

import contextlib
import functools
import os
import signal
import socket
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread
from anyio.abc import SocketStream

from nix_daemon_protocol.build_result import BuildResultStatus

from ..add import delete_path
from ..hintfmt import magenta
from . import cgroup as build_cgroup, users as build_users
from .outputs import BuildFailure
from .restriction import SOCKET_NAME, RestrictionContext
from .usage import USAGE_NAME, read_usage, with_host
from .vm import (
    BUILDER_SOCKET_NAME as VM_BUILDER_SOCKET_NAME,
    CONTROL_SOCKET_NAME as VM_CONTROL_SOCKET_NAME,
    GUEST_DNS as VM_GUEST_DNS,
    STATUS_NAME as VM_STATUS_NAME,
    VMM_USAGE_NAME,
    BuildVm,
    allow as vm_allow,
    ending as vm_ending,
    status as vm_status,
    unshareable as vm_unshareable,
    uplink as vm_uplink,
)

if TYPE_CHECKING:
    from nix_daemon_protocol.basic_derivation import BasicDerivation
    from nix_daemon_protocol.store_path import StorePath

    from .settings import BuildSettings
    from .spec import ChildSpec
    from .usage import BuildUsage, UsageSample

__all__ = [
    "BuildPaths",
    "BuildSite",
    "BuilderFailed",
    "Endpoint",
    "HostSite",
    "Launch",
    "VmSite",
    "builder_ended",
    "choose_site",
    "listen",
]


@dataclass
class BuildPaths:
    """The directories of one build: the top directory, the builder's directory in it, and the chroot."""

    top: Path
    tmp: Path
    chroot_parent: Path | None
    root: Path | None
    """The chroot's root; `None` for a build without the chroot."""

    def remove(self) -> None:
        for path in (self.chroot_parent, self.top):
            if path is not None:
                delete_path(path)

    def keep(self) -> None:
        """`cleanupBuild(false)` with `keep-failed` (`derivation-builder.cc:1697`): the chroot goes, the build
        directory stays for anyone to read."""
        if self.chroot_parent is not None:
            delete_path(self.chroot_parent)
        self.top.chmod(0o755)
        self.tmp.chmod(0o755)


@dataclass
class BuilderFailed:
    """The builder exited non-zero: `statusToString` without the leading word (`processes.cc:378`)."""

    reason: str


def builder_ended(code: int | None) -> BuilderFailed | None:
    if code is not None and code < 0:
        return BuilderFailed(f"failed due to signal {-code} ({signal.strsignal(-code)})")
    if code:
        return BuilderFailed(f"failed with exit code {code}")
    return None


@dataclass
class Endpoint:
    """The store's socket for a recursive builder, and what the build may add through it."""

    listener: socket.socket
    restriction: RestrictionContext
    control: socket.socket | None = None
    """The helper's end of the socket it binds added paths on; `_run` passes it to the helper."""


@dataclass
class Launch:
    """How to start the helper: its command, the descriptors it inherits, and its working directory."""

    argv: list[str]
    fds: list[int] = field(default_factory=list)
    cwd: Path | None = None
    status_read: int = -1
    """The parent's end of the pipe the helper writes a set-up error to; -1 when the site reports in a file."""
    status_write: int = -1
    """The helper's end. The parent closes it once the helper runs, or the read never sees its end."""


class BuildSite(ABC):
    """Where the sandbox helper runs."""

    waits_for_exit: bool
    """After the log ends, the parent gives the builder `EXIT_GRACE` to exit before the helper kills it."""
    samples_on_host: bool
    """The parent samples the build's cgroup while it runs; a site that measures for itself does not."""

    @abstractmethod
    async def endpoint(
        self,
        stack: contextlib.AsyncExitStack,
        paths: BuildPaths,
        inputs: set[StorePath],
        real_store_dir: Path,
        *,
        submitting: bool,
        repairing: bool,
        user: build_users.BuildUser | None,
    ) -> Endpoint:
        """`startDaemon` (`derivation-builder.cc:1171`): the socket is up before the builder starts."""

    def refusal(self) -> str | None:
        """Why this site cannot run the build, after the recursive socket is up; `None` when it can."""
        return None

    def host_files(self, root: Path, files: list[tuple[str, str, bool]]) -> list[tuple[str, str, bool]]:
        """The binds that give the sandbox each host file of *files*, a fixed-output build's network files."""
        del root
        return files

    async def start(self, stack: contextlib.AsyncExitStack, paths: BuildPaths) -> None:
        """Start what the build needs beside the helper, for as long as *stack* holds."""
        del stack, paths

    @abstractmethod
    def configure(self, spec: ChildSpec, sample_seconds: float) -> None:
        """Set the parts of the helper's job that this site decides."""

    @abstractmethod
    async def launch(self, paths: BuildPaths, spec_file: Path, cgroup: Path | None, *, recursive: bool) -> Launch:
        """The helper's command for the job in *spec_file*."""

    @abstractmethod
    def setup_error(self, paths: BuildPaths, launch: Launch) -> str:
        """The helper's set-up error, empty when it set up the build."""

    @abstractmethod
    def ended(
        self, paths: BuildPaths, drv_path: StorePath, returncode: int | None
    ) -> BuildFailure | BuilderFailed | None:
        """How the builder ended, from the helper's own exit status *returncode*."""

    @abstractmethod
    async def usage(
        self, paths: BuildPaths, cgroup: Path | None, samples: tuple[UsageSample, ...]
    ) -> BuildUsage | None:
        """What the build used, once it has ended."""


@dataclass
class HostSite(BuildSite):
    """The helper runs on the host's kernel, in namespaces with the chroot, or as a plain process without it."""

    chroot: bool
    waits_for_exit: bool = True
    samples_on_host: bool = True

    async def endpoint(
        self,
        stack: contextlib.AsyncExitStack,
        paths: BuildPaths,
        inputs: set[StorePath],
        real_store_dir: Path,
        *,
        submitting: bool,
        repairing: bool,
        user: build_users.BuildUser | None,
    ) -> Endpoint:
        if not self.chroot:
            # `addDependencyImpl` (`derivation-builder.cc:901`) binds nothing: the builder sees the store.
            restriction = RestrictionContext(
                inputs, None, real_store_dir, submitting=submitting, repairing=repairing, binds=False
            )
            listener = await anyio.to_thread.run_sync(listen, paths.tmp / SOCKET_NAME)
            build_users.chown_to(user, paths.tmp / SOCKET_NAME)
            return Endpoint(listener, restriction)
        ours, control = socket.socketpair()
        stack.callback(control.close)
        stream = await stack.enter_async_context(await SocketStream.from_socket(ours))
        restriction = RestrictionContext(inputs, stream, real_store_dir, submitting=submitting, repairing=repairing)
        listener = await anyio.to_thread.run_sync(listen, paths.tmp / SOCKET_NAME)
        build_users.chown_to(user, paths.tmp / SOCKET_NAME)
        return Endpoint(listener, restriction, control)

    def configure(self, spec: ChildSpec, sample_seconds: float) -> None:
        """The job keeps `build()`'s cgroup and samples nothing: the parent samples the host cgroup itself."""

    async def launch(self, paths: BuildPaths, spec_file: Path, cgroup: Path | None, *, recursive: bool) -> Launch:
        status_read, status_write = os.pipe()
        argv = [sys.executable, "-m", "pynixd_store.builder.child", str(spec_file), str(status_write)]
        return Launch(argv, [status_write], None, status_read, status_write)

    def setup_error(self, paths: BuildPaths, launch: Launch) -> str:
        with os.fdopen(launch.status_read, "rb") as status_file:
            return status_file.read().decode(errors="surrogateescape")

    def ended(self, paths: BuildPaths, drv_path: StorePath, returncode: int | None) -> BuilderFailed | None:
        return builder_ended(returncode)

    async def usage(
        self, paths: BuildPaths, cgroup: Path | None, samples: tuple[UsageSample, ...]
    ) -> BuildUsage | None:
        if cgroup is None:
            return read_usage(paths.top / USAGE_NAME)
        read = await anyio.to_thread.run_sync(build_cgroup.read, cgroup)
        return replace(read, samples=samples)


@dataclass
class VmSite(BuildSite):
    """The helper runs in a libkrun microVM, which sees only the build's store paths (nixidae/nixidae#1186).

    The guest reports through files in the top directory. The host cgroup holds the VMM, and the guest measures
    the build in a cgroup of its own.
    """

    vm: BuildVm
    real_store_dir: Path
    visible: frozenset[StorePath]
    sources: tuple[str, ...]
    """The `sandbox-paths` sources the helper binds."""
    network: bool = False
    """A fixed-output or impure build: the guest gets a NIC that passt serves."""
    net_fd: int | None = None
    """The VM's end of passt's socketpair, once `start` made it."""
    waits_for_exit: bool = False
    samples_on_host: bool = False

    async def endpoint(
        self,
        stack: contextlib.AsyncExitStack,
        paths: BuildPaths,
        inputs: set[StorePath],
        real_store_dir: Path,
        *,
        submitting: bool,
        repairing: bool,
        user: build_users.BuildUser | None,
    ) -> Endpoint:
        # The guest's init listens in the build directory, and the image holds no host socket.
        restriction = RestrictionContext(
            inputs,
            None,
            real_store_dir,
            submitting=submitting,
            repairing=repairing,
            reveal=functools.partial(vm_allow, paths.top, real_store_dir),
        )
        controls = await anyio.to_thread.run_sync(listen, paths.top / VM_CONTROL_SOCKET_NAME)
        listener = await anyio.to_thread.run_sync(listen, paths.top / VM_BUILDER_SOCKET_NAME)
        group = await stack.enter_async_context(anyio.create_task_group())
        stack.callback(group.cancel_scope.cancel)
        group.start_soon(_attach_control, controls, restriction)
        return Endpoint(listener, restriction)

    def host_files(self, root: Path, files: list[tuple[str, str, bool]]) -> list[tuple[str, str, bool]]:
        """Copies in the chroot: a file in `/` is no share of its own, and the store holds the CA file outside the
        build's closure. A fetch reads them as it starts, so a copy serves as a bind does. The guest's resolver is
        passt's, not the host's."""
        etc = root / "etc"
        etc.chmod(0o755)
        try:
            for target, source, optional in files:
                if optional and not os.path.exists(source):
                    continue
                copy = root / target.lstrip("/")
                copy.parent.mkdir(parents=True, exist_ok=True)
                if target == "/etc/resolv.conf":
                    copy.write_text(f"nameserver {VM_GUEST_DNS}\n")
                else:
                    copy.write_bytes(Path(source).read_bytes())
        finally:
            etc.chmod(0o555)
        return []

    async def start(self, stack: contextlib.AsyncExitStack, paths: BuildPaths) -> None:
        if self.network:
            if self.vm.passt is None:
                raise ValueError("a build VM without passt has no network")
            self.net_fd = await stack.enter_async_context(vm_uplink(self.vm.passt, paths.top))

    def refusal(self) -> str | None:
        if (file := vm_unshareable(self.sources, self.real_store_dir)) is not None:
            return f"sandbox-paths names the file '{magenta(file)}' in /, which the build VM cannot share"
        return None

    def configure(self, spec: ChildSpec, sample_seconds: float) -> None:
        spec["cgroup"] = str(build_cgroup.GUEST)
        spec["cgroup_report"] = True
        spec["sample_seconds"] = sample_seconds

    async def launch(self, paths: BuildPaths, spec_file: Path, cgroup: Path | None, *, recursive: bool) -> Launch:
        if paths.chroot_parent is None:
            raise ValueError("a build VM runs the sandbox only")
        await self.vm.make_disk(paths.top, paths.tmp)
        argv = self.vm.argv(
            top=paths.top,
            chroot_parent=paths.chroot_parent,
            real_store_dir=self.real_store_dir,
            visible=self.visible,
            sources=self.sources,
            spec_file=spec_file,
            build_dir=paths.tmp,
            recursive=recursive,
            net_fd=self.net_fd,
            cgroup=cgroup,
        )
        # libkrun connects to the host's sockets by their names in the top directory.
        return Launch(argv, [] if self.net_fd is None else [self.net_fd], paths.top)

    def setup_error(self, paths: BuildPaths, launch: Launch) -> str:
        return vm_status(paths.top)

    def ended(
        self, paths: BuildPaths, drv_path: StorePath, returncode: int | None
    ) -> BuildFailure | BuilderFailed | None:
        end = vm_ending(paths.top)
        if end is not None and end.returncode is not None:
            return builder_ended(end.returncode)
        if end is not None:
            return BuildFailure(
                BuildResultStatus.MISC_FAILURE, f"the build VM of '{magenta(drv_path)}' stopped: {end.init_error}"
            )
        # The helper's process makes the status file, so it tells a guest that stopped while booting from one
        # that stopped under the builder. So does the VMM's CPU time.
        started = "after" if (paths.top / VM_STATUS_NAME).exists() else "before"
        vmm = read_usage(paths.top / VMM_USAGE_NAME)
        cpu = "" if vmm is None else f", its VMM used {(vmm.cpu_user_us + vmm.cpu_system_us) / 1e6:.2f} s of CPU"
        return BuildFailure(
            BuildResultStatus.MISC_FAILURE,
            f"the build VM of '{magenta(drv_path)}' stopped {started} the init started the helper, with no word "
            f"from either (pynixd-krun exited with {returncode}{cpu})",
        )

    async def usage(
        self, paths: BuildPaths, cgroup: Path | None, samples: tuple[UsageSample, ...]
    ) -> BuildUsage | None:
        usage = read_usage(paths.top / USAGE_NAME)
        host = (
            read_usage(paths.top / VMM_USAGE_NAME)
            if cgroup is None
            else await anyio.to_thread.run_sync(build_cgroup.read, cgroup)
        )
        if usage is not None and host is not None:
            return with_host(usage, host)
        return usage


def choose_site(
    drv: BasicDerivation,
    settings: BuildSettings,
    vm: BuildVm | None,
    *,
    chroot: bool,
    sandboxed: bool,
    user: build_users.BuildUser | None,
    real_store_dir: Path,
    visible: frozenset[StorePath],
) -> tuple[BuildSite, str | None]:
    """The site of a build, and why it is not the microVM when the store has one."""
    host = HostSite(chroot=chroot)
    if vm is None:
        return host, None
    if not chroot:
        return host, "the build has no sandbox"
    if not sandboxed and vm.passt is None:
        return host, "a fixed-output or impure build reaches the network, and the build VM has none"
    # The guest kernel has no binfmt handler and no IA32 emulation: another system builds on the host's kernel.
    if drv.platform not in {settings.system, "builtin"}:
        return host, f"the build is for {drv.platform}, not {settings.system}"
    # The guest maps no build user yet: as root with build users, the namespaces build.
    if user is not None:
        return host, "the build runs as a build user"
    # The guest has its own `/dev` and `/proc`.
    sources = tuple(p.source for p in settings.sandbox_paths if not p.source.startswith(("/dev/", "/proc/")))
    return VmSite(vm, real_store_dir, visible, sources, network=not sandboxed), None


async def _attach_control(listener: socket.socket, restriction: RestrictionContext) -> None:
    """Give *restriction* the guest helper's control connection, the first one, and close *listener*."""
    with listener:
        listener.setblocking(False)
        while True:
            await anyio.wait_readable(listener)
            try:
                connection, _address = listener.accept()
            except BlockingIOError:
                continue
            break
    async with await SocketStream.from_socket(connection) as stream:
        restriction.attach(stream)
        await anyio.sleep_forever()


def listen(path: Path) -> socket.socket:
    """`createUnixDomainSocket(path, 0600)` (`derivation-builder.cc:1174`).

    It binds through the directory's descriptor: the build directory of a deep store is longer than `sun_path`.
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    directory = os.open(path.parent, os.O_PATH | os.O_DIRECTORY)
    try:
        sock.bind(f"/proc/self/fd/{directory}/{path.name}")
    finally:
        os.close(directory)
    path.chmod(0o600)
    sock.listen()
    return sock
