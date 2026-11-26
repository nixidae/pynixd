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
import subprocess
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
from .darwin_vm import DarwinBuildVm
from .outputs import BuildFailure
from .restriction import SOCKET_NAME, RestrictionContext
from .runner import EXIT_NAME as RUNNER_EXIT_NAME, STATUS_NAME as RUNNER_STATUS_NAME, RunnerSandbox, drv_refusal
from .sibling import (
    CONTROL_SOCKET_NAME as SIBLING_CONTROL_SOCKET_NAME,
    STATUS_NAME as SIBLING_STATUS_NAME,
    SiblingSandbox,
)
from .usage import USAGE_NAME, read_usage, with_host
from .vm import (
    BUILDER_SOCKET_NAME as VM_BUILDER_SOCKET_NAME,
    CONTROL_SOCKET_NAME as VM_CONTROL_SOCKET_NAME,
    GUEST_DNS as VM_GUEST_DNS,
    ROSETTA_SYSTEM as VM_ROSETTA_SYSTEM,
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
    from collections.abc import Callable

    from nix_daemon_protocol.basic_derivation import BasicDerivation
    from nix_daemon_protocol.store_path import StorePath

    from .helpers import Helpers
    from .settings import BuildSettings
    from .spec import ChildSpec
    from .usage import BuildUsage, UsageSample

__all__ = [
    "BuildPaths",
    "BuildSite",
    "BuilderFailed",
    "DarwinVmSite",
    "Endpoint",
    "HostSite",
    "Launch",
    "RunnerSite",
    "SiblingSite",
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

    async def remove(self, helpers: Helpers | None) -> None:
        paths = [path for path in (self.chroot_parent, self.top) if path is not None]
        if helpers is not None and self.chroot_parent is not None and await helpers.remove(paths):
            return
        for path in paths:
            await anyio.to_thread.run_sync(delete_path, path)

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
    forks: bool = False
    """A zygote may fork this helper in place of *argv*: `child.run` of the job, with the status pipe."""


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
        files_of: Callable[[StorePath], Path],
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


_fresh_proc: bool | None = None


async def fresh_proc_here(scratch: Path) -> bool:
    """Whether a sandbox on this host may mount a `/proc` of its own (`sibling.fresh_proc_allowed`). Asked once per
    process, in a process of its own: the probe forks, and this one has threads."""
    global _fresh_proc
    if _fresh_proc is None:
        argv = [sys.executable, "-I", "-m", "pynixd_store.builder.sibling", "fresh-proc", str(scratch)]
        _fresh_proc = (await anyio.run_process(argv, check=False)).returncode == 0
    return _fresh_proc


def pid_namespace(fresh_proc: bool, build_user: tuple[int, int] | None) -> bool:
    """Whether the sandbox gets a pid namespace and a fresh `/proc` of its own.

    NIX-DEVIATION (#27): `prepareSandbox` (`linux-derivation-builder.cc:745`) mounts a fresh `/proc` in every
    sandbox, and the kernel refuses it in a container that masks paths of its own `/proc`: every build fails.
    pynixd binds the container's `/proc` and keeps its pid namespace, as the sibling sandbox does. A build user
    then ends the build's processes (`killUser`); without one they would outlive the build, so the build keeps
    the pid namespace and fails as Nix's does. Measured: sg1's static builders, under the build pods' seccomp
    profile, which allows the namespaces. To reverse: a pod with `procMount: Unmasked`, which needs
    `hostUsers: false`, and nixkube's store volume refuses that pod while the kernel cannot idmap an overlay.
    """
    return fresh_proc or build_user is None


@dataclass
class HostSite(BuildSite):
    """The helper runs on the host's kernel, in namespaces with the chroot, or as a plain process without it."""

    chroot: bool
    waits_for_exit: bool = True
    samples_on_host: bool = True
    fresh_proc: bool = True
    """A sandbox here may mount a `/proc` of its own; `start` asks."""

    async def endpoint(
        self,
        stack: contextlib.AsyncExitStack,
        paths: BuildPaths,
        inputs: set[StorePath],
        files_of: Callable[[StorePath], Path],
        *,
        submitting: bool,
        repairing: bool,
        user: build_users.BuildUser | None,
    ) -> Endpoint:
        if not self.chroot:
            # `addDependencyImpl` (`derivation-builder.cc:901`) binds nothing: the builder sees the store.
            restriction = RestrictionContext(
                inputs, None, files_of, submitting=submitting, repairing=repairing, binds=False
            )
            listener = await anyio.to_thread.run_sync(listen, paths.tmp / SOCKET_NAME)
            await anyio.to_thread.run_sync(build_users.chown_to, user, paths.tmp / SOCKET_NAME)
            return Endpoint(listener, restriction)
        ours, control = socket.socketpair()
        stack.callback(control.close)
        stream = await stack.enter_async_context(await SocketStream.from_socket(ours))
        restriction = RestrictionContext(inputs, stream, files_of, submitting=submitting, repairing=repairing)
        listener = await anyio.to_thread.run_sync(listen, paths.tmp / SOCKET_NAME)
        await anyio.to_thread.run_sync(build_users.chown_to, user, paths.tmp / SOCKET_NAME)
        return Endpoint(listener, restriction, control)

    async def start(self, stack: contextlib.AsyncExitStack, paths: BuildPaths) -> None:
        if self.chroot and sys.platform == "linux":
            self.fresh_proc = await fresh_proc_here(paths.top.parent)

    def configure(self, spec: ChildSpec, sample_seconds: float) -> None:
        """The job keeps `build()`'s cgroup and samples nothing: the parent samples the host cgroup itself."""
        if spec["chroot"]:
            spec["pid_namespace"] = pid_namespace(self.fresh_proc, spec["build_user"])

    async def launch(self, paths: BuildPaths, spec_file: Path, cgroup: Path | None, *, recursive: bool) -> Launch:
        status_read, status_write = os.pipe()
        argv = [sys.executable, "-m", "pynixd_store.builder.child", str(spec_file), str(status_write)]
        # darwin's frameworks refuse a fork with no exec.
        return Launch(argv, [status_write], None, status_read, status_write, forks=sys.platform == "linux")

    def setup_error(self, paths: BuildPaths, launch: Launch) -> str:
        with os.fdopen(launch.status_read, "rb") as status_file:
            return status_file.read().decode(errors="surrogateescape")

    def ended(self, paths: BuildPaths, drv_path: StorePath, returncode: int | None) -> BuilderFailed | None:
        return builder_ended(returncode)

    async def usage(
        self, paths: BuildPaths, cgroup: Path | None, samples: tuple[UsageSample, ...]
    ) -> BuildUsage | None:
        if cgroup is None:
            return await anyio.to_thread.run_sync(read_usage, paths.top / USAGE_NAME)
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
    user: build_users.BuildUser | None = None
    """As root, the build user. The helper runs as the guest's root and maps it, as it does on the host, and the
    VMM, as root, writes each share's file as the guest process that asked for it."""
    tso: bool = False
    """An x86_64 build under Rosetta, whose vCPUs keep x86's memory order."""
    waits_for_exit: bool = False
    samples_on_host: bool = False

    async def endpoint(
        self,
        stack: contextlib.AsyncExitStack,
        paths: BuildPaths,
        inputs: set[StorePath],
        files_of: Callable[[StorePath], Path],
        *,
        submitting: bool,
        repairing: bool,
        user: build_users.BuildUser | None,
    ) -> Endpoint:
        # The guest's init listens in the build directory, and the image holds no host socket.
        restriction = RestrictionContext(
            inputs,
            None,
            files_of,
            submitting=submitting,
            repairing=repairing,
            reveal=functools.partial(vm_allow, paths.top, self.real_store_dir),
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
        # `argv` stats the sources and writes the guest's files.
        argv = await anyio.to_thread.run_sync(
            lambda: self.vm.argv(
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
                socket_owner=None if self.user is None else (self.user.uid, self.user.gid),
                tso=self.tso,
            )
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
        usage = await anyio.to_thread.run_sync(read_usage, paths.top / USAGE_NAME)
        if cgroup is None:
            host = await anyio.to_thread.run_sync(read_usage, paths.top / VMM_USAGE_NAME)
        else:
            host = await anyio.to_thread.run_sync(build_cgroup.read, cgroup)
        if usage is not None and host is not None:
            return with_host(usage, host)
        return usage


@dataclass
class DarwinVmSite(BuildSite):
    """The darwin helper runs in a macOS guest started from a snapshot (`darwin_vm.py`, nixidae/nixidae#1168).

    The guest reports through files in the top directory, as the Linux build VM does, and the launcher moves the
    outputs into the store before it exits.
    """

    vm: DarwinBuildVm
    real_store_dir: Path
    visible: frozenset[StorePath]
    outputs: list[str]
    """The store paths the builder may make, at their real place."""
    keep_failed: bool = False
    """`keep-failed`: the guest copies a failed build's directory back from its tmpfs."""
    waits_for_exit: bool = False
    samples_on_host: bool = False

    async def endpoint(
        self,
        stack: contextlib.AsyncExitStack,
        paths: BuildPaths,
        inputs: set[StorePath],
        files_of: Callable[[StorePath], Path],
        *,
        submitting: bool,
        repairing: bool,
        user: build_users.BuildUser | None,
    ) -> Endpoint:
        """The host's socket, as for a build on the host; the guest relays its own to it. A path the builder adds
        joins the store share's allowlist."""
        restriction = RestrictionContext(
            inputs,
            None,
            files_of,
            submitting=submitting,
            repairing=repairing,
            reveal=functools.partial(vm_allow, paths.top, self.real_store_dir),
            binds=False,
        )
        listener = await anyio.to_thread.run_sync(listen, paths.tmp / SOCKET_NAME)
        await anyio.to_thread.run_sync(build_users.chown_to, user, paths.tmp / SOCKET_NAME)
        return Endpoint(listener, restriction)

    def configure(self, spec: ChildSpec, sample_seconds: float) -> None:
        """The helper measures with `wait4`, as on a darwin host."""

    async def launch(self, paths: BuildPaths, spec_file: Path, cgroup: Path | None, *, recursive: bool) -> Launch:
        visible = frozenset(str(self.real_store_dir / path.name) for path in self.visible)
        job = self.vm.write_job(
            paths,
            spec_file,
            self.real_store_dir,
            visible,
            self.outputs,
            keep_failed=self.keep_failed,
            recursive=recursive,
        )
        return Launch([sys.executable, "-I", "-m", "pynixd_store.builder.darwin_vm", str(job)])

    def setup_error(self, paths: BuildPaths, launch: Launch) -> str:
        return vm_status(paths.top)

    def ended(
        self, paths: BuildPaths, drv_path: StorePath, returncode: int | None
    ) -> BuildFailure | BuilderFailed | None:
        end = vm_ending(paths.top)
        if end is not None and end.returncode is not None:
            return builder_ended(end.returncode)
        reason = end.init_error if end is not None else f"its launcher exited with {returncode} and said nothing"
        return BuildFailure(
            BuildResultStatus.MISC_FAILURE, f"the darwin build VM of '{magenta(drv_path)}' stopped: {reason}"
        )

    async def usage(
        self, paths: BuildPaths, cgroup: Path | None, samples: tuple[UsageSample, ...]
    ) -> BuildUsage | None:
        usage = read_usage(paths.top / USAGE_NAME)
        host = read_usage(paths.top / VMM_USAGE_NAME)
        if usage is not None and host is not None:
            return with_host(usage, host)
        return usage


@dataclass
class SiblingSite(BuildSite):
    """The helper runs in the pod's sandbox container (`sibling.py`, nixidae/nixidae#1560, spike).

    Both containers mount the build's volume at one path, so the builder's socket in the build directory needs no
    relay. The helper reports in the top directory. The daemon container's cgroup does not hold the builder: the
    sandbox container measures it with its own cgroup (`sibling.measured_by_the_container`), else with `wait4`.
    """

    sandbox: SiblingSandbox
    waits_for_exit: bool = True
    samples_on_host: bool = False

    async def endpoint(
        self,
        stack: contextlib.AsyncExitStack,
        paths: BuildPaths,
        inputs: set[StorePath],
        files_of: Callable[[StorePath], Path],
        *,
        submitting: bool,
        repairing: bool,
        user: build_users.BuildUser | None,
    ) -> Endpoint:
        restriction = RestrictionContext(inputs, None, files_of, submitting=submitting, repairing=repairing)
        controls = await anyio.to_thread.run_sync(listen, paths.top / SIBLING_CONTROL_SOCKET_NAME)
        listener = await anyio.to_thread.run_sync(listen, paths.tmp / SOCKET_NAME)
        await anyio.to_thread.run_sync(build_users.chown_to, user, paths.tmp / SOCKET_NAME)
        group = await stack.enter_async_context(anyio.create_task_group())
        stack.callback(group.cancel_scope.cancel)
        group.start_soon(_attach_control, controls, restriction)
        return Endpoint(listener, restriction)

    def configure(self, spec: ChildSpec, sample_seconds: float) -> None:
        spec["pid_namespace"] = False
        spec["cgroup"] = None

    async def launch(self, paths: BuildPaths, spec_file: Path, cgroup: Path | None, *, recursive: bool) -> Launch:
        argv = [sys.executable, "-I", "-m", "pynixd_store.builder.sibling", "wait"]
        argv += [str(self.sandbox.rendezvous), str(spec_file), str(paths.top), *(["recursive"] if recursive else [])]
        return Launch(argv)

    def setup_error(self, paths: BuildPaths, launch: Launch) -> str:
        try:
            return (paths.top / SIBLING_STATUS_NAME).read_bytes().decode(errors="surrogateescape")
        except FileNotFoundError:
            return ""

    def ended(self, paths: BuildPaths, drv_path: StorePath, returncode: int | None) -> BuilderFailed | None:
        return builder_ended(returncode)

    async def usage(
        self, paths: BuildPaths, cgroup: Path | None, samples: tuple[UsageSample, ...]
    ) -> BuildUsage | None:
        return await anyio.to_thread.run_sync(read_usage, paths.top / USAGE_NAME)


@dataclass
class RunnerSite(BuildSite):
    """The builder is the pod's runner container (`runner.py`, nixidae/nixidae#1560, spike).

    The helper on the host starts no builder: it lets the runner start and copies its log. The runner has no
    socket of pynixd's and no other program, so a build that needs either is refused here.
    """

    sandbox: RunnerSandbox
    refused: str | None = None
    waits_for_exit: bool = True
    samples_on_host: bool = False

    async def endpoint(
        self,
        stack: contextlib.AsyncExitStack,
        paths: BuildPaths,
        inputs: set[StorePath],
        files_of: Callable[[StorePath], Path],
        *,
        submitting: bool,
        repairing: bool,
        user: build_users.BuildUser | None,
    ) -> Endpoint:
        """A socket that nothing reaches: `refusal` refuses a recursive build, and `build()` asks it afterwards."""
        restriction = RestrictionContext(inputs, None, files_of, submitting=submitting, repairing=repairing)
        return Endpoint(await anyio.to_thread.run_sync(listen, paths.tmp / SOCKET_NAME), restriction)

    def refusal(self) -> str | None:
        return self.refused

    def configure(self, spec: ChildSpec, sample_seconds: float) -> None:
        """The runner's own process is the builder: the job only tells the helper what the runner must run."""

    async def launch(self, paths: BuildPaths, spec_file: Path, cgroup: Path | None, *, recursive: bool) -> Launch:
        argv = [sys.executable, "-I", "-m", "pynixd.provisioning.runner_wait", str(paths.top), str(spec_file)]
        return Launch([*argv, self.sandbox.container, str(self.sandbox.ready)])

    def setup_error(self, paths: BuildPaths, launch: Launch) -> str:
        try:
            return (paths.top / RUNNER_STATUS_NAME).read_bytes().decode(errors="surrogateescape")
        except FileNotFoundError:
            return ""

    def ended(
        self, paths: BuildPaths, drv_path: StorePath, returncode: int | None
    ) -> BuildFailure | BuilderFailed | None:
        try:
            code = int((paths.top / RUNNER_EXIT_NAME).read_text())
        except FileNotFoundError:
            return BuildFailure(
                BuildResultStatus.MISC_FAILURE,
                f"the runner of '{magenta(drv_path)}' did not report how it ended (its helper exited with {returncode})",
            )
        return builder_ended(code)

    async def usage(
        self, paths: BuildPaths, cgroup: Path | None, samples: tuple[UsageSample, ...]
    ) -> BuildUsage | None:
        """The runner's cgroup is not the controller's to read."""
        return None


def _runner_refusal(drv: BasicDerivation, settings: BuildSettings, user: build_users.BuildUser | None) -> str | None:
    """Why the runner container cannot run *drv*: what Nix's sandbox gives a build and the runner does not."""
    if (refused := drv_refusal(drv)) is not None:
        return refused
    if settings.external_builders:
        return "the runner container runs no external builder"
    if user is not None:
        return "the runner container runs the build as the controller's uid, not as a build user"
    if settings.sandbox_paths:
        targets = ", ".join(sorted(p.target for p in settings.sandbox_paths))
        return f"the runner container binds no sandbox-paths ({targets})"
    return None


def choose_site(
    drv: BasicDerivation,
    settings: BuildSettings,
    vm: BuildVm | DarwinBuildVm | SiblingSandbox | RunnerSandbox | None,
    *,
    chroot: bool,
    sandboxed: bool,
    user: build_users.BuildUser | None,
    real_store_dir: Path,
    visible: frozenset[StorePath],
    darwin_sandbox: bool = False,
    outputs: list[str] | None = None,
) -> tuple[BuildSite, str | None]:
    """The site of a build, and why it is not the store's build VM when the store has one.

    *darwin_sandbox*: a darwin build that uses the sandbox, whose outputs are *outputs*.
    """
    host = HostSite(chroot=chroot)
    if vm is None:
        return host, None
    if isinstance(vm, RunnerSandbox):
        return RunnerSite(vm, _runner_refusal(drv, settings, user)), None
    if isinstance(vm, SiblingSandbox):
        # A build without the chroot as well: the sandbox container keeps it from the daemon's key and files.
        return SiblingSite(vm), None
    if isinstance(vm, DarwinBuildVm):
        return _choose_darwin(drv, settings, vm, host, darwin_sandbox, real_store_dir, visible, outputs or [])
    if not chroot:
        return host, "the build has no sandbox"
    if not sandboxed and vm.passt is None:
        return host, "a fixed-output or impure build reaches the network, and the build VM has none"
    # On Linux the guest kernel has no binfmt handler and no IA32 emulation: another system builds on the host's
    # kernel. On a Mac the guest builds Linux's systems (`BuildVm.foreign_systems`).
    if drv.platform not in {settings.system, "builtin"} and not vm.builds(drv.platform):
        return host, f"the build is for {drv.platform}, not {settings.system}"
    # A uid-range build's ids need the guest's cgroup namespace and `/sys`, which the VM does not give yet.
    if user is not None and user.count != 1:
        return host, "the build runs as a range of ids"
    # The guest has its own `/dev` and `/proc`.
    sources = tuple(p.source for p in settings.sandbox_paths if not p.source.startswith(("/dev/", "/proc/")))
    tso = drv.platform == VM_ROSETTA_SYSTEM and vm.rosetta is not None
    return VmSite(vm, real_store_dir, visible, sources, network=not sandboxed, user=user, tso=tso), None


def _choose_darwin(  # noqa: PLR0913 -- choose_site's arguments that a darwin build reads
    drv: BasicDerivation,
    settings: BuildSettings,
    vm: DarwinBuildVm,
    host: HostSite,
    sandbox: bool,
    real_store_dir: Path,
    visible: frozenset[StorePath],
    outputs: list[str],
) -> tuple[BuildSite, str | None]:
    if not sandbox:
        return host, "the build has no sandbox"
    if drv.platform not in {settings.system, "builtin"}:
        return host, f"the build is for {drv.platform}, not {settings.system}"
    return DarwinVmSite(vm, real_store_dir, visible, outputs, keep_failed=settings.keep_failed), None


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

    The build directory of a deep store is longer than `sun_path`. On Linux it binds through the directory's
    descriptor; on darwin a long path binds from its directory (`_bind_in_directory`).
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        if sys.platform == "linux":
            directory = os.open(path.parent, os.O_PATH | os.O_DIRECTORY)
            try:
                sock.bind(f"/proc/self/fd/{directory}/{path.name}")
            finally:
                os.close(directory)
        elif len(os.fsencode(path)) < SUN_PATH_DARWIN:
            sock.bind(str(path))
        else:
            _bind_in_directory(sock, path)
    except BaseException:
        sock.close()
        raise
    path.chmod(0o600)
    sock.listen()
    return sock


SUN_PATH_DARWIN = 104
"""`sizeof(sockaddr_un.sun_path)` on darwin, the terminating NUL included."""


def _bind_in_directory(sock: socket.socket, path: Path) -> None:
    """`bindConnectProcHelper` (`unix-domain-socket.cc:56`): bind *sock* by its base name from its directory.

    darwin has no `/proc/self/fd`. Nix forks a process that changes directory; pynixd runs threads, so a fork is
    unsafe here, and a new interpreter that inherits the socket does the same.
    """
    script = "import os, socket, sys; os.chdir(sys.argv[1]); socket.socket(fileno=int(sys.argv[2])).bind(sys.argv[3])"
    fd = sock.fileno()
    done = subprocess.run(
        [sys.executable, "-I", "-c", script, str(path.parent), str(fd), path.name],
        pass_fds=(fd,),
        capture_output=True,
        check=False,
    )
    if done.returncode != 0:
        reason = done.stderr.decode(errors="replace").strip().splitlines()[-1:] or ["no reason"]
        raise OSError(f"cannot bind to socket at '{path}': {reason[0]}")
