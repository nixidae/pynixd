"""A build in a libkrun microVM: `pynixd-krun` boots a guest that runs the sandbox helper (nixidae/nixidae#1186).

The guest mounts each share at its host path, so the helper's spec means the same inside. The stores are
read-only, each with an allowlist: the build store shows its input closure, and the host's store shows the
closure the helper runs from. A builder that escapes the helper's namespaces sees no other store path. A
`sandbox-paths` directory outside the stores, such as `/run/binfmt` or a ccache directory, is a writable share
of its own, and so is a file's parent, whose allowlist shows the file alone. The top directory (the spec) and
the chroot, where the outputs land for `collect_outputs`, are writable shares. The build directory is a sparse
XFS image the guest mounts over its place in the top directory: a build makes many small files, and each
operation on a share is a round trip to the host.

A `recursive-nix` or `builder-rpc-v0` builder reaches the store's socket through vsock. The guest's init listens
at the socket's place in the build directory and joins each connection to the host's socket in the top
directory. The helper gets its control socket the same way, as descriptor 4. A path the builder adds joins the
build store's allowlist before the helper binds it.

A fixed-output or impure build gets eth0, a NIC on a socketpair whose other end passt serves (`uplink`). The
guest has the same fixed addresses in every build: passt translates them to the host's own sockets.

On a Mac (nixidae/nixidae#1149) the guest is the same aarch64-linux guest on Hypervisor.framework, and builds
aarch64-linux, and x86_64-linux through Rosetta. libkrunfw is a Linux library, so the launcher boots the kernel
file `kernel` names. The build directory's image is ext4: mkfs.xfs builds on Linux only. No passt, so a
fixed-output or impure Linux build has no site on a Mac.
"""

from __future__ import annotations

import os
import socket
import sys
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import anyio

from .restriction import SOCKET_NAME

if TYPE_CHECKING:
    from nix_daemon_protocol.store_path import StorePath

#: Where the helper's interpreter and its closure live: the store pynixd itself runs from.
HOST_STORE = Path("/nix/store")
STATUS_NAME = "vm-status"
EXIT_NAME = "vm-exit"
DISK_NAME = "build.xfs"
VMM_USAGE_NAME = "vm-usage.json"
#: The build store's allowlist, which `allow` extends while the guest runs.
STORE_ALLOWLIST_NAME = "vm-allowlist-store"
#: The host's end of the builder's socket and of the helper's control socket, in the top directory. The launcher
#: runs there, so libkrun connects by name: a top directory is longer than `sun_path`.
BUILDER_SOCKET_NAME = "vm-builder.sock"
CONTROL_SOCKET_NAME = "vm-control.sock"
CONTROL_FD = 4
#: How long the guest caches a store lookup that finds nothing: the longest build is the bound that matters.
NEGATIVE_TIMEOUT_MS = 24 * 3600 * 1000
#: The guest's side of passt's network, the same for every build. passt answers for the gateway and the resolver.
#: Not slirp's 10.0.2.15: a QEMU or vivarium guest has that address, and a fetch to the host's own address would
#: reach the build VM itself (measured in the functional guest).
GUEST_ADDRESS = "10.90.35.2"
GUEST_PREFIX = 24
GUEST_GATEWAY = "10.90.35.1"
GUEST_DNS = "10.90.35.3"
#: passt's own MTU.
GUEST_MTU = 65520
PASST_LOG_NAME = "vm-passt.log"
#: Apple's Rosetta for Linux, which macOS installs with `softwareupdate --install-rosetta`.
ROSETTA_DIR = Path("/Library/Apple/usr/libexec/oah/RosettaLinux")
GUEST_SYSTEM = "aarch64-linux"
ROSETTA_SYSTEM = "x86_64-linux"


@dataclass(frozen=True)
class BuildVm:
    launcher: Path
    init: Path
    python: Path
    """The interpreter that runs the helper in the guest: a store path, unlike an editable `sys.executable`."""
    helper_closure: frozenset[str]
    """The host store paths the helper runs from: its interpreter and every module it imports."""
    mkfs: Path
    """`mkfs.xfs`, which makes the build directory's image on the host."""
    memory_mib: int
    cpus: int
    dax_window_mib: int
    disk_gib: int
    """The build directory's size. The image is sparse: the host pays for what the build writes."""
    passt: Path | None = None
    """passt, which gives a fixed-output or impure build its network; `None` keeps those builds on the host."""
    kernel: Path | None = None
    """The guest kernel, a raw arm64 Image, on a Mac; `None` boots libkrunfw's."""
    rosetta: Path | None = None
    """Apple's RosettaLinux directory, which runs x86_64 binaries in the guest; `None` builds no x86_64-linux."""
    foreign_systems: frozenset[str] = frozenset()
    """The systems other than the host's that the guest builds: on a Mac, Linux's."""
    sandbox_shell: Path | None = None
    """On a Mac, a static Linux busybox: a Linux build's `/bin/sh`, as Nix's Linux default `sandbox-paths` has it
    (`SANDBOX_SHELL`). The Mac's own `sandbox-paths` are darwin's."""

    @classmethod
    def from_environment(
        cls, *, memory_mib: int, cpus: int, dax_window_mib: int, disk_gib: int, network: bool = False
    ) -> BuildVm:
        """What the package names: the launcher (`PYNIXD_KRUN`), the init (`PYNIXD_KRUN_INIT`), the helper's
        interpreter (`PYNIXD_KRUN_PYTHON`) and its closure (`PYNIXD_KRUN_CLOSURE`, a file of store paths), and
        the build directory's `mkfs.xfs` or, on a Mac, `mke2fs` (`PYNIXD_KRUN_MKFS`); with *network*, passt
        (`PYNIXD_KRUN_PASST`). On a Mac, also the guest's kernel (`PYNIXD_KRUN_KERNEL`) and a Linux build's
        `/bin/sh` (`PYNIXD_KRUN_SANDBOX_SHELL`); Rosetta is used when macOS has it installed."""
        darwin = sys.platform == "darwin"
        names: tuple[str, ...] = ("PYNIXD_KRUN", "PYNIXD_KRUN_INIT", "PYNIXD_KRUN_PYTHON", "PYNIXD_KRUN_CLOSURE")
        names += ("PYNIXD_KRUN_MKFS", *(("PYNIXD_KRUN_KERNEL", "PYNIXD_KRUN_SANDBOX_SHELL") if darwin else ()))
        names += ("PYNIXD_KRUN_PASST",) if network else ()
        missing = [name for name in names if name not in os.environ]
        if missing:
            raise ValueError(f"build_vm needs {', '.join(missing)} in the environment; the pynixd package sets them")
        if darwin and network:
            raise ValueError("build_vm: network needs passt, which runs on Linux only")
        if darwin and dax_window_mib == 0:
            raise ValueError("build_vm: dax_window_mib is above 0 on a Mac, where libkrun maps no empty window")
        closure = Path(os.environ["PYNIXD_KRUN_CLOSURE"]).read_text().split()
        rosetta = ROSETTA_DIR if darwin and (ROSETTA_DIR / "rosetta").exists() else None
        return cls(
            launcher=Path(os.environ["PYNIXD_KRUN"]),
            init=Path(os.environ["PYNIXD_KRUN_INIT"]),
            python=Path(os.environ["PYNIXD_KRUN_PYTHON"]),
            helper_closure=frozenset(closure),
            mkfs=Path(os.environ["PYNIXD_KRUN_MKFS"]),
            memory_mib=memory_mib,
            cpus=cpus,
            dax_window_mib=dax_window_mib,
            disk_gib=disk_gib,
            passt=Path(os.environ["PYNIXD_KRUN_PASST"]) if network else None,
            kernel=Path(os.environ["PYNIXD_KRUN_KERNEL"]) if darwin else None,
            sandbox_shell=Path(os.environ["PYNIXD_KRUN_SANDBOX_SHELL"]) if darwin else None,
            rosetta=rosetta,
            foreign_systems=(
                frozenset({GUEST_SYSTEM, *((ROSETTA_SYSTEM,) if rosetta else ())}) if darwin else frozenset()
            ),
        )

    async def make_disk(self, top: Path, build_dir: Path) -> None:
        """The build directory's image, `top/build.xfs`, holding what *build_dir* holds (the files pynixd wrote
        there for the builder), with their owners.

        `mkfs.xfs -p DIR` fills the image from a directory, unprivileged. A 64 MiB log, the least XFS takes: the
        default log of a 1 TiB image is 513 MB of the host's disk (measured), this one 65 MB, in 48 ms. `mke2fs -d
        DIR` does the same for ext4; no journal, as a build directory need not outlive a crash.
        """
        image = top / DISK_NAME
        await anyio.to_thread.run_sync(_sparse, image, self.disk_gib * 1024**3)
        if self.mkfs.name == "mke2fs":
            argv = [str(self.mkfs), "-q", "-F", "-t", "ext4", "-O", "^has_journal", "-m", "0"]
            argv += ["-E", "nodiscard,lazy_itable_init=1", "-d", str(build_dir), str(image)]
        else:
            argv = [str(self.mkfs), "-q", "-K", "-l", "size=64m", "-p", str(build_dir), str(image)]
        await anyio.run_process(argv)

    def builds(self, system: str) -> bool:
        """Whether the guest builds for *system* while the host does not."""
        return system in self.foreign_systems

    def argv(
        self,
        *,
        top: Path,
        chroot_parent: Path,
        real_store_dir: Path,
        visible: Iterable[StorePath],
        sources: Iterable[str],
        spec_file: Path,
        build_dir: Path,
        recursive: bool = False,
        net_fd: int | None = None,
        cgroup: Path | None = None,
        socket_owner: tuple[int, int] | None = None,
        tso: bool = False,
    ) -> list[str]:
        """The launcher's command line, after it writes the allowlists into *top*.

        *visible* names the build store's paths the guest may see, as store paths. *sources* are the host paths
        the helper binds (`sandbox-paths`). One in a store shows its store path. One outside both stores is
        writable, as Nix binds it read-write and the host's permissions decide (`programs.ccache`'s cache): a
        directory is a share of its own, and a file is a share of its parent that shows the file alone, because
        virtio-fs mounts only a directory. The helper reads *spec_file* and gets `top/vm-status` as its file
        descriptor 3. A *recursive* build gets the two sockets of the module's docstring. The guest's NIC is on
        *net_fd*, passt's peer (`uplink`). The launcher must run in *top*. The VMM joins *cgroup*, and the
        launcher writes the VMM's rusage to `top/vm-usage.json`. A recursive build's socket belongs to
        *socket_owner*, the build user when the helper runs as root. With *tso*, an x86_64 build under Rosetta,
        the vCPUs keep x86's memory order.
        """
        shown: dict[Path, set[str]] = {
            real_store_dir: {str(real_store_dir / path.name) for path in visible},
        }
        shown.setdefault(HOST_STORE, set()).update(self.helper_closure)
        writable: dict[Path, set[str] | None] = {}
        for source in map(Path, sources):
            store = _store_of(source, real_store_dir)
            if store is not None:
                shown[store].add(str(store / source.relative_to(store).parts[0]))
            elif source.is_dir():
                writable[source] = None
            # A missing source fails in the helper, as it does on the host.
            elif source.exists():
                filtered = writable.setdefault(source.parent, set())
                if filtered is not None:
                    filtered.add(str(source))
        ro = _outermost(shown)
        rw = _outermost_writable(writable)
        # The build's own two directories mount inside a share or beside it, and one inside a filtered share
        # needs its mount point shown there.
        for inner in (chroot_parent, top):
            for root, paths in (*ro.items(), *rw.items()):
                if paths is not None and inner.is_relative_to(root):
                    paths.add(str(inner))
        argv = [
            str(self.launcher),
            f"--init={self.init}",
            f"--cpus={self.cpus}",
            f"--memory={self.memory_mib}",
            f"--dax-window={self.dax_window_mib * 1024 * 1024}",
            # Without a builder socket, a store share gains no name while the build runs: its allowlist is fixed
            # before the guest starts, and a path another build adds is outside it. With one, a path the
            # builder adds joins the allowlist, and the guest may have looked its name up before.
            f"--negative-timeout={0 if recursive else NEGATIVE_TIMEOUT_MS}",
        ]
        shares: list[tuple[Path, str, list[str] | None]] = [
            *((root, "ro", sorted(paths)) for root, paths in ro.items()),
            *((root, "rw-allow" if paths is not None else "rw", sorted(paths or ())) for root, paths in rw.items()),
            (chroot_parent, "rw", None),
            (top, "rw", None),
        ]
        # A share inside another mounts after it; at one depth, the read-only ones first.
        for i, (root, kind, allowed) in enumerate(sorted(shares, key=lambda s: (len(s[0].parts), s[1] != "ro"))):
            if kind == "rw":
                argv.append(f"--rw={root}")
                continue
            name = STORE_ALLOWLIST_NAME if kind == "ro" and real_store_dir.is_relative_to(root) else f"vm-allowlist-{i}"
            allowlist = top / name
            allowlist.write_text("".join(f"{p}\n" for p in allowed or ()))
            argv.append(f"--{kind}={root}:{allowlist}")
        argv += [
            f"--disk={top / DISK_NAME}:{build_dir}",
        ]
        if recursive:
            argv += [
                f"--vsock-fd={CONTROL_FD}:{CONTROL_SOCKET_NAME}",
                f"--vsock-listen={build_dir / SOCKET_NAME}:{BUILDER_SOCKET_NAME}",
            ]
        if net_fd is not None:
            argv.append(f"--net={net_fd}:{GUEST_ADDRESS}/{GUEST_PREFIX}:{GUEST_GATEWAY}:{GUEST_MTU}")
        if cgroup is not None:
            argv.append(f"--cgroup={cgroup}")
        if socket_owner is not None:
            argv.append(f"--socket-owner={socket_owner[0]}:{socket_owner[1]}")
        if self.kernel is not None:
            argv.append(f"--kernel={self.kernel}")
        if self.rosetta is not None:
            argv.append(f"--rosetta={self.rosetta}")
        if tso:
            argv.append("--tso")
        argv += [
            f"--usage={top / VMM_USAGE_NAME}",
            f"--cwd={top}",
            f"--status={top / STATUS_NAME}",
            f"--exit={top / EXIT_NAME}",
            "--",
            str(self.python),
            # Isolated: a writable share holds what earlier builders wrote, and the working directory and
            # `PYTHON*` must not reach `sys.path`.
            "-I",
            "-m",
            "pynixd_store.builder.child",
            str(spec_file),
            "3",
            *([str(CONTROL_FD)] if recursive else []),
        ]
        return argv


def allow(top: Path, real_store_dir: Path, path: StorePath) -> None:
    """Show the store path *path* in the guest: libkrun reads the allowlist again when a lookup misses."""
    with (top / STORE_ALLOWLIST_NAME).open("a") as file:
        file.write(f"{real_store_dir / path.name}\n")


def _store_of(path: Path, real_store_dir: Path) -> Path | None:
    for root in (real_store_dir, HOST_STORE):
        if path.is_relative_to(root) and path != root:
            return root
    return None


def unshareable(sources: Iterable[str], real_store_dir: Path) -> str | None:
    """The first source that is a file directly in `/`, or `None`.

    virtio-fs mounts only a directory: the kernel fixes the root's mode at `S_IFDIR` (`fs/fuse/virtio_fs.c`,
    `virtio_fs_ctx_set_defaults`) and takes no `rootmode=`. So a file is a share of its parent, and a share of
    `/` would cover the guest's own root.
    """
    return next(
        (
            s
            for s in sources
            if _store_of(Path(s), real_store_dir) is None
            and Path(s).parent == Path("/")
            and Path(s).exists()
            and not Path(s).is_dir()
        ),
        None,
    )


def _outermost_writable(shares: dict[Path, set[str] | None]) -> dict[Path, set[str] | None]:
    """*shares* with each share inside another folded into it. `None` shows a whole share.

    A share inside a whole one adds nothing. One inside a filtered share shows through it: a whole directory
    as its own path, a filtered one as the paths it lists.
    """
    merged: dict[Path, set[str] | None] = {}
    for root in sorted(shares, key=lambda r: len(r.parts)):
        outer = next((r for r in merged if root.is_relative_to(r)), None)
        if outer is None:
            merged[root] = None if shares[root] is None else set(shares[root] or ())
            continue
        outer_paths = merged[outer]
        if outer_paths is not None:
            inner = shares[root]
            outer_paths.update({str(root)} if inner is None else inner)
    return merged


def _outermost(shown: dict[Path, set[str]]) -> dict[Path, set[str]]:
    """*shown* with each share inside another folded into it: one mount, whose allowlist holds both."""
    merged: dict[Path, set[str]] = {}
    for root in sorted(shown, key=lambda r: len(r.parts)):
        outer = next((r for r in merged if root.is_relative_to(r)), None)
        merged.setdefault(outer or root, set()).update(shown[root])
    return merged


@dataclass(frozen=True)
class GuestEnd:
    """What the guest's init wrote to `top/vm-exit`."""

    returncode: int | None
    """How the helper ended, as `Popen.returncode` says it (a signal is negative); `None` when the init stopped the
    guest first."""
    init_error: str | None = None
    """The init's own error, the one that stopped the guest."""


def ending(top: Path) -> GuestEnd | None:
    """How the guest ended, or `None` when it wrote nothing: it stopped before the helper did, and before its init
    could say why."""
    try:
        how, _, rest = (top / EXIT_NAME).read_text().rstrip("\n").partition(" ")
    except FileNotFoundError:
        return None
    if how == "init":
        return GuestEnd(None, rest)
    code = int(rest)
    return GuestEnd(-code if how == "signal" else code)


def status(top: Path) -> str:
    """What the helper wrote to its file descriptor 3: an error that stopped the setup, or nothing."""
    try:
        return (top / STATUS_NAME).read_bytes().decode(errors="surrogateescape")
    except FileNotFoundError:
        return ""


def _sparse(path: Path, size: int) -> None:
    with path.open("wb") as file:
        file.truncate(size)


@asynccontextmanager
async def uplink(passt: Path, top: Path) -> AsyncIterator[int]:
    """The VM's end of a socketpair that *passt* serves, for the launcher's `--net`; passt's log in *top*.

    passt exits when every copy of this end is closed: the launcher's when the VMM ends, this one at the exit.
    It forwards no port into the guest and maps no host loopback. DNS to `GUEST_DNS` goes to the first resolver
    of the host's `/etc/resolv.conf`, which passt reaches from the host, `127.0.0.53` included.
    """
    ours, theirs = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        with await anyio.to_thread.run_sync(lambda: (top / PASST_LOG_NAME).open("wb")) as log:
            process = await anyio.open_process(
                [
                    str(passt),
                    "--foreground",
                    "--quiet",
                    "--fd",
                    str(theirs.fileno()),
                    "--address",
                    f"{GUEST_ADDRESS}/{GUEST_PREFIX}",
                    "--gateway",
                    GUEST_GATEWAY,
                    "--dns-forward",
                    GUEST_DNS,
                    "--map-host-loopback",
                    "none",
                    # VMs at once must not share a host UDP port: pynixd/nix/passt/0001-*.patch.
                    "--no-udp-port-keep",
                    "--mtu",
                    str(GUEST_MTU),
                ],
                stdin=None,
                stdout=log,
                stderr=log,
                pass_fds=(theirs.fileno(),),
            )
    except BaseException:
        ours.close()
        raise
    finally:
        theirs.close()
    try:
        yield ours.fileno()
    finally:
        ours.close()
        with anyio.CancelScope(shield=True):
            with anyio.move_on_after(5):
                await process.wait()
            if process.returncode is None:
                process.kill()
                await process.wait()
