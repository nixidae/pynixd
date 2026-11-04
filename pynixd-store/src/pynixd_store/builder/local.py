"""A local build in pynixd's sandbox: `DerivationBuildingGoal` and `DerivationBuilderImpl` for one derivation.

`pynixd/docs/design/builder.md` holds the scope. `unsupported` names what the
daemon still builds; the caller sends those there before anything starts.
"""

from __future__ import annotations

import contextlib
import errno
import json
import os
import signal
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import anyio.from_thread
import anyio.to_thread
import structlog

from nix_daemon_protocol.build_result import BuildResultStatus
from nix_daemon_protocol.derivation_output import OutputKind
from nix_daemon_protocol.signature import Signature

from ..add import PathLock, delete_path
from ..hintfmt import ANSI_BOLD, ANSI_NORMAL, ANSI_RED, magenta
from ..signing import SecretKey, fingerprint
from .buildlog import BuildLog
from .env import HOME_DIR, SANDBOX_BUILD_DIR, build_env
from .outputs import BuildFailure, collect_outputs
from .spec import ChildSpec

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Sequence

    from nix_daemon_protocol.basic_derivation import BasicDerivation
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from ..engine import StoreEngine
    from .buildlog import Output
    from .settings import BuildSettings

_log = structlog.get_logger(__name__)

SANDBOX_UID, SANDBOX_GID = 1000, 100
"""`sandboxUid` and `sandboxGid` without build users (`linux-derivation-builder.cc:430`)."""

DISK_FULL_BYTES = 8 * 1024 * 1024
"""Less free space than this after a failure is a full disk (`decideWhetherDiskFull`, `derivation-builder.cc:304`)."""

DEV_FILES = ("/dev/full", "/dev/null", "/dev/random", "/dev/tty", "/dev/urandom", "/dev/zero")
OUTPUT_CHECKS = (
    "allowedReferences",
    "allowedRequisites",
    "disallowedReferences",
    "disallowedRequisites",
    "outputChecks",
    "unsafeDiscardReferences",
)


def unsupported(drv: BasicDerivation, settings: BuildSettings) -> str | None:
    """Why pynixd leaves this build to the daemon, or `None` when it builds it."""
    if not sys.platform.startswith("linux"):
        return "pynixd builds on Linux only"
    if drv.builder.startswith("builtin:"):
        return "a builtin builder"
    if drv.platform != settings.system and drv.platform not in settings.extra_platforms:
        return f"the platform {drv.platform}"
    if settings.sandbox == "false" or drv.env.get("__noChroot") == "1":
        return "a build without the sandbox"
    features = set(drv.required_system_features)
    if features & {"recursive-nix", "uid-range"}:
        return f"the features {sorted(features & {'recursive-nix', 'uid-range'})}"
    if "__json" in drv.env or "exportReferencesGraph" in drv.env:
        return "structured attrs or exportReferencesGraph"
    if any(name in drv.env for name in OUTPUT_CHECKS):
        return "output checks"
    kinds = {o.kind for o in drv.outputs.values()}
    if kinds - {OutputKind.INPUT_ADDRESSED, OutputKind.CA_FIXED}:
        return "a floating, impure or deferred output"
    if OutputKind.CA_FIXED in kinds and len(drv.outputs) != 1:
        return "a fixed output beside another output"
    return None


@dataclass
class LocalBuildResult:
    status: BuildResultStatus
    error_msg: str = ""
    start_time: int = 0
    stop_time: int = 0
    outputs: dict[str, ValidPathInfo] = field(default_factory=dict)


@dataclass
class _BuilderFailed:
    """The builder exited non-zero: `statusToString` without the leading word (`processes.cc:378`)."""

    reason: str


@dataclass
class _Paths:
    top: Path
    tmp: Path
    chroot_parent: Path
    root: Path

    def remove(self) -> None:
        for path in (self.chroot_parent, self.top):
            delete_path(path)


def _drv_name(drv_path: str) -> str:
    return drv_path.rsplit("/", 1)[-1].split("-", 1)[1].removesuffix(".drv")


def _prepare(engine: StoreEngine, drv_path: str, settings: BuildSettings) -> _Paths:
    """`startBuild` up to the environment, and `setupBuildChroot` (`unix/build/chroot.cc:12`)."""
    layout = engine.layout
    build_dir = Path(settings.build_dir) if settings.build_dir else layout.state_dir / "builds"
    build_dir.mkdir(parents=True, exist_ok=True)
    top = Path(tempfile.mkdtemp(prefix="nix-", dir=build_dir))
    tmp = top / "build"
    tmp.mkdir(0o700)

    chroot_parent = layout.real_store_dir / (drv_path.rsplit("/", 1)[-1] + ".chroot")
    delete_path(chroot_parent)
    chroot_parent.mkdir(0o700)
    root = chroot_parent / "root"
    root.mkdir(0o750)
    (root / "tmp").mkdir()
    (root / "tmp").chmod(0o1777)
    store = root / str(layout.store_dir).lstrip("/")
    store.mkdir(parents=True)
    store.chmod(0o1775)
    etc = root / "etc"
    etc.mkdir()
    (etc / "passwd").write_text(
        f"root:x:0:0:Nix build user:{SANDBOX_BUILD_DIR}:/noshell\n"
        f"nixbld:x:{SANDBOX_UID}:{SANDBOX_GID}:Nix build user:{SANDBOX_BUILD_DIR}:/noshell\n"
        "nobody:x:65534:65534:Nobody:/:/noshell\n"
    )
    (etc / "group").write_text(f"root:x:0:\nnixbld:!:{SANDBOX_GID}:\nnogroup:x:65534:\n")
    dev = root / "dev"
    dev.mkdir()
    (dev / "fd").symlink_to("/proc/self/fd")
    for i, name in enumerate(("stdin", "stdout", "stderr")):
        (dev / name).symlink_to(f"/proc/self/fd/{i}")
    return _Paths(top, tmp, chroot_parent, root)


def _sandbox_store_paths(settings: BuildSettings, store_dir: str) -> set[str]:
    """`toStorePath` of each `sandbox-paths` source that `isInStore`."""
    prefix = f"{store_dir}/"
    return {
        prefix + p.source.removeprefix(prefix).split("/", 1)[0]
        for p in settings.sandbox_paths
        if p.source.startswith(prefix) and len(p.source) > len(prefix)
    }


def _binds(
    engine: StoreEngine,
    paths: _Paths,
    inputs: set[str],
    sandbox_closure: set[str],
    settings: BuildSettings,
    sandboxed: bool,
) -> list[tuple[str, str, bool]]:
    """`pathsInChroot`: `sandbox-paths` and the closure of its store paths, the build directory, the input
    closure and `/dev` (`enterChroot`).

    NIX-DEFECT (#23): `build/derivation-building-goal.cc:957` binds each path of
    the `sandbox-paths` closure from its store path, and the chroot builder
    translates only the inputs (`unix/build/chroot-derivation-builder.cc:54`).
    In a store at `?root=` the source is absent and the build fails. pynixd
    binds the real path, as for the inputs; Lillecarl/nix#392 holds the report.
    """
    binds: dict[str, tuple[str, bool]] = {p.target: (p.source, p.optional) for p in settings.sandbox_paths}
    real = engine.layout.real_store_dir
    for path in sandbox_closure:
        binds[path] = (str(real / path.rsplit("/", 1)[-1]), False)
    binds[SANDBOX_BUILD_DIR] = (str(paths.tmp), False)
    for path in inputs:
        binds[path] = (str(real / path.rsplit("/", 1)[-1]), False)
    if "/dev" not in binds:
        for name in DEV_FILES:
            binds.setdefault(name, (name, False))
    if not sandboxed:
        (paths.root / "etc/nsswitch.conf").write_text("hosts: files dns\nservices: files\n")
        for name in ("/etc/resolv.conf", "/etc/services", "/etc/hosts"):
            if os.path.exists(name):
                binds.setdefault(name, (os.path.realpath(name), False))
    else:
        (paths.root / "etc/hosts").write_text("127.0.0.1 localhost\n::1 localhost\n")
    (paths.root / "etc").chmod(0o555)
    return [(target, source, optional) for target, (source, optional) in sorted(binds.items())]


def _sign(infos: Iterable[ValidPathInfo], secret_key_files: Sequence[str]) -> None:
    """`Store::signPathInfo` (`store-api.cc:1238`): each key file read again, a bad one fails the build."""
    keys = [SecretKey.from_file(Path(file)) for file in secret_key_files]
    for info in infos:
        fp = fingerprint(info.path, info.info.nar_hash, info.info.nar_size, info.info.references)
        for key in keys:
            info.info.sigs.add(Signature(key.sign_fingerprint(fp)))


async def build(
    engine: StoreEngine,
    drv_path: str,
    drv: BasicDerivation,
    settings: BuildSettings,
    *,
    on_lines: Callable[[list[Output]], Awaitable[None]],
) -> LocalBuildResult:
    """Build *drv*, whose inputs are valid, and register its outputs. `unsupported(drv)` must be `None`.

    *on_lines* receives the builder's output as it comes: Nix's log lines and its `@nix` events.
    """
    layout = engine.layout
    locks = [PathLock(layout.real_store_dir / o.path.rsplit("/", 1)[-1]) for o in drv.outputs.values() if o.path]
    for lock in locks:
        # Not cancellable: an abandoned `flock` would hold the lock with no owner to release it.
        await anyio.to_thread.run_sync(lock.acquire)
    paths: _Paths | None = None
    try:
        valid = {o.path for o in drv.outputs.values() if await engine.is_valid_path_uncached(o.path)}
        if valid == {o.path for o in drv.outputs.values()}:
            return LocalBuildResult(BuildResultStatus.ALREADY_VALID)
        inputs = {str(p) for p in await engine.query_closure(sorted(str(p) for p in drv.input_srcs))}
        # An invalid path drops out of the closure, as Nix ignores `InvalidPath` here.
        sandbox_closure = {
            str(p) for p in await engine.query_closure(sorted(_sandbox_store_paths(settings, str(layout.store_dir))))
        }
        paths = await anyio.to_thread.run_sync(_prepare, engine, drv_path, settings)
        fixed = any(o.kind is OutputKind.CA_FIXED for o in drv.outputs.values())
        env = build_env(
            drv.env,
            store_dir=str(layout.store_dir),
            build_cores=settings.build_cores,
            rewrites={},
            fixed_output=fixed,
            impure_source=os.environ,
        )
        if env is None:
            raise ValueError(f"pynixd cannot build the environment of '{drv_path}'")
        for name, contents in env.files.items():
            (paths.tmp / name).write_text(contents)
        spec: ChildSpec = {
            "builder": drv.builder,
            "args": list(drv.args),
            "env": env.variables,
            "root": str(paths.root),
            "binds": await anyio.to_thread.run_sync(
                _binds, engine, paths, inputs, sandbox_closure, settings, not fixed
            ),
            "private_network": not fixed,
            "sandbox_uid": SANDBOX_UID,
            "sandbox_gid": SANDBOX_GID,
            "work_dir": SANDBOX_BUILD_DIR,
            "shm_size": settings.sandbox_shm_size,
            "no_new_privileges": not settings.allow_new_privileges,
            "filter_syscalls": settings.filter_syscalls,
        }
        if os.path.exists(HOME_DIR):
            raise ValueError(f"home directory '{HOME_DIR}' exists; please remove it to assure purity of builds")
        start = int(time.time())
        log = bytearray()
        lines = BuildLog(settings.log_lines)

        async def keep(piece: bytes) -> None:
            log.extend(piece)
            await on_lines(lines.feed(piece))

        failure = await _run(paths, spec, settings, drv_path, keep)
        stop = int(time.time())
        # Nix flushes at the builder's end of file, which a timeout never reaches.
        if not (isinstance(failure, BuildFailure) and failure.status is BuildResultStatus.TIMED_OUT):
            await on_lines(lines.flush())
        if log:
            await engine.add_build_log(drv_path.rsplit("/", 1)[-1], bytes(log))
        if isinstance(failure, _BuilderFailed):
            disk_full = await anyio.to_thread.run_sync(_disk_full, layout.real_store_dir, paths.tmp)
            failure = BuildFailure(
                BuildResultStatus.TRANSIENT_FAILURE if fixed or disk_full else BuildResultStatus.PERMANENT_FAILURE,
                builder_failure_message(drv_path, drv, failure.reason, list(lines.tail), settings, disk_full=disk_full),
            )
        if failure is not None:
            return LocalBuildResult(failure.status, failure.message, start, stop)
        try:
            collected = await anyio.to_thread.run_sync(
                lambda: collect_outputs(
                    drv_path,
                    drv.outputs,
                    drv_name=_drv_name(drv_path),
                    chroot_root=paths.root,
                    real_store_dir=layout.real_store_dir,
                    store_dir=str(layout.store_dir),
                    input_paths=inputs,
                    valid=valid,
                    is_valid=lambda path: anyio.from_thread.run(engine.is_valid_path_uncached, path),
                )
            )
        except BuildFailure as e:
            return LocalBuildResult(e.status, e.message, start, stop)
        try:
            await anyio.to_thread.run_sync(_sign, collected.infos.values(), settings.secret_key_files)
            if collected.failure is not None:
                await engine.register_valid_paths(collected.early)
                return LocalBuildResult(collected.failure.status, collected.failure.message, start, stop)
            await engine.register_valid_paths(collected.infos.values())
        finally:
            collected.release()
        return LocalBuildResult(BuildResultStatus.BUILT, "", start, stop, collected.infos)
    finally:
        with anyio.CancelScope(shield=True):
            if paths is not None:
                await anyio.to_thread.run_sync(paths.remove)
            for lock in locks:
                lock.release(delete=True)


async def _run(
    paths: _Paths,
    spec: ChildSpec,
    settings: BuildSettings,
    drv_path: str,
    on_log: Callable[[bytes], Awaitable[None]],
) -> BuildFailure | _BuilderFailed | None:
    """Run the helper on a pseudoterminal, as `startBuild` does, and wait under the two timeouts."""
    spec_file = paths.top / "spec.json"
    spec_file.write_text(json.dumps(spec))
    master, slave = os.openpty()
    status_read, status_write = os.pipe()
    try:
        process = await anyio.open_process(
            [sys.executable, "-m", "pynixd_store.builder.child", str(spec_file), str(status_write)],
            stdin=None,
            stdout=slave,
            stderr=slave,
            pass_fds=(status_write,),
        )
    finally:
        os.close(slave)
        os.close(status_write)
    timed_out: str | None = None
    try:
        with anyio.move_on_after(settings.timeout or None) as overall:
            while True:
                with anyio.move_on_after(settings.max_silent_time or None) as silence:
                    await anyio.wait_readable(master)
                if silence.cancelled_caught:
                    timed_out = f"timed out after {settings.max_silent_time} seconds of silence"
                    break
                try:
                    piece = os.read(master, 64 * 1024)
                except OSError as e:
                    if e.errno != errno.EIO:
                        raise
                    piece = b""
                if not piece:
                    break
                await on_log(piece)
        if overall.cancelled_caught:
            timed_out = f"timed out after {settings.timeout} seconds"
    finally:
        with anyio.CancelScope(shield=True):
            if timed_out is not None or process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    if timed_out is not None:
                        process.kill()
            await process.wait()
            os.close(master)
    with os.fdopen(status_read, "rb") as status_file:
        setup_error = status_file.read().decode(errors="surrogateescape")
    if timed_out is not None:
        return BuildFailure(BuildResultStatus.TIMED_OUT, f"building '{magenta(drv_path)}' {timed_out}")
    if setup_error:
        return BuildFailure(BuildResultStatus.MISC_FAILURE, f"while setting up the build environment: {setup_error}")
    code = process.returncode
    if code is not None and code < 0:
        return _BuilderFailed(f"failed due to signal {-code} ({signal.strsignal(-code)})")
    if code:
        return _BuilderFailed(f"failed with exit code {code}")
    return None


def _disk_full(*paths: Path) -> bool:
    return any(_free_bytes(path) < DISK_FULL_BYTES for path in paths)


def _free_bytes(path: Path) -> int:
    try:
        st = os.statvfs(path)
    except OSError:
        return DISK_FULL_BYTES
    return st.f_bavail * st.f_bsize


def builder_failure_message(
    drv_path: str, drv: BasicDerivation, reason: str, tail: list[str], settings: BuildSettings, *, disk_full: bool
) -> str:
    """`fixupBuilderFailureErrorMessage` (`derivation-building-goal.cc:1157`).

    The daemon's logger is never verbose, so the tail always shows.
    """
    msg = f"Cannot build '{magenta(drv_path)}'.\nReason: {ANSI_RED}builder {reason}{ANSI_NORMAL}."
    outputs = sorted(o.path for o in drv.outputs.values() if o.path)
    if outputs:
        msg += "\nOutput paths:" + "".join(f"\n  {magenta(path)}" for path in outputs)
    if tail:
        msg += f"\nLast {len(tail)} log lines:\n" + "".join(f"> {line}\n" for line in tail)
        command = "nix log" if settings.nix_command else "nix-store -l"
        msg += f"For full logs, run:\n  {ANSI_BOLD}{command} {drv_path}{ANSI_NORMAL}"
    if disk_full:
        msg += "\nnote: build failure may have been caused by lack of free disk space"
    return msg
