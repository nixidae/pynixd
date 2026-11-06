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
import socket
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
from anyio.abc import SocketStream

from nix_daemon_protocol.basic_derivation import bool_attr
from nix_daemon_protocol.build_result import BuildResultStatus
from nix_daemon_protocol.derivation_output import OutputKind
from nix_daemon_protocol.signature import Signature

from .. import optimise
from ..add import PathLock, delete_path
from ..content_address import Method, fixed_output_address, output_method
from ..hintfmt import ANSI_BOLD, ANSI_NORMAL, ANSI_RED, magenta, show_error
from ..signing import SecretKey, fingerprint
from . import buildenv
from .buildlog import BuildLog
from .env import (
    HOME_DIR,
    SANDBOX_BUILD_DIR,
    build_env,
    export_requests,
    hash_placeholder,
    rewrite_strings,
    structured_attrs_reason,
)
from .outputs import (
    BuildFailure,
    PlainError,
    check_outputs,
    collect_outputs,
    compare_outputs,
    discarded_references,
    fallback_path,
    output_checks,
    random_path,
)
from .restriction import SOCKET_NAME, RestrictionContext
from .spec import BuildenvSpec, ChildSpec, FetchSpec
from .vm import BuildVm, returncode as vm_returncode, status as vm_status, unshareable as vm_unshareable

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence

    from nix_daemon_protocol.basic_derivation import BasicDerivation
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from ..engine import StoreEngine
    from .buildlog import Output
    from .outputs import Collected
    from .restriction import RecursiveServer
    from .settings import BuildSettings

_log = structlog.get_logger(__name__)

SANDBOX_UID, SANDBOX_GID = 1000, 100
"""`sandboxUid` and `sandboxGid` without build users (`linux-derivation-builder.cc:430`)."""

DISK_FULL_BYTES = 8 * 1024 * 1024
"""Less free space than this after a failure is a full disk (`decideWhetherDiskFull`, `derivation-builder.cc:304`)."""

DEV_FILES = ("/dev/full", "/dev/null", "/dev/random", "/dev/tty", "/dev/urandom", "/dev/zero")

FETCH_SCHEMES = frozenset({"http", "https", "file"})
"""The schemes pynixd's `builtin:fetchurl` fetches."""

BUILDER_RPC = "builder-rpc-v0"
"""`drvFeatureBuilderRpcV0`: the builder submits its outputs over its daemon (`derivations.hh:24`)."""


def unsupported(drv: BasicDerivation, settings: BuildSettings, *, store_dir: str, relocated: bool) -> str | None:
    """Why pynixd leaves this build to the daemon, or `None` when it builds it.

    *relocated* is whether the store's real directory is not its logical one (`isRelocatedStore`).
    """
    if not sys.platform.startswith("linux"):
        return "pynixd builds on Linux only"
    builtin = drv.builder.startswith("builtin:")
    if builtin and (reason := _builtin_unsupported(drv, settings)) is not None:
        return reason
    # A builtin builds on any platform (`derivation-building-goal.cc:420`).
    if drv.platform != settings.system and drv.platform not in settings.extra_platforms and not builtin:
        return f"the platform {drv.platform}"
    if "__json" in drv.env and (reason := structured_attrs_reason(drv.env)) is not None:
        return reason
    # `runPostBuildHook` (`derivation-building-goal.cc:1079`) and the kept build directory
    # (`cleanupBuild`, `derivation-builder.cc:1697`, never for a builtin) are the daemon's alone.
    if settings.post_build_hook:
        return "a build with a post-build-hook"
    if settings.keep_failed and not builtin:
        return "a build with keep-failed"
    if settings.sandbox == "false" or bool_attr(drv.env, "__noChroot", default=False):
        return "a build without the sandbox"
    features = set(drv.required_system_features)
    if "uid-range" in features:
        return "the feature uid-range"
    # `startDaemon` (`derivation-builder.cc:1155`) refuses the build in the daemon's own words.
    if "recursive-nix" in features and not settings.recursive_nix:
        return "recursive-nix without the experimental feature"
    if BUILDER_RPC in features:
        if not settings.dynamic_derivations:
            return "builder-rpc-v0 without dynamic-derivations"
        # `startBuild` (`unix-derivation-builder.cc:355`) refuses an input-addressed one in Nix's words, and
        # pynixd registers realisations for a floating output alone.
        if {o.kind for o in drv.outputs.values()} != {OutputKind.CA_FLOATING}:
            return "builder-rpc-v0 with an output that is not floating"
    try:
        export_requests(drv.env, store_dir)
    except ValueError as e:
        return f"exportReferencesGraph with {e}"
    try:
        output_checks(drv.env)
        discarded_references(drv.env)
    except ValueError as e:
        return f"structured attrs with {e}"
    kinds = {o.kind for o in drv.outputs.values()}
    if OutputKind.DEFERRED in kinds:
        return "a deferred output"
    if len(kinds) != 1 or (OutputKind.CA_FIXED in kinds and len(drv.outputs) != 1):
        return f"the outputs {sorted(drv.outputs)} of kinds {sorted(k.name for k in kinds)}"
    # `makeDerivationBuilder` (`derivation-builder.cc:1777`): relaxed builds an unsandboxed type without the
    # chroot, unless the store is relocated. pynixd always builds in one.
    if settings.sandbox == "relaxed" and not relocated and kinds & {OutputKind.CA_FIXED, OutputKind.IMPURE}:
        return "an unsandboxed derivation with sandbox = relaxed"
    # `moveOutputToTempDir` (`derivation-builder.cc:1417`) restores a fixed output with fsync; pynixd's
    # restore has none, as its own writes leave this setting to the daemon (`write_router._fsync`).
    if OutputKind.CA_FIXED in kinds and settings.fsync_store_paths:
        return "a fixed output with fsync-store-paths"
    git = any(o.method and output_method(o.method)[0] is Method.GIT for o in drv.outputs.values())
    if git and not settings.git_hashing:
        return "a Git-hashed output without git-hashing"
    return None


def _url_unsupported(url: str) -> str | None:
    """A URL whose transfer pynixd leaves to the daemon: S3 needs AWS credentials resolved on the host
    (`derivation-builder.cc:622`), and `displayUri` re-renders a URL with credentials (`filetransfer.cc:1280`)."""
    scheme, sep, rest = url.partition("://")
    if not sep or scheme.lower() not in FETCH_SCHEMES:
        return f"builtin:fetchurl of the URL scheme '{scheme}'"
    authority = rest.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    if "@" in authority:
        return "builtin:fetchurl of a URL with credentials"
    return None


def _builtin_unsupported(drv: BasicDerivation, settings: BuildSettings) -> str | None:
    if drv.builder == "builtin:buildenv":
        return _buildenv_unsupported(drv)
    if drv.builder == "builtin:fetchurl":
        return _fetchurl_unsupported(drv, settings)
    if drv.builder == "builtin:unpack-channel":
        return _unpack_channel_unsupported(drv)
    return f"the builtin builder '{drv.builder}'"


def _unpack_channel_unsupported(drv: BasicDerivation) -> str | None:
    """Structured attrs hide the attributes in `__json`; an output but `out` is a `map::at` throw."""
    if "__json" in drv.env:
        return "builtin:unpack-channel with structured attrs"
    if set(drv.outputs) != {"out"}:
        return f"builtin:unpack-channel with the outputs {sorted(drv.outputs)}"
    return None


def _buildenv_unsupported(drv: BasicDerivation) -> str | None:
    """Structured attrs hide `derivations` in `__json`; a malformed one is a `stoi` throw or undefined behaviour."""
    if "__json" in drv.env:
        return "builtin:buildenv with structured attrs"
    if set(drv.outputs) != {"out"}:
        return f"builtin:buildenv with the outputs {sorted(drv.outputs)}"
    if "derivations" in drv.env and buildenv.packages(drv.env["derivations"]) is None:
        return "builtin:buildenv with derivations that Nix cannot read"
    return None


def _buildenv_spec(drv: BasicDerivation, out: str) -> BuildenvSpec:
    """`builtinBuildenv` reads the derivation's own attributes, not the builder's environment (`buildenv.cc:175`)."""
    derivations = drv.env.get("derivations")
    return {
        "out": out,
        "packages": None if derivations is None else buildenv.packages(derivations),
        "manifest": drv.env.get("manifest"),
    }


def _fetchurl_unsupported(drv: BasicDerivation, settings: BuildSettings) -> str | None:
    """Why the daemon runs this builtin. Nix's own refusals (`builtins/fetchurl.cc:25`) stay there too."""
    if "__json" in drv.env:
        return "builtin:fetchurl with structured attrs"
    out = drv.outputs.get("out")
    if out is None or out.kind not in (OutputKind.CA_FIXED, OutputKind.IMPURE):
        return "builtin:fetchurl with no fixed or impure output 'out'"
    if "url" not in drv.env:
        return "builtin:fetchurl with no url"
    for url in (drv.env["url"], *(_mirror_urls(drv, settings))):
        if (reason := _url_unsupported(url)) is not None:
            return reason
    return None


def _mirror_urls(drv: BasicDerivation, settings: BuildSettings) -> list[str]:
    """The hashed mirrors `builtinFetchurl` tries for a flat fixed output (`builtins/fetchurl.cc:76`)."""
    out = drv.outputs["out"]
    if out.kind is not OutputKind.CA_FIXED or not out.method:
        return []
    method, algo = output_method(out.method)
    if method not in (Method.FLAT, Method.TEXT):
        return []
    digest = fixed_output_address(out.method, out.hash_digest).digest.hex()
    return [f"{m if m.endswith('/') else m + '/'}{algo}/{digest}" for m in settings.transfer.hashed_mirrors]


def _read_or_empty(path: str | None) -> str:
    """`readFile` with `SystemError` ignored, as `runChild` reads netrc and the CA file (`:956`)."""
    if path is None:
        return ""
    try:
        return Path(path).read_bytes().decode("utf-8", "surrogateescape")
    except OSError:
        return ""


def _fetch_spec(drv: BasicDerivation, settings: BuildSettings, out: str) -> FetchSpec:
    transfer = settings.transfer
    return {
        "main_url": drv.env["url"],
        "mirrors": _mirror_urls(drv, settings),
        "out": out,
        "unpack": drv.env.get("unpack", "") == "1",
        "executable": drv.env.get("executable") == "1",
        "tmp_dir": SANDBOX_BUILD_DIR,
        "netrc_data": _read_or_empty(transfer.netrc_file),
        "netrc_file": transfer.netrc_file,
        "ca_data": _read_or_empty(transfer.ssl_cert_file),
        "ca_file": "",
        "nix_version": settings.nix_version,
        "user_agent_suffix": transfer.user_agent_suffix,
        "http2": transfer.http2,
        "http3": transfer.http3,
        "connect_timeout": transfer.connect_timeout,
        "stalled_download_timeout": transfer.stalled_download_timeout,
        "download_speed": transfer.download_speed,
        "tries": transfer.tries,
        "retry_delay_ms": transfer.retry_delay_ms,
        "retry_delay_rate_limited_ms": transfer.retry_delay_rate_limited_ms,
        "retry_max_delay_ms": transfer.retry_max_delay_ms,
        "retry_jitter": transfer.retry_jitter,
    }


@dataclass
class LocalBuildResult:
    status: BuildResultStatus
    error_msg: str = ""
    start_time: int = 0
    stop_time: int = 0
    outputs: dict[str, ValidPathInfo] = field(default_factory=dict)
    paths: dict[str, str] = field(default_factory=dict)
    """The path of each output, on success: built, or valid before."""


@dataclass
class _BuilderFailed:
    """The builder exited non-zero: `statusToString` without the leading word (`processes.cc:378`)."""

    reason: str


@dataclass(frozen=True)
class _Guest:
    """A build that runs in a microVM, and the build store paths it sees."""

    vm: BuildVm
    real_store_dir: Path
    visible: frozenset[str]
    sources: tuple[str, ...]
    """The `sandbox-paths` sources the helper binds."""


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


async def _export_references(
    engine: StoreEngine, requests: Mapping[str, Sequence[str]], inputs: set[str]
) -> dict[str, list[ValidPathInfo]]:
    """`Store::exportReferences` (`store-api.cc:830`) of each name's paths, sorted as a `StorePathSet`."""
    exported: dict[str, list[ValidPathInfo]] = {}
    for name, requested in requests.items():
        for path in requested:
            if path not in inputs:
                raise BuildFailure(
                    BuildResultStatus.INPUT_REJECTED,
                    f"cannot export references of path '{magenta(path)}' because it is not in the input closure "
                    "of the derivation",
                )
        paths = {p.path for p in await engine.query_closure(requested)}
        outputs: set[str] = set()
        for drv_path in sorted(p for p in paths if p.endswith(".drv")):
            drv = await engine.read_derivation(drv_path)
            if drv is None:
                raise ValueError(f"cannot read derivation '{drv_path}'")
            for output in drv.outputs:
                if not output.path:
                    raise PlainError("exportReferences on CA derivations is not yet implemented")
                outputs.add(output.path)
        # `computeFSClosure` throws `InvalidPath` where `query_closure` drops the path.
        if missing := sorted(outputs - {p.path for p in await engine.query_valid_paths(outputs)}):
            raise PlainError(f"path '{magenta(missing[0])}' is not valid")
        paths |= {p.path for p in await engine.query_closure(sorted(outputs))}
        exported[name] = sorted(await engine.query_path_infos(sorted(paths)), key=lambda info: info.path.path)
    return exported


def _optimise(real_store_dir: Path, links_dir: Path, names: Iterable[str]) -> None:
    """`LocalStore::optimisePath` (`optimise-store.cc:331`): a fresh inode set for each path."""
    links_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        optimise.optimise_path(
            real_store_dir / name,
            set(),
            real_store_dir=real_store_dir,
            links_dir=links_dir,
            stats=optimise.OptimiseStats(),
        )


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
    realised: Mapping[str, str],
    check: bool = False,
    repair: bool = False,
    recursive: RecursiveServer | None = None,
    vm: BuildVm | None = None,
) -> LocalBuildResult:
    """Build *drv*, whose inputs are valid, and register its outputs. `unsupported(drv)` must be `None`.

    *on_lines* receives the builder's output as it comes: Nix's log lines and its `@nix` events.
    *realised* is the path of each output the store holds a realisation for (`checkPathValidity`,
    `derivation-building-goal.cc:1329`). With *check*, each output is valid, and the build
    compares what it makes with it and registers nothing new. With *repair*, an output is valid only when
    its contents match its registered hash (`derivation-building-goal.cc:1334`); the build replaces the rest.
    *recursive* serves the builder of a `recursive-nix` or `builder-rpc-v0` derivation; it must be given for one.
    With *vm*, a sandboxed build with no builder socket runs in a microVM, and the helper inside it.
    """
    layout = engine.layout
    store_dir = str(layout.store_dir)
    drv_name = _drv_name(drv_path)
    impure = any(o.kind is OutputKind.IMPURE for o in drv.outputs.values())
    known = {
        name: random_path(name, drv_name, store_dir) if impure else realised.get(name, o.path)
        for name, o in drv.outputs.items()
    }
    locks = [PathLock(layout.real_store_dir / path.rsplit("/", 1)[-1]) for path in known.values() if path]
    for lock in locks:
        # Not cancellable: an abandoned `flock` would hold the lock with no owner to release it.
        await anyio.to_thread.run_sync(lock.acquire)
    paths: _Paths | None = None
    try:
        valid = {
            path
            for path in known.values()
            if path and await engine.is_valid_path_uncached(path) and (not repair or await engine.contents_good(path))
        }
        if valid == set(known.values()) and not check:
            return LocalBuildResult(BuildResultStatus.ALREADY_VALID, paths=known)
        # `derivation-building-goal.cc:503`: an output on disk that is not valid goes before the build.
        for path in known.values():
            if path and path not in valid:
                await anyio.to_thread.run_sync(delete_path, layout.real_store_dir / path.rsplit("/", 1)[-1])
        # The sandbox needs no hash rewrite, so a known path is its own scratch path (`derivation-builder.cc:417`).
        scratch = {name: path or fallback_path(drv_path, name, drv_name, store_dir) for name, path in known.items()}
        inputs = {str(p) for p in await engine.query_closure(sorted(str(p) for p in drv.input_srcs))}
        # An invalid path drops out of the closure, as Nix ignores `InvalidPath` here.
        sandbox_closure = {
            str(p) for p in await engine.query_closure(sorted(_sandbox_store_paths(settings, str(layout.store_dir))))
        }
        try:
            exported = await _export_references(engine, export_requests(drv.env, store_dir), inputs)
        except BuildFailure as e:
            return LocalBuildResult(e.status, e.message)
        except PlainError as e:
            return LocalBuildResult(BuildResultStatus.MISC_FAILURE, show_error(str(e)))
        paths = await anyio.to_thread.run_sync(_prepare, engine, drv_path, settings)
        fixed = any(o.kind is OutputKind.CA_FIXED for o in drv.outputs.values())
        sandboxed = not (fixed or impure)
        # `inputRewrites` (`derivation-builder.cc:435`): the sandbox builds each output at its scratch path.
        rewrites = {hash_placeholder(name): path for name, path in scratch.items()}
        env = build_env(
            drv.env,
            outputs=drv.outputs,
            store_dir=store_dir,
            build_cores=settings.build_cores,
            rewrites=rewrites,
            fixed_output=fixed,
            sandboxed=sandboxed,
            impure_source={**os.environ, **settings.impure_env},
            exported=exported,
        )
        for name, contents in env.files.items():
            (paths.tmp / name).write_text(contents, encoding="utf-8")
        fetch = (
            await anyio.to_thread.run_sync(_fetch_spec, drv, settings, scratch["out"])
            if drv.builder == "builtin:fetchurl"
            else None
        )
        spec: ChildSpec = {
            "builder": drv.builder,
            "args": [rewrite_strings(arg, rewrites) for arg in drv.args],
            "env": env.variables,
            "root": str(paths.root),
            "binds": await anyio.to_thread.run_sync(
                _binds, engine, paths, inputs, sandbox_closure, settings, sandboxed
            ),
            "private_network": sandboxed,
            "sandbox_uid": SANDBOX_UID,
            "sandbox_gid": SANDBOX_GID,
            "work_dir": SANDBOX_BUILD_DIR,
            "shm_size": settings.sandbox_shm_size,
            "no_new_privileges": not settings.allow_new_privileges,
            "filter_syscalls": settings.filter_syscalls,
            "fetchurl": fetch,
            "buildenv": _buildenv_spec(drv, scratch["out"]) if drv.builder == "builtin:buildenv" else None,
            "unpack_channel": (
                {"out": scratch["out"], "channel_name": drv.env.get("channelName"), "src": drv.env.get("src")}
                if drv.builder == "builtin:unpack-channel"
                else None
            ),
            "store_dir": store_dir,
        }
        if os.path.exists(HOME_DIR):
            raise ValueError(f"home directory '{HOME_DIR}' exists; please remove it to assure purity of builds")
        start = int(time.time())
        log = bytearray()
        lines = BuildLog(settings.log_lines)

        async def keep(piece: bytes) -> None:
            log.extend(piece)
            await on_lines(lines.feed(piece))

        control: socket.socket | None = None
        restriction: RestrictionContext | None = None
        submitting = BUILDER_RPC in drv.required_system_features
        async with contextlib.AsyncExitStack() as stack:
            if submitting or "recursive-nix" in drv.required_system_features:
                if recursive is None:
                    raise ValueError("a recursive-nix or builder-rpc-v0 build needs a server for its builder")
                ours, control = socket.socketpair()
                stack.callback(control.close)
                stream = await stack.enter_async_context(await SocketStream.from_socket(ours))
                restriction = RestrictionContext(inputs, stream, layout.real_store_dir, submitting=submitting)
                # `startDaemon` (`derivation-builder.cc:1171`): the socket is up before the builder starts.
                spec["env"]["NIX_REMOTE"] = f"unix://{SANDBOX_BUILD_DIR}/{SOCKET_NAME}"
                listener = await anyio.to_thread.run_sync(_listen, paths.tmp / SOCKET_NAME)
                await stack.enter_async_context(recursive(listener, restriction))
            guest: _Guest | None = None
            if vm is not None and sandboxed and control is None:
                # The guest has its own `/dev` and `/proc`.
                sources = tuple(
                    p.source for p in settings.sandbox_paths if not p.source.startswith(("/dev/", "/proc/"))
                )
                if (file := vm_unshareable(sources, layout.real_store_dir)) is not None:
                    return LocalBuildResult(
                        BuildResultStatus.MISC_FAILURE,
                        f"sandbox-paths names the file '{magenta(file)}', and the build VM shares only directories",
                    )
                guest = _Guest(vm, layout.real_store_dir, frozenset(inputs | sandbox_closure), sources)
            failure = await _run(paths, spec, settings, drv_path, keep, control, guest)
        if restriction is not None:
            # The paths the builder added may be referenced (`derivation-builder.cc:1434`).
            inputs |= restriction.added
        stop = int(time.time())
        # Nix flushes at the builder's end of file, which a timeout never reaches.
        if not (isinstance(failure, BuildFailure) and failure.status is BuildResultStatus.TIMED_OUT):
            await on_lines(lines.flush())
        if log:
            await engine.add_build_log(drv_path.rsplit("/", 1)[-1], bytes(log))
        if isinstance(failure, _BuilderFailed):
            disk_full = await anyio.to_thread.run_sync(_disk_full, layout.real_store_dir, paths.tmp)
            failure = BuildFailure(
                BuildResultStatus.TRANSIENT_FAILURE
                if not sandboxed or disk_full
                else BuildResultStatus.PERMANENT_FAILURE,
                builder_failure_message(drv_path, drv, failure.reason, list(lines.tail), settings, disk_full=disk_full),
            )
        if failure is not None:
            return LocalBuildResult(failure.status, failure.message, start, stop)
        if restriction is not None and restriction.submitting:
            return await _check_submitted(engine, drv_path, drv, restriction.submitted, start, stop)
        try:
            collected = await anyio.to_thread.run_sync(
                lambda: collect_outputs(
                    drv_path,
                    drv.outputs,
                    scratch=scratch,
                    drv_name=drv_name,
                    chroot_root=paths.root,
                    real_store_dir=layout.real_store_dir,
                    store_dir=store_dir,
                    input_paths=inputs,
                    # `derivation-builder.cc:1131`: a check looks at every output.
                    valid=set() if check else valid,
                    is_valid=lambda path: anyio.from_thread.run(engine.is_valid_path_uncached, path),
                    discard=discarded_references(drv.env),
                    check=check,
                )
            )
        except BuildFailure as e:
            return LocalBuildResult(e.status, e.message, start, stop)
        except PlainError as e:
            return LocalBuildResult(BuildResultStatus.MISC_FAILURE, show_error(str(e)))
        try:
            if check:
                return await _compare(engine, drv_path, drv, collected, settings, known, start, stop)
            if settings.auto_optimise_store:
                # `registerOutputs` (`derivation-builder.cc:1600`) skips an output that is valid already.
                fresh = [
                    info.path.path.rsplit("/", 1)[-1]
                    for info in collected.infos.values()
                    if not await engine.is_valid_path_uncached(info.path.path)
                ]
                await anyio.to_thread.run_sync(_optimise, layout.real_store_dir, layout.links_dir, fresh)
            await anyio.to_thread.run_sync(_sign, collected.infos.values(), settings.secret_key_files)
            failure = collected.failure or await anyio.to_thread.run_sync(
                lambda: check_outputs(
                    drv_path,
                    output_checks(drv.env),
                    collected.infos,
                    store_dir=store_dir,
                    path_info=lambda path: anyio.from_thread.run(engine.query_path_info, path),
                )
            )
            if failure is not None:
                await engine.register_valid_paths(collected.early)
                return LocalBuildResult(failure.status, failure.message, start, stop)
            await engine.register_valid_paths(collected.infos.values())
        finally:
            collected.release()
        built = {name: info.path.path for name, info in collected.infos.items()}
        return LocalBuildResult(BuildResultStatus.BUILT, "", start, stop, collected.infos, known | built)
    finally:
        with anyio.CancelScope(shield=True):
            if paths is not None:
                await anyio.to_thread.run_sync(paths.remove)
            for lock in locks:
                lock.release(delete=True)


async def _check_submitted(
    engine: StoreEngine, drv_path: str, drv: BasicDerivation, submitted: Mapping[str, str], start: int, stop: int
) -> LocalBuildResult:
    """`checkSubmittedOutputs` (`derivation-builder-impl.cc:715`) up to the realisations, which the caller registers.

    The builder added each output already, so nothing moves and nothing is signed: a content-addressed output
    needs only its realisation signed.
    """
    infos: dict[str, ValidPathInfo] = {}
    for name, path in submitted.items():
        info = await engine.query_path_info(path)
        if info is None:
            return LocalBuildResult(
                BuildResultStatus.MISC_FAILURE, show_error(f"path '{magenta(path)}' is not valid"), start, stop
            )
        infos[name] = info
    failure = await anyio.to_thread.run_sync(
        lambda: check_outputs(
            drv_path,
            output_checks(drv.env),
            infos,
            store_dir=str(engine.layout.store_dir),
            path_info=lambda path: anyio.from_thread.run(engine.query_path_info, path),
        )
    )
    if failure is not None:
        return LocalBuildResult(failure.status, failure.message, start, stop)
    for name in drv.outputs:
        if name not in submitted:
            return LocalBuildResult(
                BuildResultStatus.OUTPUT_REJECTED,
                f"builder for '{magenta(drv_path)}' failed to submit output path for '{magenta(name)}'",
                start,
                stop,
            )
    built = {name: infos[name] for name in drv.outputs}
    return LocalBuildResult(
        BuildResultStatus.BUILT, "", start, stop, built, {name: info.path.path for name, info in built.items()}
    )


async def _compare(
    engine: StoreEngine,
    drv_path: str,
    drv: BasicDerivation,
    collected: Collected,
    settings: BuildSettings,
    known: dict[str, str],
    start: int,
    stop: int,
) -> LocalBuildResult:
    """The end of `registerOutputs` in check mode (`derivation-builder.cc:1546`, `:1633`).

    A valid output the check verified becomes `ultimate` and is signed again, even when a later one differs.
    """
    registered = {path: await engine.query_path_info(path) for path in known.values()}
    failure, verified = compare_outputs(drv_path, collected.infos, registered, engine.layout.real_store_dir)
    if verified:
        await anyio.to_thread.run_sync(_sign, verified, settings.secret_key_files)
        await engine.register_valid_paths(verified)
    failure = (
        failure
        or collected.failure
        or await anyio.to_thread.run_sync(
            lambda: check_outputs(
                drv_path,
                output_checks(drv.env),
                collected.infos,
                store_dir=str(engine.layout.store_dir),
                path_info=lambda path: anyio.from_thread.run(engine.query_path_info, path),
            )
        )
    )
    if failure is not None:
        return LocalBuildResult(failure.status, failure.message, start, stop)
    return LocalBuildResult(BuildResultStatus.BUILT, "", start, stop, paths=known)


async def _run(
    paths: _Paths,
    spec: ChildSpec,
    settings: BuildSettings,
    drv_path: str,
    on_log: Callable[[bytes], Awaitable[None]],
    control: socket.socket | None = None,
    guest: _Guest | None = None,
) -> BuildFailure | _BuilderFailed | None:
    """Run the helper on a pseudoterminal, as `startBuild` does, and wait under the two timeouts.

    *control* is the helper's end of the socket it binds added paths on; it closes here once the helper has it.
    With *guest*, the helper runs in a microVM, which reports through files in the top directory.
    """
    spec_file = paths.top / "spec.json"
    spec_file.write_text(json.dumps(spec))
    master, slave = os.openpty()
    if guest is not None:
        status_read = status_write = -1
        argv = guest.vm.argv(
            top=paths.top,
            chroot_parent=paths.chroot_parent,
            real_store_dir=guest.real_store_dir,
            visible=guest.visible,
            sources=guest.sources,
            spec_file=spec_file,
        )
        fds: list[int] = []
    else:
        status_read, status_write = os.pipe()
        argv = [sys.executable, "-m", "pynixd_store.builder.child", str(spec_file), str(status_write)]
        fds = [status_write]
    if control is not None:
        argv.append(str(control.fileno()))
        fds.append(control.fileno())
    try:
        process = await anyio.open_process(argv, stdin=None, stdout=slave, stderr=slave, pass_fds=fds)
    finally:
        os.close(slave)
        if status_write >= 0:
            os.close(status_write)
        if control is not None:
            control.close()
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
    if guest is not None:
        setup_error = vm_status(paths.top)
    else:
        with os.fdopen(status_read, "rb") as status_file:
            setup_error = status_file.read().decode(errors="surrogateescape")
    if timed_out is not None:
        return BuildFailure(BuildResultStatus.TIMED_OUT, f"building '{magenta(drv_path)}' {timed_out}")
    if setup_error:
        return BuildFailure(BuildResultStatus.MISC_FAILURE, f"while setting up the build environment: {setup_error}")
    if guest is not None:
        code = vm_returncode(paths.top)
        if code is None:
            return BuildFailure(
                BuildResultStatus.MISC_FAILURE,
                f"the build VM of '{magenta(drv_path)}' stopped before its builder ended "
                f"(pynixd-krun exited with {process.returncode})",
            )
        return _builder_ended(code)
    return _builder_ended(process.returncode)


def _builder_ended(code: int | None) -> _BuilderFailed | None:
    if code is not None and code < 0:
        return _BuilderFailed(f"failed due to signal {-code} ({signal.strsignal(-code)})")
    if code:
        return _BuilderFailed(f"failed with exit code {code}")
    return None


def _listen(path: Path) -> socket.socket:
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
