"""A local build in pynixd's sandbox: `DerivationBuildingGoal` and `DerivationBuilderImpl` for one derivation.

`pynixd/docs/design/builder.md` holds the scope. `unsupported` names what the
daemon still builds; the caller sends those there before anything starts.
"""

from __future__ import annotations

import contextlib
import errno
import functools
import json
import os
import signal
import socket
import sys
import tempfile
import termios
import time
import tty
from dataclasses import dataclass, field, replace
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
from nix_daemon_protocol.protocol import Verbosity
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.store_path import StorePath

from .. import optimise
from ..add import PathLock, delete_path, sync_store_path
from ..content_address import Method, fixed_output_address, output_method
from ..hintfmt import ANSI_BOLD, ANSI_NORMAL, ANSI_RED, magenta, missing_feature, show_error
from ..signing import SecretKey, fingerprint
from . import buildenv, cgroup as build_cgroup, hooks, users as build_users
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
from .jsonlog import Message
from .outputs import (
    BuildFailure,
    PlainError,
    check_outputs,
    collect_outputs,
    compare_outputs,
    discarded_references,
    fallback_path,
    fallback_path_of,
    output_checks,
    random_path,
)
from .restriction import SOCKET_NAME, RestrictionContext
from .spec import BuildenvSpec, ChildSpec, FetchSpec
from .usage import USAGE_NAME, read_usage, with_host
from .vm import (
    BUILDER_SOCKET_NAME as VM_BUILDER_SOCKET_NAME,
    CONTROL_SOCKET_NAME as VM_CONTROL_SOCKET_NAME,
    VMM_USAGE_NAME,
    BuildVm,
    allow as vm_allow,
    returncode as vm_returncode,
    status as vm_status,
    unshareable as vm_unshareable,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence

    from nix_daemon_protocol.basic_derivation import BasicDerivation
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from ..engine import StoreEngine
    from .buildlog import Output
    from .outputs import Collected
    from .restriction import RecursiveServer
    from .settings import BuildSettings
    from .usage import BuildUsage, UsageSample

_log = structlog.get_logger(__name__)

SANDBOX_UID, SANDBOX_GID = 1000, 100
"""`sandboxUid` and `sandboxGid` without build users (`linux-derivation-builder.cc:430`)."""

USER_POLL_SECONDS = 0.5
"""How often a build that found every build user busy asks again; Nix's goal waits too (`waitForAWhile`)."""

DISK_FULL_BYTES = 8 * 1024 * 1024
"""Less free space than this after a failure is a full disk (`decideWhetherDiskFull`, `derivation-builder.cc:304`)."""

DEV_FILES = ("/dev/full", "/dev/null", "/dev/random", "/dev/tty", "/dev/urandom", "/dev/zero")

BUILDER_RPC = "builder-rpc-v0"
"""`drvFeatureBuilderRpcV0`: the builder submits its outputs over its daemon (`derivations.hh:24`)."""


def uses_sandbox(drv: BasicDerivation, settings: BuildSettings, *, relocated: bool) -> bool:
    """`makeDerivationBuilder` (`unix/build/derivation-builder.cc:1751`): whether the build gets the chroot.

    *relocated* is `isRelocatedStore`: a store whose real directory is not its logical one always sandboxes.
    `build` refuses `__noChroot` under `sandbox = true` first, as Nix does.
    """
    if relocated or settings.sandbox == "true":
        return True
    if settings.sandbox == "false":
        return False
    return not bool_attr(drv.env, "__noChroot", default=False) and all(
        o.kind not in (OutputKind.CA_FIXED, OutputKind.IMPURE) for o in drv.outputs.values()
    )


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
    features = set(drv.required_system_features)
    # NIX-DEFECT (#23): `unix/user-lock.cc:177` checks `start-id` and `id-count`, settings from
    # `nix.conf`, with `assert`, so a typo aborts the process; an error naming the setting is what
    # it should be. pynixd sends the build to the daemon and copies no abort; Lillecarl/nix#395.
    if (
        settings.auto_allocate_uids
        and settings.auto_allocate_feature
        and build_users.use_build_users(settings.build_users_group, True)
        and (
            settings.start_id <= 0
            or settings.id_count % build_users.IDS_PER_BUILD
            or settings.start_id + settings.id_count > 0xFFFFFFFF
        )
    ):
        return "auto-allocate-uids with start-id or id-count that Nix asserts on"
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
    git = any(o.method and output_method(o.method)[0] is Method.GIT for o in drv.outputs.values())
    if git and not settings.git_hashing:
        return "a Git-hashed output without git-hashing"
    return None


def _builder_refusal(drv_path: StorePath, drv: BasicDerivation, settings: BuildSettings, *, chroot: bool) -> str | None:
    """What `makeDerivationBuilder` (`derivation-builder.cc:1751`) throws before the build starts, or `None`."""
    if settings.sandbox == "true" and bool_attr(drv.env, "__noChroot", default=False):
        return f"derivation '{magenta(drv_path)}' has '__noChroot' set, but that's not allowed when 'sandbox' is 'true'"
    if not chroot and "uid-range" in drv.required_system_features:
        return "feature 'uid-range' is only supported in sandboxed builds"
    return None


def _user_refusal(settings: BuildSettings, *, auto: bool) -> str | None:
    """What `AutoUserLock::acquire` (`user-lock.cc:176`) throws before it takes a user, or `None`."""
    if auto and not settings.auto_allocate_feature:
        return missing_feature("auto-allocate-uids")
    return None


def _chroot_refusal(
    drv: BasicDerivation, settings: BuildSettings, user: build_users.BuildUser | None, *, chroot: bool
) -> str | None:
    """What `prepareUser` (`linux-derivation-builder.cc:448`) and `setupBuildChroot` (`chroot.cc:52`) throw."""
    if chroot and user is not None and user.count != 1 and not settings.cgroups_feature:
        return missing_feature("cgroups")
    if chroot and "uid-range" in drv.required_system_features and (user is None or user.count < 65536):
        return f"feature 'uid-range' requires the setting '{magenta('auto-allocate-uids')}' to be enabled"
    return None


def rejection(drv_path: StorePath, drv: BasicDerivation, settings: BuildSettings) -> str | None:
    """`reject` (`derivation-building-goal.cc:289`): why this store cannot build *drv*, in Nix's words, or `None`.

    Each reason that applies adds its lines, in Nix's order. The hook comes first (`:620`), so a caller asks
    `hands_off` before it answers with this.
    """
    reasons = ""
    if settings.max_jobs == 0:
        reasons += (
            f"\nReason: {ANSI_RED}local builds are disabled{ANSI_NORMAL} (max-jobs = 0)"
            "\nHint: set 'max-jobs' to a non-zero value to enable local builds, "
            "or configure remote builders via 'builders'"
        )
    foreign = drv.platform != settings.system and drv.platform not in settings.extra_platforms
    if foreign and not drv.builder.startswith("builtin:"):
        reasons += (
            f"\nReason: {ANSI_RED}platform mismatch{ANSI_NORMAL}"
            f"\nRequired system: '{magenta(drv.platform)}'\nCurrent system: '{magenta(settings.system)}'"
        )
    required = set(drv.required_system_features)
    # `getRequiredSystemFeatures` (`derivation-options.cc:361`): an output path not known yet needs the feature.
    if any(o.kind in (OutputKind.CA_FLOATING, OutputKind.IMPURE, OutputKind.DEFERRED) for o in drv.outputs.values()):
        required.add("ca-derivations")
    if not required <= settings.system_features:
        reasons += (
            f"\nReason: {ANSI_RED}missing system features{ANSI_NORMAL}"
            f"\nRequired features: {{{', '.join(sorted(required))}}}"
            f"\nAvailable features: {{{', '.join(sorted(settings.system_features))}}}"
        )
    if not reasons:
        return None
    return f"Cannot build '{magenta(drv_path)}'.{reasons}"


def hands_off(settings: BuildSettings) -> bool:
    """Whether the daemon would offer a build to a builder before it refused it.

    `findExternalDerivationBuilderIfSupported` (`derivation-building-goal.cc:411`) takes it first. Else the
    hook declines at once with no machines (`build-remote.cc:116`); `expandBuilderLines` (`machines.cc:98`)
    skips a missing `@file` and throws on any other error, which pynixd counts as machines.
    """
    if settings.external_builders:
        return True
    return bool(settings.build_hook) and bool(_builder_lines(settings.builders))


def _builder_lines(builders: str) -> list[str]:
    lines: list[str] = []
    for line in builders.split("\n"):
        for entry in line.split("#", 1)[0].split(";"):
            entry = entry.strip()
            if not entry:
                continue
            if not entry.startswith("@"):
                lines.append(entry)
                continue
            try:
                text = Path(entry[1:].strip()).read_text()
            except FileNotFoundError:
                continue
            except OSError:
                return [entry]
            lines += _builder_lines(text)
    return lines


def _url_unsupported(url: str) -> str | None:
    """A URL whose transfer pynixd leaves to the daemon: S3 needs AWS credentials resolved on the host
    (`derivation-builder.cc:622`). curl takes every other scheme, as in Nix's `curlFileTransfer`."""
    scheme, sep, _rest = url.partition("://")
    if not sep or scheme.lower() == "s3":
        return f"builtin:fetchurl of the URL scheme '{scheme}'"
    return None


def _builtin_unsupported(drv: BasicDerivation, settings: BuildSettings) -> str | None:
    if drv.builder == "builtin:buildenv":
        return _buildenv_unsupported(drv)
    if drv.builder == "builtin:fetchurl":
        return _fetchurl_unsupported(drv, settings)
    if drv.builder == "builtin:unpack-channel":
        return _unpack_channel_unsupported(drv)
    return None


def _builtin_env(drv: BasicDerivation) -> dict[str, str]:
    """The `env` a builtin reads: `StructuredAttrs::tryExtract` takes `__json` out of it (`derivations.cc:1031`).

    A structured-attrs derivation keeps every attribute in `__json`, so the builtin finds its own missing.
    """
    return {name: value for name, value in drv.env.items() if name != "__json"}


def _unpack_channel_unsupported(drv: BasicDerivation) -> str | None:
    """An output but `out` is a `map::at` throw."""
    if set(drv.outputs) != {"out"}:
        return f"builtin:unpack-channel with the outputs {sorted(drv.outputs)}"
    return None


def _buildenv_unsupported(drv: BasicDerivation) -> str | None:
    """A malformed `derivations` is a `stoi` throw or undefined behaviour."""
    env = _builtin_env(drv)
    if set(drv.outputs) != {"out"}:
        return f"builtin:buildenv with the outputs {sorted(drv.outputs)}"
    if "derivations" in env and buildenv.packages(env["derivations"]) is None:
        return "builtin:buildenv with derivations that Nix cannot read"
    return None


def _buildenv_spec(drv: BasicDerivation, out: str) -> BuildenvSpec:
    """`builtinBuildenv` reads the derivation's own attributes, not the builder's environment (`buildenv.cc:175`)."""
    env = _builtin_env(drv)
    derivations = env.get("derivations")
    return {
        "out": out,
        "packages": None if derivations is None else buildenv.packages(derivations),
        "manifest": env.get("manifest"),
    }


def _fetchurl_unsupported(drv: BasicDerivation, settings: BuildSettings) -> str | None:
    """Why the daemon runs this builtin. The helper refuses a plain output in Nix's words (`builtins/fetchurl.cc:28`)."""
    if "out" not in drv.outputs:
        return "builtin:fetchurl with no output 'out'"
    main = _builtin_env(drv).get("url")
    for url in ((main,) if main is not None else ()) + tuple(_mirror_urls(drv, settings)):
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


def _fetch_spec(drv: BasicDerivation, settings: BuildSettings, out: str, build_dir: str) -> FetchSpec:
    transfer = settings.transfer
    env = _builtin_env(drv)
    return {
        "fixed_or_impure": drv.outputs["out"].kind in (OutputKind.CA_FIXED, OutputKind.IMPURE),
        "main_url": env.get("url"),
        "mirrors": _mirror_urls(drv, settings),
        "out": out,
        "unpack": env.get("unpack", "") == "1",
        "executable": env.get("executable") == "1",
        "tmp_dir": build_dir,
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
    paths: dict[str, StorePath] = field(default_factory=dict)
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
    visible: frozenset[StorePath]
    sources: tuple[str, ...]
    """The `sandbox-paths` sources the helper binds."""
    recursive: bool
    """The builder has the store's socket."""


@dataclass
class _Paths:
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


def _prepare(
    engine: StoreEngine,
    drv_path: StorePath,
    settings: BuildSettings,
    user: build_users.BuildUser | None = None,
    *,
    chroot: bool = True,
) -> _Paths:
    """`startBuild` up to the environment, and `setupBuildChroot` (`unix/build/chroot.cc:12`) with *chroot*.

    With *user*, the build directory is the build user's, and the chroot's root and store its group's.

    NIX-DEVIATION (#27): without the chroot, `setBuildTmpDir`
    (`unix/build/derivation-builder-impl.hh:215`) gives the builder the top
    directory itself. pynixd gives it the `build` directory in it, as in the
    sandbox, because the top directory holds the helper's job and its usage
    file, which the build user must not replace. Only the path differs; a
    build that prints `$NIX_BUILD_TOP` shows it.
    """
    layout = engine.layout
    build_dir = Path(settings.build_dir) if settings.build_dir else layout.state_dir / "builds"
    build_dir.mkdir(parents=True, exist_ok=True)
    if user is not None:
        build_users.check_not_world_writable(build_dir)
    top = Path(tempfile.mkdtemp(prefix="nix-", dir=build_dir))
    tmp = top / "build"
    tmp.mkdir(0o700)
    build_users.chown_to(user, tmp)
    if not chroot:
        if user is not None:
            # The build user passes through the top directory, and lists or replaces nothing in it.
            top.chmod(0o711)
        return _Paths(top, tmp, None, None)

    real = layout.real_path(drv_path)
    chroot_parent = real.with_name(f"{real.name}.chroot")
    delete_path(chroot_parent)
    chroot_parent.mkdir(0o700)
    root = chroot_parent / "root"
    # `setupBuildChroot` (`chroot.cc:30`): the root of a build of many ids is its own.
    id_range = user is not None and user.count != 1
    root.mkdir(0o755 if id_range else 0o750)
    if user is not None:
        os.chown(root, user.uid if id_range else 0, user.gid)
    (root / "tmp").mkdir()
    (root / "tmp").chmod(0o1777)
    store = root / str(layout.store_dir).lstrip("/")
    store.mkdir(parents=True)
    store.chmod(0o1775)
    if user is not None:
        os.chown(store, 0, user.gid)
    etc = root / "etc"
    etc.mkdir()
    if id_range:
        build_users.chown_to(user, etc)
    uid, gid = (0, 0) if id_range else (SANDBOX_UID, SANDBOX_GID)
    (etc / "passwd").write_text(
        f"root:x:0:0:Nix build user:{SANDBOX_BUILD_DIR}:/noshell\n"
        f"nixbld:x:{uid}:{gid}:Nix build user:{SANDBOX_BUILD_DIR}:/noshell\n"
        "nobody:x:65534:65534:Nobody:/:/noshell\n"
    )
    (etc / "group").write_text(f"root:x:0:\nnixbld:!:{gid}:\nnogroup:x:65534:\n")
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
    inputs: set[StorePath],
    sandbox_closure: set[StorePath],
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
    root = paths.root
    if root is None:
        raise ValueError("a build without the chroot binds nothing")
    binds: dict[str, tuple[str, bool]] = {p.target: (p.source, p.optional) for p in settings.sandbox_paths}
    for path in sandbox_closure:
        binds[str(path)] = (str(engine.layout.real_path(path)), False)
    binds[SANDBOX_BUILD_DIR] = (str(paths.tmp), False)
    for path in inputs:
        binds[str(path)] = (str(engine.layout.real_path(path)), False)
    if "/dev" not in binds:
        for name in DEV_FILES:
            binds.setdefault(name, (name, False))
    if not sandboxed:
        (root / "etc/nsswitch.conf").write_text("hosts: files dns\nservices: files\n")
        for name in ("/etc/resolv.conf", "/etc/services", "/etc/hosts"):
            if os.path.exists(name):
                binds.setdefault(name, (os.path.realpath(name), False))
    else:
        (root / "etc/hosts").write_text("127.0.0.1 localhost\n::1 localhost\n")
    (root / "etc").chmod(0o555)
    return [(target, source, optional) for target, (source, optional) in sorted(binds.items())]


async def _export_references(
    engine: StoreEngine, requests: Mapping[str, Sequence[StorePath]], inputs: set[StorePath]
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
        paths = await engine.query_closure(str(p) for p in requested)
        outputs: set[StorePath] = set()
        for drv_path in sorted(p for p in paths if p.is_derivation()):
            drv = await engine.read_derivation(str(drv_path))
            if drv is None:
                raise ValueError(f"cannot read derivation '{drv_path}'")
            for output in drv.outputs:
                if not output.path:
                    raise PlainError("exportReferences on CA derivations is not yet implemented")
                outputs.add(StorePath(output.path))
        # `computeFSClosure` throws `InvalidPath` where `query_closure` drops the path.
        if missing := sorted(outputs - await engine.query_valid_paths(str(p) for p in outputs)):
            raise PlainError(f"path '{magenta(missing[0])}' is not valid")
        paths |= await engine.query_closure(str(p) for p in sorted(outputs))
        exported[name] = sorted(
            await engine.query_path_infos(str(p) for p in sorted(paths)), key=lambda info: info.path
        )
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


def _sync(real_store_dir: Path, names: Iterable[str]) -> None:
    for name in names:
        sync_store_path(real_store_dir / name)


def _sign(infos: Iterable[ValidPathInfo], secret_key_files: Sequence[str]) -> None:
    """`Store::signPathInfo` (`store-api.cc:1238`): each key file read again, a bad one fails the build."""
    keys = [SecretKey.from_file(Path(file)) for file in secret_key_files]
    for info in infos:
        fp = fingerprint(info.path, info.info.nar_hash, info.info.nar_size, info.info.references)
        for key in keys:
            info.info.sigs.add(Signature(key.sign_fingerprint(fp)))


def _known(drv: BasicDerivation, realised: Mapping[str, StorePath], drv_name: str) -> dict[str, StorePath]:
    """The path of each output known before the build; an output with none is absent."""
    if any(o.kind is OutputKind.IMPURE for o in drv.outputs.values()):
        return {name: random_path(name, drv_name) for name in drv.outputs}
    known: dict[str, StorePath] = {}
    for name, output in drv.outputs.items():
        if name in realised:
            known[name] = realised[name]
        elif output.path:
            known[name] = StorePath(output.path)
    return known


async def build(
    engine: StoreEngine,
    drv_path: StorePath,
    drv: BasicDerivation,
    settings: BuildSettings,
    *,
    on_lines: Callable[[list[Output]], Awaitable[None]],
    realised: Mapping[str, StorePath],
    check: bool = False,
    repair: bool = False,
    recursive: RecursiveServer | None = None,
    vm: BuildVm | None = None,
    on_usage: Callable[[BuildUsage], None] | None = None,
    sample_seconds: float = 0,
) -> LocalBuildResult:
    """Build *drv*, whose inputs are valid, and register its outputs. `unsupported(drv)` must be `None`.

    *on_lines* receives the builder's output as it comes: Nix's log lines and its `@nix` events.
    *on_usage* receives what the builder used, once it has ended, when the helper measured it.
    In a cgroup, it holds a sample of the running build every *sample_seconds*; 0 takes none.
    *realised* is the path of each output the store holds a realisation for (`checkPathValidity`,
    `derivation-building-goal.cc:1329`). With *check*, each output is valid, and the build
    compares what it makes with it and registers nothing new. With *repair*, an output is valid only when
    its contents match its registered hash (`derivation-building-goal.cc:1334`); the build replaces the rest.
    *recursive* serves the builder of a `recursive-nix` or `builder-rpc-v0` derivation; it must be given for one.
    With *vm*, a sandboxed build for the store's own system runs in a microVM, and the helper inside it.
    """
    layout = engine.layout
    store_dir = str(layout.store_dir)
    drv_name = drv_path.base_name().removesuffix(".drv")
    chroot = uses_sandbox(drv, settings, relocated=layout.store_dir != layout.real_store_dir)
    impure = any(o.kind is OutputKind.IMPURE for o in drv.outputs.values())
    known = _known(drv, realised, drv_name)
    held = {path: PathLock(layout.real_path(path)) for path in known.values()}
    locks = list(held.values())
    for lock in locks:
        # Not cancellable: an abandoned `flock` would hold the lock with no owner to release it.
        await anyio.to_thread.run_sync(lock.acquire)
    paths: _Paths | None = None
    user: build_users.BuildUser | None = None
    # Nix deletes the build directory only after a build that succeeded (`cleanupBuild(true)`, `:246`).
    built = False
    # `derivation-building-goal.cc:496`, `:854`: the output locks' files go only once each output is valid.
    settled = False
    redirected: dict[StorePath, StorePath] = {}
    try:
        if build_users.use_build_users(settings.build_users_group, settings.auto_allocate_uids):
            auto = settings.auto_allocate_uids
            if (refused := _user_refusal(settings, auto=auto)) is not None:
                return LocalBuildResult(BuildResultStatus.MISC_FAILURE, show_error(refused))
            pool = build_users.pool_dir(layout.state_dir, layout.relocated, auto=auto)
            # `getBuildUser`: the chroot builder allocates in a user namespace (`linux-derivation-builder.cc:443`).
            take = (
                functools.partial(
                    build_users.acquire_auto,
                    pool,
                    settings.build_users_group,
                    start_id=settings.start_id,
                    id_count=settings.id_count,
                    user_namespace=chroot,
                    ids=build_users.IDS_PER_BUILD if "uid-range" in drv.required_system_features else 1,
                )
                if auto
                else functools.partial(build_users.acquire, pool, settings.build_users_group)
            )
            try:
                while (user := await anyio.to_thread.run_sync(take)) is None:
                    await anyio.sleep(USER_POLL_SECONDS)
            except build_users.BuildUserError as e:
                return LocalBuildResult(BuildResultStatus.MISC_FAILURE, show_error(str(e)))
        # `PathStatus` (`derivation-building-goal.cc:1340`): *present* is valid or corrupt, *valid* only valid.
        # NIX-DEFECT (#23): `checkPathValidity` (`derivation-building-goal.cc:1356`) gives an output with a
        # realisation `Valid` whenever its path is, so with `ca-derivations` a repair never checks its contents,
        # says "someone beat us to it", and repairs nothing. pynixd checks every known path, realised or not, and
        # repairs it. Lillecarl/nix#400.
        present = {path for path in known.values() if await engine.is_valid_path_uncached(str(path))}
        valid = {path for path in present if not repair or await engine.contents_good(str(path))}
        repairing = repair and BUILDER_RPC in drv.required_system_features
        if repairing:
            # The builder adds each output itself, and an add that repairs locks the path it replaces: the build
            # lets go of each output it repairs, or that add waits for it forever.
            for path, lock in held.items():
                if path not in valid:
                    lock.release(delete=False)
        if len(known) == len(drv.outputs) and valid == set(known.values()) and not check:
            settled = True
            return LocalBuildResult(BuildResultStatus.ALREADY_VALID, paths=known)
        # `tryToBuild` finds the outputs invalid first, then `makeDerivationBuilder` and `startBuild` throw.
        refused = _builder_refusal(drv_path, drv, settings, chroot=chroot) or _chroot_refusal(
            drv, settings, user, chroot=chroot
        )
        if refused is not None:
            return LocalBuildResult(BuildResultStatus.MISC_FAILURE, show_error(refused))
        # `derivation-building-goal.cc:503`: an output on disk that is not valid goes before the build.
        for path in known.values():
            if path not in valid:
                await anyio.to_thread.run_sync(delete_path, layout.real_path(path))
        # `derivation-builder.cc:417`: the sandbox builds a known path in place. Without it, a present path builds
        # at its fallback path, and the builder sees the fallback's hash for its own.
        scratch: dict[str, StorePath] = {}
        for name in drv.outputs:
            if (own := known.get(name)) is None:
                scratch[name] = fallback_path(drv_path, name, drv_name, store_dir)
            elif not chroot and own in present:
                scratch[name] = fallback_path_of(drv_path, own, store_dir)
            else:
                scratch[name] = own
        redirected = {path: scratch[name] for name, path in known.items() if path != scratch[name]}
        for path in redirected.values():
            await anyio.to_thread.run_sync(delete_path, layout.real_path(path))
        inputs = await engine.query_closure(sorted(str(p) for p in drv.input_srcs))
        # An invalid path drops out of the closure, as Nix ignores `InvalidPath` here.
        sandbox_closure = await engine.query_closure(sorted(_sandbox_store_paths(settings, store_dir)))
        try:
            exported = await _export_references(engine, export_requests(drv.env, store_dir), inputs)
        except BuildFailure as e:
            return LocalBuildResult(e.status, e.message)
        except PlainError as e:
            return LocalBuildResult(BuildResultStatus.MISC_FAILURE, show_error(str(e)))
        try:
            paths = await anyio.to_thread.run_sync(
                functools.partial(_prepare, engine, drv_path, settings, user, chroot=chroot)
            )
        except build_users.BuildUserError as e:
            return LocalBuildResult(BuildResultStatus.MISC_FAILURE, show_error(str(e)))
        fixed = any(o.kind is OutputKind.CA_FIXED for o in drv.outputs.values())
        sandboxed = not (fixed or impure)
        # `inputRewrites` (`derivation-builder.cc:435`): each output builds at its scratch path.
        rewrites = {hash_placeholder(name): str(path) for name, path in scratch.items()} | {
            path.hash_part(): fallback.hash_part() for path, fallback in redirected.items()
        }
        build_dir = SANDBOX_BUILD_DIR if chroot else str(paths.tmp)
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
            build_dir=build_dir,
        )
        for name, contents in env.files.items():
            (paths.tmp / name).write_text(contents, encoding="utf-8")
        fetch = (
            await anyio.to_thread.run_sync(_fetch_spec, drv, settings, str(scratch["out"]), build_dir)
            if drv.builder == "builtin:fetchurl"
            else None
        )
        # `getUIDCount() != 1`: the builder is root over the whole slot (`linux-derivation-builder.cc:430`).
        id_range = user is not None and user.count != 1
        spec: ChildSpec = {
            "builder": drv.builder,
            "args": [rewrite_strings(arg, rewrites) for arg in drv.args],
            "env": env.variables,
            "chroot": chroot,
            "root": "" if paths.root is None else str(paths.root),
            "binds": (
                await anyio.to_thread.run_sync(_binds, engine, paths, inputs, sandbox_closure, settings, sandboxed)
                if chroot
                else []
            ),
            "private_network": sandboxed,
            "sandbox_uid": 0 if id_range else SANDBOX_UID,
            "sandbox_gid": 0 if id_range else SANDBOX_GID,
            "work_dir": build_dir,
            "shm_size": settings.sandbox_shm_size,
            "no_new_privileges": not settings.allow_new_privileges,
            "filter_syscalls": settings.filter_syscalls,
            "fetchurl": fetch,
            "buildenv": _buildenv_spec(drv, str(scratch["out"])) if drv.builder == "builtin:buildenv" else None,
            "unpack_channel": (
                {
                    "out": str(scratch["out"]),
                    "channel_name": _builtin_env(drv).get("channelName"),
                    "src": _builtin_env(drv).get("src"),
                }
                if drv.builder == "builtin:unpack-channel"
                else None
            ),
            "store_dir": store_dir,
            "usage_file": str(paths.top / USAGE_NAME),
            "cgroup": None,
            "cgroup_report": False,
            "sample_seconds": 0,
            "build_user": None if user is None else (user.uid, user.gid),
            "build_groups": [] if user is None else list(user.groups),
            "map_fd": None,
            "id_range": id_range,
        }
        # `startBuild` (`derivation-builder.cc:465`): only a build that rewrites hashes, one without the chroot.
        if not chroot and os.path.exists(HOME_DIR):
            return LocalBuildResult(
                BuildResultStatus.MISC_FAILURE,
                show_error(
                    f"home directory {magenta(chr(34) + HOME_DIR + chr(34))} exists; please remove it to assure "
                    "purity of builds without sandboxing"
                ),
            )
        # `startDaemon` (`derivation-builder.cc:787`) requires the feature before it starts the builder.
        if "recursive-nix" in drv.required_system_features and not settings.recursive_nix:
            return LocalBuildResult(BuildResultStatus.MISC_FAILURE, show_error(missing_feature("recursive-nix")))
        start = int(time.time())
        log = bytearray()
        lines = BuildLog(settings.log_lines)

        async def keep(piece: bytes) -> None:
            log.extend(piece)
            await on_lines(lines.feed(piece))

        control: socket.socket | None = None
        restriction: RestrictionContext | None = None
        submitting = BUILDER_RPC in drv.required_system_features
        # The guest kernel has no binfmt handler and no IA32 emulation: another system builds on the host's kernel.
        # The guest maps no build user yet: as root with build users, the namespaces build.
        in_vm = (
            vm is not None and chroot and sandboxed and drv.platform in {settings.system, "builtin"} and user is None
        )
        async with contextlib.AsyncExitStack() as stack:
            if submitting or "recursive-nix" in drv.required_system_features:
                if recursive is None:
                    raise ValueError("a recursive-nix or builder-rpc-v0 build needs a server for its builder")
                # `startDaemon` (`derivation-builder.cc:1171`): the socket is up before the builder starts.
                spec["env"]["NIX_REMOTE"] = f"unix://{build_dir}/{SOCKET_NAME}"
                if not chroot:
                    # `addDependencyImpl` (`derivation-builder.cc:901`) binds nothing: the builder sees the store.
                    restriction = RestrictionContext(
                        inputs, None, layout.real_store_dir, submitting=submitting, repairing=repairing, binds=False
                    )
                    listener = await anyio.to_thread.run_sync(_listen, paths.tmp / SOCKET_NAME)
                    build_users.chown_to(user, paths.tmp / SOCKET_NAME)
                elif in_vm:
                    # The guest's init listens in the build directory, and the image holds no host socket.
                    restriction = RestrictionContext(
                        inputs,
                        None,
                        layout.real_store_dir,
                        submitting=submitting,
                        repairing=repairing,
                        reveal=functools.partial(vm_allow, paths.top, layout.real_store_dir),
                    )
                    controls = await anyio.to_thread.run_sync(_listen, paths.top / VM_CONTROL_SOCKET_NAME)
                    listener = await anyio.to_thread.run_sync(_listen, paths.top / VM_BUILDER_SOCKET_NAME)
                    group = await stack.enter_async_context(anyio.create_task_group())
                    stack.callback(group.cancel_scope.cancel)
                    group.start_soon(_attach_control, controls, restriction)
                else:
                    ours, control = socket.socketpair()
                    stack.callback(control.close)
                    stream = await stack.enter_async_context(await SocketStream.from_socket(ours))
                    restriction = RestrictionContext(
                        inputs, stream, layout.real_store_dir, submitting=submitting, repairing=repairing
                    )
                    listener = await anyio.to_thread.run_sync(_listen, paths.tmp / SOCKET_NAME)
                    build_users.chown_to(user, paths.tmp / SOCKET_NAME)
                await stack.enter_async_context(recursive(listener, restriction))
            guest: _Guest | None = None
            if in_vm and vm is not None:
                # The guest has its own `/dev` and `/proc`.
                sources = tuple(
                    p.source for p in settings.sandbox_paths if not p.source.startswith(("/dev/", "/proc/"))
                )
                if (file := vm_unshareable(sources, layout.real_store_dir)) is not None:
                    return LocalBuildResult(
                        BuildResultStatus.MISC_FAILURE,
                        f"sandbox-paths names the file '{magenta(file)}' in /, which the build VM cannot share",
                    )
                guest = _Guest(
                    vm, layout.real_store_dir, frozenset(inputs | sandbox_closure), sources, restriction is not None
                )
            # A VM build's host cgroup holds the VMM, and the guest measures the build in a cgroup of its own.
            cgroup = await stack.enter_async_context(build_cgroup.build_cgroup())
            spec["cgroup"] = None if cgroup is None else str(cgroup)
            if id_range and user is not None:
                # `prepareBuild` (`linux-derivation-builder.cc:449`): a build of many ids needs its own cgroup,
                # which its root manages (`:499`).
                if cgroup is None:
                    return LocalBuildResult(
                        BuildResultStatus.MISC_FAILURE, show_error("cannot determine the cgroups file system")
                    )
                for owned in (cgroup, cgroup / "cgroup.procs", cgroup / "cgroup.threads"):
                    build_users.chown_to(user, owned)
            if guest is not None:
                spec["cgroup"] = str(build_cgroup.GUEST)
                spec["cgroup_report"] = True
                spec["sample_seconds"] = sample_seconds
            samples: list[UsageSample] = []
            sampler = await stack.enter_async_context(anyio.create_task_group())
            stack.callback(sampler.cancel_scope.cancel)
            if cgroup is not None and sample_seconds > 0 and guest is None:
                sampler.start_soon(build_cgroup.sample_every, cgroup, sample_seconds, samples)
            failure = await _run(paths, spec, settings, drv_path, keep, control, guest, cgroup, user)
            # Not `sampler.cancel_scope.cancel()` here: this body runs in that scope, and the read below would be
            # cancelled too. The stack's callback stops the sampler on the way out.
            taken = tuple(samples)
            usage: BuildUsage | None
            if guest is not None:
                usage = read_usage(paths.top / USAGE_NAME)
                host = (
                    read_usage(paths.top / VMM_USAGE_NAME)
                    if cgroup is None
                    else await anyio.to_thread.run_sync(build_cgroup.read, cgroup)
                )
                if usage is not None and host is not None:
                    usage = with_host(usage, host)
            elif cgroup is not None:
                read = await anyio.to_thread.run_sync(build_cgroup.read, cgroup)
                usage = replace(read, samples=taken)
            else:
                usage = read_usage(paths.top / USAGE_NAME)
        if on_usage is not None and usage is not None:
            on_usage(usage)
        if restriction is not None:
            # The paths the builder added may be referenced (`derivation-builder.cc:1434`).
            inputs |= restriction.added
        stop = int(time.time())
        # Nix flushes at the builder's end of file, which a timeout never reaches.
        if not (isinstance(failure, BuildFailure) and failure.status is BuildResultStatus.TIMED_OUT):
            await on_lines(lines.flush())
        # Empty too: `openLogFile` (`derivation-building-goal.cc:793`) makes the file before a line arrives,
        # and none without `keep-build-log` (`:1184`).
        if settings.keep_build_log:
            await engine.add_build_log(drv_path.name, bytes(log))
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
            submitted = await _check_submitted(
                engine, drv_path, drv, restriction.submitted, start, stop, known=known if check else None
            )
            built = submitted.status is BuildResultStatus.BUILT
            return submitted
        try:
            collected = await anyio.to_thread.run_sync(
                lambda: collect_outputs(
                    drv_path,
                    drv.outputs,
                    scratch=scratch,
                    known=known,
                    drv_name=drv_name,
                    chroot_root=Path("/") if paths.root is None else paths.root,
                    real_store_dir=layout.real_store_dir,
                    store_dir=store_dir,
                    input_paths=inputs,
                    # `derivation-builder.cc:1131`: a check looks at every output.
                    valid=set() if check else valid,
                    is_valid=lambda path: anyio.from_thread.run(engine.is_valid_path_uncached, str(path)),
                    discard=discarded_references(drv.env),
                    check=check,
                    repair=repair,
                    build_uids=None if user is None else (user.uid, user.last_uid),
                    fresh=not chroot,
                    positions=engine.hashes_self_reference_positions,
                )
            )
        except BuildFailure as e:
            return LocalBuildResult(e.status, e.message, start, stop)
        except PlainError as e:
            return LocalBuildResult(BuildResultStatus.MISC_FAILURE, show_error(str(e)))
        try:
            if check:
                compared = await _compare(
                    engine,
                    drv_path,
                    drv,
                    collected,
                    settings,
                    known,
                    start,
                    stop,
                    user=user,
                    tmp=paths.tmp,
                    on_lines=on_lines,
                )
                built = compared.status is BuildResultStatus.BUILT
                return compared
            # `registerOutputs` (`derivation-builder.cc:1600`) skips an output that is valid already.
            fresh = [
                info.path.name
                for info in collected.infos.values()
                if not await engine.is_valid_path_uncached(str(info.path))
            ]
            if settings.auto_optimise_store:
                await anyio.to_thread.run_sync(_optimise, layout.real_store_dir, layout.links_dir, fresh)
            if settings.fsync_store_paths:
                # NIX-DEFECT (#23): `registerOutputs` (`derivation-builder.cc:1600`) registers an output with no
                # `recursiveSync`, so a crash after the build can leave a valid output with lost contents; only a
                # fixed output's copy starts its writeback (`:1417`). `fsync-store-paths` promises the sync before
                # registration, as `addToStore` does it (`local-store.cc:1136`). pynixd syncs each new output;
                # Lillecarl/nix#402.
                await anyio.to_thread.run_sync(_sync, layout.real_store_dir, fresh)
            await anyio.to_thread.run_sync(_sign, collected.infos.values(), settings.secret_key_files)
            failure = collected.failure or await anyio.to_thread.run_sync(
                lambda: check_outputs(
                    drv_path,
                    output_checks(drv.env),
                    collected.infos,
                    store_dir=store_dir,
                    path_info=lambda path: anyio.from_thread.run(engine.query_path_info, str(path)),
                )
            )
            if failure is not None:
                await engine.register_valid_paths(collected.early)
                return LocalBuildResult(failure.status, failure.message, start, stop)
            await engine.register_valid_paths(collected.infos.values())
        finally:
            collected.release()
        built = True
        made = {name: info.path for name, info in collected.infos.items()}
        return LocalBuildResult(
            BuildResultStatus.BUILT,
            "",
            start,
            stop,
            collected.infos,
            {name: out for name in drv.outputs if (out := made.get(name) or known.get(name)) is not None},
        )
    finally:
        with anyio.CancelScope(shield=True):
            if paths is not None:
                # A builtin's directory may hold privileged files, such as a copy of netrc (`:1695`).
                if settings.keep_failed and not built and not drv.builder.startswith("builtin:"):
                    await anyio.to_thread.run_sync(paths.keep)
                    await on_lines([Message(Verbosity.ERROR, f'note: keeping build directory "{paths.tmp}"')])
                else:
                    await anyio.to_thread.run_sync(paths.remove)
            # NIX-DEFECT (#23): `cleanupBuild` (`derivation-builder.cc:1678`) deletes the scratch path of a valid
            # output only with `force`, which a build that succeeded passes, so a failed rebuild without the
            # sandbox leaves its scratch outputs in the store, unregistered, until a garbage collection. pynixd
            # could delete them on every path; it copies Nix, as the parity run compares the store directory.
            # Lillecarl/nix#401.
            if built:
                for path in redirected.values():
                    await anyio.to_thread.run_sync(delete_path, layout.real_path(path))
            for lock in locks:
                lock.release(delete=built or settled)
            if user is not None:
                user.release()


async def _check_submitted(
    engine: StoreEngine,
    drv_path: StorePath,
    drv: BasicDerivation,
    submitted: Mapping[str, StorePath],
    start: int,
    stop: int,
    *,
    known: Mapping[str, StorePath] | None = None,
) -> LocalBuildResult:
    """`checkSubmittedOutputs` (`derivation-builder-impl.cc:713`) up to the realisations, which the caller registers.

    The builder added each output already, so nothing moves and nothing is signed: a content-addressed output
    needs only its realisation signed. With *known*, the realised path of each output, the build is a check: it
    compares each submitted path with it and registers nothing.
    """
    infos: dict[str, ValidPathInfo] = {}
    for name, path in submitted.items():
        info = await engine.query_path_info(str(path))
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
            path_info=lambda path: anyio.from_thread.run(engine.query_path_info, str(path)),
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
    if known is not None:
        # NIX-DEFECT (#23): `checkSubmittedOutputs` (`derivation-builder-impl.cc:762`) has no check mode ("TODO:
        # handle --check"). It registers the submitted realisation, so a rebuild that differs fails with "Trying
        # to register a realisation ... but we already have another one locally" and status MiscFailure. pynixd
        # answers what a check of any other output answers, NotDeterministic. Lillecarl/nix#405.
        real_store_dir = engine.layout.real_store_dir
        for name, info in built.items():
            if info.path != known[name]:
                old, new = (magenta(f'"{real_store_dir / path.name}"') for path in (known[name], info.path))
                return LocalBuildResult(
                    BuildResultStatus.NOT_DETERMINISTIC,
                    f"derivation '{magenta(drv_path)}' may not be deterministic: output {old} differs from {new}",
                    start,
                    stop,
                )
        return LocalBuildResult(BuildResultStatus.BUILT, "", start, stop, paths=dict(known))
    return LocalBuildResult(
        BuildResultStatus.BUILT, "", start, stop, built, {name: info.path for name, info in built.items()}
    )


async def _compare(
    engine: StoreEngine,
    drv_path: StorePath,
    drv: BasicDerivation,
    collected: Collected,
    settings: BuildSettings,
    known: dict[str, StorePath],
    start: int,
    stop: int,
    *,
    user: build_users.BuildUser | None,
    tmp: Path,
    on_lines: Callable[[list[Output]], Awaitable[None]],
) -> LocalBuildResult:
    """The end of `registerOutputs` in check mode (`derivation-builder.cc:1546`, `:1633`).

    A valid output the check verified becomes `ultimate` and is signed again, even when a later one differs.
    With `keep-failed` or a `diff-hook`, a rebuilt output that differs moves from where the build made it to
    `<path>.check`, and the hook compares the two as *user*, with the build directory *tmp* (`:1552`).
    """
    registered = {path: await engine.query_path_info(str(path)) for path in known.values()}
    failure, verified, kept = await anyio.to_thread.run_sync(
        lambda: compare_outputs(
            drv_path,
            collected.infos,
            registered,
            engine.layout.real_store_dir,
            known=known,
            rebuilt=collected.actual if settings.keep_failed or settings.diff_hook else None,
        )
    )
    if kept is not None and settings.diff_hook:
        await on_lines(await hooks.diff_hook(settings.diff_hook, user, *kept, drv_path, tmp))
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
                path_info=lambda path: anyio.from_thread.run(engine.query_path_info, str(path)),
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
    drv_path: StorePath,
    on_log: Callable[[bytes], Awaitable[None]],
    control: socket.socket | None = None,
    guest: _Guest | None = None,
    cgroup: Path | None = None,
    user: build_users.BuildUser | None = None,
) -> BuildFailure | _BuilderFailed | None:
    """Run the helper on a pseudoterminal, as `startBuild` does, and wait under the two timeouts.

    *control* is the helper's end of the socket it binds added paths on; it closes here once the helper has it.
    With *guest*, the helper runs in a microVM, which reports through files in the top directory, and its VMM
    joins *cgroup*.
    """
    spec_file = paths.top / "spec.json"
    maps: socket.socket | None = None
    helper_maps: socket.socket | None = None
    if user is not None and spec["chroot"]:
        maps, helper_maps = socket.socketpair()
        spec = {**spec, "map_fd": helper_maps.fileno()}
    # 0600: without the chroot the build user can reach the top directory, and the job may hold a netrc.
    with open(os.open(spec_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as job:
        job.write(json.dumps(spec))
    master, slave = os.openpty()
    # `openSlave` (`derivation-builder.cc:980`): raw, so no `\n` becomes `\r\n` in the log.
    mode = termios.tcgetattr(slave)
    tty.cfmakeraw(mode)
    termios.tcsetattr(slave, termios.TCSANOW, mode)
    if user is not None:
        # `startBuild` (`derivation-builder.cc:491`).
        os.fchmod(slave, 0o600)
        os.fchown(slave, user.uid, 0)
    if guest is not None:
        if paths.chroot_parent is None:
            raise ValueError("a build VM runs the sandbox only")
        status_read = status_write = -1
        await guest.vm.make_disk(paths.top, paths.tmp)
        argv = guest.vm.argv(
            top=paths.top,
            chroot_parent=paths.chroot_parent,
            real_store_dir=guest.real_store_dir,
            visible=guest.visible,
            sources=guest.sources,
            spec_file=spec_file,
            build_dir=paths.tmp,
            recursive=guest.recursive,
            cgroup=cgroup,
        )
        fds: list[int] = []
        # libkrun connects to the host's sockets by their names in the top directory.
        cwd: Path | None = paths.top
    else:
        status_read, status_write = os.pipe()
        argv = [sys.executable, "-m", "pynixd_store.builder.child", str(spec_file), str(status_write)]
        fds = [status_write]
        cwd = None
    if control is not None:
        argv.append(str(control.fileno()))
        fds.append(control.fileno())
    if helper_maps is not None:
        fds.append(helper_maps.fileno())
    try:
        process = await anyio.open_process(argv, stdin=None, stdout=slave, stderr=slave, pass_fds=fds, cwd=cwd)
    finally:
        os.close(slave)
        if status_write >= 0:
            os.close(status_write)
        if control is not None:
            control.close()
        if helper_maps is not None:
            helper_maps.close()
    if maps is not None and user is not None:
        with maps:
            await _map_build_user(maps, process.pid, user)
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


async def _map_build_user(maps: socket.socket, pid: int, user: build_users.BuildUser) -> None:
    """The parent's half of `startChild` (`linux-derivation-builder.cc:627`): map the build user into the helper's
    new user namespace once it says that namespace exists. A helper that failed first closes its end."""
    await anyio.wait_readable(maps)
    if maps.recv(1) != b"u":
        return
    proc = Path("/proc", str(pid))
    single = user.count == 1
    (proc / "uid_map").write_text(f"{SANDBOX_UID if single else 0} {user.uid} {user.count}")
    if single:
        (proc / "setgroups").write_text("deny")
    (proc / "gid_map").write_text(f"{SANDBOX_GID if single else 0} {user.gid} {user.count}")
    maps.send(b"g")


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
    drv_path: StorePath,
    drv: BasicDerivation,
    reason: str,
    tail: list[str],
    settings: BuildSettings,
    *,
    disk_full: bool,
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
