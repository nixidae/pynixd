"""Nix's darwin builder (`darwin/build/darwin-derivation-builder.cc`): no chroot, a sandbox profile instead.

The store side reads the derivation's darwin attributes and checks `getPathsInSandbox`; the helper writes the
profile after `setUser` and applies it, as Nix's child does. `useSandbox` picks the full profile or the minimal
one; a darwin build always has one of the two.
"""

from __future__ import annotations

import ctypes
import os
import posixpath
import re
import stat
import struct
from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.basic_derivation import bool_attr, structured_attrs

from ..hintfmt import magenta

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .spec import DarwinSpec

PROFILE_BREAK = 1 << 14
"""Characters of path rules per `allow` group: the profile interpreter refuses a large one (NixOS/nix#4119)."""

# The rules of Nix's `sandbox-defaults.sb`, in its order: a later rule wins over an earlier one.
DEFAULTS = """
(define TMPDIR (param "_GLOBAL_TMP_DIR"))

(deny default)

(deny file-write-setugid)

(allow process-fork)

(allow sysctl-read)

(allow ipc-posix*)

(allow ipc-sysv*)

(allow system-socket)

(allow signal (target same-sandbox))

(allow mach-lookup (global-name "com.apple.system.opendirectoryd.libinfo"))

; Unix sockets only here: a wider network rule would let a builder reach out of the sandbox.
(allow file* process-exec network-outbound network-inbound
       (literal "/tmp")
       (subpath TMPDIR)
       (subpath (param "_NIX_BUILD_TOP")))

(allow file-read*
       (literal "/System/Library/CoreServices/SystemVersion.plist")
       (literal "/System/Library/CoreServices/SystemVersionCompat.plist"))

; clang writes to /dev/null in configure tests.
(allow file-read-metadata (literal "/dev"))

(if (param "_ALLOW_LOCAL_NETWORKING")
    (begin
      (allow network* (remote ip "localhost:*"))
      (allow network-inbound (local ip "*:*"))

      (allow file-read-metadata
             (literal "/var")
             (literal "/etc")
             (literal "/etc/resolv.conf")
             (literal "/private/etc/resolv.conf"))

      (allow file-read*
             (literal "/private/var/run/resolv.conf"))

      (allow file-read-metadata (literal "/etc/hosts"))
      (allow file-read*         (literal "/private/etc/hosts"))
      (allow network-outbound (remote unix-socket (path-literal "/private/var/run/mDNSResponder")))))

(allow file*
       (literal "/dev/null")
       (literal "/dev/random")
       (literal "/dev/stderr")
       (literal "/dev/stdin")
       (literal "/dev/stdout")
       (literal "/dev/tty")
       (literal "/dev/urandom")
       (literal "/dev/zero")
       (subpath "/dev/fd"))

(allow file*
       (literal "/dev/ptmx")
       (regex #"^/dev/pty[a-z]+")
       (regex #"^/dev/ttys[0-9]+"))

(allow file* (literal "/dev/dtracehelper"))

(allow file-read* (subpath "/usr/share/zoneinfo"))

(allow file-read* (subpath "/usr/share/locale"))

(allow file-read-metadata
       (literal "/etc")
       (literal "/var")
       (literal "/private/var/tmp"))

; /bin/sh on macOS 10.15 and later.
(allow file*
       (literal "/private/var/select/sh"))

; Rosetta 2.
(allow file-read*
       (subpath "/Library/Apple/usr/libexec/oah")
       (subpath "/System/Library/Apple/usr/libexec/oah")
       (subpath "/System/Library/LaunchDaemons/com.apple.oahd.plist")
       (subpath "/Library/Apple/System/Library/LaunchDaemons/com.apple.oahd.plist"))

"""

# Nix's `sandbox-network.sb`: what a fixed-output or impure build adds.
NETWORK = """
(allow network* (local ip) (remote ip))

(allow file-read-metadata
       (literal "/var")
       (literal "/etc")
       (literal "/etc/resolv.conf")
       (literal "/private/etc/resolv.conf"))

(allow file-read*
       (literal "/private/var/run/resolv.conf"))

(allow network-outbound (remote unix-socket (path-literal "/private/var/run/mDNSResponder")))
(allow mach-lookup (global-name "com.apple.SystemConfiguration.DNSConfiguration"))

(allow mach-lookup (global-name "com.apple.trustd"))
(allow mach-lookup (global-name "com.apple.trustd.agent"))

"""

# Nix's `sandbox-minimal.sb`: a build without the sandbox still makes no setuid file.
MINIMAL = """
(allow default)

(deny file-write-setugid)

"""


class ProfileError(Exception):
    """An error `DarwinDerivationBuilder::setUser` throws, which the helper reports as a set-up error."""


def string_attr(env: Mapping[str, str], name: str) -> str | None:
    """`getStringAttr` (`derivation-options.cc:24`). `structured_attrs_reason` refused a structured non-string."""
    attrs = structured_attrs(env)
    value: Any = env.get(name) if attrs is None else attrs.get(name)
    return value if isinstance(value, str) else None


def string_set_attr(env: Mapping[str, str], name: str) -> list[str] | None:
    """`getStringSetAttr` (`derivation-options.cc:83`), sorted as its `StringSet`."""
    attrs = structured_attrs(env)
    if attrs is None:
        value = env.get(name)
        # `tokenizeString`'s separators; `str.split` splits on more.
        return None if value is None else sorted({w for w in re.split(r"[ \t\n\r]+", value) if w})
    words = attrs.get(name)
    return None if not isinstance(words, list) else sorted({w for w in words if isinstance(w, str)})


def additional_profile(env: Mapping[str, str]) -> str:
    """`__sandboxProfile`: rules a derivation appends to the full profile."""
    return string_attr(env, "__sandboxProfile") or ""


def allow_local_networking(env: Mapping[str, str]) -> bool:
    return bool_attr(env, "__darwinAllowLocalNetworking", default=False)


def canon_path(path: str) -> str:
    """`canonPath` without symlinks: one `/` between names, no `.`, a `..` takes the name before it."""
    if not path.startswith("/"):
        raise ValueError(f"not an absolute path: '{path}'")
    parts: list[str] = []
    for part in path.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/" + "/".join(parts)


def _in_dir(path: str, directory: str) -> bool:
    """`isDirOrInDir`."""
    return path == directory or path.startswith(directory.rstrip("/") + "/")


def paths_in_sandbox(
    defaults: Mapping[str, tuple[str, bool]],
    build_dir: str,
    store_dir: str,
    drv_path: str,
    impure_host_deps: list[str],
    allowed_prefixes: list[str],
) -> dict[str, tuple[str, bool]]:
    """`getPathsInSandbox` (`unix/build/derivation-builder.cc:518`): target to `(source, optional)`.

    *defaults* is `sandbox-paths` and the closure of its store paths. Raises `ValueError` with Nix's message.
    """
    if store_dir.startswith(build_dir):
        raise ValueError("`sandbox-build-dir` must not contain the storeDir")
    paths = dict(defaults)
    paths[build_dir] = (build_dir, False)
    allowed = [canon_path(a) for a in allowed_prefixes]
    for dep in impure_host_deps:
        canon = canon_path(dep)
        if not any(_in_dir(canon, prefix) for prefix in allowed):
            raise ValueError(
                f"derivation '{magenta(drv_path)}' requested impure path '{magenta(dep)}', "
                "but it was not in allowed-impure-host-deps"
            )
        # macOS 11 and later have no `/usr/lib/libSystem*.dylib`, so an impure host dependency may be absent.
        paths[dep] = (dep, True)
    return paths


def _ancestry(paths: Mapping[str, tuple[str, bool]], store_dir: str) -> list[str]:
    """Every directory above each path, and the store directory with its own: `realpath` stats each one."""
    ancestry: set[str] = set()
    for target in paths:
        cur = target
        while cur != "/":
            cur = posixpath.dirname(cur)
            ancestry.add(cur)
    cur = store_dir
    while cur != "/":
        ancestry.add(cur)
        cur = posixpath.dirname(cur)
    return sorted(ancestry)


def profile(spec: DarwinSpec) -> str:
    """The profile `DarwinDerivationBuilder::setUser` (`darwin-derivation-builder.cc:41`) builds.

    Raises `ProfileError` for a mapped path or a required path that is absent, as Nix's child throws.
    """
    text = "(version 1)\n"
    if not spec["sandbox"]:
        return text + MINIMAL
    paths = {target: (source, optional) for target, source, optional in spec["paths"]}
    ancestry = _ancestry(paths, spec["store_dir"])
    for path in spec["inputs"]:
        paths[path] = (path, False)
    text += "(deny default)\n" if spec["log_violations"] else "(deny default (with no-log))\n"
    text += DEFAULTS
    if spec["network"]:
        text += NETWORK
    text += "(allow file-read* file-write* process-exec\n"
    for path in spec["scratch"]:
        text += f'\t(subpath "{path}")\n'
    text += ")\n"
    # Without file-write* here, `access()` answers EPERM for an input.
    text += "(allow file-read* file-write* process-exec\n"
    group_end = len(text) + PROFILE_BREAK
    for target, (source, optional) in sorted(paths.items()):
        if len(text) >= group_end:
            text += ")\n(allow file-read* file-write* process-exec\n"
        if target != source:
            raise ProfileError(
                f"can't map {magenta(chr(34) + target + chr(34))} to {magenta(chr(34) + source + chr(34))}: "
                "mismatched impure paths not supported on Darwin"
            )
        try:
            st = os.lstat(target)
        except OSError as e:
            if optional:
                continue
            # NIX-DEFECT (#23): `darwin-derivation-builder.cc:128` opens a quote it never closes. pynixd copies it.
            raise ProfileError(f"getting attributes of required path '{magenta(target)}: {e.strerror}") from e
        text += f'\t(subpath "{target}")\n' if stat.S_ISDIR(st.st_mode) else f'\t(literal "{target}")\n'
    text += ")\n"
    text += "(allow file-read*\n"
    for directory in ancestry:
        text += f'\t(literal "{directory}")\n'
    text += ")\n"
    return text + spec["additional_profile"]


class _IpcsCommand(ctypes.Structure):
    """`struct IpcsCommand` (`darwin-derivation-builder.cc:25`): the cursor of a `kern.sysv.ipcs.*` walk."""

    _fields_ = (
        ("magic", ctypes.c_uint32),
        ("op", ctypes.c_uint32),
        ("cursor", ctypes.c_uint32),
        ("datalen", ctypes.c_uint32),
        ("data", ctypes.c_void_p),
    )


IPCS_MAGIC = 0x00000001
IPC_RMID = 0
#: Each kind: its sysctl, the iteration op, and `sizeof` its `*id_ds`, measured against the macOS 14.4 SDK.
#: Each one starts with `struct ipc_perm`, whose `uid` is at offset 0 and `_key` at 20.
IPC_KINDS = {
    "shm": (b"kern.sysv.ipcs.shm", 0x00000002, 76),
    "msg": (b"kern.sysv.ipcs.msg", 0x00000200, 116),
    "sem": (b"kern.sysv.ipcs.sem", 0x00000020, 72),
}


def sysv_ipc_keys(uid: int) -> dict[str, list[int]]:
    """The key of each System V IPC object *uid* owns, by kind (`shm`, `msg`, `sem`)."""
    libc = ctypes.CDLL(None, use_errno=True)
    keys: dict[str, list[int]] = {}
    for kind, (sysctl, op, size) in IPC_KINDS.items():
        data = ctypes.create_string_buffer(size)
        command = _IpcsCommand(IPCS_MAGIC, op, 0, size, ctypes.cast(data, ctypes.c_void_p))
        length = ctypes.c_size_t(ctypes.sizeof(command))
        found = keys.setdefault(kind, [])
        while True:
            ctypes.memset(data, 0, size)
            if libc.sysctlbyname(
                sysctl, ctypes.byref(command), ctypes.byref(length), ctypes.byref(command), ctypes.sizeof(command)
            ):
                break
            owner, key = struct.unpack_from("=I", data.raw, 0)[0], struct.unpack_from("=i", data.raw, 20)[0]
            if owner == uid:
                found.append(key)
    return keys


def cleanup_sysv_ipc(uid: int) -> None:
    """`cleanupSysVIPCForUser` (`darwin-derivation-builder.cc:209`): darwin has no IPC namespace, so a build
    user's objects outlive its processes. Each one goes by its key, and a failure is skipped, as in Nix."""
    libc = ctypes.CDLL(None, use_errno=True)
    keys = sysv_ipc_keys(uid)
    ids = {
        "shm": [i for key in keys["shm"] if (i := libc.shmget(key, 0, 0)) != -1],
        "msg": [i for key in keys["msg"] if (i := libc.msgget(key, 0)) != -1],
        "sem": [i for key in keys["sem"] if (i := libc.semget(key, 0, 0)) != -1],
    }
    for i in ids["shm"]:
        libc.shmctl(i, IPC_RMID, None)
    for i in ids["msg"]:
        libc.msgctl(i, IPC_RMID, None)
    for i in ids["sem"]:
        libc.semctl(i, 0, IPC_RMID)


def global_tmp_dir(environ: Mapping[str, str]) -> str:
    """`defaultTempDir()` with its symlinks resolved and no trailing `/`, which a `subpath` refuses."""
    return os.path.realpath(environ.get("TMPDIR") or "/tmp").rstrip("/")


def parameters(spec: DarwinSpec, environ: Mapping[str, str]) -> list[str]:
    """`sandbox_init_with_parameters`' parameters: names and values in turn."""
    params = ["_NIX_BUILD_TOP", spec["build_top"], "_GLOBAL_TMP_DIR", global_tmp_dir(environ)]
    if spec["allow_local_networking"]:
        params += ["_ALLOW_LOCAL_NETWORKING", "1"]
    return params
