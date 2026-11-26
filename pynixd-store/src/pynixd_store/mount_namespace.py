"""A root pynixd's own mount namespace, where a read-only store is writable.

Nix's `tryEnterPrivateMountNamespace` and `remountReadOnlyWritable`
(`libutil/linux/linux-namespaces.cc`): `nix` enters the namespace at start when
it runs as root (`nix/main.cc`), and `LocalStore::makeStoreWritable`
(`local-store.cc`) remounts the store there. NixOS mounts `/nix/store`
read-only, so a daemonless pynixd needs both to open it.
"""

from __future__ import annotations

import ctypes
import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from .errors import PynixdError

if TYPE_CHECKING:
    from pathlib import Path

MS_NOSUID: Final = 2
MS_NODEV: Final = 4
MS_NOEXEC: Final = 8
MS_SYNCHRONOUS: Final = 16
MS_REMOUNT: Final = 32
MS_NOSYMFOLLOW: Final = 256
MS_NOATIME: Final = 1024
MS_NODIRATIME: Final = 2048
MS_BIND: Final = 4096
MS_RELATIME: Final = 1 << 21
ST_NOSYMFOLLOW: Final = 0x2000
"""`linux/statfs.h`; Python's `os` has no name for it."""

LINUX: Final = sys.platform == "linux"
# In a user namespace these flags are locked, and a remount that drops one fails with EPERM.
# Python names most of the `ST_*` flags on Linux alone.
_KEPT: Final = (
    (
        (os.ST_NODEV, MS_NODEV),
        (os.ST_NOSUID, MS_NOSUID),
        (os.ST_NOEXEC, MS_NOEXEC),
        (os.ST_NOATIME, MS_NOATIME),
        (os.ST_NODIRATIME, MS_NODIRATIME),
        (os.ST_RELATIME, MS_RELATIME),
        (os.ST_SYNCHRONOUS, MS_SYNCHRONOUS),
        (ST_NOSYMFOLLOW, MS_NOSYMFOLLOW),
    )
    if LINUX
    else ()
)


@dataclass(slots=True)
class _Namespace:
    private: bool = False


_NAMESPACE: Final = _Namespace()


def enter_private() -> None:
    """Move this process to a mount namespace of its own, when it runs as root on Linux.

    Call it before the process starts a thread: `unshare(CLONE_NEWNS)` fails
    with EINVAL in a process whose threads share its file system attributes.
    A failure leaves the process where it was, as Nix does: a writable store
    needs no namespace, and `make_writable` refuses a read-only one.
    """
    if not LINUX or os.geteuid() != 0 or _NAMESPACE.private:
        return
    try:
        os.unshare(os.CLONE_NEWNS)
    except OSError:
        return
    _NAMESPACE.private = True


def remount_flags(statvfs_flags: int) -> int:
    """The `mount` flags that remount a bind mount with these `statvfs` flags writable, and keep the rest."""
    flags = MS_REMOUNT | MS_BIND
    for kept, flag in _KEPT:
        if statvfs_flags & kept:
            flags |= flag
    return flags


def make_writable(path: Path) -> None:
    """Remount *path* writable if its mount is read-only; only in the namespace `enter_private` made."""
    if not LINUX:
        return
    found = os.statvfs(path).f_flag
    if not found & os.ST_RDONLY:
        return
    if not _NAMESPACE.private:
        raise PynixdError(
            f"cannot remount {path} writable: not in a private mount namespace, "
            "so the remount would affect the host mount table"
        )
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.mount(None, os.fsencode(path), b"none", remount_flags(found), None) == -1:
        raise PynixdError(f"remounting {path} writable: {os.strerror(ctypes.get_errno())}")
