"""Build users (`unix/user-lock.cc`): `SimpleUserLock`, one member of `build-users-group` per build, or
`AutoUserLock`, one slot of `auto-allocate-uids` per build.

A lock is `flock` on `<state>/userpool/<uid>` or `<state>/userpool2/slot-<n>`, the file Nix's own lock takes,
so a `nix-daemon` beside pynixd never picks the same user.
"""

from __future__ import annotations

import errno
import fcntl
import grp
import os
import pwd
from dataclasses import dataclass
from pathlib import Path

DEFAULT_STATE_DIR = Path("/nix/var/nix")
IDS_PER_BUILD = 1 << 16
"""`maxIdsPerBuild` on Linux (`local-settings.hh:131`): the ids of one `auto-allocate-uids` slot."""


class BuildUserError(Exception):
    """A `build-users-group` Nix refuses, in Nix's words."""


@dataclass
class BuildUser:
    uid: int
    gid: int
    fd: int
    groups: tuple[int, ...] = ()
    """The user's supplementary groups but *gid*, such as `kvm`: an unsandboxed builder keeps them (`setUser`)."""

    def release(self) -> None:
        os.close(self.fd)


def use_build_users(group: str, auto_allocate_uids: bool) -> bool:
    """`useBuildUsers` (`user-lock.cc:237`)."""
    return bool(group or auto_allocate_uids) and os.geteuid() == 0


def pool_dir(state_dir: Path, relocated: bool, *, auto: bool = False) -> Path:
    """`settings.nixStateDir / "userpool"`, or `"userpool2"` with *auto*: the process's state directory, not a
    chroot store's.

    `nix daemon --store <root>` reads no `NIX_STATE_DIR` for the root (nixidae/nixidae#210), so every chroot
    store on a host shares one pool. A relocated store's daemon gets its `NIX_STATE_DIR`.
    """
    base = state_dir if relocated else Path(os.environ.get("NIX_STATE_DIR", DEFAULT_STATE_DIR))
    return base / ("userpool2" if auto else "userpool")


def acquire(pool: Path, group: str) -> BuildUser | None:
    """A free member of *group*, locked, or `None` when every one builds already.

    Blocking: it reads the group and user databases.
    """
    try:
        entry = grp.getgrnam(group)
    except KeyError:
        raise BuildUserError(f"the group '{group}' specified in 'build-users-group' does not exist") from None
    if not entry.gr_mem:
        raise BuildUserError(f"the build users group '{group}' has no members")
    pool.mkdir(parents=True, exist_ok=True)
    for name in entry.gr_mem:
        try:
            user = pwd.getpwnam(name)
        except KeyError:
            raise BuildUserError(f"the user '{name}' in the group '{group}' does not exist") from None
        uid = user.pw_uid
        fd = os.open(pool / str(uid), os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            os.close(fd)
            if e.errno != errno.EWOULDBLOCK:
                raise
            continue
        if uid in (os.getuid(), os.geteuid()):
            os.close(fd)
            raise BuildUserError(f"the Nix user should not be a member of '{group}'")
        # `SimpleUserLock::acquire` (`user-lock.cc:123`).
        groups = tuple(g for g in os.getgrouplist(name, user.pw_gid) if g != entry.gr_gid)
        return BuildUser(uid, entry.gr_gid, fd, groups)
    return None


def acquire_auto(pool: Path, group: str, *, start_id: int, id_count: int, user_namespace: bool) -> BuildUser | None:
    """`AutoUserLock::acquire` (`user-lock.cc:165`): a free slot of `userpool2`, locked, or `None` when each one
    builds already. A slot's uid has no account; in a user namespace its gid is the uid too.

    Blocking: it reads the user and group databases.
    """
    pool.mkdir(parents=True, exist_ok=True)
    for slot in range(id_count // IDS_PER_BUILD):
        fd = os.open(pool / f"slot-{slot}", os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            os.close(fd)
            if e.errno != errno.EWOULDBLOCK:
                raise
            continue
        uid = start_id + slot * IDS_PER_BUILD
        try:
            name = pwd.getpwuid(uid).pw_name
        except KeyError:
            pass
        else:
            os.close(fd)
            raise BuildUserError(f"auto-allocated UID {uid} clashes with existing user account '{name}'")
        if user_namespace:
            return BuildUser(uid, uid, fd)
        try:
            gid = grp.getgrnam(group).gr_gid
        except KeyError:
            os.close(fd)
            raise BuildUserError(f"the group '{group}' specified in 'build-users-group' does not exist") from None
        return BuildUser(uid, gid, fd)
    return None


def chown_to(user: BuildUser | None, path: Path) -> None:
    """`chownToBuilder`: nothing without a build user."""
    if user is not None:
        os.chown(path, user.uid, user.gid)


def check_not_world_writable(path: Path) -> None:
    """`checkNotWorldWritable` (`derivation-builder.cc:356`): a build user must not share its parents."""
    for at in (path, *path.parents):
        # `lstat`: a symlink is mode 0777, so it fails too.
        if at.lstat().st_mode & 0o002:
            raise BuildUserError(f'Path "{at}" is world-writable or a symlink. That\'s not allowed for security.')
