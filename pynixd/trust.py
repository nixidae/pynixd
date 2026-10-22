"""Who may connect to the store, and who is trusted.

A port of `authPeer` and `matchUser` in `src/nix/unix/daemon.cc:178-257` of
Nix 2.34.8. pynixd applies Nix's rules itself, rather than connecting to
nix-daemon as the client, so that it can grow its own per-user policy later.
Issue #56 holds the decision and the churn of these rules across releases.
"""

from __future__ import annotations

import grp
import pwd
import socket
import struct
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .exceptions import PynixdError
from .serde.auth import Role

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class PeerRefusedError(PynixdError):
    """The peer is not in `allowed-users`, or is a build user."""


@dataclass(frozen=True)
class Peer:
    """The credentials the kernel recorded when the client connected."""

    pid: int | None
    uid: int | None
    gid: int | None


@dataclass(frozen=True)
class TrustPolicy:
    """The three settings `authPeer` reads. The defaults are Nix's."""

    trusted_users: tuple[str, ...] = ("root",)
    allowed_users: tuple[str, ...] = ("*",)
    build_users_group: str = ""
    substituters: tuple[str, ...] = ("https://cache.nixos.org/",)
    trusted_substituters: tuple[str, ...] = ()
    experimental_features: tuple[str, ...] = ()
    """The daemon's own: it ignores a client's (`_PASSED_TO_EVERYONE`)."""

    @classmethod
    def from_nix_config(cls, config: Mapping[str, Any]) -> TrustPolicy:
        """Read the policy from the output of `nix config show --json`."""

        def value(name: str) -> Any:
            return config[name]["value"]

        return cls(
            trusted_users=tuple(value("trusted-users")),
            allowed_users=tuple(value("allowed-users")),
            build_users_group=value("build-users-group") or "",
            substituters=tuple(value("substituters")),
            trusted_substituters=tuple(value("trusted-substituters")),
            experimental_features=tuple(value("experimental-features")) if "experimental-features" in config else (),
        )


_UNRESTRICTED_SETTINGS = frozenset({"timeout", "max-silent-time", "build-poll-interval", "connect-timeout"})
"""What an untrusted client may set, `daemon.cc:297`. Nix compares the
primary name alone, so an alias such as `build-timeout` is refused."""

_SUBSTITUTER_NAMES = frozenset({"substituters", "binary-caches"})

_PASSED_TO_EVERYONE = frozenset({"ssh-auth-sock", "experimental-features", "plugin-files"})
"""Nix ignores these for every client, trusted or not. The upstream daemon
ignores them on pynixd's connection too, with its own message."""


def warning_text(message: str) -> str:
    """What `warn()` puts on the wire: `Logger::warn` in `src/libutil/logging.cc:39`.

    The colour codes travel to the client, which strips them when it prints
    to something other than a terminal.
    """
    return f"\x1b[35;1mwarning:\x1b[0m {message}\n"


def restrict_overrides(overrides: Mapping[str, str], policy: TrustPolicy) -> tuple[dict[str, str], list[str]]:
    """The overrides an untrusted client keeps, and Nix's warning for each one dropped.

    `ClientSettings::apply` in `src/libstore/daemon.cc:234-310`. The fixed
    fields of SetOptions (keep-going, cores and the rest) apply to every
    client, so only the overrides pass through here.

    NIX-DEVIATION (#27): `daemon.cc:253-270` matches a substituter as a
    parsed `StoreReference` since Nix 2.34. pynixd compares the strings, with
    the same retry that adds a trailing slash, which is the rule before 2.34.
    Two spellings of one store, such as query parameters in another order, are
    trusted by Nix and refused here. The cost is one warning and one unused
    substituter for such a client; a port of `StoreReference::parse` costs
    far more. Measure a client that sends such a spelling to reverse this.
    """
    trusted_subs = set(policy.trusted_substituters) | set(policy.substituters)
    kept: dict[str, str] = {}
    warnings: list[str] = []
    # Sorted, as `ClientSettings::apply` walks a `std::map` (`daemon.cc:232`).
    for name, value in sorted(overrides.items()):
        if name in _PASSED_TO_EVERYONE or name in _UNRESTRICTED_SETTINGS or (name == "builders" and value == ""):
            kept[name] = value
        elif name in _SUBSTITUTER_NAMES:
            subs = []
            for sub in value.split():
                if sub in trusted_subs:
                    subs.append(sub)
                elif not sub.endswith("/") and f"{sub}/" in trusted_subs:
                    subs.append(f"{sub}/")
                else:
                    warnings.append(
                        f"ignoring untrusted substituter '{sub}', you are not a trusted user.\n"
                        "Run `man nix.conf` for more information on the `substituters` configuration option."
                    )
            kept[name] = " ".join(subs)
        else:
            warnings.append(
                f"ignoring the client-specified setting '{name}', because it is a restricted setting "
                "and you are not a trusted user"
            )
    return kept, warnings


def peer_of(sock: socket.socket) -> Peer:
    """The credentials of the process at the other end of a Unix socket."""
    if sys.platform == "darwin":
        # `struct xucred`: cr_version, cr_uid, cr_ngroups, cr_groups[16]. The
        # first group is the primary one. Nix reads the same option,
        # `getPeerInfo` in `src/libutil/unix/unix-domain-socket.cc`.
        raw = sock.getsockopt(0, getattr(socket, "LOCAL_PEERCRED", 1), struct.calcsize("IIh16I"))
        _version, uid, ngroups, *groups = struct.unpack("IIh16I", raw)
        return Peer(pid=None, uid=uid, gid=groups[0] if ngroups > 0 else None)
    raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    pid, uid, gid = struct.unpack("3i", raw)
    return Peer(pid=pid, uid=uid, gid=gid)


def _member(user: str, group_name: str) -> bool:
    try:
        return user in grp.getgrnam(group_name).gr_mem
    except KeyError:
        return False


def match_user(user: str | None, group: str | None, users: Sequence[str]) -> bool:
    """`matchUser` of Nix: `*`, the user's name, or `@group`.

    `@group` matches the primary group by name, or any group that lists the
    user as a member. Nix reads group membership from `gr_mem` alone, so a
    user whose only tie to a group is its primary gid matches through the
    first test and not the second.
    """
    if "*" in users:
        return True
    if user is not None and user in users:
        return True
    for entry in users:
        if not entry.startswith("@"):
            continue
        if group is not None and group == entry[1:]:
            return True
        if user is not None and _member(user, entry[1:]):
            return True
    return False


def _user_name(uid: int | None) -> str | None:
    if uid is None:
        return None
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def _group_name(gid: int | None) -> str | None:
    if gid is None:
        return None
    try:
        return grp.getgrgid(gid).gr_name
    except KeyError:
        return str(gid)


def authorise(peer: Peer, policy: TrustPolicy) -> tuple[Role, str | None]:
    """`authPeer` of Nix: the role of the peer, and its user name.

    Raises `PeerRefusedError` with Nix's message for a peer that may not
    connect. A uid with no passwd entry is named by its number, as Nix does.
    Blocking: it reads the passwd and group databases.
    """
    user = _user_name(peer.uid)
    group = _group_name(peer.gid)
    trusted = match_user(user, group, policy.trusted_users)
    refused = not trusted and not match_user(user, group, policy.allowed_users)
    # Nix compares the name against the setting, and an unset setting is
    # empty, so a nameless group can never match it.
    if refused or (policy.build_users_group and group == policy.build_users_group):
        raise PeerRefusedError(f"user '{user or '<unknown>'}' is not allowed to connect to the Nix daemon")
    return (Role.ADMIN if trusted else Role.USER), user
