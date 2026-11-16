"""Install pynixd as the Nix daemon of a machine that has no Nix: nixidae/nixidae#1365.

The tarball holds the store paths at `nix/store/...` and this program at
`nix/.pynixd/install`, so `tar -x -C /` puts the store in place. This program
registers those paths, makes the default profile, writes the configuration and
starts pynixd under systemd, or under launchd on darwin. pynixd then owns the
store: no nix-daemon runs.

On darwin, /nix must exist before the tarball unpacks: the root volume is read
only, so /nix is an APFS volume that `synthetic.conf` names. The caller makes
it, because this program is inside the tarball.
"""

from __future__ import annotations

import argparse
import contextlib
import grp
import json
import os
import pwd
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

ROOT: Final = Path("/nix/.pynixd")
DAEMON_SOCKET: Final = Path("/nix/var/nix/daemon-socket/socket")
PROFILE: Final = Path("/nix/var/nix/profiles/default")
NIX_CONF: Final = Path("/etc/nix/nix.conf")
PYNIXD_CONF: Final = Path("/etc/pynixd/pynixd.json")
GHA_CACHE_CREDENTIALS: Final = Path("/etc/pynixd/gha-cache.json")
"""Not under `/run/pynixd`: the unit's `RuntimeDirectory`, which systemd empties when pynixd stops."""
UNIT_DIR: Final = Path("/etc/systemd/system")
UNITS: Final = ("pynixd.socket", "pynixd.service")
DARWIN: Final = sys.platform == "darwin"
# `launchdLabel` in `default.nix`.
LAUNCHD_LABEL: Final = "org.nixidae.pynixd"
LAUNCHD_JOB: Final = Path(f"/Library/LaunchDaemons/{LAUNCHD_LABEL}.plist")
LAUNCHD_LOG: Final = Path("/var/log/pynixd.log")
# Nix's darwin installer: `NIX_BUILD_GROUP_ID`, `NIX_FIRST_BUILD_UID` and
# `NIX_USER_COUNT` (`scripts/install-darwin-multi-user.sh`). 351, not 301:
# macOS 15 takes 300 to 304 for its own accounts.
BUILD_GROUP: Final = "nixbld"
BUILD_GID: Final = 350
FIRST_BUILD_UID: Final = 351
BUILD_USERS: Final = 32
SOCKET_WAIT: Final = 60.0


@dataclass(frozen=True, slots=True)
class Bundle:
    """What the tarball's build wrote into `manifest.json`: the store paths this program needs."""

    nix: Path
    profile: Path
    cacert: Path
    units: Path

    @classmethod
    def read(cls, root: Path) -> Bundle:
        data = json.loads((root / "manifest.json").read_text())
        return cls(
            nix=Path(data["nix"]),
            profile=Path(data["profile"]),
            cacert=Path(data["cacert"]),
            units=Path(data["units"]),
        )


def _run(argv: list[str], *, stdin: Path | None = None) -> None:
    print(f"pynixd-install: {' '.join(argv)}", file=sys.stderr)
    if stdin is None:
        subprocess.run(argv, check=True)
        return
    with stdin.open("rb") as handle:
        subprocess.run(argv, check=True, stdin=handle)


def nix_conf(bundle: Bundle, trusted_users: list[str], extra: str, *, darwin: bool) -> str:
    """The machine's `nix.conf`.

    On Linux, builds run as auto-allocated users, so the installer adds no
    account. On darwin `auto-allocate-uids` alone gives no build user
    (`user-lock.cc:242`), so builds run as the `nixbld` group's users.

    `extra` goes between the defaults and what pynixd's builds need. A later
    line replaces an earlier one, so `extra` may replace a default but not the
    build users: `experimental-features = nix-command` there would otherwise
    turn off `auto-allocate-uids`, and every build would fail.
    """
    defaults = [
        "experimental-features = nix-command flakes",
        "max-jobs = auto",
        f"ssl-cert-file = {bundle.cacert}/etc/ssl/certs/ca-bundle.crt",
    ]
    users = (
        [f"build-users-group = {BUILD_GROUP}"]
        if darwin
        else [
            "build-users-group =",
            "auto-allocate-uids = true",
            "use-cgroups = true",
            "extra-experimental-features = auto-allocate-uids cgroups",
        ]
    )
    required = [*users, f"extra-trusted-users = {' '.join(['root', *trusted_users])}"]
    body = extra if not extra or extra.endswith("\n") else extra + "\n"
    return "\n".join(defaults) + "\n" + body + "\n".join(required) + "\n"


def pynixd_conf(gha_cache: bool) -> dict[str, object]:  # object: JSON, which `PynixdSettings` parses
    """pynixd on the daemon socket, serving the machine's store `/` with no daemon under it.

    With *gha_cache*, also the job's GitHub Actions cache, from `GHA_CACHE_CREDENTIALS`.
    """
    stores: dict[str, object] = {
        "local": {
            "type": "local-socket",
            "store_path": "/",
            # A store at `/` reads as the machine's own daemon's unless it says otherwise.
            "managed": True,
            "daemon": False,
            "pynixd_writes": True,
        },
    }
    if gha_cache:
        stores["gha-cache"] = {"type": "gha-cache", "credentials_file": str(GHA_CACHE_CREDENTIALS)}
    return {
        "unix_path": str(DAEMON_SOCKET),
        "ssh_port": None,
        "http_port": None,
        "stores": stores,
    }


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    print(f"pynixd-install: wrote {path}", file=sys.stderr)


def _dscl(*args: str) -> None:
    _run(["/usr/bin/dscl", ".", *args])


def darwin_build_users() -> None:
    """The `nixbld` group and its `_nixbld<N>` users, as Nix's darwin installer makes them.

    An account that exists is left as it is, so a second install changes nothing.
    """
    try:
        grp.getgrnam(BUILD_GROUP)
    except KeyError:
        _dscl("-create", f"/Groups/{BUILD_GROUP}")
        _dscl("-create", f"/Groups/{BUILD_GROUP}", "PrimaryGroupID", str(BUILD_GID))
        _dscl("-create", f"/Groups/{BUILD_GROUP}", "RealName", "Nix build group")
    for n in range(1, BUILD_USERS + 1):
        name = f"_nixbld{n}"
        try:
            pwd.getpwnam(name)
        except KeyError:
            path = f"/Users/{name}"
            _dscl("-create", path)
            for key, value in (
                ("UniqueID", str(FIRST_BUILD_UID + n - 1)),
                ("PrimaryGroupID", str(BUILD_GID)),
                ("NFSHomeDirectory", "/var/empty"),
                ("UserShell", "/sbin/nologin"),
                ("IsHidden", "1"),
                ("RealName", f"Nix build user {n}"),
            ):
                _dscl("-create", path, key, value)
        if name not in grp.getgrnam(BUILD_GROUP).gr_mem:
            _run(["/usr/sbin/dseditgroup", "-o", "edit", "-t", "user", "-a", name, BUILD_GROUP])


def _wait_for_socket(path: Path, timeout: float) -> None:
    """launchd has no `Type=notify`: pynixd is up when its socket takes a connection."""
    deadline = time.monotonic() + timeout
    while True:
        with socket.socket(socket.AF_UNIX) as client, contextlib.suppress(OSError):
            client.connect(str(path))
            return
        if time.monotonic() > deadline:
            raise SystemExit(f"pynixd-install: pynixd did not listen on {path} in {timeout:.0f} s; see {LAUNCHD_LOG}")
        time.sleep(0.2)


def start_launchd(bundle: Bundle) -> None:
    # A copy, not a link: launchd refuses a job whose file is not root's, and a store path is not a file it owns.
    shutil.copyfile(bundle.units / f"{LAUNCHD_LABEL}.plist", LAUNCHD_JOB)
    LAUNCHD_JOB.chmod(0o644)
    # A job a first install left loaded: `bootstrap` refuses to load it twice.
    subprocess.run(["/bin/launchctl", "bootout", f"system/{LAUNCHD_LABEL}"], check=False, capture_output=True)
    _run(["/bin/launchctl", "bootstrap", "system", str(LAUNCHD_JOB)])
    _wait_for_socket(DAEMON_SOCKET, SOCKET_WAIT)


def start_systemd(bundle: Bundle) -> None:
    for unit in UNITS:
        target = UNIT_DIR / unit
        target.unlink(missing_ok=True)
        target.symlink_to(bundle.units / unit)
    systemctl = shutil.which("systemctl")
    if systemctl is None:
        raise SystemExit("pynixd-install: no systemctl; only systemd machines are supported")
    _run([systemctl, "daemon-reload"])
    # The service is `Type=notify`: `start` returns once pynixd says its listener is bound.
    _run([systemctl, "start", *UNITS])


def install(bundle: Bundle, trusted_users: list[str], extra: str, gha_cache: Path | None = None) -> None:
    for name in trusted_users:
        pwd.getpwnam(name)  # KeyError names a user the machine lacks, before anything is written.
    if gha_cache is not None:
        # The runtime token: root's alone, and gone with the runner.
        GHA_CACHE_CREDENTIALS.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(GHA_CACHE_CREDENTIALS, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as out:
            out.write(gha_cache.read_text())
    _run([str(bundle.nix / "bin" / "nix-store"), "--load-db"], stdin=ROOT / "reginfo")
    _run([str(bundle.nix / "bin" / "nix-env"), "--profile", str(PROFILE), "--set", str(bundle.profile)])
    if DARWIN:
        darwin_build_users()
    _write(NIX_CONF, nix_conf(bundle, trusted_users, extra, darwin=DARWIN))
    _write(PYNIXD_CONF, json.dumps(pynixd_conf(gha_cache is not None), indent=2) + "\n")
    DAEMON_SOCKET.parent.mkdir(parents=True, exist_ok=True)
    if DARWIN:
        start_launchd(bundle)
    else:
        start_systemd(bundle)


def main() -> None:
    parser = argparse.ArgumentParser(prog="pynixd-install", description=__doc__.splitlines()[0])
    parser.add_argument("--trusted-user", action="append", default=[], help="a user to add to trusted-users")
    parser.add_argument("--extra-conf", type=Path, help="a file appended to nix.conf")
    parser.add_argument(
        "--gha-cache",
        type=Path,
        metavar="CREDENTIALS",
        help="use the job's GitHub Actions cache: a JSON file of the runner's ACTIONS_* variables",
    )
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise SystemExit("pynixd-install: run as root")
    extra = args.extra_conf.read_text() if args.extra_conf is not None else ""
    install(Bundle.read(ROOT), args.trusted_user, extra, args.gha_cache)


if __name__ == "__main__":
    main()
