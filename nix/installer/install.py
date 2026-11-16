"""Install pynixd as the Nix daemon of a machine that has no Nix: nixidae/nixidae#1365.

The tarball holds the store paths at `nix/store/...` and this program at
`nix/.pynixd/install`, so `tar -x -C /` puts the store in place. This program
registers those paths, makes the default profile, writes the configuration and
starts pynixd under systemd. pynixd then owns the store: no nix-daemon runs.
"""

from __future__ import annotations

import argparse
import json
import os
import pwd
import shutil
import subprocess
import sys
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


def nix_conf(bundle: Bundle, trusted_users: list[str], extra: str) -> str:
    """The machine's `nix.conf`: builds as auto-allocated users, so the installer adds no account.

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
    required = [
        "build-users-group =",
        "auto-allocate-uids = true",
        "use-cgroups = true",
        "extra-experimental-features = auto-allocate-uids cgroups",
        f"extra-trusted-users = {' '.join(['root', *trusted_users])}",
    ]
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
    _write(NIX_CONF, nix_conf(bundle, trusted_users, extra))
    _write(PYNIXD_CONF, json.dumps(pynixd_conf(gha_cache is not None), indent=2) + "\n")
    DAEMON_SOCKET.parent.mkdir(parents=True, exist_ok=True)
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
