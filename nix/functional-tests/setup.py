#!/usr/bin/env python3
"""Prepare Nix's own functional test suite to run against a daemon.

Read README.md in this directory first. It gives the mode, and the reason
each patch below is necessary. The patches stay bash, because the suite is
bash; this program only places them.

Usage:

    NIX_SRC=... [WORK=/scratch/nixft] setup.py

NIX_SRC is the source of Nix, which holds `tests/functional`. WORK is the
working directory. It needs meson and ninja on PATH, and the standard
library alone, as `compare.py` does.

Each patch fails loudly when upstream moves the line it needs. A silent
patch that applies to nothing is the worst result.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


class StalePatchError(Exception):
    """A patch found no anchor, or a different count of anchors."""


def _lines(text: str) -> list[str]:
    return text.splitlines(keepends=True)


def insert_after(text: str, anchor: str, insert: str, what: str) -> str:
    """Put `insert` after the first line that equals `anchor`."""
    lines = _lines(text)
    for index, line in enumerate(lines):
        if line.rstrip("\n") == anchor:
            return "".join([*lines[: index + 1], insert, *lines[index + 1 :]])
    msg = f"holds no line for {what!r}; the patch is stale"
    raise StalePatchError(msg)


def count_containing(text: str, needle: str) -> int:
    """The number of lines that hold `needle`, as `grep -c -F` counts."""
    return sum(needle in line for line in _lines(text))


# --- Patch 1: the daemon must not inherit NIX_REMOTE=daemon ---------------
#
# `startDaemon` sets NIX_REMOTE=daemon after it starts the daemon, so upstream
# always starts a daemon with an empty value. This mode exports NIX_REMOTE
# before the first test, so the daemon inherited `daemon`, and it opened
# itself as its own store. Each worker then became a client of the daemon, and
# that client forked another worker. The argv of a worker holds the process id
# of its client, and those ids were other workers:
#
#     81490  81448  Ssl  nix 81488      <- 81488 is a worker
#     81492  81448  Ssl  nix 81490      <- 81490 is a worker
#
# One test made 16,451 processes in under two minutes, and the machine went
# into swap.
DAEMON_LAUNCH = "    PATH=$DAEMON_PATH nix --extra-experimental-features 'nix-command' daemon &"
DAEMON_LAUNCH_PATCHED = "    PATH=$DAEMON_PATH NIX_REMOTE= nix --extra-experimental-features 'nix-command' daemon &"

# --- Patch 2: give the daemon the store again after a wipe ----------------
#
# `doClearStore` runs `rm -rf "$NIX_STATE_DIR"`, and the database of the store
# is in that directory. The daemon holds that database open. 60 of the 203
# scripts call `clearStore`. `restartDaemon` returns at once when no daemon
# runs, so the plain `NIX_REMOTE=''` mode does not change.
RESTART_AFTER_CLEAR = """\
    # Added for the daemon run. The lines above deleted the database that the
    # daemon holds open. Give it the new store.
    restartDaemon
"""


def patch_functions(text: str) -> str:
    """Patches 1 and 2, to `common/functions.sh`."""
    lines = _lines(text)
    launches = [i for i, line in enumerate(lines) if line.rstrip("\n") == DAEMON_LAUNCH]
    if len(launches) != 1:
        msg = "holds no startDaemon launch line; the patch is stale"
        raise StalePatchError(msg)
    lines[launches[0]] = DAEMON_LAUNCH_PATCHED + "\n"
    return insert_after("".join(lines), "    clearProfiles", RESTART_AFTER_CLEAR, "doClearStore's clearProfiles call")


# --- Patch 3: initialise the database against the local store -------------
#
# `nix-store --init` runs before any daemon, so no socket exists yet, and
# NIX_REMOTE already says `daemon`.
STORE_INIT = "nix-store --init"
STORE_INIT_PATCHED = "NIX_REMOTE= nix-store --init"

# --- Patch 4: start a daemon for every test -------------------------------
#
# Only 19 of the 203 scripts call `startDaemon` themselves. Those 19 keep
# working: `startDaemon` returns at once when a daemon already runs, and
# `restartDaemon` kills the daemon and starts it again.
START_DAEMON = """
# Added for the daemon run. `vars.sh` sets NIX_REMOTE from NIX_REMOTE_.
if [[ "${NIX_REMOTE:-}" == daemon ]]; then
    startDaemon
fi
"""


def patch_init(text: str) -> str:
    """Patches 3 and 4, to `common/init.sh`."""
    lines = _lines(text)
    inits = [i for i, line in enumerate(lines) if line.rstrip("\n") == STORE_INIT]
    if not inits:
        msg = f"holds no {STORE_INIT!r} line; the patch is stale"
        raise StalePatchError(msg)
    for index in inits:
        lines[index] = STORE_INIT_PATCHED + "\n"
    return insert_after("".join(lines), 'test -e "$NIX_STATE_DIR"/db/db.sqlite', START_DAEMON, "init.sh sanity check")


# --- Patch 5: the remote builds reach a daemon ----------------------------
#
# `build-remote.sh` builds with `--store $TEST_ROOT/machine0`, a chroot store
# that the client opens itself, so no daemon took part in its three tests. A
# daemon over machine0, started from `$DAEMON_PATH` as `startDaemon` starts
# one, puts the daemon under test between the client and that store in both
# arms. The client's `--builders` then reach that daemon.
#
# The first builder speaks `ssh://`, Nix's legacy serve protocol, which pynixd
# does not speak (nixidae/nixidae#1242). It becomes `ssh-ng://` in both arms,
# so the placement on three builders is still measured. Over `ssh-ng://`, Nix
# sends no `--keep-failed` to the builder (Lillecarl/nix#413), so the suite's
# Nix carries `nix-ssh-ng-keep-failed.patch`.
LEGACY_BUILDER = '  "ssh://localhost?remote-store=$TEST_ROOT/machine1?'

# The clients of the test open machine0 through that daemon, named in
# NIX_REMOTE and not with `--store`. A trusted client's `--store` reaches the
# daemon as an override, and `ClientSettings::apply` (`daemon.cc:287`) sets it
# in the worker. The worker's build hook opens that store, the daemon itself,
# and its new worker waits for the database lock of the first: the test hung
# for 300 s. NIX_REMOTE is the setting's default, which the client does not
# send. Lillecarl/nix#412.
MACHINE0_STORE_ARG = '--store "$TEST_ROOT/machine0"'
MACHINE0_STORE_ARGS = 4

# The script is sourced twice by `build-remote-input-addressed.sh`. The second
# run deletes machine0, so the first daemon stops before that.
STOP_MACHINE0 = """
# Added for the daemon run. Stop the daemon over machine0 of an earlier run.
if [[ -n "${machine0Pid:-}" ]]; then
    kill "$machine0Pid" 2> /dev/null || true
    wait "$machine0Pid" 2> /dev/null || true
fi
"""

START_MACHINE0 = """
# Added for the daemon run. A Unix socket path takes 107 bytes, so the socket
# goes under $WORK, as the shim puts its own.
machine0Socket=$WORK/socks/m0-$(printf '%s' "$TEST_ROOT" | cksum | cut -d' ' -f1).socket
mkdir -p "$WORK/socks"
# A client of `unix://...?root=` refuses a root that holds no `nix/store` yet,
# and the daemon makes it only at its first operation. Opening the store
# makes it.
NIX_REMOTE= nix --extra-experimental-features 'nix-command' store info --store "$TEST_ROOT/machine0" > /dev/null
rm -f "$machine0Socket"
NIX_DAEMON_SOCKET_PATH=$machine0Socket PATH=$DAEMON_PATH NIX_REMOTE=$TEST_ROOT/machine0 \\
    nix --extra-experimental-features 'nix-command' daemon --store "$TEST_ROOT/machine0" &
machine0Pid=$!
trap 'kill "$machine0Pid" 2> /dev/null || true; wait "$machine0Pid" 2> /dev/null || true; killDaemon' EXIT
for ((i = 0; i < 100; i++)); do
    [[ -S $machine0Socket ]] && break
    sleep 0.1
done
[[ -S $machine0Socket ]] || fail "the daemon over machine0 did not start"
export NIX_REMOTE="unix://$machine0Socket?root=$TEST_ROOT/machine0"
"""


def patch_build_remote(text: str) -> str:
    """Patch 5, to `build-remote.sh`."""
    if count_containing(text, LEGACY_BUILDER) != 1:
        msg = "holds no ssh:// builder line; the patch is stale"
        raise StalePatchError(msg)
    lines = [
        line.replace('"ssh://', '"ssh-ng://', 1) if line.startswith(LEGACY_BUILDER) else line for line in _lines(text)
    ]
    text = "".join(lines)
    if count_containing(text, MACHINE0_STORE_ARG) != MACHINE0_STORE_ARGS:
        msg = f"holds not {MACHINE0_STORE_ARGS} {MACHINE0_STORE_ARG!r}; the patch is stale"
        raise StalePatchError(msg)
    text = text.replace(MACHINE0_STORE_ARG, "")
    text = insert_after(text, ")", STOP_MACHINE0, "the end of build-remote.sh's builders")
    return insert_after(text, 'rm -rf "$TEST_ROOT/machine"* || true', START_MACHINE0, "build-remote.sh's machine wipe")


def replace_line(text: str, old: str, new: str) -> str:
    """Replace the one line that equals `old` with `new`."""
    lines = _lines(text)
    found = [i for i, line in enumerate(lines) if line.rstrip("\n") == old]
    if len(found) != 1:
        msg = f"holds {len(found)} lines {old!r}, not 1; the patch is stale"
        raise StalePatchError(msg)
    lines[found[0]] = new
    return "".join(lines)


# --- Patches 6 to 8: three scripts run through the daemon -----------------
#
# Each script skips under a daemon with `needLocalStore`, and each needs one
# thing on the daemon's side that the script gives its client. The gate goes,
# and the daemon gets that thing. nixidae/nixidae#1256.
GC_SYNC_GATE = 'needLocalStore "the GC test needs a synchronisation point"'
GC_SYNC = """\
# Added for the daemon run. The collector reads its two synchronisation
# points from its own environment (`gc.cc:398`, `:536`), so the daemon needs
# them. The script makes these two FIFOs below.
_NIX_TEST_GC_SYNC_1=$TEST_ROOT/test2.fifo _NIX_TEST_GC_SYNC_2=$TEST_ROOT/test.fifo restartDaemon
"""

FREE_SPACE_GATE = 'needLocalStore "\N{LEFT DOUBLE QUOTATION MARK}min-free\N{RIGHT DOUBLE QUOTATION MARK} and \N{LEFT DOUBLE QUOTATION MARK}max-free\N{RIGHT DOUBLE QUOTATION MARK} are daemon options"'
FREE_SPACE = """\
# Added for the daemon run. The daemon reads the free space from this file at
# its first operation, before the script writes it below, so it starts with the
# script's first value. `--min-free` and `--max-free` reach the daemon as
# settings of a trusted client.
echo 1100 > "$TEST_ROOT/fake-free"
_NIX_TEST_FREE_SPACE_FILE=$TEST_ROOT/fake-free restartDaemon
"""

EXTERNAL_BUILDERS_GATE = (
    "needLocalStore \"'--external-builders' can\N{RIGHT SINGLE QUOTATION MARK}t be used with the daemon\""
)
EXTERNAL_BUILDERS = """\
# Added for the daemon run. The daemon refuses the client's
# `--external-builders` unless its own configuration has the feature.
enableFeatures external-builders
restartDaemon
"""


PATCHES = {
    "common/functions.sh": patch_functions,
    "common/init.sh": patch_init,
    "build-remote.sh": patch_build_remote,
    "gc-non-blocking.sh": lambda text: replace_line(text, GC_SYNC_GATE, GC_SYNC),
    "gc-auto.sh": lambda text: replace_line(text, FREE_SPACE_GATE, FREE_SPACE),
    "external-builders.sh": lambda text: replace_line(text, EXTERNAL_BUILDERS_GATE, EXTERNAL_BUILDERS),
}


def _make_writable(root: Path) -> None:
    root.chmod(root.stat().st_mode | 0o200)
    for parent, dirs, files in os.walk(root):
        for name in [*dirs, *files]:
            path = Path(parent, name)
            if not path.is_symlink():
                path.chmod(path.stat().st_mode | 0o200)


def prepare(nix_src: Path, work: Path) -> Path:
    """Copy the suite to `work/src`, patch it, and return the resolved `work`."""
    # A store path holds no write permission, so a plain removal of a previous
    # run fails on the store of a test. `run.sh` and `init.sh` of Nix make the
    # tree writable first, for the same reason.
    if work.exists():
        _make_writable(work)
        shutil.rmtree(work)
    work.mkdir(parents=True)
    # Nix refuses a store when a parent directory of it is a symbolic link, and
    # it says so: "the path ... is a symlink; this is not allowed for the Nix
    # store and its parent directories". The store of each test is under this
    # directory, so the name must hold no link. `/scratch` of the build machine
    # is one.
    work = work.resolve()
    # `tests/functional/.version` and `tests/functional/nix-meson-build-support`
    # are symbolic links to `../../`, so the whole source must come across.
    src = work / "src"
    shutil.copytree(nix_src, src, symlinks=True)
    _make_writable(src)
    functional = src / "tests/functional"
    for relative, patch in PATCHES.items():
        path = functional / relative
        try:
            path.write_text(patch(path.read_text()))
        except StalePatchError as error:
            msg = f"{path} {error}"
            raise StalePatchError(msg) from error
    return work


def main() -> int:
    nix_src = os.environ.get("NIX_SRC", "")
    if not nix_src:
        print("setup.py: set NIX_SRC to the source of Nix. For example:", file=sys.stderr)
        print(
            "  NIX_SRC=$(nix build --file . internal.pkgs.nixVersions.latest.src --no-link --print-out-paths)",
            file=sys.stderr,
        )
        return 2
    if not Path(nix_src, "tests/functional/common").is_dir():
        print(f"setup.py: {nix_src} holds no tests/functional/common", file=sys.stderr)
        return 2
    try:
        work = prepare(Path(nix_src), Path(os.environ.get("WORK") or "/scratch/nixft"))
    except StalePatchError as error:
        print(f"setup.py: {error}", file=sys.stderr)
        return 1
    functional = work / "src/tests/functional"
    subprocess.run(["meson", "setup", str(functional), str(work / "build")], check=True)  # noqa: S607 -- meson is on the closure's PATH
    print(f"READY {work}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
