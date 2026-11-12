#!/usr/bin/env bash
# Run Nix's functional test suite against a daemon. `setup.py` runs first.
#
# Inputs:
#   WORK                 the working directory of setup.py. Default /scratch/nixft.
#   JOBS                 tests at a time. Default 1.
#   NIX_DAEMON_PACKAGE   the package whose `bin/nix daemon` serves the tests.
#                        Default: the Nix on PATH. Give the pynixd shim here to
#                        test pynixd.
#
# Each argument goes to `meson test`, so `run.sh --suite ca` runs one suite.
set -euo pipefail

WORK=${WORK:-/scratch/nixft}

if [[ ! -d "$WORK/build" ]]; then
    echo "run.sh: no build directory at $WORK/build. Run setup.py first." >&2
    exit 2
fi

# `setup.py` resolved this name, and the store of each test must hold no
# symbolic link in any parent. See the comment there.
WORK=$(readlink -f "$WORK")
# The store of each test goes under here. It stays inside the work directory,
# so one version of Nix cannot reach the stores of another.
export TMPDIR=${TMPDIR_ROOT:-$WORK/tmp}

# A store path holds no write permission, so a plain `rm -rf` of the previous
# run fails, and `set -e` then ends this script before one test starts.
# `init.sh` does the same two steps, for the same reason.
if [[ -e "$TMPDIR" ]]; then
    chmod -R u+w "$TMPDIR"
    rm -rf "$TMPDIR"
fi
mkdir -p "$TMPDIR"

# `vars.sh` exports NIX_REMOTE from this. The meson test environment sets
# NIX_REMOTE='', and `vars.sh` runs later and writes over that value.
export NIX_REMOTE_=daemon

# `vars.sh` reads NIX_STORE under `set -u`, so the name must exist. It must
# also be empty. A builder reads NIX_STORE to find the store directory, and
# each test has its own store at `$TEST_ROOT/nix/store`. Nix's NixOS functional
# test sets `/nix/store`, because there the store really is `/nix/store`.
export NIX_STORE=

# The suite marks a daemon run with this name. Two scripts read it to choose
# the exit code they expect: `fetchurl.sh:89` and `dyn-drv/failing-outer.sh:9`,
# and both say "work around the daemon not returning a 100 status correctly".
# `isDaemonNewer` reads it as well, and answers "yes" for every version when it
# is absent, so each version gate passes and states nothing.
if [[ -z "${NIX_DAEMON_PACKAGE:-}" ]]; then
    NIX_DAEMON_PACKAGE=$(dirname "$(dirname "$(readlink -f "$(command -v nix)")")")
fi
export NIX_DAEMON_PACKAGE
echo "run.sh: daemon package is $NIX_DAEMON_PACKAGE"
"$NIX_DAEMON_PACKAGE/bin/nix" daemon --version

# A watchdog. A defect in this harness made the daemon fork without bound
# twice, and each time the machine went into swap before anyone saw it. A
# healthy run needs some tens of processes, so 1500 is far above a good run and
# far below the 16,451 that hurt.
watchdog() {
    local count
    while sleep 10; do
        count=$(find /proc -maxdepth 1 -regex '/proc/[0-9]+' 2>/dev/null | wc -l)
        if [[ "$count" -gt "${WATCHDOG_LIMIT:-1500}" ]]; then
            echo "WATCHDOG: $count processes, killing the run" >&2
            pkill -f 'meson test' 2>/dev/null || true
            pkill -9 -f 'bash -x -e -u' 2>/dev/null || true
            pkill -9 -x nix 2>/dev/null || true
            return 1
        fi
    done
}
watchdog &
watchdog_pid=$!
# The pid must expand now, and not when the shell exits.
# shellcheck disable=SC2064
trap "kill $watchdog_pid 2>/dev/null || true" EXIT

cd "$WORK/build"
# One test at a time. Each script starts its own daemon and runs real builds,
# so a parallel run drives a small machine into swap. The tests then sit
# swapped out and reach the 300 s timeout of each test, which reads as a
# failure and is not one. Nix's own NixOS functional test also passes `-j1`.
meson test --print-errorlogs --num-processes "${JOBS:-1}" "$@" || true

echo "=== SUMMARY ==="
jq -r '.result' "$WORK/build/meson-logs/testlog.json" | sort | uniq -c | sort -rn
echo "=== FAILURES ==="
jq -r 'select(.result == "FAIL") | .name' "$WORK/build/meson-logs/testlog.json"

# Keep the log of this run. The next `meson test` writes over the file, and a
# comparison of two runs needs both. `compare.py` beside this script reads two
# of these and states which tests the second run alone fails.
if [[ -n "${SAVE_LOG:-}" ]]; then
    cp "$WORK/build/meson-logs/testlog.json" "$SAVE_LOG"
    echo "run.sh: log saved at $SAVE_LOG"
fi
