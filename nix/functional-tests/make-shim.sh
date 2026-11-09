#!/usr/bin/env bash
# Build the package that goes in the `NIX_DAEMON_PACKAGE` place, so that
# pynixd answers the functional tests of Nix.
#
# `run.sh` puts `$NIX_DAEMON_PACKAGE/bin` first on the PATH that `startDaemon`
# uses, and `startDaemon` then runs `nix daemon`. So `bin/nix` of this package
# sends `daemon` to pynixd. It sends every other command to the real Nix,
# because `db-migration.sh` and `user-envs-migration.sh` call
# `$NIX_DAEMON_PACKAGE/bin/nix` for ordinary commands, and `isDaemonNewer`
# calls it for `daemon --version`.
#
# Inputs:
#   WORK         the working directory of setup.sh. Default /scratch/nixft.
#   PYNIXD_BIN   the pynixd program. Default: pynixd on PATH.
#   REAL_NIX     the nix program. Default: nix on PATH.
#
# It prints the path of the package, for `NIX_DAEMON_PACKAGE`.
set -euo pipefail

WORK=${WORK:-/scratch/nixft}
SHIM=$WORK/shim
PYNIXD_BIN=${PYNIXD_BIN:-$(command -v pynixd || true)}
REAL_NIX=${REAL_NIX:-$(command -v nix || true)}

if [[ -z "$PYNIXD_BIN" || ! -x "$PYNIXD_BIN" ]]; then
    echo "make-shim.sh: set PYNIXD_BIN to the pynixd program" >&2
    exit 2
fi
if [[ -z "$REAL_NIX" || ! -x "$REAL_NIX" ]]; then
    echo "make-shim.sh: set REAL_NIX to the nix program" >&2
    exit 2
fi

rm -rf "${SHIM:?}"
mkdir -p "$SHIM/bin"

# The two paths come from this shell. Everything after them belongs to the
# shim, and this shell must not read any of it.
cat > "$SHIM/bin/nix" <<EOF
#!/usr/bin/env bash
REAL_NIX=$REAL_NIX
PYNIXD_BIN=$PYNIXD_BIN
EOF

cat >> "$SHIM/bin/nix" <<'EOF'
set -euo pipefail

# Find the subcommand. `startDaemon` runs
#   nix --extra-experimental-features 'nix-command' daemon
# so `daemon` is the last word and not the first. A test of `$1` alone sends
# every daemon to the real Nix, and the suite then measures nothing.
#
# The word after a flag is not the subcommand either:
# `--extra-experimental-features` takes a value, so "the first word that is not
# a flag" finds `nix-command`. This looks for the word `daemon` in any place.
# The suite runs no other command that holds that word.
is_daemon=false
wants_version=false
for arg in "$@"; do
    case "$arg" in
        daemon) is_daemon=true ;;
        --version) wants_version=true ;;
        *) ;;
    esac
done

# Only `nix daemon` belongs to pynixd. `nix daemon --version` does not: the
# suite asks it for a version number, and pynixd states the version of the
# protocol and not of Nix.
if [[ "$is_daemon" != true ]] || [[ "$wants_version" == true ]]; then
    exec "$REAL_NIX" "$@"
fi

if [[ -z "${NIX_STORE_DIR:-}" || -z "${NIX_STATE_DIR:-}" || -z "${NIX_LOG_DIR:-}" || -z "${NIX_DAEMON_SOCKET_PATH:-}" ]]; then
    echo "shim: NIX_STORE_DIR, NIX_STATE_DIR, NIX_LOG_DIR and NIX_DAEMON_SOCKET_PATH must be set" >&2
    exit 2
fi

# **The suite uses a relocated store, and pynixd serves one since #176.**
# `common/vars.sh` exports `NIX_STORE_DIR=$TEST_ROOT/store` and
# `NIX_STATE_DIR=$TEST_ROOT/var/nix`, so the two directories come straight
# from the environment of the test. `setup.sh` used to rewrite that layout to
# a chroot store, and that patch is gone.
#
# `readlink -f` is necessary. Nix refuses a store when a parent directory of it
# is a symbolic link, and it says so: "the path ... is a symlink; this is not
# allowed for the Nix store and its parent directories". `vars.sh` resolves
# NIX_STORE_DIR for the same reason.
store_dir=$(readlink -f "$NIX_STORE_DIR")
state_dir=$(readlink -f "$NIX_STATE_DIR")

# `NIX_LOG_DIR` is independent of the state in Nix, so pynixd must hear it to
# write a build trace where the suite reads it. `vars.sh` sets it to
# `$TEST_ROOT/var/log/nix` and `init.sh` makes `$NIX_LOG_DIR/drvs`.
log_dir=$(readlink -f "$NIX_LOG_DIR")

# The work of pynixd goes beside the test, and not in the store of the test.
# `TEST_ROOT` is what `vars.sh` builds both directories from.
work=${TEST_ROOT:-$(dirname "$(dirname "$state_dir")")}

# **The upstream socket goes under $WORK and not under $TEST_ROOT.** A Unix
# socket path takes 107 bytes, and `$TEST_ROOT` carries the name of the test:
# `build-remote-content-addressed-floating` made
# `$TEST_ROOT/pynixd-upstream.socket` 110 bytes, and that test could not run
# through pynixd at all. Nix binds such a path with a helper that chdirs and
# pynixd cannot connect to it -- issue #44.
#
# A checksum of `$TEST_ROOT` keeps two tests apart in ten digits at most,
# whatever they are called. `$WORK/socks` is short by construction: the
# wrapper refuses a `$WORK` longer than 60 characters.
socks=${WORK:?}/socks
mkdir -p "$socks"
upstream=$socks/$(printf '%s' "$work" | cksum | cut -d' ' -f1).socket

# The trial routes put every write in pynixd's engine. `NIXFT_OP_ROUTES={}`
# runs a deployment's defaults instead, where every write goes to the daemon.
routes=${NIXFT_OP_ROUTES:-'{"NarFromPath": "shadow", "QuerySubstitutablePathInfo": "shadow", "QuerySubstitutablePathInfos": "shadow", "AddToStore": "pynixd", "AddToStoreNar": "pynixd", "AddMultipleToStore": "pynixd", "AddIndirectRoot": "pynixd", "AddPermRoot": "pynixd", "AddSignatures": "pynixd", "RegisterDrvOutput": "pynixd", "AddBuildLog": "pynixd", "OptimiseStore": "pynixd", "VerifyStore": "pynixd", "CollectGarbage": "pynixd", "EnsurePath": "pynixd"}'}

# `NIXFT_NO_DAEMON=1`: no Nix daemon under pynixd. pynixd opens the store and
# answers every operation itself; a shadow route needs the daemon, so none is set.
daemon=true
if [[ "${NIXFT_NO_DAEMON:-}" == 1 ]]; then
    daemon=false
    routes='{}'
fi

config=$work/pynixd-test-config.json
cat > "$config" <<JSON
{
  "stores": {
    "local": {
      "type": "local-socket",
      "store_dir": "$store_dir",
      "state_dir": "$state_dir",
      "log_dir": "$log_dir",
      "socket_path": "$upstream",
      "nix_bin": "$REAL_NIX",
      "use_db": true,
      "monitor": false,
      "probe": false,
      "pynixd_writes": true,
      "daemon": $daemon,
      "op_routes": $routes
    }
  },
  "unix_path": "$NIX_DAEMON_SOCKET_PATH",
  "ssh_port": null,
  "http_port": null
}
JSON

export PYNIXD_CONFIG=$config
# `reach.py` counts the operations of each test from `client_op_timing`, an
# info line, so the whole log goes beside the test. The test's stderr keeps
# what the default level gives it: an info line there reaches the output that
# a script greps.
export PYNIXD_LOG_LEVEL=INFO
# The suite starts this in the background and waits for the socket to appear.
exec "$PYNIXD_BIN" daemon 2> >(tee -a "$work/pynixd.log" | grep --line-buffered -v '"level": "\(info\|debug\)"' >&2)
EOF

chmod +x "$SHIM/bin/nix"
echo "$SHIM"
