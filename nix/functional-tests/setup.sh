#!/usr/bin/env bash
# Prepare Nix's own functional test suite to run against a daemon.
#
# Read README.md in this directory first. It gives the mode, and the reason
# each patch below is necessary.
#
# Inputs:
#   NIX_SRC   the source of Nix, which holds tests/functional. Required.
#   WORK      the working directory. Default /scratch/nixft.
#
# Needs meson, ninja, jq, git and nix on PATH.
set -euo pipefail

NIX_SRC=${NIX_SRC:-}
WORK=${WORK:-/scratch/nixft}

if [[ -z "$NIX_SRC" ]]; then
    echo "setup.sh: set NIX_SRC to the source of Nix. For example:" >&2
    echo "  NIX_SRC=\$(nix build --file . internal.pkgs.nixVersions.nix_2_34.src --no-link --print-out-paths)" >&2
    exit 2
fi
if [[ ! -d "$NIX_SRC/tests/functional/common" ]]; then
    echo "setup.sh: $NIX_SRC holds no tests/functional/common" >&2
    exit 2
fi

# `head -n N` then the new text then `tail -n +N+1` puts text after line N.
# The suite is bash, and each patch must fail loudly when upstream moves the
# line it needs. A silent patch that applies to nothing is the worst result.
insert_after() {
    local file=$1 pattern=$2 what=$3 line
    line=$(grep -n -- "$pattern" "$file" | head -1 | cut -d: -f1)
    if [[ -z "$line" ]]; then
        echo "setup.sh: $file holds no line for '$what'; the patch is stale" >&2
        exit 1
    fi
    {
        head -n "$line" "$file"
        cat
        tail -n "+$((line + 1))" "$file"
    } > "$file.new"
    mv "$file.new" "$file"
}

# A store path holds no write permission, so a plain `rm -rf` of a previous
# run fails on the store of a test and `set -e` then ends this script. `run.sh`
# and `init.sh` of Nix take the same two steps, for the same reason.
if [[ -e "$WORK" ]]; then
    chmod -R u+w "${WORK:?}"
    rm -rf "${WORK:?}"
fi
mkdir -p "$WORK"
# Nix refuses a store when a parent directory of it is a symbolic link, and it
# says so: "the path ... is a symlink; this is not allowed for the Nix store
# and its parent directories". The store of each test is under this directory,
# so the name must hold no link. `/scratch` of the build machine is one.
WORK=$(readlink -f "$WORK")
# `tests/functional/.version` and `tests/functional/nix-meson-build-support`
# are symbolic links to `../../`, so the whole source must come across.
cp -r "$NIX_SRC/." "$WORK/src"
chmod -R u+w "$WORK/src"

INIT="$WORK/src/tests/functional/common/init.sh"
FUNCS="$WORK/src/tests/functional/common/functions.sh"

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
launch="    PATH=\$DAEMON_PATH nix --extra-experimental-features 'nix-command' daemon &"
if [[ "$(grep -c -F -x -- "$launch" "$FUNCS")" != "1" ]]; then
    echo "setup.sh: functions.sh holds no startDaemon launch line; the patch is stale" >&2
    exit 1
fi
replacement="    PATH=\$DAEMON_PATH NIX_REMOTE= nix --extra-experimental-features 'nix-command' daemon &"
awk -v old="$launch" -v new="$replacement" '$0 == old { print new; next } { print }' \
    "$FUNCS" > "$FUNCS.new"
mv "$FUNCS.new" "$FUNCS"

# --- Patch 2: give the daemon the store again after a wipe ----------------
#
# `doClearStore` runs `rm -rf "$NIX_STATE_DIR"`, and the database of the store
# is in that directory. The daemon holds that database open. 60 of the 203
# scripts call `clearStore`. `restartDaemon` returns at once when no daemon
# runs, so the plain `NIX_REMOTE=''` mode does not change.
insert_after "$FUNCS" '^    clearProfiles$' "doClearStore's clearProfiles call" <<'EOF'
    # Added for the daemon run. The lines above deleted the database that the
    # daemon holds open. Give it the new store.
    restartDaemon
EOF

# --- Patch 3: initialise the database against the local store -------------
#
# `nix-store --init` runs before any daemon, so no socket exists yet, and
# NIX_REMOTE already says `daemon`.
sed -i 's/^nix-store --init$/NIX_REMOTE= nix-store --init/' "$INIT"
if ! grep -q '^NIX_REMOTE= nix-store --init$' "$INIT"; then
    echo "setup.sh: init.sh holds no 'nix-store --init' line; the patch is stale" >&2
    exit 1
fi

# --- Patch 4: start a daemon for every test -------------------------------
#
# Only 19 of the 203 scripts call `startDaemon` themselves. Those 19 keep
# working: `startDaemon` returns at once when a daemon already runs, and
# `restartDaemon` kills the daemon and starts it again.
# `$NIX_STATE_DIR` is a part of the pattern that grep reads, and not a name to
# expand here.
# shellcheck disable=SC2016
insert_after "$INIT" '^test -e "\$NIX_STATE_DIR"/db/db.sqlite$' "init.sh sanity check" <<'EOF'

# Added for the daemon run. `vars.sh` sets NIX_REMOTE from NIX_REMOTE_.
if [[ "${NIX_REMOTE:-}" == daemon ]]; then
    startDaemon
fi
EOF

echo "patches applied:"
grep -n "NIX_REMOTE= nix --extra" "$FUNCS"
grep -n "    restartDaemon" "$FUNCS"
grep -n "NIX_REMOTE= nix-store --init" "$INIT"
grep -n "    startDaemon" "$INIT"

meson setup "$WORK/src/tests/functional" "$WORK/build"
echo "READY $WORK"
