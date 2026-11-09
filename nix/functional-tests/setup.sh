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
    echo "  NIX_SRC=\$(nix build --file . internal.pkgs.nixVersions.latest.src --no-link --print-out-paths)" >&2
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
REMOTE="$WORK/src/tests/functional/build-remote.sh"
legacy='  "ssh://localhost?remote-store=$TEST_ROOT/machine1?'
if [[ "$(grep -c -F -- "$legacy" "$REMOTE")" != "1" ]]; then
    echo "setup.sh: build-remote.sh holds no ssh:// builder line; the patch is stale" >&2
    exit 1
fi
awk -v old="$legacy" 'index($0, old) == 1 { sub(/"ssh:\/\//, "\"ssh-ng://") } { print }' "$REMOTE" > "$REMOTE.new"
mv "$REMOTE.new" "$REMOTE"

# The clients of the test open machine0 through that daemon, named in
# NIX_REMOTE and not with `--store`. A trusted client's `--store` reaches the
# daemon as an override, and `ClientSettings::apply` (`daemon.cc:287`) sets it
# in the worker. The worker's build hook opens that store, the daemon itself,
# and its new worker waits for the database lock of the first: the test hung
# for 300 s. NIX_REMOTE is the setting's default, which the client does not
# send. Lillecarl/nix#412.
store_arg='--store "$TEST_ROOT/machine0"'
if [[ "$(grep -c -F -- "$store_arg" "$REMOTE")" != "4" ]]; then
    echo "setup.sh: build-remote.sh holds not 4 '$store_arg'; the patch is stale" >&2
    exit 1
fi
awk -v old="$store_arg" -v new='' \
    '{ while ((i = index($0, old)) > 0) $0 = substr($0, 1, i - 1) new substr($0, i + length(old)); print }' \
    "$REMOTE" > "$REMOTE.new"
mv "$REMOTE.new" "$REMOTE"

# The script is sourced twice by `build-remote-input-addressed.sh`. The second
# run deletes machine0, so the first daemon stops before that.
insert_after "$REMOTE" '^)$' "the end of build-remote.sh's builders" <<'EOF'

# Added for the daemon run. Stop the daemon over machine0 of an earlier run.
if [[ -n "${machine0Pid:-}" ]]; then
    kill "$machine0Pid" 2> /dev/null || true
    wait "$machine0Pid" 2> /dev/null || true
fi
EOF
insert_after "$REMOTE" '^rm -rf "\$TEST_ROOT/machine"\* || true$' "build-remote.sh's machine wipe" <<'EOF'

# Added for the daemon run. A Unix socket path takes 107 bytes, so the socket
# goes under $WORK, as the shim puts its own.
machine0Socket=$WORK/socks/m0-$(printf '%s' "$TEST_ROOT" | cksum | cut -d' ' -f1).socket
mkdir -p "$WORK/socks"
# A client of `unix://...?root=` refuses a root that holds no `nix/store` yet,
# and the daemon makes it only at its first operation. Opening the store
# makes it.
NIX_REMOTE= nix --extra-experimental-features 'nix-command' store info --store "$TEST_ROOT/machine0" > /dev/null
rm -f "$machine0Socket"
NIX_DAEMON_SOCKET_PATH=$machine0Socket PATH=$DAEMON_PATH NIX_REMOTE=$TEST_ROOT/machine0 \
    nix --extra-experimental-features 'nix-command' daemon --store "$TEST_ROOT/machine0" &
machine0Pid=$!
trap 'kill "$machine0Pid" 2> /dev/null || true; wait "$machine0Pid" 2> /dev/null || true; killDaemon' EXIT
for ((i = 0; i < 100; i++)); do
    [[ -S $machine0Socket ]] && break
    sleep 0.1
done
[[ -S $machine0Socket ]] || fail "the daemon over machine0 did not start"
export NIX_REMOTE="unix://$machine0Socket?root=$TEST_ROOT/machine0"
EOF
echo "patches applied:"
grep -n "NIX_REMOTE= nix --extra" "$FUNCS"
grep -n "    restartDaemon" "$FUNCS"
grep -n "NIX_REMOTE= nix-store --init" "$INIT"
grep -n "    startDaemon" "$INIT"
grep -n 'ssh-ng://localhost?remote-store=$TEST_ROOT/machine1\|export NIX_REMOTE=\|daemon --store' "$REMOTE"

meson setup "$WORK/src/tests/functional" "$WORK/build"
echo "READY $WORK"
