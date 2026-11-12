#!/usr/bin/env bash
# Build a package for the `NIX_DAEMON_PACKAGE` place that records the wire.
#
# It wraps another such package. `bin/nix daemon` of this one starts the
# recorder, and the recorder starts `bin/nix daemon` of the inner package. So
# the same shim records a plain Nix daemon and pynixd, and the two runs differ
# in the inner package alone. That is the whole point: a difference in the two
# recordings is a difference in the two daemons, and not in the harness.
#
#   client -> $NIX_DAEMON_SOCKET_PATH -> recorder -> iSocket -> inner daemon
#
# The recorder gives the inner daemon `NIX_DAEMON_SOCKET_PATH=iSocket`, so the
# inner package needs no change and does not know that it is inner.
#
# Inputs:
#   WORK        the working directory of setup.py. Default /scratch/nixft.
#   INNER       the package whose `bin/nix daemon` answers. Required.
#   REAL_NIX    the nix program, for every command that is not `daemon`.
#   OUT_ROOT    the directory for the recordings. Required.
#   PYTHON      a python that can import nix_daemon_protocol. Required.
#   SHIM_DIR    where to write this shim. Default $WORK/record-shim.
#
# It prints the path of the package, for `NIX_DAEMON_PACKAGE`.
set -euo pipefail

WORK=${WORK:-/scratch/nixft}
SHIM=${SHIM_DIR:-$WORK/record-shim}
INNER=${INNER:-}
OUT_ROOT=${OUT_ROOT:-}
PYTHON=${PYTHON:-}
REAL_NIX=${REAL_NIX:-$(command -v nix || true)}

if [[ -z "$INNER" || ! -x "$INNER/bin/nix" ]]; then
    echo "make-record-shim.sh: set INNER to a package that has bin/nix" >&2
    exit 2
fi
if [[ -z "$OUT_ROOT" ]]; then
    echo "make-record-shim.sh: set OUT_ROOT to the recording directory" >&2
    exit 2
fi
if [[ -z "$PYTHON" || ! -x "$PYTHON" ]]; then
    echo "make-record-shim.sh: set PYTHON to a python that has nix_daemon_protocol" >&2
    exit 2
fi
if [[ -z "$REAL_NIX" || ! -x "$REAL_NIX" ]]; then
    echo "make-record-shim.sh: set REAL_NIX to the nix program" >&2
    exit 2
fi

rm -rf "${SHIM:?}"
mkdir -p "$SHIM/bin"

# The four paths come from this shell. Everything after them belongs to the
# shim, and this shell must not read any of it.
cat > "$SHIM/bin/nix" <<EOF
#!/usr/bin/env bash
REAL_NIX=$REAL_NIX
INNER=$INNER
OUT_ROOT=$OUT_ROOT
PYTHON=$PYTHON
EOF

cat >> "$SHIM/bin/nix" <<'EOF'
set -euo pipefail

# The same rule as `make-shim.sh`: `daemon` is a word in any place, and
# `daemon --version` is not ours. Read the comment there for the reason.
is_daemon=false
wants_version=false
for arg in "$@"; do
    case "$arg" in
        daemon) is_daemon=true ;;
        --version) wants_version=true ;;
        *) ;;
    esac
done

if [[ "$is_daemon" != true ]] || [[ "$wants_version" == true ]]; then
    exec "$REAL_NIX" "$@"
fi

if [[ -z "${NIX_DAEMON_SOCKET_PATH:-}" ]]; then
    echo "record-shim: NIX_DAEMON_SOCKET_PATH must be set" >&2
    exit 2
fi

# `iSocket` beside `dSocket`, and the two names are the same length. A Unix
# socket name has to fit in `sun_path`, which is 108 bytes, and the test root
# already holds the suite and the test name. A longer name here would fail
# only for the tests whose name is long, which is the worst way to fail.
inner=$(dirname "$NIX_DAEMON_SOCKET_PATH")/iSocket

# One directory for each test, under the name that the suite gives the test.
# The control run and the candidate run then write the same names, and the
# comparison lines them up with no other record.
key=${TEST_SUITE_NAME:-default}/${TEST_NAME:-unnamed}

# One more directory for each daemon of that test. `restartDaemon` starts a
# second recorder, and without this the second one writes over the first.
# The count is the same in both runs, because both run the same script.
out=$OUT_ROOT/$key
mkdir -p "$out"
index=$(find "$out" -mindepth 1 -maxdepth 1 -type d | wc -l)
out=$out/daemon-$index
mkdir -p "$out"

exec "$PYTHON" -m nix_daemon_protocol.wirelog record \
    --listen "$NIX_DAEMON_SOCKET_PATH" \
    --connect "$inner" \
    --out "$out" \
    -- "$INNER/bin/nix" "$@"
EOF

chmod +x "$SHIM/bin/nix"
echo "$SHIM"
