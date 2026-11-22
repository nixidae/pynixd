#!/usr/bin/env bash
# The builder of a `reapi` derivation: Siso compiles one file through the
# Buildbarn frontend that the sandbox has at /run/reapi.sock. `strict_remote`
# forbids a local run, so the build fails when the socket does not answer.
set -euo pipefail
export PATH="$coreutils/bin"
export HOME="$TMPDIR"

root="$TMPDIR/root"
mkdir -p "$root/build/config/siso" "$root/src" "$root/out/x"

cat > "$root/build/config/siso/main.star" <<'EOF'
load("@builtin//encoding.star", "json")
load("@builtin//struct.star", "module")

def init(ctx):
    step_config = {
        "rules": [
            {
                "name": "cc",
                "action": "cc",
                "remote": True,
                "strict_remote": True,
                "use_system_input": True,
                "platform": {"OSFamily": "linux"},
                "timeout": "120s",
            },
        ],
    }
    return module("config", step_config = json.encode(step_config), filegroups = {}, handlers = {})
EOF

cat > "$root/src/hello.c" <<'EOF'
int hello(void) { return 42; }
EOF

cat > "$root/out/x/build.ninja" <<EOF
rule cc
  command = $cc/bin/cc -c \${in} -o \${out}
build obj/hello.o: cc ../../src/hello.c
build build.ninja: phony
default obj/hello.o
EOF

cd "$root"
if ! SISO_REAPI_ADDRESS=unix:///run/reapi.sock RBE_service_no_security=true \
  "$siso/bin/siso" ninja -C out/x -reapi_instance "" > "$TMPDIR/siso.log" 2>&1; then
  cat "$TMPDIR/siso.log"
  exit 1
fi
mkdir -p "$out"
cp "$TMPDIR/siso.log" out/x/obj/hello.o "$out/"
