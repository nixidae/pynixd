#!/usr/bin/env bash
# The builder of a `reapi` derivation that compiles C++ the way a nixpkgs
# build would remotely: the cc-wrapper's environment reaches the worker through
# nix-remote.env (lib.buildbarn.remoteCompile), and Siso's `remote_wrapper`
# loads it. The source compiles only with a define that exists nowhere but in
# that environment, and includes a system header and a two-step header chain.
# `-MD`, as Chromium: the depfile names the system headers by absolute store
# path, which Siso accepts only with siso-system-deps.patch.
set -euo pipefail
export PATH="$coreutils/bin"
export HOME="$TMPDIR"

root="$TMPDIR/root"
mkdir -p "$root/build/config/siso" "$root/src/inc" "$root/out/x"

"$writeEnv" "$root/nix-remote.env"

cat > "$root/build/config/siso/main.star" <<EOF
load("@builtin//encoding.star", "json")
load("@builtin//struct.star", "module")

def init(ctx):
    step_config = {
        "rules": [
            {
                "name": "cxx",
                "action": "cxx",
                "inputs": ["nix-remote.env"],
                "remote": True,
                "strict_remote": True,
                "remote_wrapper": "$remoteExec",
                "use_system_input": True,
                "platform": {"OSFamily": "linux"},
                "timeout": "120s",
            },
        ],
    }
    return module("config", step_config = json.encode(step_config), filegroups = {}, handlers = {})
EOF

cat > "$root/src/inc/b.h" <<'EOF'
inline int answer() { return 42; }
EOF
cat > "$root/src/inc/a.h" <<'EOF'
#include "b.h"
EOF
cat > "$root/src/hello.cc" <<'EOF'
#include <cstdio>
#include "inc/a.h"
#ifndef NIX_REMOTE_ENV_OK
#error NIX_REMOTE_ENV_OK is not defined: the compile did not get nix-remote.env
#endif
int hello() { std::printf("%d\n", answer()); return 0; }
EOF

cat > "$root/out/x/build.ninja" <<EOF
rule cxx
  command = $cc/bin/c++ -MD -MF \${out}.d -c \${in} -o \${out}
  deps = gcc
  depfile = \${out}.d
build obj/hello.o: cxx ../../src/hello.cc
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
cp "$TMPDIR/siso.log" out/x/obj/hello.o nix-remote.env "$out/"
