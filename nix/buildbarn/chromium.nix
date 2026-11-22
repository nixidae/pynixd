/**
  A nixpkgs Chromium whose C, C++ and assembler compiles run on Buildbarn.

  The derivation requires the `reapi` system feature, so it lands on a
  builder that binds a Buildbarn frontend at `socket`. nixpkgs' own build
  phase stays: a `ninja` on `PATH` runs Siso instead, with a `main.star`
  that sends the `cc`, `cxx` and `asm` rules to remote execution through
  `lib.buildbarn.remoteCompile`, with no local fallback. Every other step
  (Python actions, links, Rust) runs locally.

  nixpkgs builds with the unbundle toolchain, which takes `CC` and `CXX` from
  the environment, bare (`clang++`). A remote action has no `PATH`, so they
  become absolute before `gn gen`.

  After each Siso run, the shim reads `siso_metrics.json` and fails the build
  if a compile ran locally. The counts go to `siso_compile_summary.json` in
  the build directory.

  # Inputs

  `chromium`
  : the unwrapped browser derivation, such as `chromium.browser`

  `socket`
  : the frontend's socket inside the sandbox

  `instance`
  : the REAPI instance name

  `remoteJobs`
  : remote compiles Siso runs at once

  `toolsOnly`
  : names of native inputs no compile reads, kept out of the workers' closure (go and cargo with rustc: 1.3 GiB)
*/
{
  lib,
  writeShellScript,
  writeText,
  jq,
  siso,
  remoteCompile,
}:
{
  chromium,
  socket ? "/run/reapi.sock",
  instance ? "",
  remoteJobs ? 96,
  toolsOnly ? [
    "go"
    "cargo"
  ],
}:
let
  inherit (chromium.stdenv) cc;
  # A native input reaches a compile only as an `-isystem` in
  # NIX_CFLAGS_COMPILE_FOR_BUILD; `check` refuses a miss at configure time.
  native = lib.filter (p: !lib.elem (lib.getName p) toolsOnly) (
    chromium.nativeBuildInputs ++ chromium.depsBuildBuild
  );
  inputs = lib.filter lib.isDerivation (chromium.buildInputs ++ native);
  compile = remoteCompile {
    name = "chromium-remote-compile";
    closure = [ cc ] ++ map lib.getDev inputs;
  };
  rule = name: {
    inherit name;
    action = name;
    inputs = [ "nix-remote.env" ];
    remote = true;
    strict_remote = true;
    remote_wrapper = "${compile.exec}";
    use_system_input = true;
    platform.OSFamily = "linux";
    timeout = "15m";
  };
  mainStar = writeText "main.star" ''
    load("@builtin//struct.star", "module")

    def init(ctx):
        return module(
            "config",
            step_config = ${
              builtins.toJSON (
                builtins.toJSON {
                  rules = map rule [
                    "cc"
                    "cxx"
                    "asm"
                  ];
                }
              )
            },
            filegroups = {},
            handlers = {},
        )
  '';
  ninja = writeShellScript "ninja" ''
    set -euo pipefail
    args=() dir=.
    while (($#)); do
      case $1 in
        -j) shift 2 ;;
        -j*) shift ;;
        -C) dir=$2; args+=("$1" "$2"); shift 2 ;;
        *) args+=("$1"); shift ;;
      esac
    done
    HOME=$TMPDIR SISO_REAPI_ADDRESS=unix://${socket} RBE_service_no_security=true \
      ${lib.getExe' siso "siso"} ninja -strict_remote -reapi_instance "${instance}" \
      -local_jobs "''${NIX_BUILD_CORES:-1}" -remote_jobs ${toString remoteJobs} "''${args[@]}"
    ${lib.getExe jq} -s '
      map(select(.rule == "cc" or .rule == "cxx" or .rule == "asm"))
      | { remote: map(select(.is_remote and (.cached | not))) | length,
          cached: map(select(.cached)) | length,
          local: map(select((.is_remote or .cached) | not)) | length }
    ' "$dir/siso_metrics.json" > "$dir/siso_compile_summary.json"
    echo "siso compiles: $(< "$dir/siso_compile_summary.json")" >&2
    ${lib.getExe jq} -e '.local == 0' "$dir/siso_compile_summary.json" > /dev/null || {
      echo "a compile ran locally; Buildbarn did not take it" >&2
      exit 1
    }
  '';
in
chromium.overrideAttrs (old: {
  requiredSystemFeatures = (old.requiredSystemFeatures or [ ]) ++ [ "reapi" ];
  preConfigure = (old.preConfigure or "") + ''
    export CC=${cc}/bin/clang CXX=${cc}/bin/clang++
  '';
  postConfigure = (old.postConfigure or "") + ''
    install -m 0644 ${mainStar} build/config/siso/main.star
    ${compile.writeEnv} nix-remote.env
    find out -name '*.ninja' -exec grep -hE '^ *(defines|include_dirs|cflags|cflags_c|cflags_cc|asmflags) =' {} + > "$TMPDIR/compile-flags"
    ${compile.check} "$TMPDIR/compile-flags"
    mkdir -p "$TMPDIR/siso-bin"
    ln -s ${ninja} "$TMPDIR/siso-bin/ninja"
    export PATH="$TMPDIR/siso-bin:$PATH"
  '';
  passthru = (old.passthru or { }) // {
    remoteCompile = compile;
  };
})
