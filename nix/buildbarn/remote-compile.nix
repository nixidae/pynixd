/**
  A compiler environment for Siso's remote compile steps.

  A remote action carries no environment (Siso sends its `env` config with
  local steps only), but nixpkgs' cc-wrapper reads its flags from the
  environment. `exec` is the Siso rule's `remote_wrapper`: it loads the
  environment from `nix-remote.env` in the action's input tree, then runs the
  command. Its store path is argv[0], so `pynixd reapi-runner` makes its
  closure valid on the worker, and `closure` names everything a compile reads
  from the store.

  `writeEnv FILE`, run inside the build, writes that file: an allow-list of the
  cc-wrapper's compile variables, with `-frandom-seed` pinned. The file is an
  input of every action, so its text is in every action digest: nothing that
  differs per derivation may be in it. It refuses a store path that `exec`'s
  closure lacks, since a worker could not read it.

  # Inputs

  `closure`
  : store paths the remote compiles read: the compiler, and the `-isystem` directories of the build's inputs

  `name`
  : name of `exec`
*/
{
  lib,
  writeShellScript,
  closureInfo,
  coreutils,
  gnugrep,
  gnused,
}:
{
  closure,
  name ? "remote-compile",
}:
let
  exec = writeShellScript name ''
    # Closure: ${lib.concatStringsSep " " (map toString closure)}
    dir=$PWD
    until [[ -e $dir/nix-remote.env ]]; do
      if [[ -z $dir ]]; then
        echo "${name}: no nix-remote.env above $PWD" >&2
        exit 127
      fi
      dir=''${dir%/*}
    done
    set -a
    . "$dir/nix-remote.env"
    set +a
    exec "$@"
  '';
  info = closureInfo { rootPaths = [ exec ]; };
  sort = lib.getExe' coreutils "sort";
  comm = lib.getExe' coreutils "comm";
  grep = lib.getExe gnugrep;
  sed = lib.getExe gnused;
  writeEnv = writeShellScript "${name}-write-env" ''
    set -euo pipefail
    file=$1
    # Compile flags and the roles that make the cc-wrapper read them. Not the
    # link variables: stdenv puts `-rpath $out/lib` in NIX_LDFLAGS.
    allowed='^NIX_(CC_WRAPPER_TARGET_(BUILD|HOST|TARGET)_[A-Za-z0-9_]+|(CFLAGS_COMPILE|CFLAGS_COMPILE_BEFORE|CXXSTDLIB_COMPILE|ENFORCE_NO_NATIVE|HARDENING_ENABLE)(_FOR_BUILD|_FOR_TARGET)?(_[A-Za-z0-9_]+)?)$'
    : > "$file"
    # Not `compgen`: nixpkgs' non-interactive bash lacks it.
    mapfile -t names < <(printf '%s\n' "''${!NIX_@}" | LC_ALL=C ${sort})
    for var in "''${names[@]}"; do
      [[ $var =~ $allowed ]] || continue
      [[ $(declare -p "$var") == "declare -x"* ]] || continue
      value=''${!var}
      # reproducible-builds.sh seeds from $out: pinned, or no action is shared across derivations.
      if [[ $var == NIX_CFLAGS_COMPILE* ]]; then
        value=$(${sed} -E 's/(^| )-frandom-seed=[^ ]*//g' <<< "$value")
        value="$value -frandom-seed=nixremote"
      fi
      printf '%s=%q\n' "$var" "$value" >> "$file"
    done
    missing=$(${grep} -oE '${builtins.storeDir}/[a-z0-9]{32}-[^/ "'"'"'=:\\]+' "$file" | LC_ALL=C ${sort} -u \
      | LC_ALL=C ${comm} -23 - <(LC_ALL=C ${sort} -u ${info}/store-paths) || true)
    if [[ -n $missing ]]; then
      echo "${name}: nix-remote.env names store paths outside the closure of ${exec}:" >&2
      echo "$missing" >&2
      exit 1
    fi
  '';
in
{
  inherit exec writeEnv;
}
