# A minimal NixOS container system, plus a pile of impure derivations.
#
# Two paths, and the system alone only exercises one of them. Evaluating
# nixpkgs asks the daemon about every derivation -- an IsValidPath and an
# AddTempRoot each -- and the realisation substitutes the closure. It does not
# *build* anything, because a minimal system is all in the store.
#
# So a thousand fresh derivations ride along, chained under one goal. Each
# reads `stamp`: a value the store cannot already hold, so the daemon must
# build it. The phase passes one value for both of its builds, so the goal is
# the same derivation twice. The first build is cold, the second is hot, and
# the second is the incremental cost of the front-end.
#
# The goal is its own derivation whose inputs are the system and the noise, so
# one `nix build` realises all of it rather than substituting a system whose
# closure is already in the store.
{
  hostName ? "rawbench",
  # A value with no other meaning, only one the store does not hold. The
  # phase picks one per run so the first build really builds.
  stamp ? builtins.currentTime,
}:
let
  pkgs = import <nixpkgs> { };
  inherit (pkgs) lib;
  count = 1000;

  deps = lib.genList (
    n:
    pkgs.runCommand "bench-noise-${hostName}-${toString n}" {
      inherit stamp;
    } "touch $out"
  ) count;

  # `toString deps` is the store paths as a string, so this drv reads them and
  # cannot be realised until every one of them is.
  top = pkgs.runCommand "bench-top-${hostName}" { } ''
    touch $out
    echo ${toString deps}
  '';

  system = pkgs.nixos (
    { modulesPath, ... }:
    {
      imports = [ (modulesPath + "/profiles/minimal.nix") ];
      boot.isContainer = true;
      networking.hostName = hostName;
      nixpkgs.hostPlatform = "x86_64-linux";
      system.stateVersion = lib.mkDefault "24.11";
    }
  );
in
pkgs.runCommand "bench-goal-${hostName}" { } ''
  touch $out
  echo ${system.config.system.build.toplevel} ${top}
''
