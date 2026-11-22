# A sandboxed Nix build runs its compile steps on Buildbarn.
#
# `nix run --file . internal.tests.buildbarn.driver -- --out ./out` boots one
# guest with pynixd as its daemon and Buildbarn beside it
# (nix/buildbarn/module.nix). tester builds derivations that require `reapi`;
# their builder runs Siso against the frontend socket in the sandbox. See
# tests/buildbarn/remote.py.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivariumLib,
}:

let
  buildbarn = pkgs.callPackages ../../../nix/buildbarn { };
in
vivariumLib.mkTest {
  name = "pynixd-buildbarn";

  defaults = vivariumLib.settingsClosure;
  settings = {
    system = pkgs.stdenv.hostPlatform.system;
    bash = "${pkgs.bash}";
    coreutils = "${pkgs.coreutils}";
    cc = "${pkgs.stdenv.cc}";
    inherit (buildbarn) siso;
    script = "${../../buildbarn/siso-build.sh}";
  };

  phases.remote = {
    script = ../../buildbarn/remote.py;
    after = [ "boot" ];
    description = "Siso in a sandbox compiles on Buildbarn, then hits its cache";
  };

  nodes.builder = {
    imports = [
      ../../../nix/nixos/default.nix
      ../../../nix/buildbarn/module.nix
    ];
    vivarium.memory = "2048M";
    services.pynixd = {
      enable = true;
      mode = "replace";
      inherit package;
      settings.ranking.min_free_bytes = 0;
    };
    services.buildbarn = {
      enable = true;
      concurrency = 2;
      casSizeBytes = 512 * 1024 * 1024;
      workerCacheSizeBytes = 256 * 1024 * 1024;
    };
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };
    nix.settings = {
      experimental-features = [ "nix-command" ];
      substituters = lib.mkForce [ ];
    };
  };
}
