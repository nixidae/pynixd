# The Nix functional suite (nixft), in a UML guest.
#
# Both arms (control + pynixd) run inside the guest off a work directory on
# the guest's disk, so nothing of the run touches the host but the guest's
# sparse disk and memory files, which the poweroff removes. The guest sees
# the host's store as a local-overlay lower layer (`hostStore`), so every
# input the suite builds against resolves locally and no fetch crosses the
# uplink; the per-test stores land in the upper layer and die with the
# guest. Issue #45.
#
# UML by direction: the suite is daemons and builds, not clocks, and UML
# needs no KVM. The backend is hardcoded, not a knob: this spec exists for
# one run shape.
{
  pkgs,
  lib ? pkgs.lib,
  pynixd-lib,
  src,
  vivariumLib,
  nixft,
}:

vivariumLib.mkTest (
  { config, ... }:
  {
    name = "nixft";

    backend = "uml";

    /*
      What the scripts need that only Nix knows.

      `mkTest` registers the closure of everything here with the guest's
      Nix database, so the harness is a valid path in there rather than a
      file Nix goes looking for a substituter for. The host store behind
      it arrives through `hostStore` on the node below.
    */
    settings = {
      nixft = "${nixft}/bin/nanopynix-nixft-nix_2_34";
    };

    phases = {
      nixft = {
        script = ../../guest/nixft.py;
        after = [ "boot" ];
        nodes = [ "nixft" ];
        description = "nix functional suite, both arms";
      };
    };

    nodes = {
      nixft = {
        vivarium = {
          # Two arms of per-test daemons and builds. The host pays touched
          # pages only, so the cap is generous and the cost follows use.
          memory = "8192M";
          # Both arms keep per-test stores under one work directory.
          # Sparse, so the host pays used blocks; the poweroff removes it.
          diskSize = 61440;
          hostStore.enable = true;
        };

        environment.systemPackages = [
          pkgs.bash
          pkgs.coreutils
        ];

        nix.settings = {
          experimental-features = [
            "nix-command"
            "flakes"
            "ca-derivations"
            "dynamic-derivations"
            "recursive-nix"
          ];
          trusted-users = [ "root" ];
        };

        # `nix.nixPath` names the nixpkgs the suite evaluates against,
        # the way the pytest guests do for `tests/nix/drv-probes.nix`.
        nix.nixPath = [ "nixpkgs=${pkgs.path}" ];
      };
    };
  }
)
