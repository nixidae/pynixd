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
# QEMU, because it is the backend that honours `vivarium.cpus`. UML has one
# processor, and the suite ran there one test at a time: about 30 minutes for
# each arm. Four at a time on QEMU: about 4.5 minutes. The backend is hardcoded, not a knob: this spec exists for one
# run shape.
{
  pkgs,
  lib ? pkgs.lib,
  pynixd-lib,
  src,
  vivariumLib,
  nixft,
  # pynixd runs no Nix daemon under it: `NIXFT_NO_DAEMON=1` of the shim. Issue #1155.
  noDaemon ? false,
}:

vivariumLib.mkTest (
  { config, ... }:
  {
    name = if noDaemon then "nixft-no-daemon" else "nixft";

    backend = "qemu";

    # Every path `settings` names is one the guests read.
    defaults =
      { settings, ... }:
      {
        vivarium.lateClosure = vivariumLib.pathsIn settings;
      };
    /*
      What the scripts need that only Nix knows.

      The guest registers the closure of everything here with its Nix
      database (`lateClosure`), so the harness is a valid path in there
      rather than a file Nix goes looking for a substituter for. The host
      store behind it arrives through `hostStore` on the node below.
    */
    settings = {
      nixft = lib.getExe nixft;
      noDaemon = if noDaemon then "1" else "";
    };

    phases = {
      nixft = {
        script = ../../guest/nixft.py;
        after = [ "boot" ];
        nodes = [ "nixft" ];
        description = "nix functional suite, both arms${lib.optionalString noDaemon ", pynixd with no daemon"}";
      };
    };

    nodes = {
      nixft = {
        vivarium = {
          # `nixft.py` runs a test on every second processor.
          cpus = 8;
          # Per-test daemons and builds, four at once. The host pays touched
          # pages only, so the cap is generous and the cost follows use.
          memory = "16384M";
          # Both arms keep per-test stores under one work directory.
          # Sparse, so the host pays used blocks; the poweroff removes it.
          diskSize = 61440;
          hostStore.enable = true;
        };

        # The suite runs as this user. As root, `-w` holds on a read-only
        # output, and `simple.sh:15` and ten more tests fail in both arms.
        # Issue #1159.
        users.users.tester = {
          isNormalUser = true;
          uid = 1000;
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
          trusted-users = [
            "root"
            "tester"
          ];
        };

        # `nix.nixPath` names the nixpkgs the suite evaluates against,
        # the way the pytest guests do for `tests/nix/drv-probes.nix`.
        nix.nixPath = [ "nixpkgs=${pkgs.path}" ];
      };
    };
  }
)
