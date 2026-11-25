# What pynixd adds to each build when a graph is many small derivations: one per
# compile step, as dynamic derivations over a ninja graph give.
#
# A QEMU guest with one ext4 disk. pynixd runs with no Nix daemon under it, and a
# stock `nix daemon` beside it is the control; each serves a chroot store of its
# own. `tests/benchmark/drv_storm.py` does the measuring. Not a gate on speed.
# `nix run --file . internal.tests.drv-storm.driver -- --out <abs>`.
{
  vivariumLib,
  src,
  devEnv,
  package,
  pkgs,
  nix,
}:

vivariumLib.mkTest {
  name = "pynixd-drv-storm";
  backend = "qemu";

  defaults = vivariumLib.settingsClosure;
  settings = {
    src = "${src}";
    nix = "${nix}";
  };

  # pynixd's spans go here, so the server's work is not in the measured guest.
  nodes.traces = {
    vivarium.memory = "1024M";
    vivarium.lan = {
      network = "bench";
      address = "10.44.1.2/24";
    };
    networking.firewall.enable = false;
    services.victoriatraces = {
      enable = true;
      # A span is searchable at once, not 30 s after it arrived.
      extraOptions = [ "-search.latencyOffset=0s" ];
    };
  };

  nodes.machine = {
    vivarium.memory = "4096M";
    vivarium.lan = {
      network = "bench";
      address = "10.44.1.1/24";
    };
    networking.hosts."10.44.1.2" = [ "traces" ];
    vivarium.disks = [
      {
        name = "bench";
        size = 8192;
      }
    ];
    nix.settings = {
      experimental-features = [
        "nix-command"
        "ca-derivations"
        "dynamic-derivations"
      ];
      substituters = pkgs.lib.mkForce [ ];
      sandbox = true;
    };
    environment.systemPackages = [
      devEnv
      package
      pkgs.e2fsprogs
    ];
  };

  phases =
    let
      script = ../../benchmark/drv_storm_phase.py;
    in
    {
      daemons = {
        inherit script;
        after = [ "boot" ];
        description = "pynixd with no daemon, and nix daemon, over a store each";
      };
      storm = {
        inherit script;
        after = [ "daemons" ];
        timeout = 3600;
        description = "storms of trivial derivations through each daemon";
      };
    };
}
