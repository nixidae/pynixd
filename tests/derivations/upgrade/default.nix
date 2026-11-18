# An upgrade of pynixd by `switch-to-configuration` keeps the work in flight.
#
# docs/design/upgrade-handover.md, "Implementation (B)", lists what this
# session checks. `nix run --file . internal.tests.upgrade.driver -- --out <abs>`.
#
# The guest boots configuration A. Each phase switches it to the next
# configuration with a user's build running and checks what happened to it.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivariumLib,
}:

let
  # The same program under another store path: what an upgrade changes.
  pynixdB = pkgs.symlinkJoin {
    name = "pynixd-b";
    paths = [ package ];
    meta.mainProgram = "pynixd";
  };

  # A successor that cannot start: `pynixd handover` works, `pynixd daemon` exits at once.
  pynixdBroken = pkgs.writeShellScriptBin "pynixd" ''
    if [ "$1" = daemon ]; then
      echo "pynixd-broken: refusing to start" >&2
      exit 1
    fi
    exec ${lib.getExe package} "$@"
  '';

  upgrade =
    { config, ... }:
    let
      cfg = config.upgrade;
    in
    {
      imports = [ ../../../nix/nixos/default.nix ];

      options.upgrade = {
        tag = lib.mkOption {
          type = lib.types.str;
          default = "A";
        };
        pynixd = lib.mkOption {
          type = lib.types.package;
          default = package;
        };
        reload = lib.mkOption {
          type = lib.types.bool;
          default = true;
        };
      };

      config = {
        services.pynixd = {
          enable = true;
          mode = "replace";
          package = cfg.pynixd;
          settings = {
            # On this one-CPU guest the local store scored under the default
            # 0.0, and the ranking dropped it.
            ranking.min_schedule_score = -1.0e9;
            # The guest's store is smaller than the 2 GiB floor.
            ranking.min_free_bytes = 0;
            # The tag changes the settings file, as a configuration change does.
            stall_traceback_path = "/var/lib/pynixd/stall-${cfg.tag}.txt";
            # The builder guest registers here; its key is nobody's to pin in a test.
            reverse_acceptor = {
              enabled = true;
              port = acceptorPort;
              authorized_builder_keys = null;
            };
          };
        };
        # The control: the same switch, restarting as before the hand-over.
        systemd.services.pynixd.reloadIfChanged = lib.mkIf (!cfg.reload) (lib.mkForce false);
      };
    };

  remoteFeature = "upgrade-remote";
  acceptorPort = 2235;

  phases = [
    "handover"
    "remote"
    "idle"
    "again"
    "refused"
    "broken"
    "control"
  ];
in
vivariumLib.mkTest {
  name = "pynixd-upgrade";
  backend = "uml";

  pythonPath = [
    ../../daemon/helpers
    ../../handover
  ];

  defaults = vivariumLib.settingsClosure;
  settings = {
    busybox = "${pkgs.busybox}";
    system = pkgs.stdenv.hostPlatform.system;
    pynixdB = "${pynixdB}";
    inherit remoteFeature;
  };

  segments.fleet = { };

  phases = lib.listToAttrs (
    lib.imap0 (
      index: name:
      lib.nameValuePair name {
        script = ../../upgrade/check.py;
        after = [ (if index == 0 then "boot" else lib.elemAt phases (index - 1)) ];
      }
    ) phases
  );

  # A reverse builder: the only store with `remoteFeature`, so a build that needs it runs here.
  nodes.builder = {
    imports = [ ../../../nix/nixos/default.nix ];
    vivarium.memory = "1024M";
    services.pynixd = {
      enable = true;
      mode = "replace";
      inherit package;
      settings = {
        ranking.min_schedule_score = -1.0e9;
        ranking.min_free_bytes = 0;
        reverse_initiator = {
          enabled = true;
          acceptor_host = "10.9.0.1";
          acceptor_port = acceptorPort;
          systems = [ pkgs.stdenv.hostPlatform.system ];
          system_features = [ remoteFeature ];
          max_jobs = 1;
          reconnect_min_delay = 1.0;
          reconnect_max_delay = 2.0;
        };
      };
    };
    nix.settings = {
      experimental-features = [ "nix-command" ];
      substituters = lib.mkForce [ ];
      system-features = [ remoteFeature ];
    };
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    vivarium.interfaces.eth1 = {
      segment = "fleet";
      addresses = [ "10.9.0.2/24" ];
    };
    environment.systemPackages = [ pkgs.procps ];
    # A reverse initiator refuses the overlap, so a change restarts it.
    vivarium.configurations.changed.services.pynixd.settings.stall_traceback_path =
      "/var/lib/pynixd/stall-changed.txt";
  };

  nodes.machine = {
    imports = [ upgrade ];
    vivarium.memory = "1024M";
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };
    environment.systemPackages = [
      pkgs.iproute2
      pkgs.procps
    ];
    nix.settings = {
      experimental-features = [ "nix-command" ];
      substituters = lib.mkForce [ ];
    };
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    vivarium.interfaces.eth1 = {
      segment = "fleet";
      addresses = [ "10.9.0.1/24" ];
    };
    networking.firewall.allowedTCPPorts = [ acceptorPort ];
    vivarium.configurations = {
      b = {
        upgrade.tag = "B";
        upgrade.pynixd = pynixdB;
      };
      c.upgrade.tag = "C";
      d.upgrade.tag = "D";
      e.upgrade.tag = "E";
      f.upgrade.tag = "F";
      broken = {
        upgrade.tag = "broken";
        upgrade.pynixd = pynixdBroken;
      };
      control = {
        upgrade.tag = "control";
        upgrade.reload = false;
      };
    };
  };
}
