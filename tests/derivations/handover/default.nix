# What an upgrade of pynixd does to work in flight, measured.
#
# docs/design/upgrade-handover.md cites the `[measure]` lines of this
# session. `nix run --file . tests.handover.driver -- --out <abs>`.
#
# The guest runs pynixd in `replace` mode beside small toy units
# (`../../handover/toy.py`) that each prove one thing systemd or nix-daemon
# does. Configuration `b` changes every unit's store path, as an upgrade
# does; `a-nostop` goes back with `stopIfChanged = false` on pynixd.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivarium,
}:

let
  vivariumLib = import (vivarium + "/lib.nix") { inherit pkgs lib; };

  # The same program under another store path: what an upgrade changes.
  pynixdB = pkgs.symlinkJoin {
    name = "pynixd-b";
    paths = [ package ];
    meta.mainProgram = "pynixd";
  };

  toy =
    tag:
    pkgs.writeShellScriptBin "toy" ''
      export TOY_TAG=${tag} TOY_SELF="$0"
      exec ${pkgs.python3}/bin/python3 ${../../handover/toy.py} "$@"
    '';

  # A configuration under `vivarium.configurations` extends this one, so
  # what differs between them is an option and not a second definition.
  handover =
    { config, ... }:
    let
      cfg = config.handover;
      exe = "${toy cfg.tag}/bin/toy";
      fdstore = extra: {
        serviceConfig = {
          Type = "notify";
          ExecStart = "${exe} fdstore --name %N";
          FileDescriptorStoreMax = 4;
        }
        // extra;
      };
      overlap = spawnBy: {
        reloadIfChanged = true;
        serviceConfig = {
          Type = "notify";
          NotifyAccess = "all";
          ExecStart = "${exe} overlap-daemon --sock /run/toy/%N.sock";
          ExecReload = "${exe} overlap-reload --mainpid $MAINPID --spawn-by ${spawnBy}";
          # As pynixd.service has; `overlap-crash` asks whether it still
          # fires for a main that is not systemd's child.
          Restart = "on-failure";
          RestartSec = "1s";
        };
      };
    in
    {
      imports = [ ../../../nix/nixos/default.nix ];

      options.handover = {
        tag = lib.mkOption {
          type = lib.types.str;
          default = "A";
        };
        pynixd = lib.mkOption {
          type = lib.types.package;
          default = package;
        };
        stopIfChanged = lib.mkOption {
          type = lib.types.bool;
          default = true;
        };
      };

      config = {
        services.pynixd = {
          enable = true;
          mode = "replace";
          package = cfg.pynixd;
          # On this one-CPU guest the local store scored under the default
          # 0.0, the ranking dropped it, and a build waited past 90 s. This
          # session measures what a restart does to a running build, so the
          # build must run.
          settings.ranking.min_schedule_score = -1.0e9;
        };
        systemd.services = {
          # The module sets `false`; `b` keeps NixOS's default, `true`, to measure it.
          pynixd.stopIfChanged = lib.mkForce cfg.stopIfChanged;
          toy-fdstore = fdstore { };
          toy-fdstore-nostop = fdstore { } // {
            stopIfChanged = false;
          };
          toy-fdstore-preserve = fdstore { FileDescriptorStorePreserve = "yes"; };
          toy-overlap = overlap "control";
          toy-overlap-old = overlap "old";
        };
        environment.systemPackages = [ (toy cfg.tag) ];
      };
    };

  phases = [
    "restart"
    "hangup"
    "fdstore"
    "switch-stop"
    "switch-nostop"
    "overlap-restart"
    "overlap-crash"
  ];
in
vivariumLib.mkTest {
  name = "pynixd-handover";
  backend = "uml";

  pythonPath = [
    ../../daemon/helpers
    ../../handover
  ];

  settings = {
    busybox = "${pkgs.busybox}";
    system = pkgs.stdenv.hostPlatform.system;
  };

  phases = lib.listToAttrs (
    lib.imap0 (
      index: name:
      lib.nameValuePair name {
        script = ../../handover/phase.py;
        after = [ (if index == 0 then "boot" else lib.elemAt phases (index - 1)) ];
      }
    ) phases
  );

  nodes.machine = {
    imports = [ handover ];
    vivarium.memory = "1024M";
    # pynixd ranks out a local store with less than 2 GiB free
    # (`min_free_bytes`), and the build then waits with no pass to wake it.
    vivarium.diskSize = 3072;
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
    # pynixd adds cache.nixos.org whatever nix.conf says (`instance.py`,
    # `_default_http_substituter_urls`). With no network its query held each
    # build in "scheduling" past 30 s; a refused connection answers at once.
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    vivarium.configurations = {
      b = {
        handover.tag = "B";
        handover.pynixd = pynixdB;
      };
      a-nostop.handover.stopIfChanged = false;
    };
  };
}
