# A build no store can take makes pynixd launch a builder through vivarium's
# cloud API; the builder registers by its join ticket, builds, idles and is
# ended. nixidae/nixidae#1198, nixidae/nixidae#1408.
#
# `nix run --file . internal.tests.provisioning.driver -- --out <abs>`.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivariumLib,
}:

let
  feature = "provisioned";
  acceptorPort = 2235;

  # Both guests: pynixd in front of the guest's nix-daemon, with no network
  # to substitute from.
  pynixdNode = settings: {
    imports = [ ../../../nix/nixos/default.nix ];
    vivarium.memory = "1024M";
    services.pynixd = {
      enable = true;
      mode = "replace";
      inherit package;
      settings = {
        # A one-CPU guest's local store scores under the default 0.0.
        ranking.min_schedule_score = -1.0e9;
        # The guest's store is smaller than the 2 GiB floor.
        ranking.min_free_bytes = 0;
      }
      // settings;
    };
    nix.settings = {
      experimental-features = [ "nix-command" ];
      substituters = lib.mkForce [ ];
    };
    # pynixd adds cache.nixos.org whatever nix.conf says; a refused
    # connection answers at once, a DNS timeout holds each build.
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    environment.systemPackages = [
      pkgs.busybox
      pkgs.jq
    ];
  };
in
vivariumLib.mkTest {
  name = "pynixd-provisioning";
  backend = "uml";

  pythonPath = [
    ../../daemon/helpers
    ../../provisioning
  ];

  defaults = vivariumLib.settingsClosure;
  settings = {
    busybox = "${pkgs.busybox}";
    system = pkgs.stdenv.hostPlatform.system;
    inherit feature;
  };

  segments.fleet.instanceAddresses = [ "10.9.0.100/24" ];

  nodes.controller = {
    imports = [
      (pynixdNode {
        reverse_acceptor = {
          enabled = true;
          port = acceptorPort;
          # Launched builders carry a join token; no other builder exists here.
          authorized_builder_keys = null;
        };
        provisioning = {
          acceptor_host = "10.9.0.1";
          acceptor_port = acceptorPort;
          heartbeat = 5.0;
          pools = [
            {
              name = "big";
              template = "builder";
              systems = [ pkgs.stdenv.hostPlatform.system ];
              features = [ feature ];
              max_instances = 1;
              idle_seconds = 15.0;
              launch_timeout = 300.0;
            }
          ];
        };
      })
    ];
    vivarium.cloud = true;
    vivarium.interfaces.eth1 = {
      segment = "fleet";
      addresses = [ "10.9.0.1/24" ];
    };
    networking.firewall.allowedTCPPorts = [ acceptorPort ];
    environment.systemPackages = [
      pkgs.curl
      pkgs.sqlite
    ];
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };
  };

  templates.builder = {
    imports = [
      (pynixdNode {
        reverse_initiator = {
          enabled = true;
          join_ticket = "/run/vivarium/user-data";
          systems = [ pkgs.stdenv.hostPlatform.system ];
          system_features = [ feature ];
          max_jobs = 1;
          reconnect_min_delay = 1.0;
          reconnect_max_delay = 5.0;
        };
      })
    ];
    vivarium.interfaces.eth1.segment = "fleet";
    nix.settings.system-features = [ feature ];
  };

  phases.provision = {
    script = ../../provisioning/phase.py;
    after = [ "boot" ];
  };
  phases.interrupt = {
    script = ../../provisioning/interrupt.py;
    after = [ "provision" ];
  };
}
