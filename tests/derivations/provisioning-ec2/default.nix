# The EC2 builder's boot path, with vivarium as the cloud: nixidae/nixidae#1420.
#
# The controller runs the real `Ec2Provider` against `fake_ec2.py`, which
# turns EC2's Query API into vivarium's cloud API. A launch boots the
# `ec2-builder` template: no Nix, as a stock Ubuntu image, and a unit that
# runs the user data as cloud-init does. The user data is the bootstrap
# `pynixd.provisioning.ec2.bootstrap` renders: it unpacks the installer
# tarball, installs pynixd with no daemon and registers by the join ticket.
#
# `nix run --file . internal.tests.provisioning-ec2.driver -- --out <abs> --offline`.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  tarball,
  vivariumLib,
}:

let
  feature = "ec2-spot";
  # The pool of a template whose installer is missing: its bootstrap fails.
  broken = "ec2-broken";
  acceptorPort = 2235;
  ec2Port = 8773;
  python = "${package.venv}/lib/venv/bin/python";

in
vivariumLib.mkTest {
  name = "pynixd-provisioning-ec2";
  backend = "uml";

  pythonPath = [
    ../../daemon/helpers
    ../../provisioning
  ];

  defaults = vivariumLib.settingsClosure;
  # The phase instantiates its build on the controller from these, as store paths: a `.drv` built here
  # would carry stdenv's whole derivation graph, sources included.
  settings = {
    stdenv = "${pkgs.stdenvNoCC}";
    bash = "${pkgs.bash}";
    system = pkgs.stdenv.hostPlatform.system;
    busybox = "${pkgs.busybox}";
    inherit feature broken;
  };

  segments.fleet.instanceAddresses = [ "10.9.0.100/24" ];

  nodes.controller = {
    imports = [ ../../../nix/nixos/default.nix ];
    vivarium.memory = "1024M";
    vivarium.cloud = true;
    vivarium.interfaces.eth1 = {
      segment = "fleet";
      addresses = [ "10.9.0.1/24" ];
    };
    vivarium.nixDatabase.extraRoots = [
      "${pkgs.stdenvNoCC}"
      "${pkgs.bash}"
      "${pkgs.busybox}"
    ];
    services.pynixd = {
      enable = true;
      mode = "replace";
      inherit package;
      settings = {
        # A one-CPU guest's local store scores under the default 0.0.
        ranking.min_schedule_score = -1.0e9;
        # The guest's store is smaller than the 2 GiB floor.
        ranking.min_free_bytes = 0;
        reverse_acceptor = {
          enabled = true;
          port = acceptorPort;
          authorized_builder_keys = null;
        };
        provisioning = {
          provider = "ec2";
          ec2 = {
            region = "eu-north-1";
            endpoint = "http://127.0.0.1:${toString ec2Port}/";
            templates.builder = {
              instance_types = [ "fake.small" ];
              image = "ec2-builder";
              installer_url = "file://${tarball}/${tarball.fileName}";
              # The guard and the deadline stay out of the way: the controller ends the builder.
              idle_poweroff = 3600;
              bootstrap_deadline = 600;
            };
            templates.broken = {
              instance_types = [ "fake.small" ];
              image = "ec2-builder";
              installer_url = "file:///nonexistent/pynixd-x86_64-linux.tar.zst";
              idle_poweroff = 3600;
              bootstrap_deadline = 600;
            };
          };
          acceptor_host = "10.9.0.1";
          acceptor_port = acceptorPort;
          heartbeat = 5.0;
          pools = [
            {
              name = "big";
              template = "builder";
              systems = [ pkgs.stdenv.hostPlatform.system ];
              features = [ feature ];
              mandatory_features = [ feature ];
              max_instances = 1;
              idle_seconds = 15.0;
              idle_max_seconds = 30.0;
              launch_timeout = 300.0;
            }
            {
              name = "broken";
              template = "broken";
              systems = [ pkgs.stdenv.hostPlatform.system ];
              features = [ broken ];
              mandatory_features = [ broken ];
              max_instances = 1;
              idle_seconds = 15.0;
              # Longer than the phase: only the builder's own poweroff may end it.
              launch_timeout = 600.0;
            }
          ];
        };
      };
    };
    # botocore's chain finds these first; without them it asks the instance metadata service, which no guest has.
    systemd.services.pynixd.environment = {
      AWS_ACCESS_KEY_ID = "AKIDFAKE";
      AWS_SECRET_ACCESS_KEY = "fake";
      AWS_CONFIG_FILE = "/nonexistent";
      AWS_SHARED_CREDENTIALS_FILE = "/nonexistent";
    };
    systemd.services.fake-ec2 = {
      wantedBy = [ "multi-user.target" ];
      before = [ "pynixd.service" ];
      serviceConfig.ExecStart = "${python} ${../../provisioning/fake_ec2.py} ${toString ec2Port}";
    };
    nix.settings = {
      experimental-features = [ "nix-command" ];
      substituters = lib.mkForce [ ];
    };
    # pynixd adds cache.nixos.org whatever nix.conf says; a refused connection answers at once.
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    networking.firewall.allowedTCPPorts = [ acceptorPort ];
    environment.systemPackages = [
      pkgs.curl
      pkgs.jq
      pkgs.sqlite
    ];
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };
  };

  # A stock image: no Nix, no database, no pynixd. Python, zstd and tar, as Ubuntu has.
  templates.ec2-builder = {
    vivarium.memory = "2048M";
    # The unpacked closure is 650 MB, on the root disk: at the default size, tar failed with ENOSPC.
    vivarium.diskSize = 4096;
    nix.enable = false;
    vivarium.nixDatabase.enable = false;
    vivarium.interfaces.eth1.segment = "fleet";
    system.extraDependencies = [ tarball ];
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    environment.systemPackages = [
      pkgs.python3
      pkgs.zstd
      pkgs.gnutar
    ];
    # cloud-init's contract for user data that starts with `#!`: root runs it once, after the network.
    systemd.services.cloud-user-data = {
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [
        "network-online.target"
        "vivarium-instance.service"
      ];
      unitConfig.ConditionPathExists = "/run/vivarium/user-data";
      path = [
        pkgs.python3
        pkgs.zstd
        pkgs.gnutar
        pkgs.systemd
      ];
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        StandardOutput = "journal+console";
        StandardError = "journal+console";
      };
      script = ''
        install -m 0700 /run/vivarium/user-data /run/cloud-user-data
        exec /run/cloud-user-data
      '';
    };
  };

  phases.ec2 = {
    script = ../../provisioning/ec2.py;
    after = [ "boot" ];
  };
  phases.bootstrap-fails = {
    script = ../../provisioning/ec2_broken.py;
    after = [ "ec2" ];
  };
}
