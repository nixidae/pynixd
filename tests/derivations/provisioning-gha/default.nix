# GitHub Actions jobs as builders, with vivarium as the runners: nixidae/nixidae#1473.
#
# The controller runs the real `GhaProvider` against `fake_gha.py`, which
# serves GitHub's Actions REST API for one workflow: `ci/workflows/builder.nix`,
# as evaluated. A dispatch boots the `gha-runner` template, which has no Nix,
# and runs the workflow's own steps there as GitHub's runner would.
#
# `nix run --file . internal.tests.provisioning-gha.driver -- --out <abs> --offline`.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  tarball,
  workflow,
  vivariumLib,
}:

let
  feature = "gha-job";
  selfend = "gha-selfend";
  acceptorPort = 2235;
  ghaPort = 8774;
  python = "${package.venv}/lib/venv/bin/python";
  # Base64 of 32 bytes: the repository secret and pynixd's key file hold the same text.
  ticketKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
  token = "ghp_vivarium";
  workflowFile = pkgs.writeText "pynixd-builder.json" (builtins.toJSON workflow);
  secretsFile = pkgs.writeText "secrets.json" (builtins.toJSON { PYNIXD_TICKET_KEY = ticketKey; });
  tokenFile = pkgs.writeText "token" token;
  keyFile = pkgs.writeText "ticket-key" ticketKey;

  template = idle: {
    runs_on = [ "gha-runner" ];
    installer_url = "file://${tarball}/${tarball.fileName}";
    idle_seconds = idle;
    bootstrap_minutes = 10;
  };
  pool = name: features: idle: idleMax: {
    inherit name features;
    template = name;
    systems = [ pkgs.stdenv.hostPlatform.system ];
    mandatory_features = features;
    max_instances = 1;
    idle_seconds = idle;
    idle_max_seconds = idleMax;
    launch_timeout = 300.0;
  };
in
vivariumLib.mkTest {
  name = "pynixd-provisioning-gha";
  backend = "uml";

  pythonPath = [
    ../../daemon/helpers
    ../../provisioning
  ];

  defaults = vivariumLib.settingsClosure;
  settings = {
    stdenv = "${pkgs.stdenvNoCC}";
    bash = "${pkgs.bash}";
    system = pkgs.stdenv.hostPlatform.system;
    busybox = "${pkgs.busybox}";
    gha_port = toString ghaPort;
    inherit feature selfend;
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
          provider = "gha";
          gha = {
            repository = "nixidae/nixidae";
            token_file = "${tokenFile}";
            ticket_key_file = "${keyFile}";
            api_url = "http://127.0.0.1:${toString ghaPort}";
            # Until the lease comes, a job's guard waits an hour: only the lease or a cancel ends it.
            templates.big = template 3600;
            templates.selfend = template 3600;
          };
          acceptor_host = "10.9.0.1";
          acceptor_port = acceptorPort;
          heartbeat = 5.0;
          pools = [
            (pool "big" [ feature ] 15.0 60.0)
            (pool "selfend" [ selfend ] 15.0 20.0)
          ];
        };
      };
    };
    systemd.services.fake-gha = {
      wantedBy = [ "multi-user.target" ];
      before = [ "pynixd.service" ];
      serviceConfig.ExecStart = lib.concatStringsSep " " [
        python
        "${../../provisioning/fake_gha.py}"
        (toString ghaPort)
        "10.9.0.1"
        "${workflowFile}"
        "${secretsFile}"
        "${tokenFile}"
        "${../../provisioning/gha_runner.py}"
      ];
    };
    nix.settings = {
      experimental-features = [ "nix-command" ];
      substituters = lib.mkForce [ ];
    };
    # pynixd adds cache.nixos.org whatever nix.conf says; a refused connection answers at once.
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    networking.firewall.allowedTCPPorts = [
      acceptorPort
      ghaPort
    ];
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

  # A hosted runner's image, as far as the job uses it: no Nix, a `runner` user with sudo, bash, curl, zstd, tar.
  templates.gha-runner = {
    vivarium.memory = "2048M";
    # The unpacked closure is 650 MB, on the root disk: at the default size, tar failed with ENOSPC.
    vivarium.diskSize = 4096;
    nix.enable = false;
    vivarium.nixDatabase.enable = false;
    vivarium.interfaces.eth1.segment = "fleet";
    system.extraDependencies = [ tarball ];
    networking.hosts."127.0.0.1" = [ "cache.nixos.org" ];
    users.users.runner = {
      isNormalUser = true;
      uid = 1001;
    };
    security.sudo.extraRules = [
      {
        users = [ "runner" ];
        commands = [
          {
            command = "ALL";
            options = [ "NOPASSWD" ];
          }
        ];
      }
    ];
    environment.systemPackages = [
      pkgs.curl
      pkgs.zstd
      pkgs.gnutar
    ];
    # The runner service: root runs the user data, `gha_runner.py`, after the network.
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
        pkgs.bash
        pkgs.curl
        pkgs.zstd
        pkgs.gnutar
        pkgs.systemd
        "/run/wrappers"
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

  phases.gha = {
    script = ../../provisioning/gha.py;
    after = [ "boot" ];
  };
  phases.guard-ends = {
    script = ../../provisioning/gha_guard.py;
    after = [ "gha" ];
  };
  # Last: it changes the fake's secret.
  phases.wrong-key = {
    script = ../../provisioning/gha_wrong_key.py;
    after = [ "guard-ends" ];
  };
}
