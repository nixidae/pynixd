{
  config,
  pkgs,
  lib,
  ...
}:

let
  cfg = config.services.pynixd;

  # The option block and the settings defaults are shared with the darwin
  # module. `../common.nix` says why, and holds the reason `package` has no
  # default.
  common = import ../common.nix { inherit lib pkgs; };

  configFile = common.configFileFor cfg.settings;

  replace = cfg.mode == "replace";
  nixPackage = config.nix.package.out;
  daemon = config.systemd.services.nix-daemon;

  # Nix's own unit under the name the upstream socket activates; see
  # `replace` below. The settings NixOS adds come over as overrides.
  upstreamUnit = pkgs.runCommand "nix-daemon-upstream-unit" { } ''
    mkdir -p $out/lib/systemd/system
    cp ${nixPackage}/lib/systemd/system/nix-daemon.service \
      $out/lib/systemd/system/nix-daemon-upstream.service
  '';
in
{
  options.services.pynixd = common.options;

  # One `mkIf`, and not a `mkMerge` with a branch outside it.
  # `environment.systemPackages` sat in a second element of that merge, so
  # importing this module installed pynixd on every system that read it,
  # whether or not the service was enabled. A module that does something when
  # it is disabled is a module that cannot be imported and left alone.
  config = lib.mkIf cfg.enable {
    services.pynixd.settings = common.settingsDefaultsFor cfg;

    /*
      `replace`: nix-daemon listens behind pynixd, under other unit names,
      and Nix's own two units are masked.

      Not a changed `ListenStream` on nix-daemon.socket.
      switch-to-configuration never restarts a changed socket
      (switch-to-configuration-ng `main.rs`: "FIXME: do something?"), so the
      socket stayed on the default path and pynixd could not start (#60).
      It does stop a unit that is gone or masked, and starts a new one, in
      either direction.

      Both units, not the socket alone: a socket for a service that is
      already running refuses to start ("Socket service nix-daemon.service
      already active, refusing"), and stopping a socket leaves its service
      up on the old descriptor. Both measured on a `tests.switch` guest.
    */
    systemd.sockets.nix-daemon.enable = lib.mkIf replace false;
    systemd.services.nix-daemon.enable = lib.mkIf replace false;
    systemd.packages = lib.mkIf replace [ upstreamUnit ];
    systemd.sockets.nix-daemon-upstream = lib.mkIf replace {
      wantedBy = [ "sockets.target" ];
      before = [ "multi-user.target" ];
      unitConfig = {
        RequiresMountsFor = "/nix/store";
        ConditionPathIsReadWrite = "/nix/var/nix/daemon-socket";
      };
      listenStreams = [ common.upstreamSocket ];
      # Nix 2.35 takes only a descriptor named `nix-daemon.socket` in
      # LISTEN_FDNAMES (`serveUnixSocket`, `activationName`); under this
      # unit's own name it listened on nothing and every client hung.
      socketConfig.FileDescriptorName = "nix-daemon.socket";
    };
    systemd.services.nix-daemon-upstream = lib.mkIf replace {
      inherit (daemon)
        path
        serviceConfig
        restartTriggers
        stopIfChanged
        ;
      # PATH is made from `path` again here.
      environment = removeAttrs daemon.environment [ "PATH" ];
    };
    /*
      The default socket is bound in `sockets.target`, before any ordinary
      service, and pynixd takes it over (`inherited_listener`). A client
      that comes before pynixd is ready waits in the backlog. Without it, a
      unit ordered only `After=nix-daemon.socket`, such as home-manager's,
      finds no daemon. Nix then opens the store directly, which a user
      cannot: "opening lock file .../big-lock: Permission denied" (#59).
    */
    systemd.sockets.pynixd = lib.mkIf replace {
      wantedBy = [ "sockets.target" ];
      before = [ "multi-user.target" ];
      listenStreams = [ common.daemonSocket ];
      socketConfig.SocketMode = "0666";
    };

    # Without build users, the roots daemon starts with the daemon.
    systemd.sockets.nix-roots-daemon.wantedBy = lib.mkIf (replace && config.nix.daemonUser != "root") [
      "nix-daemon-upstream.service"
    ];

    environment.etc."pynixd/pynixd.json".source = configFile;

    systemd.services.pynixd = {
      description = "pynixd - Python Nix daemon protocol proxy";
      wantedBy = [ "multi-user.target" ];
      # No `wants` on the daemon service: its socket starts it, and a
      # service started on its own takes the socket's path itself (#60).
      # `pynixd.socket` as a requirement, not only through sockets.target:
      # switch-to-configuration starts a new service before a new socket,
      # and the socket of a running service refuses to start.
      after =
        if replace then
          [
            "pynixd.socket"
            "nix-daemon-upstream.socket"
          ]
        else
          [
            "nix-daemon.socket"
            "nix-daemon.service"
          ];
      requires = lib.mkIf replace [
        "pynixd.socket"
        "nix-daemon-upstream.socket"
      ];
      # In `replace` mode this is the daemon, so it is up before anything
      # that uses one.
      before = lib.mkIf replace [ "multi-user.target" ];
      restartTriggers = [ configFile ];
      # One restart job, not stop then start: a stop takes `pynixd.socket`
      # down with the service, and the machine's daemon socket refused
      # clients for 4 s of a switch (8 of 14 queries, `tests.handover`).
      # nixpkgs sets the same on nix-daemon, for the same reason; pynixd has
      # no `ExecStop` either.
      stopIfChanged = false;
      # pynixd reads `trusted-users` and `allowed-users` with `nix config
      # show`, from the same Nix as nix-daemon.
      path = [ config.nix.package ];

      serviceConfig = {
        # pynixd says READY=1 once its listeners are bound. `simple` called
        # it active two seconds before the socket existed.
        Type = "notify";
        ExecStart = "${lib.getExe cfg.package} daemon";
        User = "root";
        Group = "root";
        RuntimeDirectory = "pynixd";
        RuntimeDirectoryMode = "755";
        Environment = "PYNIXD_CONFIG=${configFile}";
        Restart = "on-failure";
        RestartSec = "5s";
        NoNewPrivileges = true;
      };
    };

    networking.firewall.allowedTCPPorts = lib.mkIf (cfg.metrics.enable && cfg.metrics.openFirewall) [
      cfg.metrics.port
    ];

    environment.systemPackages = [ cfg.package ];
  };
}
