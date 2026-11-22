/**
  Buildbarn, a Remote Execution API server, one role or all of them per
  machine. Options: options.nix; configurations: render.nix.

  Roles: `storage`, `scheduler`, `frontend` and `worker` (bb_worker and
  bb_runner; they share the build directory, so they share a machine).
  `enable` turns on all four. On one machine the roles talk over Unix
  sockets under `runtimeDirectory`. A role on another machine is an
  `endpoint` (address, optional TLS); a role that serves other machines
  adds TCP addresses through its `listen` option.

  With `nixSandbox`, every Nix build on the machine sees the frontend at
  `/run/reapi.sock`, and the machine advertises the `reapi` system feature.
  A derivation that requires `reapi` runs where that socket exists, and its
  REAPI client (Siso: `SISO_REAPI_ADDRESS=unix:///run/reapi.sock`) runs its
  actions on the workers.

  The frontend lets a client read the action cache, never write it. The
  worker writes the cache, straight to storage. So a build cannot plant a
  result only while no untrusted client reaches storage itself: its Unix
  socket is mode 0700, and its TCP listener needs
  `storage.listen.tls.clientCertificateAuthorities` or a network that only
  the other roles reach.
*/
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.buildbarn;
  packages = pkgs.callPackages ./. { };
  rendered = import ./render.nix { inherit lib; } cfg;
  run = cfg.runtimeDirectory;
  state = cfg.stateDirectory;
  anyRole = lib.any (role: cfg.${role}.enable) [
    "storage"
    "scheduler"
    "frontend"
    "worker"
  ];
  storeDirs =
    lib.optionals cfg.storage.enable [
      "storage/cs"
      "storage/cms"
      "storage/ac"
      "storage/fsac"
    ]
    ++ lib.optional cfg.worker.enable "worker/cs";

  # Ordering only toward roles on this machine: a unit for a remote role does not exist.
  localUnits =
    names: map (name: "buildbarn-${name}.service") (lib.filter (name: cfg.${name}.enable) names);

  service = name: program: after: {
    description = "Buildbarn ${name}";
    wantedBy = [ "multi-user.target" ];
    inherit after;
    requires = after;
    serviceConfig = {
      ExecStart = "${program} ${pkgs.writeText "bb-${name}.jsonnet" rendered.configs.${name}}";
      User = "buildbarn";
      Group = "buildbarn";
      Restart = "on-failure";
      # A remote role may come up later; the default 100 ms hits the start limit first.
      RestartSec = "2s";
      # The frontend's socket must be one a build user can connect to.
      UMask = if name == "frontend" then "0000" else "0077";
    };
  };
in
{
  imports = [ ./options.nix ];

  config = lib.mkIf anyRole {
    services.buildbarn.worker.id.hostname = lib.mkDefault config.networking.hostName;

    assertions = rendered.assertions ++ [
      {
        assertion = !cfg.nixSandbox || cfg.frontend.enable;
        message = "services.buildbarn.nixSandbox binds the frontend's socket: enable services.buildbarn.frontend.";
      }
    ];

    users.users.buildbarn = {
      isSystemUser = true;
      group = "buildbarn";
    };
    users.groups.buildbarn = { };

    systemd.tmpfiles.rules = [
      "d ${run} 0755 buildbarn buildbarn -"
      "d ${state} 0750 buildbarn buildbarn -"
    ]
    ++ lib.optional cfg.storage.enable "d ${state}/storage 0750 buildbarn buildbarn -"
    ++ lib.optionals cfg.worker.enable [
      "d ${state}/worker 0750 buildbarn buildbarn -"
      "d ${state}/worker/build 0750 buildbarn buildbarn -"
      "d ${state}/worker/cache 0750 buildbarn buildbarn -"
    ]
    ++ lib.concatMap (dir: [
      "d ${state}/${dir} 0750 buildbarn buildbarn -"
      "d ${state}/${dir}/persistent_state 0750 buildbarn buildbarn -"
    ]) storeDirs;

    systemd.services = lib.mkMerge [
      (lib.mkIf cfg.storage.enable {
        buildbarn-storage = service "storage" (lib.getExe' packages.bb-storage "bb_storage") [ ];
      })
      (lib.mkIf cfg.scheduler.enable {
        buildbarn-scheduler =
          service "scheduler" (lib.getExe' packages.bb-remote-execution "bb_scheduler")
            (localUnits [ "storage" ]);
      })
      (lib.mkIf cfg.frontend.enable {
        buildbarn-frontend = service "frontend" (lib.getExe' packages.bb-storage "bb_storage") (localUnits [
          "storage"
          "scheduler"
        ]);
      })
      (lib.mkIf cfg.worker.enable {
        buildbarn-runner = service "runner" (lib.getExe' packages.bb-remote-execution "bb_runner") [ ];
        buildbarn-worker = service "worker" (lib.getExe' packages.bb-remote-execution "bb_worker") (
          localUnits [
            "storage"
            "scheduler"
          ]
          ++ [ "buildbarn-runner.service" ]
        );
      })
    ];

    nix.settings = lib.mkIf cfg.nixSandbox {
      extra-sandbox-paths = [ "/run/reapi.sock=${run}/frontend.sock" ];
      # `extra-`: `system-features` alone would replace NixOS's default list.
      extra-system-features = [ "reapi" ];
    };
  };
}
