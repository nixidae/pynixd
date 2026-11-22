/**
  Buildbarn on one machine: storage, frontend, scheduler, worker and runner,
  joined over Unix sockets under /run/buildbarn. The layout follows
  bb-deployments' `bare` deployment.

  With `nixSandbox`, every Nix build on the machine sees the frontend at
  `/run/reapi.sock`, and the machine advertises the `reapi` system feature.
  A derivation that requires `reapi` runs where that socket exists, and its
  REAPI client (Siso: `SISO_REAPI_ADDRESS=unix:///run/reapi.sock`) runs its
  actions here.

  The frontend lets a client read the action cache, never write it: a build
  cannot plant a result. The worker writes the cache, straight to storage.
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
  run = "/run/buildbarn";
  state = "/var/lib/buildbarn";
  allow = {
    allow = { };
  };
  deny = {
    deny = { };
  };

  grpcClient = socket: {
    grpc = {
      client.address = "unix:${socket}";
      enableCompression = true;
    };
  };
  storageClient = grpcClient "${run}/storage.sock";
  cdcParameterCache = {
    cacheSize = 1000;
    cacheDuration = "60s";
    cacheReplacementPolicy = "LEAST_RECENTLY_USED";
  };
  topology = {
    chunkStorage = storageClient;
    chunkMappingStorage.chunkMappingValidating.backend = storageClient;
    inherit cdcParameterCache;
    actionCache.completenessChecking = {
      backend = storageClient;
      maximumTotalTreeSizeBytes = 64 * 1024 * 1024;
    };
    fileSystemAccessCache = storageClient;
  };

  # A block-device-backed local store; `dir` holds its files and persistent state.
  localStore = dir: sizeBytes: newBlocks: {
    local = {
      keyLocationMap = {
        onBlockDevice.file = {
          path = "${dir}/key_location_map";
          sizeBytes = 64 * 1024 * 1024;
        };
        maximumGetAttempts = 16;
        maximumPutAttempts = 64;
      };
      oldBlocks = 8;
      currentBlocks = 24;
      inherit newBlocks;
      blocksOnBlockDevice = {
        source.file = {
          path = "${dir}/blocks";
          inherit sizeBytes;
        };
        spareBlocks = 3;
      };
      persistent = {
        stateDirectoryPath = "${dir}/persistent_state";
        minimumEpochInterval = "300s";
      };
    };
  };
  storeDirs = [
    "storage/cs"
    "storage/cms"
    "storage/ac"
    "storage/fsac"
    "worker/cs"
  ];

  maximumMessageSizeBytes = 16 * 1024 * 1024;

  configs = {
    storage = {
      grpcServers = [
        {
          listenPaths = [ "${run}/storage.sock" ];
          authenticationPolicy = allow;
        }
      ];
      inherit maximumMessageSizeBytes;
      global = { };
      supportedCompressors = [ "ZSTD" ];
      contentAddressableStorageServer = {
        contentAddressableStorage = {
          chunkStorage = lib.recursiveUpdate (localStore "${state}/storage/cs" cfg.casSizeBytes 3) {
            local.chunkingParameters = {
              minChunkSizeBytes = 256 * 1024;
              horizonSizeBytes = 8 * 256 * 1024;
            };
          };
          chunkMappingStorage = localStore "${state}/storage/cms" (64 * 1024 * 1024) 1;
        };
        getAuthorizer = allow;
        putAuthorizer = allow;
        findMissingAuthorizer = allow;
        maximumChunkCount = 100000;
      };
      actionCache = {
        backend = localStore "${state}/storage/ac" (64 * 1024 * 1024) 1;
        getAuthorizer = allow;
        putAuthorizer = allow;
      };
      fileSystemAccessCache = {
        backend = localStore "${state}/storage/fsac" (64 * 1024 * 1024) 1;
        getAuthorizer = allow;
        putAuthorizer = allow;
      };
    };

    frontend = {
      grpcServers = [
        {
          listenPaths = [ "${run}/frontend.sock" ];
          authenticationPolicy = allow;
        }
      ];
      schedulers."".endpoint.address = "unix:${run}/scheduler-client.sock";
      inherit maximumMessageSizeBytes;
      global = { };
      supportedCompressors = [ "ZSTD" ];
      contentAddressableStorageServer = {
        contentAddressableStorage = {
          inherit (topology) chunkStorage chunkMappingStorage cdcParameterCache;
        };
        getAuthorizer = allow;
        putAuthorizer = allow;
        findMissingAuthorizer = allow;
        maximumChunkCount = 100000;
      };
      actionCache = {
        backend = topology.actionCache;
        getAuthorizer = allow;
        putAuthorizer = deny;
      };
      fileSystemAccessCache = {
        backend = topology.fileSystemAccessCache;
        getAuthorizer = allow;
        putAuthorizer = deny;
      };
      executeAuthorizer = allow;
    };

    scheduler = {
      clientGrpcServers = [
        {
          listenPaths = [ "${run}/scheduler-client.sock" ];
          authenticationPolicy = allow;
        }
      ];
      workerGrpcServers = [
        {
          listenPaths = [ "${run}/scheduler-worker.sock" ];
          authenticationPolicy = allow;
        }
      ];
      contentAddressableStorage = {
        inherit (topology) chunkStorage chunkMappingStorage cdcParameterCache;
      };
      inherit maximumMessageSizeBytes;
      global = { };
      executeAuthorizer = allow;
      modifyDrainsAuthorizer = allow;
      killOperationsAuthorizer = allow;
      synchronizeAuthorizer = allow;
      actionRouter.simple = {
        platformKeyExtractor.action = { };
        invocationKeyExtractors = [
          { correlatedInvocationsId = { }; }
          { toolInvocationId = { }; }
        ];
        initialSizeClassAnalyzer = {
          defaultExecutionTimeout = "1800s";
          maximumExecutionTimeout = "7200s";
        };
      };
      platformQueueWithNoWorkersTimeout = "900s";
    };

    worker = {
      blobstore = {
        inherit (topology) actionCache;
        contentAddressableStorage = {
          chunkStorage.readCaching = {
            slow = topology.chunkStorage;
            fast = localStore "${state}/worker/cs" cfg.workerCacheSizeBytes 3;
            replicator.deduplicating.local = { };
          };
          inherit (topology) chunkMappingStorage cdcParameterCache;
        };
      };
      inherit maximumMessageSizeBytes;
      scheduler.address = "unix:${run}/scheduler-worker.sock";
      global = { };
      buildDirectories = [
        {
          native = {
            buildDirectoryPath = "${state}/worker/build";
            cacheDirectoryPath = "${state}/worker/cache";
            maximumCacheFileCount = 100000;
            maximumCacheSizeBytes = cfg.workerCacheSizeBytes;
            cacheReplacementPolicy = "LEAST_RECENTLY_USED";
          };
          runners = [
            {
              endpoint.address = "unix:${run}/runner.sock";
              inherit (cfg) concurrency;
              maximumFilePoolFileCount = 100000;
              maximumFilePoolSizeBytes = 4 * 1024 * 1024 * 1024;
              platform.properties = lib.mapAttrsToList (name: value: { inherit name value; }) cfg.platform;
              workerId.hostname = config.networking.hostName;
            }
          ];
        }
      ];
      filePool.blockDevice.file = {
        path = "${state}/worker/filepool";
        sizeBytes = 4 * 1024 * 1024 * 1024;
      };
      inputDownloadConcurrency = 16;
      outputUploadConcurrency = 16;
      directoryCache = {
        maximumCount = 10000;
        maximumSizeBytes = 64 * 1024 * 1024;
        cacheReplacementPolicy = "LEAST_RECENTLY_USED";
      };
    };

    runner = {
      buildDirectoryPath = "${state}/worker/build";
      global = { };
      grpcServers = [
        {
          listenPaths = [ "${run}/runner.sock" ];
          authenticationPolicy = allow;
        }
      ];
    };
  };

  configFile =
    name:
    pkgs.writeText "bb-${name}.json" (
      builtins.toJSON (lib.recursiveUpdate configs.${name} cfg.settings.${name})
    );

  service = name: program: extra: {
    description = "Buildbarn ${name}";
    wantedBy = [ "multi-user.target" ];
    after = extra.after or [ ];
    requires = extra.after or [ ];
    serviceConfig = {
      ExecStart = "${program} ${configFile name}";
      User = "buildbarn";
      Group = "buildbarn";
      Restart = "on-failure";
      # The frontend's socket must be one a build user can connect to.
      UMask = if name == "frontend" then "0000" else "0077";
    };
  };
in
{
  options.services.buildbarn = {
    enable = lib.mkEnableOption "Buildbarn, a Remote Execution API server, on this machine alone";
    concurrency = lib.mkOption {
      type = lib.types.ints.positive;
      default = 8;
      description = "Actions the worker runs at once.";
    };
    platform = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default.OSFamily = "linux";
      description = ''
        The worker's platform properties. An action runs here only when its
        platform equals this one. Siso runs a step locally when the step's
        platform is empty, so a client sends at least one property.
      '';
    };
    casSizeBytes = lib.mkOption {
      type = lib.types.ints.positive;
      default = 16 * 1024 * 1024 * 1024;
      description = "Size of the content-addressable storage, a sparse file.";
    };
    workerCacheSizeBytes = lib.mkOption {
      type = lib.types.ints.positive;
      default = 8 * 1024 * 1024 * 1024;
      description = "Size of the worker's local copy of the storage.";
    };
    settings = lib.genAttrs (lib.attrNames configs) (
      name:
      lib.mkOption {
        type = lib.types.attrsOf lib.types.anything;
        default = { };
        description = "Merged over the generated configuration of bb_${name} (lib.recursiveUpdate).";
      }
    );
    nixSandbox = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = ''
        Bind the frontend's socket into every Nix build sandbox at
        `/run/reapi.sock`, and advertise the `reapi` system feature.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    users.users.buildbarn = {
      isSystemUser = true;
      group = "buildbarn";
    };
    users.groups.buildbarn = { };

    systemd.tmpfiles.rules = [
      "d ${run} 0755 buildbarn buildbarn -"
      "d ${state} 0750 buildbarn buildbarn -"
      "d ${state}/storage 0750 buildbarn buildbarn -"
      "d ${state}/worker 0750 buildbarn buildbarn -"
      "d ${state}/worker/build 0750 buildbarn buildbarn -"
      "d ${state}/worker/cache 0750 buildbarn buildbarn -"
    ]
    ++ lib.concatMap (dir: [
      "d ${state}/${dir} 0750 buildbarn buildbarn -"
      "d ${state}/${dir}/persistent_state 0750 buildbarn buildbarn -"
    ]) storeDirs;

    systemd.services = {
      buildbarn-storage = service "storage" (lib.getExe' packages.bb-storage "bb_storage") { };
      buildbarn-frontend = service "frontend" (lib.getExe' packages.bb-storage "bb_storage") {
        after = [
          "buildbarn-storage.service"
          "buildbarn-scheduler.service"
        ];
      };
      buildbarn-scheduler =
        service "scheduler" (lib.getExe' packages.bb-remote-execution "bb_scheduler")
          {
            after = [ "buildbarn-storage.service" ];
          };
      buildbarn-runner = service "runner" (lib.getExe' packages.bb-remote-execution "bb_runner") { };
      buildbarn-worker = service "worker" (lib.getExe' packages.bb-remote-execution "bb_worker") {
        after = [
          "buildbarn-scheduler.service"
          "buildbarn-runner.service"
        ];
      };
    };

    nix.settings = lib.mkIf cfg.nixSandbox {
      extra-sandbox-paths = [ "/run/reapi.sock=${run}/frontend.sock" ];
      # `extra-`: `system-features` alone would replace NixOS's default list.
      extra-system-features = [ "reapi" ];
    };
  };
}
