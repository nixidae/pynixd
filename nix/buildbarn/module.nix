/**
  Buildbarn, a Remote Execution API server, one role or all of them per
  machine. The layout follows bb-deployments' `bare` deployment.

  Roles: `storage`, `scheduler`, `frontend` and `worker` (bb_worker and
  bb_runner; they share the build directory, so they share a machine).
  `enable` turns on all four. On one machine the roles talk over Unix
  sockets under /run/buildbarn. A role on another machine is an `endpoint`
  (address, optional TLS); a role that serves other machines adds TCP
  addresses through its `listen` option.

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
  inherit (lib) mkOption types;
  cfg = config.services.buildbarn;
  packages = pkgs.callPackages ./. { };
  run = "/run/buildbarn";
  state = "/var/lib/buildbarn";
  sockets = {
    storage = "${run}/storage.sock";
    frontend = "${run}/frontend.sock";
    schedulerClient = "${run}/scheduler-client.sock";
    schedulerWorker = "${run}/scheduler-worker.sock";
    runner = "${run}/runner.sock";
  };
  roles = [
    "storage"
    "scheduler"
    "frontend"
    "worker"
  ];
  anyRole = lib.any (role: cfg.${role}.enable) roles;

  keyPairType = types.submodule {
    options = {
      certificateFile = mkOption {
        type = types.str;
        description = "PEM certificate chain. Buildbarn reads it again every hour.";
      };
      keyFile = mkOption {
        type = types.str;
        description = ''
          PEM private key, readable by the `buildbarn` user. Never a store
          path: the store is world-readable.
        '';
      };
    };
  };

  endpointType = types.submodule {
    options = {
      address = mkOption {
        type = types.str;
        example = "buildbarn-storage.example:8981";
        description = "gRPC address: `host:port`, or `unix:/path` for a socket.";
      };
      tls = mkOption {
        default = null;
        description = "TLS toward the endpoint. Null is plaintext.";
        type = types.nullOr (
          types.submodule {
            options = {
              certificateAuthorities = mkOption {
                type = types.nullOr types.str;
                default = null;
                description = ''
                  PEM data of the CAs that sign the server's certificate. Null
                  (and no `certificateAuthoritiesFile`) trusts the system roots.
                '';
              };
              certificateAuthoritiesFile = mkOption {
                type = types.nullOr types.str;
                default = null;
                description = "As `certificateAuthorities`, read from this file when the service starts.";
              };
              serverName = mkOption {
                type = types.nullOr types.str;
                default = null;
                description = "Name to verify on the server's certificate, when it is not the address's host.";
              };
              keyPair = mkOption {
                type = types.nullOr keyPairType;
                default = null;
                description = "Client certificate, for a server that requires one.";
              };
            };
          }
        );
      };
    };
  };

  listenOption =
    what:
    mkOption {
      default = { };
      description = "TCP listener of ${what}, beside its Unix socket.";
      type = types.submodule {
        options = {
          addresses = mkOption {
            type = types.listOf types.str;
            default = [ ];
            example = [ ":8981" ];
            description = "TCP addresses to listen on. Empty: the Unix socket alone.";
          };
          tls = mkOption {
            default = null;
            description = "TLS on the TCP addresses. Null is plaintext.";
            type = types.nullOr (
              types.submodule {
                options = {
                  keyPair = mkOption {
                    type = keyPairType;
                    description = "Server certificate.";
                  };
                  clientCertificateAuthorities = mkOption {
                    type = types.nullOr types.str;
                    default = null;
                    description = ''
                      PEM data of the CAs that sign client certificates. Set, a
                      client without such a certificate is refused. Null (and no
                      `clientCertificateAuthoritiesFile`) admits every client.
                    '';
                  };
                  clientCertificateAuthoritiesFile = mkOption {
                    type = types.nullOr types.str;
                    default = null;
                    description = "As `clientCertificateAuthorities`, read from this file when the service starts.";
                  };
                };
              }
            );
          };
        };
      };
    };

  # Null when the role runs elsewhere: then the machine must name it.
  endpointOption =
    what: role: socket:
    mkOption {
      type = types.nullOr endpointType;
      default = if cfg.${role}.enable then { address = "unix:${socket}"; } else null;
      defaultText = lib.literalExpression ''if config.services.buildbarn.${role}.enable then { address = "unix:${socket}"; } else null'';
      description = "Where the other roles on this machine reach ${what}.";
    };

  allow = {
    allow = { };
  };
  deny = {
    deny = { };
  };

  # bb_* read their configuration as Jsonnet, so a file renders as `importstr`:
  # `fileMarker` survives builtins.toJSON, and `importFiles` swaps it in.
  # builtins.split drops string context: without addContextFrom, a store path
  # named only in a file option leaves the closure.
  fileMarker = path: "@importstr@${path}@";
  importFiles =
    text:
    lib.addContextFrom text (
      lib.concatMapStrings (
        part: if builtins.isList part then "importstr ${builtins.toJSON (lib.head part)}" else part
      ) (builtins.split "\"@importstr@([^@\"]*)@\"" text)
    );
  pem =
    inline: file:
    if inline != null && file != null then
      throw "services.buildbarn: set a PEM inline or as a file, not both"
    else if file != null then
      fileMarker file
    else
      inline;

  renderKeyPair = keyPair: {
    files = {
      certificatePath = keyPair.certificateFile;
      privateKeyPath = keyPair.keyFile;
      # bb-storage refuses a key pair from files without it (pkg/util/tls.go).
      refreshInterval = "3600s";
    };
  };

  # An empty `tls` turns TLS on with the system roots, so null drops the key.
  renderClient =
    endpoint:
    {
      inherit (endpoint) address;
    }
    // lib.optionalAttrs (endpoint.tls != null) {
      tls = lib.filterAttrs (_: value: value != null) {
        serverCertificateAuthorities = pem endpoint.tls.certificateAuthorities endpoint.tls.certificateAuthoritiesFile;
        inherit (endpoint.tls) serverName;
        clientKeyPair = lib.mapNullable renderKeyPair endpoint.tls.keyPair;
      };
    };

  renderServers =
    socket: listen:
    let
      clientCAs =
        if listen.tls == null then
          null
        else
          pem listen.tls.clientCertificateAuthorities listen.tls.clientCertificateAuthoritiesFile;
    in
    [
      {
        listenPaths = [ socket ];
        authenticationPolicy = allow;
      }
    ]
    ++ lib.optional (listen.addresses != [ ]) (
      {
        listenAddresses = listen.addresses;
        authenticationPolicy =
          if clientCAs != null then
            {
              tlsClientCertificate = {
                clientCertificateAuthorities = clientCAs;
                # bb-storage requires both expressions (pkg/x509/configuration.go).
                validationJmespathExpression.expression = "`true`";
                metadataExtractionJmespathExpression.expression = "`{}`";
              };
            }
          else
            allow;
      }
      // lib.optionalAttrs (listen.tls != null) {
        tls.serverKeyPair = renderKeyPair listen.tls.keyPair;
      }
    );

  storageClient = {
    grpc = {
      client = renderClient cfg.storage.endpoint;
      enableCompression = true;
    };
  };
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
  storeDirs =
    lib.optionals cfg.storage.enable [
      "storage/cs"
      "storage/cms"
      "storage/ac"
      "storage/fsac"
    ]
    ++ lib.optional cfg.worker.enable "worker/cs";

  maximumMessageSizeBytes = 16 * 1024 * 1024;

  configs = {
    storage = {
      grpcServers = renderServers sockets.storage cfg.storage.listen;
      inherit maximumMessageSizeBytes;
      global = { };
      supportedCompressors = [ "ZSTD" ];
      contentAddressableStorageServer = {
        contentAddressableStorage = {
          chunkStorage = lib.recursiveUpdate (localStore "${state}/storage/cs" cfg.storage.casSizeBytes 3) {
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
      grpcServers = renderServers sockets.frontend cfg.frontend.listen;
      schedulers."".endpoint = renderClient cfg.scheduler.clientEndpoint;
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
      clientGrpcServers = renderServers sockets.schedulerClient cfg.scheduler.clientListen;
      workerGrpcServers = renderServers sockets.schedulerWorker cfg.scheduler.workerListen;
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
            fast = localStore "${state}/worker/cs" cfg.worker.cacheSizeBytes 3;
            replicator.deduplicating.local = { };
          };
          inherit (topology) chunkMappingStorage cdcParameterCache;
        };
      };
      inherit maximumMessageSizeBytes;
      scheduler = renderClient cfg.scheduler.workerEndpoint;
      global = { };
      buildDirectories = [
        {
          native = {
            buildDirectoryPath = "${state}/worker/build";
            cacheDirectoryPath = "${state}/worker/cache";
            maximumCacheFileCount = 100000;
            maximumCacheSizeBytes = cfg.worker.cacheSizeBytes;
            cacheReplacementPolicy = "LEAST_RECENTLY_USED";
          };
          runners = [
            {
              endpoint.address = "unix:${sockets.runner}";
              inherit (cfg.worker) concurrency;
              maximumFilePoolFileCount = 100000;
              maximumFilePoolSizeBytes = 4 * 1024 * 1024 * 1024;
              platform.properties = lib.mapAttrsToList (name: value: { inherit name value; }) cfg.worker.platform;
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
          listenPaths = [ sockets.runner ];
          authenticationPolicy = allow;
        }
      ];
    };
  };

  configFile =
    name:
    pkgs.writeText "bb-${name}.jsonnet" (
      importFiles (builtins.toJSON (lib.recursiveUpdate configs.${name} cfg.settings.${name}))
    );

  # Ordering only toward roles on this machine: a unit for a remote role does not exist.
  localUnits =
    names: map (name: "buildbarn-${name}.service") (lib.filter (name: cfg.${name}.enable) names);

  service = name: program: after: {
    description = "Buildbarn ${name}";
    wantedBy = [ "multi-user.target" ];
    inherit after;
    requires = after;
    serviceConfig = {
      ExecStart = "${program} ${configFile name}";
      User = "buildbarn";
      Group = "buildbarn";
      Restart = "on-failure";
      # A remote role may come up later; the default 100 ms hits the start limit first.
      RestartSec = "2s";
      # The frontend's socket must be one a build user can connect to.
      UMask = if name == "frontend" then "0000" else "0077";
    };
  };

  roleEnable =
    what:
    mkOption {
      type = types.bool;
      default = cfg.enable;
      defaultText = lib.literalExpression "config.services.buildbarn.enable";
      description = "Whether to run ${what} on this machine.";
    };
in
{
  options.services.buildbarn = {
    enable = lib.mkEnableOption "every Buildbarn role on this machine";

    storage = {
      enable = roleEnable "the storage (bb_storage: CAS and action cache)";
      listen = listenOption "the storage";
      endpoint = endpointOption "the storage" "storage" sockets.storage;
      casSizeBytes = mkOption {
        type = types.ints.positive;
        default = 16 * 1024 * 1024 * 1024;
        description = "Size of the content-addressable storage, a sparse file.";
      };
    };

    scheduler = {
      enable = roleEnable "the scheduler (bb_scheduler)";
      clientListen = listenOption "the scheduler's client service, which frontends call";
      workerListen = listenOption "the scheduler's worker service, which workers call";
      clientEndpoint =
        endpointOption "the scheduler's client service" "scheduler"
          sockets.schedulerClient;
      workerEndpoint =
        endpointOption "the scheduler's worker service" "scheduler"
          sockets.schedulerWorker;
    };

    frontend = {
      enable = roleEnable "the frontend (bb_storage), which REAPI clients call";
      listen = listenOption "the frontend";
    };

    worker = {
      enable = roleEnable "a worker (bb_worker and bb_runner)";
      concurrency = mkOption {
        type = types.ints.positive;
        default = 8;
        description = "Actions the worker runs at once.";
      };
      platform = mkOption {
        type = types.attrsOf types.str;
        default.OSFamily = "linux";
        description = ''
          The worker's platform properties. An action runs here only when its
          platform equals this one. Siso runs a step locally when the step's
          platform is empty, so a client sends at least one property.
        '';
      };
      cacheSizeBytes = mkOption {
        type = types.ints.positive;
        default = 8 * 1024 * 1024 * 1024;
        description = "Size of the worker's local copy of the storage.";
      };
    };

    settings = lib.genAttrs (lib.attrNames configs) (
      name:
      mkOption {
        type = types.attrsOf types.anything;
        default = { };
        description = "Merged over the generated configuration of bb_${name} (lib.recursiveUpdate).";
      }
    );

    nixSandbox = mkOption {
      type = types.bool;
      default = cfg.frontend.enable;
      defaultText = lib.literalExpression "config.services.buildbarn.frontend.enable";
      description = ''
        Bind the frontend's socket into every Nix build sandbox at
        `/run/reapi.sock`, and advertise the `reapi` system feature.
      '';
    };
  };

  config = lib.mkIf anyRole {
    assertions =
      let
        needs = role: option: value: {
          assertion = !cfg.${role}.enable || value != null;
          message = "services.buildbarn.${role} runs here, and the ${option} runs elsewhere: set services.buildbarn.${option}.";
        };
      in
      [
        (needs "frontend" "storage.endpoint" cfg.storage.endpoint)
        (needs "scheduler" "storage.endpoint" cfg.storage.endpoint)
        (needs "worker" "storage.endpoint" cfg.storage.endpoint)
        (needs "frontend" "scheduler.clientEndpoint" cfg.scheduler.clientEndpoint)
        (needs "worker" "scheduler.workerEndpoint" cfg.scheduler.workerEndpoint)
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
      extra-sandbox-paths = [ "/run/reapi.sock=${sockets.frontend}" ];
      # `extra-`: `system-features` alone would replace NixOS's default list.
      extra-system-features = [ "reapi" ];
    };
  };
}
