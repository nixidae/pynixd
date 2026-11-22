/**
  Buildbarn's configurations from evaluated `services.buildbarn` options.
  The layout follows bb-deployments' `bare` deployment.

  # Inputs

  `cfg`
  : `config.services.buildbarn`, from module.nix or from `render` (lib.nix)

  Returns `{ assertions; configs; }`. `configs` holds the Jsonnet text of
  each component on this machine: `storage`, `scheduler`, `frontend`, and
  `worker` with `runner`.
*/
{ lib }:
cfg:
let
  state = cfg.stateDirectory;
  sockets = import ./sockets.nix cfg.runtimeDirectory;

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

  # A store in sparse files under `dir`, with its persistent state beside them.
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

  maximumMessageSizeBytes = 16 * 1024 * 1024;

  components = {
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
              workerId = cfg.worker.id;
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

  running = lib.filter (name: cfg.${if name == "runner" then "worker" else name}.enable) (
    lib.attrNames components
  );

  needs = role: option: value: {
    assertion = !cfg.${role}.enable || value != null;
    message = "services.buildbarn.${role} runs here, and the ${option} runs elsewhere: set services.buildbarn.${option}.";
  };
in
{
  assertions = [
    (needs "frontend" "storage.endpoint" cfg.storage.endpoint)
    (needs "scheduler" "storage.endpoint" cfg.storage.endpoint)
    (needs "worker" "storage.endpoint" cfg.storage.endpoint)
    (needs "frontend" "scheduler.clientEndpoint" cfg.scheduler.clientEndpoint)
    (needs "worker" "scheduler.workerEndpoint" cfg.scheduler.workerEndpoint)
  ];

  configs = lib.genAttrs running (
    name: importFiles (builtins.toJSON (lib.recursiveUpdate components.${name} cfg.settings.${name}))
  );
}
