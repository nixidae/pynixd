/**
  The options of `services.buildbarn`, without effects. The NixOS module
  (module.nix) imports them; `render` (lib.nix) evaluates them alone, so a
  configuration made elsewhere follows the same defaults and rules.
*/
{ config, lib, ... }:
let
  inherit (lib) mkOption types;
  cfg = config.services.buildbarn;
  sockets = import ./sockets.nix cfg.runtimeDirectory;

  keyPairType = types.submodule {
    options = {
      certificateFile = mkOption {
        type = types.str;
        description = "PEM certificate chain. Buildbarn reads it again every hour.";
      };
      keyFile = mkOption {
        type = types.str;
        description = ''
          PEM private key, readable by the user Buildbarn runs as. Never a
          store path: the store is world-readable.
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
                  clientCertificateValidation = mkOption {
                    type = types.str;
                    default = "`true`";
                    example = "contains(uris, 'spiffe://example.org/buildbarn/ci')";
                    description = ''
                      JMESPath expression over the client certificate, which a
                      client must make true. bb-storage gives it the
                      certificate's `dnsNames`, `emailAddresses` and `uris`,
                      never the subject. The default admits every certificate
                      the CAs signed.
                    '';
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
      defaultText = lib.literalExpression ''if config.services.buildbarn.${role}.enable then { address = "unix:<runtimeDirectory>/…"; } else null'';
      description = "Where the other roles on this machine reach ${what}.";
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

    stateDirectory = mkOption {
      type = types.str;
      default = "/var/lib/buildbarn";
      description = ''
        Persistent state: the storage's block files (sparse files on a
        filesystem, no block device) and the worker's build and cache
        directories, which must share one filesystem.
      '';
    };
    runtimeDirectory = mkOption {
      type = types.str;
      default = "/run/buildbarn";
      description = "The Unix sockets between roles on one machine.";
    };

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
      id = mkOption {
        type = types.attrsOf types.str;
        default = { };
        example.hostname = "builder-1";
        description = "Labels that name this worker to the scheduler. The NixOS module sets `hostname`.";
      };
      closures = {
        enable = lib.mkEnableOption ''
          `pynixd reapi-runner` between bb_worker and bb_runner. Before an
          action runs, it makes the closure of the action's program valid
          through the local Nix daemon, and roots it, when the program is in
          the store. A client with a relative compiler path puts a store path
          in front of it (Siso: `remote_wrapper`). The daemon needs a
          substituter that holds the closure
        '';
        package = mkOption {
          type = types.nullOr types.package;
          default = null;
          description = "The pynixd package that provides `pynixd reapi-runner`.";
        };
        daemonSocket = mkOption {
          type = types.str;
          default = "/nix/var/nix/daemon-socket/socket";
          description = "The Nix daemon that makes the closures valid.";
        };
      };
    };

    settings =
      lib.genAttrs
        [
          "storage"
          "frontend"
          "scheduler"
          "worker"
          "runner"
        ]
        (
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
        NixOS only: bind the frontend's socket into every Nix build sandbox
        at `/run/reapi.sock`, and advertise the `reapi` system feature.
      '';
    };
  };
}
