# What the NixOS module and the darwin module of pynixd both need.
#
# The two service blocks have nothing in common -- one is a systemd unit and
# the other is a launchd job -- but the option block and the settings defaults
# are the same text twice. Two copies of an option block drift, and the drift
# is silent: a renamed option keeps evaluating on the platform that was
# changed and stops on the other.
#
# The service block stays in each platform's own file. That is the part a
# person opens the module to read, and an indirection there costs more than it
# saves. Nobody reads an option block to learn what a service does.
{ lib, pkgs }:

let
  jsonFormat = pkgs.formats.json { };

  # Where every Nix client looks for the daemon, and where nix-daemon moves
  # to in `replace` mode. The same directory, so the upstream socket keeps
  # nix-daemon.socket's `ConditionPathIsReadWrite`.
  daemonSocket = "/nix/var/nix/daemon-socket/socket";
  upstreamSocket = "/nix/var/nix/daemon-socket/upstream";
in
{
  inherit jsonFormat daemonSocket upstreamSocket;

  # `options.services.pynixd`, whole. Each platform module assigns this to
  # that attribute and adds nothing.
  options = {
    enable = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Enable pynixd, a Python Nix daemon protocol proxy.";
    };

    mode = lib.mkOption {
      type = lib.types.enum [
        "beside"
        "replace"
      ];
      default = "beside";
      description = ''
        How pynixd sits next to the machine's Nix daemon.

        `beside`: pynixd listens on its own socket, `unix_path`, and a
        client opts in with `--store unix:///run/pynixd/pynixd.sock`.
        Everything else still talks to nix-daemon.

        `replace`: pynixd takes nix-daemon's socket,
        `/nix/var/nix/daemon-socket/socket`, and nix-daemon moves to
        `daemon-socket/upstream` behind it, as `nix-daemon-upstream.socket`
        and `.service`. Nix's own two units are masked, so that
        `switch-to-configuration` moves the sockets in both directions.
        Every client that uses the
        daemon then talks to pynixd: `nix build` as a user, `ssh-ng://`
        through `nix-daemon --stdio`, remote builds. NixOS only.

        Root is the exception in both modes. Nix's `auto` store opens the
        store directly for a user who can write it, so root's `nix`
        reaches neither daemon unless it passes `--store daemon`.

        pynixd builds nothing itself in either mode: every build and every
        write to the store goes to nix-daemon, with its build users and its
        sandbox.
      '';
    };

    package = lib.mkOption {
      type = lib.types.package;
      description = "The pynixd package to use.";
      # No default, on purpose, and this file must not add one. A default reads
      # `pynixd/default.nix` and that project's own `flake.lock`, which builds
      # a second pynixd pinned apart from the one the consumer evaluates. The
      # consumer states `packages.pynixd` (docs/getting-started.md).
    };

    # **pynixd already serves `/metrics`, and nothing could reach it.**
    # `http_enable_metrics` is true by default in `config.py`, but the HTTP
    # server starts only when `http_port` is set, and that defaults to null.
    #
    # This exists because the obvious way to turn it on is wrong. Setting
    # `settings.http_port` alone also serves the binary cache on the same
    # port -- `http_enable_cache` defaults to true -- and `http_host` binds
    # every interface, so a person reaching for a metrics port publishes a
    # cache to all of them. `http_metrics_no_auth` then leaves `/metrics`
    # unauthenticated on it.
    #
    # So the safe combination is the easy one here, and the cache stays off
    # unless somebody asks for it in `settings`.
    metrics = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = ''
          Serve Prometheus metrics on `listenAddress:port`.

          This starts pynixd's HTTP server with the binary cache endpoints
          off. Set `settings.http_enable_cache` to serve both from one port.
        '';
      };

      port = lib.mkOption {
        type = lib.types.port;
        default = 9099;
        description = ''
          The port for `/metrics`. Arbitrary: pynixd holds no entry in the
          Prometheus port registry.
        '';
      };

      listenAddress = lib.mkOption {
        type = lib.types.str;
        default = "127.0.0.1";
        description = ''
          The address to serve metrics on. Localhost, and not the every-
          interface bind `http_host` defaults to: `/metrics` answers without
          authentication, so publishing it is a decision and not a default.
        '';
      };

      openFirewall = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = "Open `port` in the firewall for a scraper on another host.";
      };
    };

    settings = lib.mkOption {
      inherit (jsonFormat) type;
      default = { };
      description = ''
        Extra settings serialized to JSON and passed to pynixd via PYNIXD_CONFIG.
        See PynixdSettings in pynixd/config.py for the full list.
        Defaults: { unix_path = "/run/pynixd/pynixd.sock" }
      '';
      example = lib.literalExpression ''
        {
          stores.builder1 = {
            type = "ssh-subprocess";
            host = "builder1";
            # The file that names the host key of this builder. A store sees
            # the whole content of every build pushed to it, so this is the
            # check that makes the far side the machine named here.
            known_hosts = "/etc/ssh/ssh_known_hosts";
            systems = [ "x86_64-linux" ];
            priority = 2.0;
          };
          stores.builder2 = {
            type = "ssh-subprocess";
            host = "builder2";
            # `null` accepts any host key, which is what every SSH store did
            # before this field existed. It is worth writing only for a peer
            # with no exposure, such as a local virtual machine.
            known_hosts = null;
            systems = [ "aarch64-linux" ];
            priority = 0.5;
          };
          schedule_mode = "auto";
        }
      '';
    };
  };

  # The settings each platform applies inside its own `mkIf cfg.enable`.
  #
  # Already wrapped in `mkDefault`, so a platform module cannot drop the
  # wrapper by accident. Without it these are not defaults: they are values,
  # and a user who sets `services.pynixd.settings.unix_path` gets a conflict
  # rather than an override.
  #
  # `/run` is reachable on darwin as well as on NixOS -- nix-darwin makes it
  # through `/etc/synthetic.conf`, as a symlink to `private/var/run` -- so the
  # socket path is shared. A platform that ever needs a different one
  # overrides it in its own merge, and does not branch in this file.
  #
  # Keep the path short. `sun_path` holds 104 bytes on darwin and 108 on
  # Linux, and pynixd refuses a longer one at startup
  # (`Server._check_unix_socket_length`). This default is 25.
  # A function of the module's own configuration, because `metrics` above
  # feeds it. One `mkDefault` layer either way: a second block of defaults
  # for the metrics case would collide with this one rather than override it.
  settingsDefaultsFor =
    cfg:
    lib.mapAttrsRecursive (n: v: lib.mkDefault v) (
      {
        unix_path = "/run/pynixd/pynixd.sock";
        ssh_port = null;
        http_port = null;
      }
      // lib.optionalAttrs (cfg.mode == "replace") {
        unix_path = daemonSocket;
        # `managed = false` with an absolute path: pynixd connects to the
        # system daemon there and starts none of its own.
        stores.local = {
          type = "local-socket";
          socket_path = upstreamSocket;
          managed = false;
        };
      }
      // lib.optionalAttrs cfg.metrics.enable {
        http_port = cfg.metrics.port;
        http_host = cfg.metrics.listenAddress;
        http_enable_metrics = true;
        # Off, and not merely absent. `config.py` defaults it to true, so a
        # metrics port would otherwise publish a binary cache as well.
        http_enable_cache = false;
      }
    );

  # The configuration file, as a derivation.
  #
  # A function of the settings, and not a bare value, because `cfg.settings`
  # exists only inside a module. Both platforms need the *same* derivation:
  # NixOS puts its path in `restartTriggers`, and darwin embeds it in the
  # plist's `EnvironmentVariables`, so each reloads the service when the
  # settings change.
  configFileFor = settings: jsonFormat.generate "pynixd.json" settings;
}
