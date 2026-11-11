# pynixd as the Nix daemon of a machine, used the way people use one.
#
# `nix build --file . internal.tests.daemon` boots four guests on one segment:
#
#   daemon     pynixd in `replace` mode, so it holds
#              /nix/var/nix/daemon-socket/socket and nix-daemon sits behind it
#   control    the same machine with nix-daemon alone
#   client     a user with an SSH key for both
#   delegator  pynixd in `replace` mode with `control` as its only build store
#
# and runs the same work against `daemon` and `control`: a local build as a
# user, a build in the far store over `ssh-ng://`, and a remote build with
# `--builders`. Each answer from `daemon` is compared with the one from
# `control`. That is the differential run of tests/parity, over the real
# paths a user takes rather than one recorded connection. `delegator` takes
# part in `dynamic` alone.
#
# OpenSSH and `nix-daemon --stdio`, not pynixd's own SSH listener: that is
# what `ssh-ng://` runs on the far side, and it reaches pynixd only because
# pynixd holds the daemon socket.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivariumLib,
}:

let
  keys = import (pkgs.path + "/nixos/tests/ssh-keys.nix") pkgs;

  /*
    Each guest instantiates the work itself, as `tester`, through its own
    daemon -- pynixd on `daemon`. See tests/daemon/helpers. The same
    expression gives the same derivation on every guest, so the two
    servers' answers compare byte for byte.

    Not a `.drv` named here: each guest registers the closure of
    `settings` (`lateClosure`), and a `.drv`'s closure is every input
    derivation's `.drv`, which a garbage-collected host no longer has.
    Measured: `keyutils-1.6.3.drv does not exist`. busybox's output is the
    whole of the builder, and a valid path in every guest.
  */

  segment = "pynixd";
  address = {
    daemon = "10.44.0.1";
    control = "10.44.0.2";
    client = "10.44.0.3";
    delegator = "10.44.0.4";
  };

  # pynixd in `replace` mode. The guest's store is smaller than the 2 GiB
  # `ranking.min_free_bytes`, which no build would pass.
  pynixdGuest = {
    imports = [ ../../../nix/nixos/default.nix ];
    services.pynixd = {
      enable = true;
      mode = "replace";
      inherit package;
      settings.ranking.min_free_bytes = 0;
    };
  };

  # Of `dynamic`: a producer of a `.drv`, and that `.drv`, which is content-addressed.
  dynamicDerivations.nix.settings.experimental-features = [
    "ca-derivations"
    "dynamic-derivations"
  ];

  common = name: {
    vivarium = {
      memory = "1024M";
      lan = {
        network = segment;
        address = "${address.${name}}/24";
      };
    };
    networking.hosts = lib.mapAttrs' (host: ip: lib.nameValuePair ip [ host ]) address;
    environment.systemPackages = [ pkgs.iproute2 ];
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
      openssh.authorizedKeys.keys = [ keys.snakeOilEd25519PublicKey ];
    };
    nix.settings = {
      experimental-features = [ "nix-command" ];
      # No network; see tests/derivations/guest for the 75 seconds a
      # substituter query costs without this.
      substituters = lib.mkForce [ ];
    };
  };

  server = name: {
    imports = [ (common name) ];
    services.openssh.enable = true;
    # 22 as well: vivarium moves sshd to `vivarium.sshPort` for the
    # host's forward, and a client on the segment dials 22 like anywhere.
    services.openssh.ports = [ 22 ];
    # `stranger` is the user outside `allowed-users` that `untrusted` needs.
    # mkForce, because NixOS defaults this to `*` and lists concatenate.
    nix.settings.allowed-users = lib.mkForce [
      "root"
      "tester"
    ];
    users.users.stranger.isNormalUser = true;
  };
in
vivariumLib.mkTest (
  { config, ... }:
  {
    name = "pynixd-daemon";

    /*
      `mixed` by default: pynixd's guest on UML and the other two on QEMU.
      pynixd is one asyncio loop, so UML's single CPU costs it nothing and
      the host pays far less for the guest (measured on the guest suites:
      1253 MiB against 2256). The client and nix-daemon's guest build,
      which wants QEMU's CPU. `uml` or `qemu` puts all three on one.
    */
    knobs.backend = {
      env = "PYNIXD_GUEST_BACKEND";
      default = "mixed";
      description = "mixed, or uml or qemu for all three guests";
    };
    backend =
      if config.resolved.backend.value == "mixed" then "qemu" else config.resolved.backend.value;

    pythonPath = [ ../../daemon/helpers ];

    # Every path `settings` names is one the guests read.
    defaults =
      { settings, ... }:
      {
        vivarium.lateClosure = vivariumLib.pathsIn settings;
      };
    settings = {
      busybox = "${pkgs.busybox}";
      system = pkgs.stdenv.hostPlatform.system;
    };

    phases = {
      prepare = {
        script = ../../daemon/prepare.py;
        after = [ "boot" ];
        description = "sshd up, keys in place, and who holds each daemon socket";
      };
      local = {
        script = ../../daemon/local.py;
        after = [ "prepare" ];
        nodes = [
          "daemon"
          "control"
        ];
        description = "a user's nix build on each server";
      };
      remote = {
        script = ../../daemon/remote.py;
        after = [ "prepare" ];
        description = "the client builds in each far store, and on each as a builder";
      };
      untrusted = {
        script = ../../daemon/untrusted.py;
        after = [
          "local"
          "remote"
        ];
        description = "an untrusted user's refusals, and a stranger's, on each server";
      };
      bypass = {
        script = ../../daemon/bypass.py;
        after = [
          "local"
          "remote"
        ];
        nodes = [
          "daemon"
          "client"
        ];
        description = "a user's request fails with pynixd stopped, locally and over ssh-ng";
      };
      dynamic = {
        script = ../../daemon/dynamic.py;
        after = [ "prepare" ];
        nodes = [
          "control"
          "delegator"
        ];
        description = "a dynamic derivation through pynixd, on a builder that already holds the produced .drv";
      };
    };

    nodes = {
      daemon = {
        imports = [
          (server "daemon")
          pynixdGuest
        ];
        vivarium.backend = lib.mkIf (config.resolved.backend.value == "mixed") "uml";
      };
      control = {
        imports = [
          (server "control")
          dynamicDerivations
        ];
        # pynixd on `delegator` connects as root: a builder refuses an
        # input-addressed `BuildDerivation` from an untrusted user (`daemon.cc:634`).
        users.users.root.openssh.authorizedKeys.keys = [ keys.snakeOilEd25519PublicKey ];
      };
      delegator = {
        imports = [
          (common "delegator")
          dynamicDerivations
          pynixdGuest
        ];
        services.pynixd.settings.stores = {
          local.no_schedule = true;
          control = {
            type = "ssh-subprocess";
            host = "control";
            username = "root";
            client_keys = [ "/etc/ssh/delegator_key" ];
            # A guest on a private segment; nothing in the path to pin against.
            known_hosts = null;
            systems = [ pkgs.stdenv.hostPlatform.system ];
          };
        };
        system.activationScripts.delegatorKey = lib.stringAfter [ "users" "etc" ] ''
          install -m 0600 ${keys.snakeOilEd25519PrivateKey} /etc/ssh/delegator_key
        '';
        vivarium.backend = lib.mkIf (config.resolved.backend.value == "mixed") "uml";
      };
      client = {
        imports = [ (common "client") ];
        # The client's own daemon imports what the far side built, and an
        # untrusted user cannot import a path nobody signed. A user who runs
        # remote builds is trusted on their own machine.
        nix.settings.trusted-users = [
          "root"
          "tester"
        ];
        programs.ssh.extraConfig = ''
          StrictHostKeyChecking no
          UserKnownHostsFile /dev/null
          IdentityFile /etc/ssh/tester_key
        '';
        system.activationScripts.testerKey = lib.stringAfter [ "users" "etc" ] ''
          install -m 0600 -o tester ${keys.snakeOilEd25519PrivateKey} /etc/ssh/tester_key
        '';
      };
    };
  }
)
