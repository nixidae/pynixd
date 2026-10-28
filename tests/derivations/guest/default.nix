# The suites, in a guest.
#
# `nix build --file . internal.tests.guest` boots one NixOS guest per suite, runs
# the suites at once as phases of their own, counts each guest's
# processes before and after, and powers them off. Nothing of it survives
# -- see tests/guest/leaks.py for why that is the point.
#
# A container by default: seconds to boot, and the host's store is the
# guest's. `PYNIXD_GUEST_BACKEND=qemu` or `=uml` runs the same session on
# the other backends. These suites once panicked a UML guest in `munmap`;
# vivarium issue #8 traced that to io_uring, which uvloop uses and
# UML's memory manager cannot map, and a UML guest now has it disabled.
{
  pkgs,
  lib ? pkgs.lib,
  pynixd-lib,
  src,
  vivarium,
  # Passed to vivarium's lib.nix, so it takes nixvenv from the same nixidae.
  sources,
  # The dev shell's environment, so a suite in a guest imports what it
  # imports on the host, nanopynix's oracle for `tests/differential`
  # included.
  devEnv,
  # `pynixd-agent`, which `tests/unit/test_agent.py` runs against the parser.
  agent,
}:

let
  vivariumLib = import (vivarium + "/lib.nix") { inherit pkgs lib sources; };

  pytestEnv = devEnv;

  /*
    A keypair for `tester`, made once at build time. Not a secret: it
    opens nothing outside a guest.

    pynixd's SSH server takes any key, and a client with none to offer
    is refused: asyncssh reads `~/.ssh`, and so does `ssh` for
    `ssh-ng://`. On the host the developer's own key answers. Measured:
    the 8 `test_sftp_server` cases failed on "Permission denied for user
    test" without it.
  */
  testerKey = pkgs.runCommand "tester-ssh-key" { nativeBuildInputs = [ pkgs.openssh ]; } ''
    mkdir $out
    ssh-keygen -q -t ed25519 -N "" -C tester -f $out/id_ed25519
  '';

  perTestTimeout = "--async-test-timeout=600";

  /*
    One phase each, generated below from this list.

    Per suite, because `nix-daemon-protocol/tests` is its own project with
    its own `pytest.ini`: it does not know `--async-test-timeout`, and
    pytest answers an unknown option with exit 4 before collecting
    anything. The 600 seconds is against the suite's own 120, which is
    written for a machine rather than a machine inside one: measured,
    `test_wire_parity[impure]` timed out at 120.016s on an idle host.

    Each suite needs only `prepare`, so a failure in one skips none of the
    others -- they are independent, and a run that stopped at the first
    would hide the second.
  */
  suites = {
    unit = {
      path = "tests/unit";
      flags = [ perTestTimeout ];
    };
    protocol = {
      path = "nix-daemon-protocol/tests";
      flags = [ ];
    };
    functional = {
      path = "tests/functional";
      flags = [ perTestTimeout ];
    };
    differential = {
      path = "tests/differential";
      flags = [ perTestTimeout ];
    };
    # Missing for 35 minutes of failure that the guest's own configuration
    # caused: it took cache.nixos.org from the NixOS default and had no
    # route to it. `substituters = lib.mkForce [ ]` below is what put it
    # back -- 2120.92s and 5 failures became 121.20s and none. Issue #37.
    parity = {
      path = "tests/parity";
      flags = [ perTestTimeout ];
    };
  };
in
vivariumLib.mkTest (
  { config, ... }:
  {
    name = "pynixd";

    knobs.backend = {
      env = "PYNIXD_GUEST_BACKEND";
      default = "container";
      description = "container, qemu, or uml";
    };
    backend = config.resolved.backend.value;

    /*
      What the scripts need that only Nix knows.

      `mkTest` registers the closure of everything here with the guest's
      Nix database, so these are valid paths in there rather than files Nix
      goes looking for a substituter for.
    */
    settings = {
      src = "${src}";
      nixpkgs = "${pkgs.path}";
      inherit suites;
      # `HELLO` in tests/_conftest/constants.py: the same `hello` that
      # `<nixpkgs>` evaluates to in the guest, so it is already valid.
      hello = "${pkgs.hello}";

      /*
        What a build with each nixpkgs builder the suites use needs,
        registered so the guest finds it valid. Without it a
        `runCommand` in `tests/functional` asked for `stdenv` and set out
        to build 497 derivations from the bootstrap seed, with no network
        to fetch a source.
      */
      buildInputs = map (drv: "${drv.inputDerivation}") [
        (pkgs.runCommand "probe" { } "touch $out")
        (pkgs.stdenvNoCC.mkDerivation {
          name = "probe";
          dontUnpack = true;
          installPhase = "touch $out";
        })
        (pkgs.writeShellApplication {
          name = "probe";
          text = "true";
        })
        (pkgs.symlinkJoin {
          name = "probe";
          paths = [
            pkgs.bash
            pkgs.coreutils
          ];
        })
      ];
    };

    phases = {
      prepare = {
        script = ../../guest/prepare.py;
        after = [ "boot" ];
      };
      leaks = {
        script = ../../guest/leaks.py;
        after = lib.attrNames suites;
        always = true;
        description = "no process outlived its suite";
      };
    }
    // lib.mapAttrs (name: suite: {
      script = ../../guest/suite.py;
      after = [ "prepare" ];
      # A guest each, so the suites run at once: the session starts phases
      # whose guests do not overlap together. `prepare` and `leaks` name no
      # guest, so they hold all three.
      nodes = [ name ];
      description = suite.path;
    }) suites;

    # One guest per suite, named after it, so events.jsonl says
    # `machine=unit` and a suite's files land in artifacts/unit/.
    nodes = lib.genAttrs (lib.attrNames suites) (_: {
      vivarium = {
        # `tests/unit` peaks well above the default 128M, and an agent that
        # is OOM-killed partway through looks exactly like a hang.
        memory = "3072M";
        # The suites make stores of their own under /tmp, on the root disk.
        diskSize = 8192;
      };

      environment.systemPackages = [
        pytestEnv
        agent
        pkgs.nix
        pkgs.coreutils
        pkgs.util-linux
      ];

      /*
        Not root, which is what the suites run as everywhere else.

        One test asserts that a store root pynixd cannot write to yields an
        inactive instance, and root can write anywhere -- so as root it is
        the test that fails and not the code. Measured: the only failure in
        `tests/unit` in a guest.
      */
      users.users.tester = {
        isNormalUser = true;
        uid = 1000;
        openssh.authorizedKeys.keyFiles = [ "${testerKey}/id_ed25519.pub" ];
      };

      # Copied and not linked: ssh refuses a private key it does not own
      # at 0600, and a store file is neither.
      systemd.tmpfiles.rules = [
        "d /home/tester/.ssh 0700 tester users -"
        "C /home/tester/.ssh/id_ed25519 - - - - ${testerKey}/id_ed25519"
        "z /home/tester/.ssh/id_ed25519 0600 tester users -"
        "C /home/tester/.ssh/id_ed25519.pub - - - - ${testerKey}/id_ed25519.pub"
        "z /home/tester/.ssh/id_ed25519.pub 0644 tester users -"
      ];

      nix.settings = {
        # What `tests/_conftest/constants.py` asks for through NIX_CONFIG.
        # Named here too, because a test that runs `nix` without that
        # fixture reads the guest's own configuration.
        experimental-features = [
          "nix-command"
          "flakes"
          "read-only-local-store"
          "ca-derivations"
          "dynamic-derivations"
          "recursive-nix"
        ];
        # The store belongs to root, so an untrusted user could not build in
        # it. Issue #29 is the same problem in the packaged check, where the
        # build user cannot be made trusted.
        trusted-users = [
          "root"
          "tester"
        ];
        # The guest has no route out, and NixOS defaults this to
        # `https://cache.nixos.org/`. So every substituter query resolved
        # nowhere, waited 15 seconds and retried five times.
        #
        # Measured: `tests/parity` took 2120.92s in the guest and 18.21s on
        # the host, and four of its five failures were recordings that agreed
        # on every wire answer and disagreed only on which operation the retry
        # warnings landed under. Issue #37.
        #
        # `[substitute]` is the one case that needs a cache. It makes its own
        # and names it with `--substituters`, so an empty list here does not
        # reach it -- that case passed in the run that measured this.
        #
        # `mkForce`, because `nix.settings` holds lists and the module system
        # merges lists by concatenation. A plain `[ ]` adds nothing and
        # removes nothing: the rendered `/etc/nix/nix.conf` still read
        # `substituters = https://cache.nixos.org/`, measured on the etc
        # derivation before this line said `mkForce`.
        substituters = lib.mkForce [ ];
      };

      # `tests/nix/drv-probes.nix` imports <nixpkgs>. Without this, the 22
      # tests that read it fail on "file 'nixpkgs' was not found in the Nix
      # search path", which names nothing about pynixd.
      nix.nixPath = [ "nixpkgs=${pkgs.path}" ];
    });
  }
)
