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
  vivariumLib,
  # The dev shell's environment, so a suite in a guest imports what it
  # imports on the host, nanopynix's oracle for `tests/differential`
  # included.
  devEnv,
  # `pynixd-agent`, which `tests/unit/test_agent.py` runs against the parser.
  agent,
  nix,
  # Nix's master branch, as `NIX_GIT_BIN`: only `builder-rpc-v0`'s parity test runs it.
  nixGit,
  # The oldest Nix pynixd supports, as `NIX_FLOOR_BIN`: its store keys a realisation by the derivation's hash.
  nixFloor,
  # The build VM's launcher and helper, which `tests/functional/test_build_vm.py` runs.
  krunVariables,
  # Each Nix functional suite `tests/unit/test_nix_script_lines.py` resolves a reference in.
  nixFunctionalTests ? [ ],
  # The extended set (`@pytest.mark.extended`, nixidae/nixidae#1247) in place of the standard one.
  extended ? false,
  # Each lane a suite in `tests/suites.toml` names: `{ devEnv; nix; }`, the environment with that lane's
  # huggorm and the `nix daemon` it runs against (nixidae/nixidae#1249).
  lanes ? { },
}:

let
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

  allSuites = builtins.fromTOML (builtins.readFile ../../suites.toml);

  # The standard run takes every suite and deselects the extended tests
  # (`addopts` in each `pyproject.toml`); the extended run takes the suites
  # that hold some and selects only those. A later `-m` replaces the earlier.
  extendedSuites = lib.mapAttrs (
    _: suite:
    suite
    // {
      args = suite.args ++ [
        "-m"
        "extended"
      ];
    }
  ) (lib.filterAttrs (_: suite: suite.extended or false) allSuites);

  # The extended run repeats a suite once per lane it names, as `<suite>-<lane>`, each in its own guest.
  laneSuites = lib.concatMapAttrs (
    name: suite:
    lib.listToAttrs (
      map (
        lane:
        lib.nameValuePair "${name}-${lib.replaceStrings [ "_" ] [ "-" ] lane}" (suite // { inherit lane; })
      ) (suite.lanes or [ ])
    )
  ) extendedSuites;

  suites = if extended then extendedSuites // laneSuites else allSuites;

  laneOf =
    suite:
    lanes.${suite.lane}
      or (throw "tests/suites.toml names the lane ${suite.lane}, and the guest has none: see `laneNix` in default.nix");
  envOf = suite: if suite ? lane then (laneOf suite).devEnv else pytestEnv;

in
vivariumLib.mkTest (
  { config, ... }:
  {
    name = if extended then "pynixd-extended" else "pynixd";

    knobs.backend = {
      env = "PYNIXD_GUEST_BACKEND";
      default = "container";
      description = "container, qemu, or uml";
    };
    knobs.noDaemon = {
      env = "PYNIXD_NO_DAEMON";
      default = "";
      description = "1 runs every test store with no Nix daemon under it";
    };
    backend = config.resolved.backend.value;

    # Every path `settings` names is one the guests read.
    defaults =
      { settings, ... }:
      {
        vivarium.lateClosure = vivariumLib.pathsIn settings;
      };
    /*
      What the scripts need that only Nix knows.

      Each guest registers the closure of everything here with its Nix
      database (`lateClosure`), so these are valid paths in there rather
      than files Nix goes looking for a substituter for.
    */
    settings = {
      src = "${src}";
      nixpkgs = "${pkgs.path}";
      # `HELLO` in tests/_conftest/constants.py: the same `hello` that
      # `<nixpkgs>` evaluates to in the guest, so it is already valid.
      hello = "${pkgs.hello}";
      nixGit = "${nixGit}";
      nixFloor = "${nixFloor}";
      inherit nixFunctionalTests;

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

    # `prepare-<suite>` in the suite's guest only: `--only unit` keeps its
    # `after` closure, so it boots one guest and not all of them. A failure
    # in one suite's chain skips no other suite: a run that stopped at the
    # first failure would hide the second.
    phases =
      lib.mapAttrs' (
        name: _:
        lib.nameValuePair "prepare-${name}" {
          script = ../../guest/prepare.py;
          after = [ "boot" ];
          nodes = [ name ];
        }
      ) suites
      // {
        leaks = {
          script = ../../guest/leaks.py;
          after = lib.attrNames suites;
          always = true;
          description = "no process outlived its suite";
        };
      };

    /*
      One phase per suite (`tests/suites.toml`), each in the guest named
      after it, so the suites run at once. `leaks` names no guest, so it
      holds all.

      `-- -k name` on the driver selects inside every suite; pair it with
      `--only`, because a suite the selection does not reach exits 5 and
      fails its phase.
    */
    vivarium.recipes.suites = lib.mapAttrs (name: suite: {
      tests = suite.path;
      # The environment installs both packages editable from /work:
      # `tests/differential` refuses a pynixd from one tree and a
      # nix_daemon_protocol from another.
      directory = "/work";
      args = [
        "--tb=short"
        "-q"
      ]
      ++ suite.args;
      user = suite.user or "tester";
      # pynixd makes a cgroup per build (`builder/cgroup.py`). Root needs
      # one too: in the agent's cgroup, pynixd would move the agent into
      # its leaf.
      delegateCgroup = true;
      environment = {
        NIX_BIN = if suite ? lane then lib.getExe (laneOf suite).nix else "$(command -v nix)";
        NIX_GIT_BIN = "${nixGit}/bin/nix";
        NIX_FLOOR_BIN = "${nixFloor}/bin/nix";
        # Without it a failure names `/tmp/pynixd-logs/...`, which dies
        # with the guest.
        PYNIXD_TEST_LOG_DIR = "/artifacts/${name}-logs";
      }
      // lib.optionalAttrs (nixFunctionalTests != [ ]) {
        NIX_FUNCTIONAL_TESTS = lib.concatStringsSep ":" nixFunctionalTests;
      }
      // lib.optionalAttrs (config.resolved.noDaemon.value == "1") {
        # Every test store with no Nix daemon under it
        # (`LocalSocketStoreSpec.daemon`), to measure the gap.
        PYNIXD_NO_DAEMON = "1";
      };
      after = [ "prepare-${name}" ];
      /*
        Measured in a QEMU guest on an idle host: `tests/unit` 27.2s,
        `nix-daemon-protocol/tests` 4.8s, `tests/parity` 122.2s.
        `tests/unit` takes 87s under UML. The rest is for a loaded
        builder.
      */
      timeout = 2400;
    }) suites;

    # One guest per suite, named after it, so events.jsonl says
    # `machine=unit` and a suite's files land in artifacts/unit/.
    nodes = lib.mapAttrs (name: suite: {
      # `tests/unit` peaks well above the default 128M, and an agent that
      # is OOM-killed partway through looks exactly like a hang. krun backs
      # only what the guest touches, so it keeps its default, the host's
      # available memory, and one xdist worker per core fits. The node's own
      # backend decides, not the run's knob: `.krun` sets only the node's.
      imports = [
        (
          { config, ... }:
          {
            vivarium.memory = lib.mkIf (config.vivarium.backend != "krun") "3072M";
          }
        )
      ];
      vivarium = {
        # The suites make stores of their own under /tmp, on the root disk.
        diskSize = 8192;
        # `/dev/kvm` for the build VM's tests. UML has none to give.
        nestedVirtualization = name == "functional" && config.resolved.backend.value != "uml";
      }
      # A `uid-range` build mounts its own `/sys` (`tests/root/test_uid_range.py`). A container
      # guest's `/sys` is read-only, and the kernel then refuses a writable one: EPERM, for Nix's
      # daemon too.
      // lib.optionalAttrs (name == "root" && config.resolved.backend.value == "container") {
        backend = "qemu";
      };

      environment.variables = krunVariables;

      environment.systemPackages = [
        (envOf suite)
        agent
        nix
        pkgs.coreutils
        pkgs.util-linux
      ];
      # A lane's `nix daemon` is `NIX_BIN`, off the PATH: it would collide with the guest's own `nix`.
      system.extraDependencies = lib.optional (suite ? lane) (laneOf suite).nix;

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
    }) suites;
  }
)
