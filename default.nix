{
  # Where every dependency lives, as directories. nix/sources.nix says how
  # this repository finds the umbrella that owns them.
  sources ? import ./nix/sources.nix,
  # Another system's derivations build on a builder of it.
  system ? builtins.currentSystem,
  pkgs ? import sources.nixpkgs {
    inherit system;
    config.allowUnfree = true;
  },
}:
let
  inherit (pkgs) lib;
  inherit (import sources.nixvenv { inherit pkgs; }) nixvenv;

  daemon-protocol = pkgs.python3Packages.callPackage ./nix/nix-daemon-protocol.nix {
    pythonBuilder = pkgs.python3Packages.buildPythonPackage;
  };

  # Not pkgsCross.musl64: that is x86_64 on every host, and the agent is
  # filed under the host's own system below (#1056).
  agent = pkgs.pkgsStatic.callPackage ./nix/agent.nix { };

  package = pkgs.python3Packages.callPackage ./nix/pynixd.nix {
    pythonBuilder = pkgs.python3Packages.buildPythonApplication;
    nix-daemon-protocol = daemon-protocol;
    agents.${pkgs.stdenv.hostPlatform.system} = agent;
  };
  library = pkgs.python3Packages.callPackage ./nix/pynixd.nix {
    pythonBuilder = pkgs.python3Packages.buildPythonPackage;
    nix-daemon-protocol = daemon-protocol;
  };

  mkTests =
    {
      name,
      testArgs,
    }:
    pkgs.writeShellApplication {
      inherit name;
      runtimeInputs = [
        (nixvenv {
          python = pkgs.python3;
          packages = ps: [
            library
            ps.pytest
            ps.pyinstrument
          ];
          executables = [ "pytest" ];
        })
      ];
      text = ''
        export NIX_BIN=${lib.getExe pkgs.nix}
        exec pytest -p no:cacheprovider --timeout=60 ${testArgs} "$@"
      '';
    };

  specifictest = mkTests {
    name = "pynixd-specifictest";
    testArgs = "";
  };
  vivarium-runner = (import (sources.vivarium + "/lib.nix") { inherit pkgs sources; }).runner;

  nanopynix = import sources.nanopynix { inherit sources; };

  /*
    The interpreter of the development shell, resolved by pyproject.nix.

    **One set, and not a path of several.**  `nanopynix-testing` is the
    oracle of `tests/differential`, and nanopynix imports the compiled
    `huggorm_bindings`, so a PYTHONPATH assembled from store paths imports
    the packages and then fails on the first attribute.  Only a set that
    resolves the whole closure works, and mixing a package built in one set
    into another set's environment does not resolve either.

    `pythonSetWith` and not `pythonSet.overrideScope`: a set lifts its
    nixpkgs packages once, from the roots it was seeded with, so a project
    this repository owns has no way in afterwards.  `projectRoots` reads each
    `pyproject.toml` beside ours and resolves its dependencies the same way.
    nanopynix documents this as the seam for a consumer, and easykubenix uses
    it for `ekn`.

    Every root here is a real pyproject project, which is why this works at
    all: `vivarium-runner` carries one at `pkgs/vivarium-runner`, and it is what
    the phase scripts in `tests/guest/` import.

    **pynixd itself does not depend on any of this, and must not.**  The
    shipped proxy is pure Python and links no C++; `nanopynix-testing`
    carries the bindings, built against one version of Nix.  This set builds
    the shell, and `nix/pynixd.nix` still builds the package.

    This repository's two projects are editable, from `root`: the checkout
    for the shell, and `/work` for a guest, which holds a copy of the tree
    there.
  */
  devPythonSetAt =
    root:
    let
      own = _name: mkEditableProject root;
    in
    nanopynix.pythonSetWith {
      projectRoots = [
        ./.
        ./nix-daemon-protocol
        (sources.vivarium + "/pkgs/vivarium-runner")
      ];
      overlay = pySelf: _pyPrev: {
        pynixd = pySelf.callPackage (own "pynixd" ./.) { };
        nix-daemon-protocol = pySelf.callPackage (own "nix-daemon-protocol" ./nix-daemon-protocol) { };
        vivarium-runner = pySelf.callPackage (mkProject "vivarium-runner" (
          sources.vivarium + "/pkgs/vivarium-runner"
        )) { };
      };
    };

  # The install points at `dir` under `root`, a string, so no store copy.
  # `projectRoot` is the build's `src`, so it holds only the metadata: the
  # whole tree there rebuilds the env on every edit, `__pycache__` included.
  # Each wheel package keeps its `__init__.py`, because hatchling writes no
  # path for a missing directory.
  mkEditableProject =
    root: dir:
    nanopynix.ps.mkEditableProject {
      projectRoot = lib.fileset.toSource {
        root = dir;
        fileset = lib.fileset.unions (
          map (f: lib.fileset.maybeMissing (dir + "/${f}")) (
            [
              "pyproject.toml"
              "README.md"
              "LICENSE"
            ]
            ++ map (p: "${p}/__init__.py")
              (lib.importTOML (dir + "/pyproject.toml")).tool.hatch.build.targets.wheel.packages
          )
        );
      };
      root = root + lib.removePrefix (toString ./.) (toString dir);
      inherit (nanopynix.pythonSet) python;
    };

  mkProject =
    name: projectRoot:
    nanopynix.ps.mkProject {
      projectRoot = nanopynix.ps.projectSource {
        src = projectRoot;
        name = "${name}-source";
      };
      inherit (nanopynix.pythonSet) python;
    };

  /*
    What `nix develop` puts on the path.  The two extras come from
    `pyproject.toml`, so the shell and the distribution read one list.
  */
  devEnvFrom =
    set:
    set.mkVirtualEnv "pynixd-dev-env" {
      pynixd = [
        "test"
        "docs"
      ];
      nanopynix-testing = [ ];
      vivarium-runner = [ ];
    };
  shellEnv = devEnvFrom (devPythonSetAt (toString ./.));
  guestEnv = devEnvFrom (devPythonSetAt "/work");

  pyinstance = nixvenv {
    python = pkgs.python3;
    packages = ps: [
      library
      ps.pytest
      # The phase scripts in `tests/guest/` import it, and the type gate
      # reads them.
      # `devEnv` gets the same package from its own pyproject root, because
      # the two environments resolve by different machinery: this one is
      # nixpkgs, and that one is pyproject.nix.
      vivarium-runner
    ];
  };

  # **The type gate does not see nanopynix, on purpose.** It reads `tests/`,
  # and `tests/differential` imports the oracle, so the two imports there
  # carry `pyright: ignore[reportMissingImports]`. Putting `nanopynix-testing`
  # in this environment would tie a gate that must run everywhere to a C++
  # closure built against one version of Nix.

  /*
    The fixer.  It rewrites files, so it is not named like a gate and nothing
    in CI runs it.

    `ruff format` and `ruff check --fix` exit 0 once they have rewritten what
    they found.  A CI step that calls them therefore passes on code that fails
    the check, and the rewrite is thrown away with the runner.  The gates in
    `checks` below call the non-mutating forms for that reason.
  */
  fix = pkgs.writeShellApplication {
    name = "pynixd-fix";
    runtimeInputs = [
      pyinstance
      pkgs.ruff
    ];
    text = ''
      ruff format .
      ruff check --fix .
    '';
  };

  # One derivation per gate, so a failure names which one, and each fails the
  # build rather than reporting into a log nobody reads.
  mkCheck =
    name: deps: text:
    pkgs.runCommand "pynixd-check-${name}" { nativeBuildInputs = deps; } ''
      cp -r ${lib.cleanSource ./.} src
      chmod -R +w src
      cd src
      ${text}
      touch "$out"
    '';

  /*
    Nix's own functional suite, against a plain daemon and against pynixd.

    One program per Nix version, and not one program: the scripts come out of
    a Nix's own source, so the client, the scripts and the control daemon have
    to be the same version.  `nix/functional-tests/README.md` gives the
    commands; `streams` is the mode that records both runs and compares the
    wire.

    Below the floor the suite does not describe a daemon this proxy claims to
    serve.  See the protocol matrix in CLAUDE.md.
  */
  supportedNixFloor = "2.34";

  # Every `nixVersions.nix_2_*` at or above the floor, by attribute name.
  supportedNix =
    let
      named = lib.filterAttrs (name: _: lib.hasPrefix "nix_2_" name) pkgs.nixVersions;
    in
    # `tryEval`, because a removed version is still an attribute and reading
    # its `version` throws: `error: nix_2_10 has been removed`.  Asking the
    # floor without this filter fails the whole evaluation of this file.
    lib.filterAttrs (
      _: nix:
      let
        version = builtins.tryEval (lib.versions.majorMinor nix.version);
      in
      version.success && lib.versionAtLeast version.value supportedNixFloor
    ) named;

  # A guest session once for each supported Nix, as every guest's
  # `nix.package`: the daemon pynixd fronts and the client both.
  byNix =
    test:
    lib.mapAttrs (
      _: nix: test.extend { modules = [ { defaults.nix.package = lib.mkForce nix; } ]; }
    ) supportedNix;

  nixFunctionalTests =
    lib.mapAttrs (
      version: nix:
      pkgs.callPackage ./nix/functional-tests/package.nix {
        inherit nix version;
        pynixd = package;
        wirelogPython = pyinstance;
      }
    ) supportedNix;

  # The workflow schema, the steps a job asks for by name, and the writer
  # that turns a workflow value into YAML. It takes `lib` and nothing else,
  # which is what lets this repository use it while pinning its own nixpkgs.
  ghalib = import sources.ghanix { inherit lib; };

  # A workflow as a value. `nixVersion` is the Nix this repository pins, and
  # the runner installs that one. ci/workflows/bootstrap.nix says what a
  # second version on the machine cost.
  ciWorkflowValue =
    module:
    import module {
      inherit lib ghalib;
      nixVersion = pkgs.nix.version;
    };

  # The workflows of pynixd's own repository, beside the file each renders
  # to. ci/workflows/*.nix hold them and say why.
  ciWorkflows = {
    pages = {
      committed = ./.github/workflows/pages.yml;
      value = ciWorkflowValue ./ci/workflows/pages.nix;
    };
  };

  # The workflows that run the tests. nixidae renders each one to
  # `.github/workflows/pynixd-<name>.yml`, with this directory as the working
  # directory of every step.
  ciTestWorkflows = {
    tests = ciWorkflowValue ./ci/workflows/tests.nix;
  };

  # Those values as the files GitHub reads.
  #
  # `ghalib.toYamlScript` and not `pkgs.formats.yaml`, which is remarshal:
  # remarshal writes a multi-line string as one escaped double-quoted scalar,
  # so a ten-line `run:` body arrives as a single 600-column line holding
  # `\n`. The script writes those as literal blocks. It quotes `'on'`, which
  # is what a workflow needs: unquoted, `on` is the boolean `true` to a YAML
  # 1.1 parser.
  #
  # No yamlfmt. nixkube runs one because treefmt formats the committed file
  # and its CI ends in `git diff --exit-code`; this repository has neither, so
  # the render is the only thing that writes the file. `ciWorkflow` below
  # compares parsed documents, so style cannot fail it either way.
  ciWorkflowFiles = lib.mapAttrs (
    name: wf:
    let
      header = ''
        # GENERATED FILE -- do not edit by hand.
        # Edit ci/workflows/${name}.nix, then run: nix run --file . ci-workflow-update
      '';
    in
    pkgs.runCommand "${name}.yml"
      {
        nativeBuildInputs = [
          (nixvenv {
            python = pkgs.python3;
            packages = ps: [ ps.pyyaml ];
            executables = [ "python3" ];
          })
        ];
        value = builtins.toJSON wf.value;
        passAsFile = [ "value" ];
      }
      ''
        {
          printf '%s\n' ${lib.escapeShellArg header}
          python3 ${ghalib.toYamlScript} "$valuePath"
        } > $out
      ''
  ) ciWorkflows;

  ci-workflow-update = pkgs.writeScriptBin "ci-workflow-update" ''
    #! ${pkgs.runtimeShell}
    set -euo pipefail
    ${lib.concatStrings (
      lib.mapAttrsToList (
        name: file: "cp --no-preserve=mode ${file} .github/workflows/${name}.yml\n"
      ) ciWorkflowFiles
    )}
  '';

  checks = {
    format = mkCheck "format" [ pkgs.ruff ] "ruff format --check .";
    lint = mkCheck "lint" [ pkgs.ruff ] "ruff check .";
    types = mkCheck "types" [
      pkgs.pyright
      pyinstance
    ] "pyright --pythonpath ${pyinstance}/bin/python .";

    # Does each committed workflow still say what its ci/workflows/*.nix says?
    #
    # Parsed, not compared as text, so key order and quoting cannot fail this.
    # yq reads YAML 1.2, where a bare `on:` key is the string "on". A YAML 1.1
    # parser answers the boolean `true` for it, which would compare a boolean
    # key against a string one and fail every run.
    #
    ciWorkflow =
      pkgs.runCommand "ci-workflow-check"
        {
          nativeBuildInputs = [
            pkgs.yq-go
            pkgs.jq
          ];
        }
        (
          lib.concatStrings (
            lib.mapAttrsToList (name: wf: ''
              yq --output-format=json '.' ${wf.committed} | jq --sort-keys . > committed.json
              jq --sort-keys . < ${pkgs.writeText "${name}-rendered.json" (builtins.toJSON wf.value)} > rendered.json
              if ! diff --unified committed.json rendered.json; then
                echo >&2
                echo ".github/workflows/${name}.yml and ci/workflows/${name}.nix disagree." >&2
                echo "Run: nix run --file . ci-workflow-update" >&2
                exit 1
              fi
            '') ciWorkflows
          )
          + "touch $out\n"
        );
  };
in
package
// {
  inherit
    package
    library
    agent
    daemon-protocol
    specifictest
    fix
    checks
    nixFunctionalTests
    ciWorkflows
    ciTestWorkflows
    ciWorkflowFiles
    ci-workflow-update
    pkgs
    ;

  pynixd-docs = pkgs.python3Packages.callPackage ./nix/docs.nix {
    pynixd = library;
    inherit nixvenv;
  };

  shell = pkgs.callPackage ./nix/shell.nix {
    inherit agent;
    devEnv = shellEnv;
    nixpkgsPath = pkgs.path;
  };
  nixosModule = import ./nix/nixos/default.nix;

  tests = rec {
    simple = pkgs.callPackage ./tests/derivations/simple {
      pynixd-lib = library;
    };
    pytest = pkgs.callPackage ./tests/derivations/pytest {
      pynixd-lib = library;
      inherit nixvenv;
      src = lib.cleanSource ./.;
    };

    # The performance benchmarks, in a guest. Not a gate: they print numbers
    # and pass unless the run fails. `nix run --file . tests.benchmark.driver
    # -- --out <abs>`.
    benchmark = pkgs.callPackage ./tests/derivations/benchmark {
      inherit package;
      src = lib.cleanSource ./.;
      inherit sources;
      inherit (sources) vivarium;
      devEnv = guestEnv;
    };

    # The same suites in a guest, where cleanup is a poweroff rather than
    # a promise.
    guest = pkgs.callPackage ./tests/derivations/guest {
      pynixd-lib = library;
      src = lib.cleanSource ./.;
      inherit sources;
      inherit (sources) vivarium;
      inherit agent;
      devEnv = guestEnv;
    };

    # The Nix functional suite in a UML guest, where cleanup is a poweroff
    # rather than a promise. `nix run --file . tests.guest-nixft.driver
    # -- --out <abs>`. Issue #45.
    guest-nixft = pkgs.callPackage ./tests/derivations/guest-nixft {
      pynixd-lib = library;
      src = lib.cleanSource ./.;
      inherit sources;
      inherit (sources) vivarium;
      nixft = nixFunctionalTests.nix_2_34;
    };

    # pynixd as the Nix daemon of a guest, against nix-daemon on another,
    # over the paths a user takes: a local build, `ssh-ng://` and
    # `--builders`.
    daemon = pkgs.callPackage ./tests/derivations/daemon {
      inherit package;
      inherit sources;
      inherit (sources) vivarium;
    };

    # A running machine switched to pynixd and back, in each mode.
    switch = pkgs.callPackage ./tests/derivations/switch {
      inherit package;
      inherit sources;
      inherit (sources) vivarium;
      nixVersions = supportedNix;
    };

    # What an upgrade does to builds in flight; docs/design/upgrade-handover.md.
    handover = pkgs.callPackage ./tests/derivations/handover {
      inherit package;
      inherit sources;
      inherit (sources) vivarium;
    };

    daemonByNix = byNix daemon;
    switchByNix = byNix switch;
  };
}
