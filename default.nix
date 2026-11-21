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
  inherit ((import sources.nixvenv { inherit pkgs sources; }).lib) nixvenv ps;

  /*
    anyio from `fixes-4.14.2` of Lillecarl/anyio: the 4.14.2 that nixpkgs
    gives the rest of the set, plus fixes meant for upstream.
    `setup_unix_local_socket` stats and unlinks a stale socket file on the
    event loop upstream (nixidae/nixidae#1547). Not upstream `master`: it
    failed four wirelog recorder tests and `test_collect` with
    `InvalidStateError` from `Future.set_result`. The fork's tarball has no
    git history for setuptools-scm, so the version is set here. Its suite is
    not this build's to run. No nixpkgs patch either: a consumer's newer
    nixpkgs patches `tests/streams/test_tls.py` with agronholm/anyio 818e4ac,
    which does not apply to the fork's tree; a fix the fork needs is a commit
    on the fork.
  */
  anyioVersion = "4.14.2+lillecarl.1";
  anyio = pkgs.python3Packages.anyio.overridePythonAttrs {
    version = anyioVersion;
    src = sources.anyio;
    patches = [ ];
    doCheck = false;
  };

  daemon-protocol = pkgs.python3Packages.callPackage ./nix/nix-daemon-protocol.nix {
    pythonBuilder = pkgs.python3Packages.buildPythonPackage;
  };
  store = pkgs.python3Packages.callPackage ./nix/pynixd-store.nix {
    pythonBuilder = pkgs.python3Packages.buildPythonPackage;
    nix-daemon-protocol = daemon-protocol;
    inherit anyio;
  };

  # Not pkgsCross.musl64: that is x86_64 on every host, and the agent is
  # filed under the host's own system below (#1056).
  agent = pkgs.pkgsStatic.callPackage ./nix/agent.nix { };

  /*
    A venv, not `buildPythonApplication`: pynixd starts its sandbox helper as
    `sys.executable -m pynixd_store.builder.child`, and only a venv's
    interpreter finds pynixd's packages with no PYTHONPATH (#1046). The
    wrapper carries `PYNIXD_AGENTS` (`pynixd/agent.py`, issue #98), and on
    Linux `krunVariables`.
  */
  venv = nixvenv {
    python = pkgs.python3;
    # The trimmed botocore here only: the test environments' moto needs every model.
    packages = _: [ (library.override { inherit botocore; }) ];
    executables = [ "pynixd" ];
  };

  /*
    What the build VM (`build_vm`, #1186) runs: its launcher and its init,
    and the helper inside the guest, which is this venv's interpreter. The
    guest's allowlist shows the helper the venv's closure and nothing else
    of the host's store.
  */
  # mkfs.xfs alone. xfsprogs' `bin` also holds xfs_scrub_all and a Python with dbus-python,
  # which bring icu4c, glib and systemd: 100 MB of the installer (nixidae/nixidae#1472).
  mkfsXfs = pkgs.runCommand "mkfs.xfs-${pkgs.xfsprogs.version}" { } ''
    install -D -m 0555 ${lib.getBin pkgs.xfsprogs}/bin/mkfs.xfs $out/bin/mkfs.xfs
  '';
  # One passt per build VM: each needs its own host UDP ports (nix/passt/0001-*.patch says why).
  # The source is pinned to the release the patch is written against: a consumer's own
  # `pkgs` may carry an older passt, and 2025_09_19 (sg1's nodes) rejects three of its hunks.
  passt = pkgs.passt.overrideAttrs (old: rec {
    version = "2026_07_16.090d739";
    src = pkgs.fetchurl {
      url = "https://passt.top/passt/snapshot/passt-${version}.tar.gz";
      hash = "sha256-UD4fkctSqMDQTmZxpmBjR1uKLV0Lf9QoqtSK1u8EQ+4=";
    };
    patches = (old.patches or [ ]) ++ [ ./nix/passt/0001-udp-kernel-picked-host-port.patch ];
  });
  krunVariables = lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
    PYNIXD_KRUN = lib.getExe krun;
    PYNIXD_KRUN_INIT = "${krun.init}/bin/pynixd-krun-init";
    PYNIXD_KRUN_PYTHON = "${venv}/lib/venv/bin/python";
    PYNIXD_KRUN_CLOSURE = "${pkgs.closureInfo { rootPaths = [ venv ]; }}/store-paths";
    PYNIXD_KRUN_MKFS = "${mkfsXfs}/bin/mkfs.xfs";
    PYNIXD_KRUN_PASST = lib.getExe' passt "passt";
  };
  /*
    The build VM on a Mac: the launcher is this system's, and the guest's
    kernel, init and helper are aarch64-linux builds, which a Linux builder
    makes. Rosetta in the guest runs x86_64-linux builds. No passt, which
    is Linux-only. Not in the package's wrapper: pynixd for a Mac would
    then need a Linux builder to build.
  */
  linuxGuest = import ./. {
    inherit sources;
    system = "aarch64-linux";
  };
  krunDarwinVariables = lib.optionalAttrs (pkgs.stdenv.hostPlatform.system == "aarch64-darwin") {
    PYNIXD_KRUN = lib.getExe krun;
    PYNIXD_KRUN_INIT = "${linuxGuest.internal.krun.init}/bin/pynixd-krun-init";
    PYNIXD_KRUN_KERNEL = "${linuxGuest.internal.krun.kernel}/share/libkrunfw/Image";
    PYNIXD_KRUN_PYTHON = "${linuxGuest.internal.venv}/lib/venv/bin/python";
    PYNIXD_KRUN_CLOSURE = "${
      pkgs.closureInfo { rootPaths = [ linuxGuest.internal.venv ]; }
    }/store-paths";
    PYNIXD_KRUN_MKFS = "${lib.getBin pkgs.e2fsprogs}/bin/mke2fs";
    # Nix's Linux `SANDBOX_SHELL`.
    PYNIXD_KRUN_SANDBOX_SHELL = "${linuxGuest.internal.pkgs.pkgsStatic.busybox}/bin/busybox";
  };
  # For a shell to source: `tests/functional/test_build_vm_darwin.py` runs in the dev shell on a Mac, and builds
  # with a static busybox of each Linux system.
  krunDarwinEnv = pkgs.writeText "pynixd-krun-darwin.env" (
    lib.concatStrings (
      lib.mapAttrsToList (name: value: "export ${name}=${value}\n") (
        krunDarwinVariables
        // {
          PYNIXD_TEST_BUSYBOX_AARCH64_LINUX = krunDarwinVariables.PYNIXD_KRUN_SANDBOX_SHELL;
          PYNIXD_TEST_BUSYBOX_X86_64_LINUX = "${
            (import sources.nixpkgs { system = "x86_64-linux"; }).pkgsStatic.busybox
          }/bin/busybox";
        }
      )
    )
  );
  /*
    What the darwin build VM (`darwin_build_vm`) runs in the guest: this
    venv's interpreter, which the guest reads from the host's store through
    an allowlist of the venv's closure. QEMU and darwin-virtiofsd come from
    the snapshot, which names those it was saved with.
  */
  darwinVmVariables = lib.optionalAttrs pkgs.stdenv.hostPlatform.isDarwin {
    PYNIXD_DARWIN_VM_PYTHON = "${venv}/lib/venv/bin/python";
    PYNIXD_DARWIN_VM_CLOSURE = "${pkgs.closureInfo { rootPaths = [ venv ]; }}/store-paths";
  };
  # The same, for a shell to source: `tests/functional/test_build_darwin_vm.py` runs in the dev shell.
  darwinVmEnv = pkgs.writeText "pynixd-darwin-vm.env" (
    lib.concatStrings (lib.mapAttrsToList (name: value: "export ${name}=${value}\n") darwinVmVariables)
  );

  package =
    let
      agents.${pkgs.stdenv.hostPlatform.system} = lib.getExe agent;
      wrapperArgs = lib.concatLists (
        lib.mapAttrsToList (name: value: [
          "--set-default"
          name
          value
        ]) ({ PYNIXD_AGENTS = builtins.toJSON agents; } // krunVariables // darwinVmVariables)
      );
    in
    pkgs.runCommand "pynixd-${library.version}"
      {
        nativeBuildInputs = [ pkgs.makeBinaryWrapper ];
        passthru = { inherit venv; };
        meta = {
          inherit (library.meta) description;
          mainProgram = "pynixd";
        };
      }
      ''
        makeWrapper ${lib.getExe venv} $out/bin/pynixd ${lib.escapeShellArgs wrapperArgs}
      '';
  # The tarball a machine without Nix unpacks into /nix; nixidae/nixidae#1365.
  installerTarball = import ./nix/installer {
    inherit pkgs package;
    nix = defaultNix;
  };
  /*
    botocore with the service models pynixd loads and no others: EC2 for the provisioner, S3, and
    what botocore's credential chain calls (STS for an assumed role, SSO and SSO-OIDC, sign-in for
    a login session). IAM is the functional suite's, for its S3 server's keys. The other 400
    services are 98 MB of the installer (nixidae/nixidae#1472). A service pynixd starts to call
    goes in this list, or botocore raises UnknownServiceError. The product's `venv` takes it.
  */
  botocore = pkgs.python3Packages.botocore.overridePythonAttrs (old: {
    postInstall = (old.postInstall or "") + ''
      for service in $out/${pkgs.python3.sitePackages}/botocore/data/*/; do
        case $(basename "$service") in
          ec2 | s3 | sts | sso | sso-oidc | signin | iam) ;;
          *) rm -r "$service" ;;
        esac
      done
    '';
    # Its own suite loads every service's model.
    doCheck = false;
  });
  /*
    The kr8s fork nanopynix builds, on this anyio. httpcore, httpx and
    httpx-ws depend on anyio too; each takes this one, or the venv holds two
    and `pythonCatchConflictsPhase` fails. Their suites are not this build's
    to run.
  */
  onAnyio =
    package: extra:
    (package.override ({ inherit anyio; } // extra)).overridePythonAttrs { doCheck = false; };
  httpcore = onAnyio pkgs.python3Packages.httpcore { };
  httpx = onAnyio pkgs.python3Packages.httpx { inherit httpcore; };
  kr8s = pkgs.python3Packages.callPackage "${sources.nanopynix}/nix/kr8s.nix" {
    src = sources.kr8s;
    inherit anyio httpx;
    httpx-ws = onAnyio pkgs.python3Packages.httpx-ws { inherit httpcore httpx; };
  };
  library = pkgs.python3Packages.callPackage ./nix/pynixd.nix {
    pythonBuilder = pkgs.python3Packages.buildPythonPackage;
    nix-daemon-protocol = daemon-protocol;
    pynixd-store = store;
    inherit
      anyio
      kr8s
      httpx
      httpcore
      ;
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
        export NIX_BIN=${lib.getExe defaultNix}
        exec pytest -p no:cacheprovider --timeout=60 ${testArgs} "$@"
      '';
    };

  specifictest = mkTests {
    name = "pynixd-specifictest";
    testArgs = "";
  };
  vivarium = import sources.vivarium { inherit pkgs sources; };
  vivariumLib = vivarium.lib;
  krun = pkgs.callPackage ./nix/krun/package.nix { inherit (vivarium.packages) libkrun; };

  # The Nix pynixd runs against unless a test names another: the newest
  # release, not `pkgs.nix`, which nixpkgs keeps a minor behind. Patched: its
  # builtin:fetchurl of s3:// deadlocked the test guests' builders (Lillecarl/nix#424).
  defaultNixRelease = pkgs.nixVersions.latest;
  # The version is the release's: a CI runner installs the release from releases.nixos.org.
  # `overrideAttrs` drops `appendPatches`, so a Nix that patches further starts here.
  defaultNixPatched = defaultNixRelease.appendPatches [ ./nix/nix-aws-credentials-in-a-child.patch ];
  # Without Nix's own suite: no cache holds a patched Nix, and its `main:build`
  # functional test failed on GitHub's aarch64 runner (run 43652156125).
  defaultNix = defaultNixPatched.overrideAttrs { doCheck = false; };
  # Its name in `nixVersions` and in `nixFunctionalTests`, such as `nix_2_35`.
  defaultNixName =
    "nix_" + lib.replaceStrings [ "." ] [ "_" ] (lib.versions.majorMinor defaultNix.version);
  # Every guest's daemon and client. NixOS defaults `nix.package` to `pkgs.nix`.
  withDefaultNix = test: test.extend { modules = [ { defaults.nix.package = defaultNix; } ]; };
  # The functional suite's Nix: `build-remote.sh` speaks `ssh-ng://` to its first builder there,
  # which needs Lillecarl/nix#413 fixed to pass. Its guests run the same Nix as their system Nix.
  nixftNix = defaultNixPatched.appendPatches [ ./nix/functional-tests/nix-ssh-ng-keep-failed.patch ];
  withNixftNix = test: test.extend { modules = [ { defaults.nix.package = nixftNix; } ]; };
  # Determinate Nix v3.24.0 as it ships: huggorm's component build of the pinned fork, without huggorm's patches.
  determinateNix =
    (import "${sources.huggorm}/nix/determinate.nix" { inherit pkgs sources; }).components.nix-cli;
  vivarium-runner = vivariumLib.runner;

  # Allow-lists, by extension, under the directories that hold source. The run
  # directories of old guests sit beside them (`out-*`, 5.2G in one checkout),
  # and a copy of `./.` took the gates five minutes and filled a QEMU guest's disk.
  sourceDirs = [
    ./pynixd
    ./tests
    ./nix
    ./docs
    ./nix-daemon-protocol
    ./pynixd-store
  ];
  filesOf =
    extensions:
    lib.fileset.unions (
      [
        ./pyproject.toml
        ./ruff.toml
        ./ruff-strict.toml
        ./mypy.ini
      ]
      ++ map (lib.fileset.fileFilter (
        file: lib.any file.hasExt extensions || file.name == "py.typed"
      )) sourceDirs
    );
  pythonExtensions = [
    "py"
    "pyi"
    "toml"
    "ini"
  ];

  # What ruff and zuban read.
  checkSrc = lib.fileset.toSource {
    root = ./.;
    fileset = filesOf pythonExtensions;
  };

  # What the suites read beside it: Nix expressions, the scripts they run, and
  # the documents a test holds to the code.
  testSrc = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      (filesOf (
        pythonExtensions
        ++ [
          "nix"
          "sh"
          "md"
          "c"
        ]
      ))
      # `docs/architecture.md` names it, and `tests/unit/test_architecture_doc.py` checks each name.
      ./GLOSSARY.md
    ];
  };

  nanopynix = import sources.nanopynix { inherit sources; };

  /*
    The interpreter of the development shell, resolved by pyproject.nix.

    **One set, and not a path of several.**  `huggorm` is the oracle of
    `tests/differential`, and it imports the compiled `huggorm_bindings`, so
    a PYTHONPATH assembled from store paths imports
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
    shipped proxy is pure Python and links no C++; `huggorm` carries the
    bindings, built against one version of Nix.  This set builds
    the shell, and `nix/pynixd.nix` still builds the package.

    This repository's two projects are editable, from `root`: the checkout
    for the shell, and `/work` for a guest, which holds a copy of the tree
    there. `lane` is a nanopynix lane: `stable` is the default one.
  */
  devPythonSetAt =
    lane: root:
    let
      own = _name: mkEditableProject root;
    in
    nanopynix.lanes.${lane}.pythonSetWith {
      projectRoots = [
        ./.
        ./nix-daemon-protocol
        ./pynixd-store
        vivariumLib.runnerSource
        vivariumLib.agentSource
        # For its build system: the overlay builds anyio from this source.
        sources.anyio
      ];
      overlay = pySelf: _pyPrev: {
        pynixd = pySelf.callPackage (own "pynixd" ./.) { };
        nix-daemon-protocol = pySelf.callPackage (own "nix-daemon-protocol" ./nix-daemon-protocol) { };
        pynixd-store = pySelf.callPackage (own "pynixd-store" ./pynixd-store) { };
        vivarium-runner = pySelf.callPackage (mkProject "vivarium-runner" vivariumLib.runnerSource) { };
        vivarium-agent = pySelf.callPackage (mkProject "vivarium-agent" vivariumLib.agentSource) { };
        # The fork, as for `library`. Every lifted package names anyio, so this one serves them all.
        anyio = pySelf.callPackage (ps.mkProject {
          projectRoot = sources.anyio;
          inherit (nanopynix.lib.pythonSet) python;
          extra = rendered: {
            env = (rendered.env or { }) // {
              SETUPTOOLS_SCM_PRETEND_VERSION = anyioVersion;
            };
          };
        }) { };
      };
    };

  # The install points at `dir` under `root`, a string, so no store copy.
  # `projectRoot` is the build's `src`, so it holds only the metadata: the
  # whole tree there rebuilds the env on every edit, `__pycache__` included.
  # Each wheel package keeps its `__init__.py`, because hatchling writes no
  # path for a missing directory.
  mkEditableProject =
    root: dir:
    ps.mkEditableProject {
      projectRoot = lib.fileset.toSource {
        root = dir;
        fileset = lib.fileset.unions (
          map (f: lib.fileset.maybeMissing (dir + "/${f}")) (
            [
              "pyproject.toml"
              "README.md"
              "LICENSE"
            ]
            ++
              map (p: "${p}/__init__.py")
                (lib.importTOML (dir + "/pyproject.toml")).tool.hatch.build.targets.wheel.packages
          )
        );
      };
      root = root + lib.removePrefix (toString ./.) (toString dir);
      inherit (nanopynix.lib.pythonSet) python;
    };

  mkProject =
    name: projectRoot:
    ps.mkProject {
      projectRoot = ps.projectSource {
        src = projectRoot;
        name = "${name}-source";
      };
      inherit (nanopynix.lib.pythonSet) python;
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
      huggorm = [ ];
      vivarium-runner = [ ];
    };
  shellEnv = devEnvFrom (devPythonSetAt "stable" (toString ./.));
  guestEnv = devEnvFrom (devPythonSetAt "stable" "/work");

  # The `nix daemon` of each lane `tests/suites.toml` names: the release that lane's huggorm Nix
  # patches, unpatched (nixidae/nixidae#1249). `defaultNix`, the default lane's, carries one patch
  # for a deadlock of the guests' builders.
  laneNix = {
    inherit (pkgs.nixVersions) nix_2_34 git;
    determinate = determinateNix;
  };
  # A lane's environment and its `nix daemon`, with this repository editable from `root`.
  lanesAt =
    root:
    lib.mapAttrs (lane: nix: {
      devEnv = devEnvFrom (devPythonSetAt lane root);
      inherit nix;
    }) laneNix;

  pyinstance = nixvenv {
    python = pkgs.python3;
    packages = ps: [
      library
      ps.pytest
      # `tests/functional/test_substitute_on_the_engine.py` serves its S3 bucket with it.
      ps.moto
      # The phase scripts in `tests/guest/` import it, and the type gate
      # reads them.
      # `devEnv` gets the same package from its own pyproject root, because
      # the two environments resolve by different machinery: this one is
      # nixpkgs, and that one is pyproject.nix.
      vivarium-runner
    ];
  };

  # **The type gate does not see huggorm, on purpose.** It reads `tests/`,
  # and `tests/differential` imports the oracle, so the imports there carry
  # `type: ignore[import-not-found]`. Putting `huggorm` in this
  # environment would tie a gate that must run everywhere to a C++
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
    runtimeInputs = [ pkgs.ruff ];
    text = ''
      ruff format .
      ruff check --fix .
    '';
  };

  /*
    The third-party packages of pynixd, nix-daemon-protocol and pynixd-store,
    without the three. zuban reads the three from the tree it checks
    (`mypy_path` in each `mypy.ini`), so a changed `.py` file builds no package.
  */
  typeEnv = nixvenv {
    python = pkgs.python3;
    packages =
      ps:
      let
        firstParty = [
          library
          daemon-protocol
          store
        ];
        dependencies = lib.concatMap (package: package.propagatedBuildInputs) firstParty;
      in
      # nixpkgs' anyio and not the fork: vivarium's packages bring nixpkgs' anyio, a venv
      # holds one, and the fork changes no type.
      lib.subtractLists (
        firstParty
        ++ [
          anyio
          kr8s
          httpx
          httpcore
        ]
      ) (lib.filter (package: package ? pythonModule) (lib.unique dependencies))
      ++ [
        ps.anyio
        (ps.callPackage "${sources.nanopynix}/nix/kr8s.nix" { src = sources.kr8s; })
        ps.pytest
        ps.moto
        ps.hypothesis
        vivarium-runner
      ];
  };

  # One zuban run for each `mypy.ini`: nix-daemon-protocol and pynixd-store
  # are distributions of their own, each with its own `src/`. Each run's status
  # counts: a loop alone answers the last one's, and `check` runs this where
  # errexit is off.
  typeCheck = ''
    failed=0
    for project in . nix-daemon-protocol pynixd-store; do
      (cd "$project" && env -u PYTHONPATH zuban mypy --python-executable ${typeEnv}/lib/venv/bin/python) || failed=1
    done
    test "$failed" = 0
  '';

  /*
    `checks.format`, `checks.lint` and `checks.types` against the working tree,
    from `pynixd/`: `nix run --file . internal.check`. It runs each gate and
    exits 1 when any failed. `fix` rewrites what ruff can.
  */
  check = pkgs.writeShellApplication {
    name = "pynixd-check";
    runtimeInputs = [
      pkgs.ruff
      pkgs.zuban
    ];
    text = ''
      status=0
      ruff format --check . || status=1
      ruff check . || status=1
      (${typeCheck}) || status=1
      exit "$status"
    '';
  };

  # One derivation per gate, so a failure names which one, and each fails the
  # build rather than reporting into a log nobody reads.
  mkCheck =
    name: deps: text:
    pkgs.runCommand "pynixd-check-${name}" { nativeBuildInputs = deps; } ''
      cp -r ${checkSrc} src
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
    serve.  See the protocol matrix in nixidae's CLAUDE.md.
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

  nixftLane = pkgs.callPackage ./nix/functional-tests/package.nix {
    nix = nixftNix;
    version = defaultNixName;
    pynixd = package;
    wirelogPython = pyinstance;
  };

  nixFunctionalTests =
    lib.mapAttrs (
      version: nix:
      pkgs.callPackage ./nix/functional-tests/package.nix {
        inherit nix version;
        pynixd = package;
        wirelogPython = pyinstance;
      }
    ) supportedNix
    // {
      # Determinate Nix as the client, the scripts and the control daemon (nixidae/nixidae#1250).
      determinate = pkgs.callPackage ./nix/functional-tests/package.nix {
        nix = determinateNix;
        testSource = sources.nix-determinate;
        version = "determinate";
        pynixd = package;
        wirelogPython = pyinstance;
      };
    };

  # The workflow schema, the steps a job asks for by name, and the writer
  # that turns a workflow value into YAML. It takes `lib` and nothing else,
  # which is what lets this repository use it while pinning its own nixpkgs.
  ghalib = (import sources.ghanix { inherit lib; }).lib;

  # A workflow as a value. `nixVersion` is the Nix this repository pins, and
  # the runner installs that one. ci/workflows/bootstrap.nix says what a
  # second version on the machine cost.
  ciWorkflowValue =
    module:
    import module {
      inherit lib ghalib;
      nixVersion = defaultNixRelease.version;
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
    extended = ciWorkflowValue ./ci/workflows/extended.nix;
    installer = ciWorkflowValue ./ci/workflows/installer.nix;
    gha-cache-probe = ciWorkflowValue ./ci/workflows/gha-cache-probe.nix;
    builder = ciWorkflowValue ./ci/workflows/builder.nix;
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
        # Edit ci/workflows/${name}.nix, then run: nix run --file . internal.ci-workflow-update
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
    # The packaged pynixd's `sys.executable` starts the sandbox helper. A
    # `buildPythonApplication` wrapper failed this: every build on pynixd's
    # own route said `No module named 'pynixd_store'`.
    packagedHelper = pkgs.runCommand "pynixd-check-packaged-helper" { } ''
      ${package.venv}/lib/venv/bin/python -c 'import sys, pynixd_store.builder.child; print(sys.executable)'
      touch $out
    '';
    format = mkCheck "format" [ pkgs.ruff ] "ruff format --check .";
    lint = mkCheck "lint" [ pkgs.ruff ] "ruff check .";
    types = mkCheck "types" [ pkgs.zuban ] typeCheck;
    # Sphinx treats a warning as an error, and the exported repository's Pages
    # run is the only other build: nixidae/nixidae#1319.
    docs = pynixd-docs;

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
                echo "Run: nix run --file . internal.ci-workflow-update" >&2
                exit 1
              fi
            '') ciWorkflows
          )
          + "touch $out\n"
        );
  };
  optionsReference = import ./nix/shared/options-reference.nix { inherit lib pkgs; } {
    title = "NixOS module options";
    inherit
      (pkgs.nixos {
        imports = [ ./nix/nixos ];
        system.stateVersion = lib.trivial.release;
      })
      options
      ;
    prefixes = [ "services.pynixd" ];
    root = ./.;
    repository = "nixidae/pynixd";
  };

  pynixd-docs = pkgs.python3Packages.callPackage ./nix/docs.nix {
    pynixd = library;
    inherit nixvenv optionsReference;
  };

  guestArgs = {
    pynixd-lib = library;
    src = testSrc;
    inherit vivariumLib;
    inherit agent krunVariables;
    devEnv = guestEnv;
    lanes = lanesAt "/work";
    nix = defaultNix;
    nixGit = pkgs.nixVersions.git;
    nixFloor = pkgs.nixVersions."nix_${lib.replaceStrings [ "." ] [ "_" ] supportedNixFloor}";
    # The suites whose lines pynixd names: the guests' Nix, master (`build-remote.sh:113`) and Determinate's
    # (`provenance.sh`).
    nixFunctionalTests = [
      "${defaultNix.src}/tests/functional"
      "${pkgs.nixVersions.git.src}/tests/functional"
      "${sources.nix-determinate}/tests/functional"
    ];
  };

  tests = rec {
    simple = pkgs.callPackage ./tests/derivations/simple {
      pynixd-lib = library;
    };
    pytest = pkgs.callPackage ./tests/derivations/pytest {
      pynixd-lib = library;
      nix = defaultNix;
      inherit nixvenv;
      src = testSrc;
    };

    # The performance benchmarks, in a guest. Not a gate: they print numbers
    # and pass unless the run fails. `nix run --file . internal.tests.benchmark.driver
    # -- --out <abs>`.
    benchmark = withDefaultNix (
      pkgs.callPackage ./tests/derivations/benchmark {
        inherit package;
        src = testSrc;
        inherit vivariumLib;
        devEnv = guestEnv;
      }
    );

    # Each way of writing temporary roots on models of a slow disk, in a QEMU
    # guest. Not a gate on speed. `nix run --file . internal.tests.temp-roots.driver -- --out <abs>`.
    temp-roots = withDefaultNix (
      pkgs.callPackage ./tests/derivations/temp-roots {
        src = testSrc;
        inherit vivariumLib;
        devEnv = guestEnv;
      }
    );

    # A store path's files copied as a NAR stream or by the kernel, on btrfs,
    # XFS and ext4, in a QEMU guest. Not a gate on speed.
    # `nix run --file . internal.tests.copy-offload.driver -- --out <abs>`.
    copy-offload = withDefaultNix (
      pkgs.callPackage ./tests/derivations/copy-offload {
        src = testSrc;
        inherit vivariumLib;
        devEnv = guestEnv;
      }
    );

    # The same suites in a guest, where cleanup is a poweroff rather than
    # a promise.
    guest = withDefaultNix (pkgs.callPackage ./tests/derivations/guest guestArgs);

    # The extended set (`@pytest.mark.extended`) of the suites `tests/suites.toml` flags, for the schedule
    # and by hand before a risky change (nixidae/nixidae#1247).
    guest-extended = withDefaultNix (
      pkgs.callPackage ./tests/derivations/guest (guestArgs // { extended = true; })
    );

    # The Nix functional suite in a UML guest, where cleanup is a poweroff
    # rather than a promise. `nix run --file . internal.tests.guest-nixft.driver
    # -- --out <abs>`. Issue #45.
    guest-nixft = withNixftNix (
      pkgs.callPackage ./tests/derivations/guest-nixft {
        pynixd-lib = library;
        src = testSrc;
        inherit vivariumLib;
        nixft = nixftLane;
      }
    );

    # The same suite with no Nix daemon under pynixd: pynixd opens and writes
    # each test store itself. Issue #1155.
    guest-nixft-no-daemon = withNixftNix (
      pkgs.callPackage ./tests/derivations/guest-nixft {
        pynixd-lib = library;
        src = testSrc;
        inherit vivariumLib;
        nixft = nixftLane;
        noDaemon = true;
      }
    );

    # The same suite with Determinate Nix as the client, the scripts and the control daemon. The guest's own Nix
    # stays the default one: `build-remote.sh` reaches it over `ssh-ng://localhost` (nixidae/nixidae#1250).
    guest-nixft-determinate = withDefaultNix (
      pkgs.callPackage ./tests/derivations/guest-nixft {
        pynixd-lib = library;
        src = testSrc;
        inherit vivariumLib;
        nixft = nixFunctionalTests.determinate;
        name = "nixft-determinate";
      }
    );

    # pynixd as the Nix daemon of a guest, against nix-daemon on another,
    # over the paths a user takes: a local build, `ssh-ng://` and
    # `--builders`.
    daemon = withDefaultNix (
      pkgs.callPackage ./tests/derivations/daemon {
        inherit package;
        inherit vivariumLib;
      }
    );

    # A running machine switched to pynixd and back, in each mode.
    switch = pkgs.callPackage ./tests/derivations/switch {
      inherit package;
      inherit vivariumLib;
      nixVersions = supportedNix;
    };

    # What an upgrade does to builds in flight; docs/design/upgrade-handover.md.
    handover = withDefaultNix (
      pkgs.callPackage ./tests/derivations/handover {
        inherit package;
        inherit vivariumLib;
      }
    );

    # An upgrade by switch hands over to the new pynixd, and the work in flight completes.
    upgrade = withDefaultNix (
      pkgs.callPackage ./tests/derivations/upgrade {
        inherit package;
        inherit vivariumLib;
      }
    );

    # A build no store takes launches a builder through vivarium's cloud API. nixidae/nixidae#1198.
    provisioning = withDefaultNix (
      pkgs.callPackage ./tests/derivations/provisioning {
        inherit package;
        inherit vivariumLib;
        inherit (nanopynix.packages) pynix;
      }
    );

    # A pod on a kubeadm node as a builder: the real provider and API server. nixidae/nixidae#1560.
    provisioning-kubernetes = withDefaultNix (
      pkgs.callPackage ./tests/derivations/provisioning-kubernetes {
        inherit package vivariumLib;
        k8sModule = vivarium.nixosModules.k8s;
      }
    );

    # The EC2 builder's boot path: the real provider, a fake endpoint, a guest with no Nix. nixidae/nixidae#1420.
    provisioning-ec2 = withDefaultNix (
      pkgs.callPackage ./tests/derivations/provisioning-ec2 {
        inherit package vivariumLib;
        tarball = installerTarball;
      }
    );
    provisioning-gha = withDefaultNix (
      pkgs.callPackage ./tests/derivations/provisioning-gha {
        inherit package vivariumLib;
        tarball = installerTarball;
        workflow = ciTestWorkflows.builder;
      }
    );

    # The tarball installed on a guest with no Nix.
    installer = pkgs.callPackage ./tests/derivations/installer {
      inherit package vivariumLib;
      tarball = installerTarball;
    };

    daemonByNix = byNix daemon;
    switchByNix = byNix switch;
  };

  shellWith =
    { devEnv, nix }:
    pkgs.callPackage ./nix/shell.nix {
      inherit agent devEnv nix;
      inherit (vivarium.packages) vivarium;
      nixGit = pkgs.nixVersions.git;
      nixpkgsPath = pkgs.path;
    };
  shell = shellWith {
    devEnv = shellEnv;
    nix = defaultNix;
  };
  # The shell of each lane in `tests/suites.toml`: `NIX_BIN` and huggorm of that lane, for the scheduled workflow.
  laneShells = lib.mapAttrs (_: shellWith) (lanesAt (toString ./.));
in

{
  packages = {
    default = package;
    pynixd = package;
    pynixd-lib = library;
    pynixd-agent = agent;
    nix-daemon-protocol = daemon-protocol;
    pynixd-store = store;
  };
  lib = { };
  nixosModules.default = ./nix/nixos;
  easykubenixModules = { };
  inherit checks;
  docs.default = pynixd-docs;
  inherit shell ciTestWorkflows;

  # Not public: the names may change without notice.
  internal = {
    # `default` is the suite of `defaultNix`, for a command that names no version.
    nixFunctionalTests = nixFunctionalTests // {
      default = nixFunctionalTests.${defaultNixName};
    };
    inherit
      pkgs
      krun
      specifictest
      fix
      check
      ciWorkflows
      ciWorkflowFiles
      ci-workflow-update
      tests
      laneShells
      defaultNix
      darwinVmVariables
      darwinVmEnv
      venv
      krunDarwinVariables
      krunDarwinEnv
      ;
    installer = installerTarball;
  };
}
