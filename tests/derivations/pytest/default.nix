{ pkgs
, pynixd-lib
, src
, nixvenv
, system ? builtins.currentSystem
}:

let
  pytestEnv = nixvenv {
    python = pkgs.python3;
    packages = ps: [
      pynixd-lib
      ps.pytest
      ps.pytest-timeout
      ps.pyinstrument
    ];
  };
in
pkgs.runCommand "pynixd-pytest"
  {
    # No `requiredSystemFeatures = [ "recursive-nix" ]`. Nothing schedules a
    # build that asks for a feature no machine advertises, so this waited for
    # a builder that does not exist -- measured, one such build sat for 5h40m
    # on one second of CPU. recursive-nix belongs to the inner daemon, which
    # the builder below configures for itself in $NIX_CONF_DIR.
    __noSandbox = true;
    allowSubstitutes = false;
    buildInputs = [
      pkgs.nix
      pkgs.openssh
      pkgs.bash
      pytestEnv
    ];
  }
  ''
    export HOME=$(mktemp -d)

    # Configure inner Nix daemon to disable sandboxing.
    mkdir -p $HOME/nix-config
    cat > $HOME/nix-config/nix.conf <<EOF
    sandbox = false
    experimental-features = nix-command recursive-nix
    EOF
    export NIX_CONF_DIR=$HOME/nix-config

    # Make nixpkgs available for test expressions that use <nixpkgs>
    export NIX_PATH="nixpkgs=${pkgs.path}"

    export NIX_BIN=${pkgs.nix}/bin/nix

    # Copy source to a writable directory
    cp -r ${src} $HOME/src
    chmod -R +w $HOME/src
    cd $HOME/src

    # `python -m pytest`, so 'import tests.*' resolves from the project root.
    # Run unit tests and functional tests that work inside a derivation.
    python -m pytest -p no:cacheprovider --timeout=120 --tb=short \
      --ignore=tests/unit/test_drv_parser.py \
      tests/unit/ \
      tests/functional/test_add_to_store_nar.py \
      tests/functional/test_collect_garbage.py \
      tests/functional/test_scheduler_logic.py

    # The protocol suite, as a second process rather than a third path above.
    # `tests/unit` and this suite interfere: four of these tests pass alone
    # and fail beside the pynixd suites. One process for both is how 57
    # failures hid behind a green run of 684, and one of them was a shipped
    # regression. Issue #33.
    #
    # It imports `nix_daemon_protocol` from the environment above, which is
    # the built package, so this asserts what ships and not the tree.
    python -m pytest -p no:cacheprovider --timeout=120 --tb=short nix-daemon-protocol/tests

    echo "All tests passed" > $out
  ''
