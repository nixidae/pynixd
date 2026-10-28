{
  lib,
  mkShell,
  zuban,
  ruff,
  sqlite,
  nix,
  # The interpreter, resolved by pyproject.nix. `default.nix` says why it is
  # one environment and not several: the oracle of `tests/differential`
  # reaches the nanopynix bindings, and a closure that deep does not survive
  # being assembled on `PYTHONPATH`. It installs pynixd and
  # nix-daemon-protocol editable, so an edit reaches the next command.
  devEnv,
  # `pkgs.path`, for `NIX_PATH`. See the shell hook.
  nixpkgsPath,
  # `pynixd-agent`, which `tests/unit/test_agent.py` runs. Issue #98.
  agent,
}:
mkShell {
  packages = [
    devEnv
    # **The client of the tests and their backend must be one version of Nix.**
    # `NIX_BIN` below pins the daemon the tests spawn, and the `nix` a test
    # runs as a client came off the PATH of the machine. Both are 2.34.8 on a
    # developer machine, so they agreed by accident.
    #
    # A GitHub runner installs 2.35.2, which asks for
    # `realisation-with-path-not-hash`. pynixd refuses it correctly, because
    # `honourable_features` claims nothing its backend cannot read, and 16
    # content-addressed tests then failed against a proxy that was right.
    # A message about the machine, not about pynixd -- the same class as the
    # `NIX_PATH` note below.
    nix
    agent
    zuban
    ruff
    sqlite
  ];
  shellHook = ''
    unset PYTHONPATH
    export NIX_BIN=${lib.getExe nix}

    # **The nixpkgs of this repository, and not the one the host happens to
    # have.** `tests/nix/drv-probes.nix` imports `<nixpkgs>` at eight places,
    # and the shell used to take whatever `NIX_PATH` the machine set. A NixOS
    # host sets one, a GitHub runner sets none, and 21 tests failed there on
    # `file 'nixpkgs' was not found in the Nix search path` -- a message about
    # the machine and not about pynixd.
    #
    # `tests/derivations/pytest` and `tests/derivations/guest` already pin it
    # this way. This is the third consumer, and the one a person uses.
    export NIX_PATH="nixpkgs=${nixpkgsPath}"
  '';
}
