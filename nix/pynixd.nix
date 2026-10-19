{
  # nixpkgs
  lib,
  # building
  pythonBuilder,
  hatchling,
  # testing
  pytest,
  # dependencies
  aiohttp,
  pyinstrument,
  aiosqlite,
  environs,
  pynacl,
  passlib,
  cachetools,
  zstandard,
  lz4,
  brotli,
  asyncssh,
  watchdog,
  structlog,
  pydantic,
  pydantic-settings,
  prometheus-client,
  rich,
  anyio,
  uvloop,
  nix-daemon-protocol,
  # System to `pynixd-agent` build (nix/agent.nix).
  agents ? { },
}:
pythonBuilder (finalAttrs: {
  pname = "pynixd";
  version = "0.1.0";
  pyproject = true;

  # A deliberate impurity, for measuring a rebuild without editing the source.
  # Commented out, not deleted: comment it back in when you need it again.
  #
  # Leave it in and the derivation hashes differently on every evaluation. That
  # makes the package unsubstitutable in principle rather than merely uncached
  # -- no `cachix push` can ever satisfy it, because by the time you push, the
  # next evaluation asks for a different path. It also leaks into every
  # consumer: nixkube's `cacheEnv` holds `pynixd-nixkube`, so an easykubenix
  # render that includes it hashes differently between runs, which breaks a
  # byte-identical render gate and makes `ekn deploy` commit on every run with
  # nothing changed.
  # impurity = builtins.currentTime or "";

  # `lib.hasSuffix` takes the suffix first, and these two read the other way
  # round until 2026-09-17. `hasSuffix name "nix"` asks whether the string
  # "nix" ends with the name of the file, which is true for a file called
  # exactly `nix` and for nothing else. So every `.nix` and every `.md` file
  # was in `src`, and an edit to one of them rebuilt this package and
  # everything downstream of it. Measured on the commit that removed the dead
  # asyncssh override: the derivation hash moved for an edit that changed no
  # input of the build.
  src = lib.cleanSourceWith {
    filter =
      name: type:
      lib.cleanSourceFilter name type
      && !lib.hasSuffix ".nix" name
      && !lib.hasSuffix ".md" name
      # lock-subtrees rewrites it at every landing of nixidae, which moved
      # this package, nixkube's cacheEnv and its init image on commits that
      # changed none of their code.
      && baseNameOf name != "flake.lock"
      # The C agent builds in nix/agent.nix.
      && !(type == "directory" && baseNameOf name == "agent");
    src = ../.;
  };

  build-system = [ hatchling ];

  dependencies = [
    asyncssh
    structlog
    aiohttp
    pyinstrument
    aiosqlite
    environs
    pynacl
    passlib
    cachetools
    zstandard
    lz4
    brotli
    pydantic
    pydantic-settings
    prometheus-client
    rich
    anyio
    uvloop
    watchdog
    nix-daemon-protocol
  ];

  nativeCheckInputs = [
    pytest
  ];

  # `PYNIXD_AGENTS` in `pynixd/agent.py`: system to the agent pynixd uploads
  # to an SSH builder. Issue #98.
  makeWrapperArgs = lib.optionals (agents != { }) [
    "--set-default"
    "PYNIXD_AGENTS"
    # Escaped once more: the Python wrapper hook expands these words in a shell.
    (lib.escapeShellArg (builtins.toJSON (lib.mapAttrs (_: agent: lib.getExe agent) agents)))
  ];

  meta = {
    description = "Python Nix daemon protocol proxy over SSH";
    mainProgram = "pynixd";
  };
})
