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

  # What `pyproject.toml` builds from, and nothing else. A file outside this
  # list moves the package, nixkube's cacheEnv and its init image: the
  # flake.lock that lock-subtrees rewrites at every landing did that.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../pyproject.toml
      ../LICENSE
      (lib.fileset.fileFilter (file: file.hasExt "py" || file.name == "py.typed") ../pynixd)
    ];
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
