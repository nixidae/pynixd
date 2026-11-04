{
  lib,
  pythonBuilder,
  hatchling,
  aiosqlite,
  anyio,
  pynacl,
  seccomp,
  structlog,
  nix-daemon-protocol,
}:
pythonBuilder (finalAttrs: {
  pname = "pynixd-store";
  version = "0.1.0";
  pyproject = true;

  src = lib.fileset.toSource {
    root = ../pynixd-store;
    fileset = lib.fileset.unions [
      ../pynixd-store/pyproject.toml
      ../pynixd-store/README.md
      ../pynixd-store/LICENSE
      (lib.fileset.fileFilter (file: file.hasExt "py" || file.name == "py.typed") ../pynixd-store/src)
    ];
  };

  build-system = [ hatchling ];
  dependencies = [
    aiosqlite
    anyio
    pynacl
    seccomp
    structlog
    nix-daemon-protocol
  ];

  meta = {
    description = "A local Nix store in Python: the file system, the database, the roots";
  };
})
