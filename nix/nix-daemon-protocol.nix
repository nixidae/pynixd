{
  lib,
  pythonBuilder,
  hatchling,
  pydantic,
}:
pythonBuilder (finalAttrs: {
  pname = "nix-daemon-protocol";
  version = "0.1.0";
  pyproject = true;

  src = lib.fileset.toSource {
    root = ../nix-daemon-protocol;
    fileset = lib.fileset.unions [
      ../nix-daemon-protocol/pyproject.toml
      ../nix-daemon-protocol/README.md
      ../nix-daemon-protocol/LICENSE
      (lib.fileset.fileFilter (
        file: file.hasExt "py" || file.name == "py.typed"
      ) ../nix-daemon-protocol/src)
    ];
  };

  build-system = [ hatchling ];
  dependencies = [ pydantic ];

  meta = {
    description = "Declarative Python codecs for the Nix daemon wire protocol";
  };
})
