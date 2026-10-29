{
  lib,
  stdenvNoCC,
  python,
  pynixd,
  sphinx,
  myst-parser,
  furo,
  nixvenv,
  optionsReference,
}:
let
  pythonEnv = nixvenv {
    inherit python;
    packages = _: [
      pynixd
      sphinx
      myst-parser
      furo
    ];
    executables = [ "sphinx-build" ];
  };
in
stdenvNoCC.mkDerivation {
  pname = "pynixd-docs";
  version = "0";

  # `docs/conf.py` puts the repository on `sys.path`, so autodoc reads
  # `pynixd/` from here.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../docs
      ../nix/shared/sphinx_shared.py
      (lib.fileset.fileFilter (file: file.hasExt "py") ../pynixd)
    ];
  };

  nativeBuildInputs = [ pythonEnv ];

  buildPhase = ''
    runHook preBuild
    cp ${optionsReference} docs/options.md
    NIXIDAE_DOCS_OFFLINE=1 sphinx-build -b html docs "$out" -W
    runHook postBuild
  '';

  dontInstall = true;
}
