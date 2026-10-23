{
  lib,
  stdenvNoCC,
  python,
  pynixd,
  sphinx,
  myst-parser,
  furo,
  nixvenv,
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
      (lib.fileset.fileFilter (file: file.hasExt "py") ../pynixd)
    ];
  };

  nativeBuildInputs = [ pythonEnv ];

  buildPhase = ''
    runHook preBuild
    PYNIXD_DOCS_OFFLINE=1 sphinx-build -b html docs "$out" -W
    runHook postBuild
  '';

  dontInstall = true;
}
