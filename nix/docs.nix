{
  lib,
  stdenvNoCC,
  python,
  pynixd,
  sphinx,
  myst-parser,
  furo,
}:
let
  pythonEnv = python.withPackages (
    _: pynixd.dependencies ++ [
      pynixd
      sphinx
      myst-parser
      furo
    ]
  );
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
