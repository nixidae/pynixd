{
  lib,
  callPackage,
  nixvenv,
  pynixd,
  optionsReference,
}:
callPackage ./shared/sphinx-site.nix { inherit nixvenv; } {
  name = "pynixd";
  root = ../.;
  # `docs/conf.py` puts the repository on `sys.path`, so autodoc reads
  # `pynixd/` from here.
  files = [ (lib.fileset.fileFilter (file: file.hasExt "py") ../pynixd) ];
  packages = [ pynixd ];
  pages."options.md" = optionsReference;
}
