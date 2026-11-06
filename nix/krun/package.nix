/*
  pynixd-krun and its guest init: a build in a libkrun microVM
  (nixidae/nixidae#1186).

  libkrun loads libkrunfw, the guest kernel, with `dlopen` by name, so the
  wrapper puts libkrunfw on the library path. The init is static: the root
  it runs from holds no other file.
*/
{
  lib,
  stdenv,
  pkgsStatic,
  libkrun,
  libkrunfw,
  makeBinaryWrapper,
}:
let
  init = pkgsStatic.stdenv.mkDerivation {
    name = "pynixd-krun-init";
    src = ./init.c;
    dontUnpack = true;
    buildPhase = ''
      runHook preBuild
      $CC -O2 -Wall -Wextra -Werror -static -o init $src
      runHook postBuild
    '';
    installPhase = ''
      runHook preInstall
      install -D init $out/bin/pynixd-krun-init
      runHook postInstall
    '';
  };
in
stdenv.mkDerivation {
  name = "pynixd-krun";
  src = ./pynixd-krun.c;
  dontUnpack = true;
  nativeBuildInputs = [ makeBinaryWrapper ];
  buildInputs = [ libkrun ];
  buildPhase = ''
    runHook preBuild
    $CC -O2 -Wall -Wextra -Werror -o pynixd-krun $src -lkrun
    runHook postBuild
  '';
  installPhase = ''
    runHook preInstall
    install -D pynixd-krun $out/libexec/pynixd-krun
    makeBinaryWrapper $out/libexec/pynixd-krun $out/bin/pynixd-krun \
      --prefix LD_LIBRARY_PATH : ${lib.makeLibraryPath [ libkrunfw ]}
    runHook postInstall
  '';
  passthru = { inherit init; };
  meta = {
    description = "A build guest for pynixd, on libkrun";
    mainProgram = "pynixd-krun";
    platforms = lib.platforms.linux;
  };
}
