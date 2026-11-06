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
  # A build's stall times read its guest cgroup's pressure files, and libkrunfw's
  # x86_64 config alone leaves PSI out (aarch64 and riscv64 set it).
  # nixidae/nixidae#1187.
  kernel = libkrunfw.overrideAttrs (old: {
    postPatch = old.postPatch + ''
      substituteInPlace config-libkrunfw_x86_64 \
        --replace-fail '# CONFIG_PSI is not set' 'CONFIG_PSI=y'
    '';
  });
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
      --prefix LD_LIBRARY_PATH : ${lib.makeLibraryPath [ kernel ]}
    runHook postInstall
  '';
  passthru = { inherit init kernel; };
  meta = {
    description = "A build guest for pynixd, on libkrun";
    mainProgram = "pynixd-krun";
    platforms = lib.platforms.linux;
  };
}
