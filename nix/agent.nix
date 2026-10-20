/**
  `pynixd-agent`, the host sampler pynixd runs on a builder and on its own
  host. Issue #98.

  Call it from `pkgsCross.musl64` (or the musl cross set of another system):
  Rust links musl statically by default, so the binary has no store
  references and runs on a builder of any distribution from a plain upload.
  The compiler is the ordinary cached rustc; only its musl standard library
  and linker wrapper come from the cross set. Static glibc is the other way
  to the same result: 1.18 MB against musl's 424 KB.
*/
{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage {
  pname = "pynixd-agent";
  version = "1.0.0";
  src = lib.fileset.toSource {
    root = ../agent;
    fileset = lib.fileset.unions [
      ../agent/Cargo.toml
      ../agent/Cargo.lock
      ../agent/src
    ];
  };
  cargoLock.lockFile = ../agent/Cargo.lock;
  # nixpkgs links Rust on musl dynamically unless told otherwise.
  env.RUSTFLAGS = "-C target-feature=+crt-static -C relocation-model=static";
  doCheck = false;
  # The default strips debug sections only; symbols are a third of the file.
  stripAllList = [ "bin" ];
  allowedReferences = [ ];
  meta.mainProgram = "pynixd-agent";
}
