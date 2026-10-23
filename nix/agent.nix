/**
  `pynixd-agent`, the host sampler pynixd runs on a builder and on its own
  host. Issue #98.

  Call it from `pkgsStatic`, the host system's static musl set: Rust links
  musl statically, so the binary has no store references and runs on a
  builder of any distribution from a plain upload. The binary cache has
  that set's rustc and linker wrapper for x86_64-linux and aarch64-linux
  (measured 2026-10-23); `pkgsCross.musl64` has them only on an x86_64
  host, and builds an x86_64 agent everywhere. Static glibc is the other way
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
