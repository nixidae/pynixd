# Nix expressions that produce derivations for unit-testing the drv parser.
# Evaluate with: nix eval --impure --file tests/nix/drv-probes.nix --json
# Then read each .drv from the store and parse it.
#
# Each attribute is { name, drvPath, type } describing the derivation.
# type helps the test classify what kind of output to expect.
{
  ## Input-addressed (traditional)
  simple =
    let
      pkgs = import <nixpkgs> { };
    in
    pkgs.runCommand "simple" { } "echo simple > $out";

  ## Input-addressed with multiple outputs
  multi-output =
    let
      pkgs = import <nixpkgs> { };
    in
    pkgs.symlinkJoin {
      name = "multi-out";
      paths = [
        pkgs.bash
        pkgs.coreutils
      ];
    };

  ## CA floating
  ca-floating =
    let
      pkgs = import <nixpkgs> { };
    in
    pkgs.runCommand "ca-floating" {
      __contentAddressed = true;
      outputHashMode = "flat";
      outputHashAlgo = "sha256";
    } "echo ca > $out";

  ## CA fixed (text-hashed)
  ca-fixed =
    let
      pkgs = import <nixpkgs> { };
    in
    pkgs.runCommand "ca-fixed" {
      outputHash = "sha256-47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
      outputHashMode = "flat";
      outputHashAlgo = "sha256";
    } "echo fixed > $out";

  ## Text-hashed (outputHashMode = "text")
  text-hashed =
    let
      pkgs = import <nixpkgs> { };
    in
    pkgs.runCommand "text-hashed" {
      outputHashMode = "text";
      outputHashAlgo = "sha256";
      passAsFile = [ "buildCommand" ];
    } "echo text > $out";

  ## Dynamic derivation (__dynamicDerivation = true)
  dynamic =
    let
      pkgs = import <nixpkgs> { };
    in
    pkgs.runCommand "result.drv" {
      __dynamicDerivation = true;
      outputHashMode = "text";
      outputHashAlgo = "sha256";
      requiredSystemFeatures = [ "recursive-nix" ];
    } "echo dynamic > $out";

  ## With requiredSystemFeatures
  with-features =
    let
      pkgs = import <nixpkgs> { };
    in
    pkgs.runCommand "with-features" {
      requiredSystemFeatures = [
        "kvm"
        "big-parallel"
      ];
    } "echo features > $out";

  ## With empty env (no build inputs, no nothing)
  minimal =
    let
      pkgs = import <nixpkgs> { };
    in
    pkgs.stdenvNoCC.mkDerivation {
      name = "minimal";
      builder = "/bin/sh";
      args = [
        "-c"
        "echo min > $out"
      ];
      system = "x86_64-linux";
    };

  ## Deferred (fixed-output with empty path — rare, occurs with content-addressed .drv files)
  # This is hard to produce directly; the .drv files for CA derivations themselves
  # contain a deferred-like output for the "out" entry.
}
