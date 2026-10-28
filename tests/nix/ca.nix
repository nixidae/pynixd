{ system, ts }:

let
  mkCADrv =
    {
      name,
      buildCommand,
      outputs ? [ "out" ],
      hashAlgo ? "sha256",
      hashMode ? "recursive",
    }:
    derivation {
      inherit name system outputs;
      builder = "/bin/sh";
      args = [
        "-c"
        buildCommand
      ];
      outputHashAlgo = hashAlgo;
      outputHashMode = hashMode;
      __contentAddressed = true;
      _timestamp = ts;
    };

  mkDrv =
    { name, buildCommand }:
    derivation {
      inherit name system;
      builder = "/bin/sh";
      args = [
        "-c"
        buildCommand
      ];
      _timestamp = ts;
    };

  simple = mkCADrv {
    name = "ca-simple";
    buildCommand = "echo ca-content-${ts} > $out";
  };

  multi_output = mkCADrv {
    name = "ca-multi";
    buildCommand = ''
      mkdir -p $out $dev
      echo out-content > $out
      echo dev-content > $dev
    '';
    outputs = [
      "out"
      "dev"
    ];
  };

  depends_on_ca = mkCADrv {
    name = "ca-depends-on-ca";
    buildCommand = "echo dep-on-${simple} > $out";
  };

  non_ca_depends_on_ca = mkDrv {
    name = "non-ca-depends-on-ca";
    buildCommand = "echo dep-on-${simple} > $out";
  };

  text_hashed = mkCADrv {
    name = "ca-text.txt";
    hashMode = "text";
    buildCommand = "echo text-content-${ts} > $out";
  };

  # Fixed-output CA: outputHash is known at evaluation time.
  # The content hash is declared upfront; the daemon verifies it
  # after the build and registers the realisation.
  fixed_ca = derivation {
    name = "ca-fixed";
    inherit system;
    builder = "/bin/sh";
    args = [
      "-c"
      "printf '%s' ca-fixed > $out"
    ];
    outputHashAlgo = "sha256";
    outputHashMode = "text";
    outputHash = builtins.hashString "sha256" "ca-fixed";
    __contentAddressed = true;
    _timestamp = ts;
  };

in
{
  inherit
    simple
    multi_output
    depends_on_ca
    non_ca_depends_on_ca
    text_hashed
    fixed_ca
    ;
}
