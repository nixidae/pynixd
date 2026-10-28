{ system, ts }:

let
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

  mkCADrv =
    {
      name,
      buildCommand,
      hashMode ? "recursive",
    }:
    derivation {
      inherit name system;
      builder = "/bin/sh";
      args = [
        "-c"
        buildCommand
      ];
      outputHashAlgo = "sha256";
      outputHashMode = hashMode;
      __contentAddressed = true;
      _timestamp = ts;
    };

  hello = mkDrv {
    name = "hello";
    buildCommand = "echo hello > $out";
  };

  producingDrv = mkCADrv {
    name = "hello.drv";
    hashMode = "text";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${builtins.unsafeDiscardOutputDependency hello.drvPath}" > $out
    '';
  };

  indirectHello = builtins.outputOf producingDrv.outPath "out";

  wrapper = mkDrv {
    name = "use-dynamic-drv";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${indirectHello}" > $out
    '';
  };

  # A produced derivation must carry its producer's name without `.drv`.
  deferredTarget = mkDrv {
    name = "deferred-producer";
    buildCommand = "echo hello > $out";
  };

  deferredProducer = mkCADrv {
    name = "deferred-producer.drv";
    hashMode = "text";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        : "$line"
      done < "${caFloat.outPath}"
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${builtins.unsafeDiscardOutputDependency deferredTarget.drvPath}" > $out
    '';
  };

  deferredWrapper = mkDrv {
    name = "use-deferred-producer";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${builtins.outputOf deferredProducer.outPath "out"}" > $out
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${hello.outPath}" >> $out
    '';
  };

  # ── Deep chain: 5 layers of nested outputOf ──────────────────
  #
  # builtins.outputOf wraps its first argument in a
  # SingleDerivedPath::Built, creating a nested struct that can be
  # chained arbitrarily deep.  At instantiation time this produces
  # a DownstreamPlaceholder string embedded in the build env.
  #
  # The outermost wrapper's .drv gets a dynamic_input_drvs entry
  # with 5 levels of childMap nesting:
  #   producer!out!out!out!out!out = target!out

  target = mkDrv {
    name = "target";
    buildCommand = "printf '%s' deep-target > $out";
  };

  producer = mkCADrv {
    name = "target.drv";
    hashMode = "text";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${builtins.unsafeDiscardOutputDependency target.drvPath}" > $out
    '';
  };

  # Chain 5 levels deep.  Each outputOf wraps the previous in
  # another SingleDerivedPath::Built{drvPath=prev, output="out"}.
  fiveDeep = builtins.outputOf (builtins.outputOf (builtins.outputOf (builtins.outputOf (builtins.outputOf producer.outPath "out") "out") "out") "out") "out";

  deepWrapper = mkDrv {
    name = "deep-5";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${fiveDeep}" > $out
    '';
  };

  # ── Crazy: mixed dependency types ──────────────────────────
  #
  # A single derivation that depends on:
  # - Regular (input_addressed) derivations
  # - CA floating derivations
  # - Dynamic derivations at different chain depths
  # - Multiple dynamic_input_drvs entries simultaneously
  #
  # This stress-tests the resolution pipeline's ability to handle
  # heterogeneous dependency graphs in one .drv file.

  baseA = mkDrv {
    name = "base-a";
    buildCommand = "printf '%s' aaa > $out";
  };

  baseB = mkDrv {
    name = "base-b";
    buildCommand = "printf '%s' bbb > $out";
  };

  caFloat = mkCADrv {
    name = "ca-float";
    buildCommand = "printf '%s' float > $out";
  };

  producerA = mkCADrv {
    name = "a.drv";
    hashMode = "text";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${builtins.unsafeDiscardOutputDependency baseA.drvPath}" > $out
    '';
  };

  producerB = mkCADrv {
    name = "b.drv";
    hashMode = "text";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${builtins.unsafeDiscardOutputDependency baseB.drvPath}" > $out
    '';
  };

  # Dynamic references at different depths
  refA = builtins.outputOf producerA.outPath "out";
  refB = builtins.outputOf refA "out"; # 2 levels deep
  refC = builtins.outputOf producerB.outPath "out"; # 1 level deep

  crazy = mkDrv {
    name = "crazy";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${refB}" >> $out
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${refC}" >> $out
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${caFloat.outPath}" >> $out
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${baseA.outPath}" >> $out
    '';
  };

  # ── Deferred + Dynamic (boundary test) ────────────────────
  #
  # A deferred derivation that depends on both a CA floating
  # derivation AND a dynamic chain.  This exercises _resolve_deferred
  # with mixed input_drvs + dynamic_input_drvs.
  #
  # KNOWN LIMITATION: Neither Nix 2.34.7 nor pynixd's goal system
  # can resolve this without the trampoline.  The .drv extension
  # constraint in BuildDerivation (a Nix data structure primitive)
  # prevents the DynamicBuildGoal from wrapping text-hashed CA
  # outputs whose names don't end in .drv.
  #
  # Nix build output:
  #   error: ... is not a valid derivation path

  caFloat2 = mkCADrv {
    name = "ca-float-2";
    buildCommand = "printf '%s' float2 > $out";
  };

  deferredDyn = mkDrv {
    name = "deferred-dyn";
    buildCommand = ''
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${caFloat2}" > $out
      while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
      done < "${refB}" >> $out
    '';
  };

in
{
  inherit
    hello
    producingDrv
    wrapper
    deferredProducer
    deferredWrapper
    target
    producer
    deepWrapper
    baseA
    baseB
    caFloat
    producerA
    producerB
    refA
    refB
    refC
    crazy
    caFloat2
    deferredDyn
    ;
}
