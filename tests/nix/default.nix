{
  pkgs ? import <nixpkgs> { },
}:

let
  system = builtins.currentSystem;

  ts =
    let
      v = builtins.getEnv "PYNIXD_TEST_TS";
    in
    if v == "" then toString builtins.currentTime else v;

  modes = {
    standard = import ./standard.nix { inherit pkgs system ts; };
    ca = import ./ca.nix { inherit system ts; };
    dyn = import ./dyn-drv.nix { inherit system ts; };
    minimal = import ./minimal.nix { inherit system ts; };
    feat = import ./system_features.nix { inherit system ts; };
    log_test = import ./log_test.nix { inherit system ts; };
  };

in
modes.standard
// {
  inherit (modes) ca;
  inherit (modes) dyn;
  inherit (modes) minimal;
  inherit (modes) feat;
  inherit (modes) log_test;
}
