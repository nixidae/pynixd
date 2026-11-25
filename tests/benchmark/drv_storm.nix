# Many small derivations, for `drv_storm.py`: what a graph with one derivation per compile step asks of a store.
{
  shell,
  nonce,
  count,
  shape,
  inputs,
  # A directory of `inputs` files, for the `tree` shape.
  tree ? null,
}:
let
  files = builtins.genList (i: builtins.toFile "h${toString i}.h" "/* ${toString i} */\n") inputs;
  shapes = {
    # No input but the shell the sandbox brings.
    bare = [ ];
    # One store path per file, as nix-ninja and Nixception do.
    inherit files;
    # Every file in one store path.
    tree = [
      (builtins.path {
        path = tree;
        name = "tree";
      })
    ];
  };
  step =
    index:
    derivation {
      name = "step-${toString index}";
      system = builtins.currentSystem;
      builder = shell;
      args = [
        "-c"
        "echo ${nonce}-${toString index} > $out"
      ];
      sources = shapes.${shape};
      # 2000 store paths are past the 128 KiB that one environment variable may hold.
      passAsFile = [ "sources" ];
      allowSubstitutes = false;
      preferLocalBuild = true;
    };
in
builtins.genList step count
