# The public surface of this repository, for a consumer who uses flakes.
#
# **This is not how the repository builds.** `default.nix` is, and nixidae
# (https://github.com/nixidae/nixidae), which this repository is exported
# from, hands it every source. This file exists because flakes have the
# market share: it lets somebody write an input for this repository and get a
# curated set of outputs, rather than nothing.
#
# So it holds no logic. It names what is public and calls `default.nix`, and
# a change to how anything is built happens there.
#
# Two inputs, and neither duplicates a pin nixidae keeps.
#
#   nixpkgs   The consumer's, and the point of the exercise. It is handed to
#             nixidae in place of the revision its flake.lock names, so
#             `inputs.<this>.inputs.nixpkgs.follows = "nixpkgs"` does what a
#             flake user expects it to.
#
#   nixidae   Which nixidae commit, and nothing else. `flake.lock` pins it,
#             and lock-subtrees writes that lock. Every other source comes
#             from that commit's own flake.lock.
{
  description = "The Nix daemon protocol in Python";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    nixidae = {
      url = "github:nixidae/nixidae";
      flake = false;
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      nixidae,
    }:
    let
      inherit (nixpkgs) lib;
      forAllSystems = lib.genAttrs lib.systems.flakeExposed;

      # nixidae's own set, with the consumer's nixpkgs in place of the one it
      # names.
      sources = import "${nixidae}/nix/wire.nix" {
        overrides.nixpkgs = nixpkgs.outPath;
      };

      each = forAllSystems (
        system:
        import ./. {
          inherit sources;
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };
        }
      );
    in
    {
      packages = forAllSystems (system: {
        inherit (each.${system}.packages) default pynixd nix-daemon-protocol;
        libpynixd = each.${system}.packages.pynixd-lib;
        pynixd-docs = each.${system}.docs.default;
      });

      devShells = forAllSystems (system: {
        default = each.${system}.shell;
      });

      nixosModules.default = ./nix/nixos;
    };
}
