# The performance benchmarks, in a guest.
#
# One ordinary NixOS machine running pynixd as its daemon (`replace` mode, as
# `tests/switch` does), so the stock nix-daemon sits behind it on
# `daemon-socket/upstream`. A client reaches either with `NIX_REMOTE`, and the
# two front the same store, which is what the C++ comparison wants: the same
# work, one daemon against the other.
#
# Not a gate: a benchmark prints numbers and passes unless the run fails.
# `nix run --file . tests.benchmark.driver -- --out <abs>`.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  src,
  vivarium,
  # Passed to vivarium's lib.nix, so it takes nixvenv from the same nixidae.
  sources,
  devEnv,
}:

let
  vivariumLib = import (vivarium + "/lib.nix") { inherit pkgs lib sources; };
in
vivariumLib.mkTest {
  name = "pynixd-benchmark";
  # A container shares the host's CPU, so the numbers are comparable and no
  # guest pays for a kernel of its own.
  backend = "container";

  settings = {
    src = "${src}";
    # `tests/benchmark/system.nix` imports <nixpkgs>, and the phase names this
    # as NIX_PATH. In `settings`, the guest's Nix database covers it.
    nixpkgs = "${pkgs.path}";
    # `replace` mode: pynixd takes `socket`, nix-daemon moves to `upstream`
    # (`nix/common.nix`).
    socket = "/nix/var/nix/daemon-socket/socket";
    upstream = "/nix/var/nix/daemon-socket/upstream";
  };

  nodes.machine = {
    imports = [ ../../../nix/nixos/default.nix ];
    vivarium.memory = "4096M";

    services.pynixd = {
      enable = true;
      mode = "replace";
      inherit package;
      # The `system` phase reads pynixd's own `client_op_timing` line, which is
      # logged at session close and at `info`. `log_level` defaults to
      # `WARNING`, so without this the op breakdown is always empty.
      settings.log_level = "info";
    };

    # The builds run as a user. Root's `nix` opens the store directly and
    # reaches neither daemon, so it could never measure one.
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };

    # The decode benchmark needs Python and the tree.
    environment.systemPackages = [ devEnv ];

    nix.settings.experimental-features = [
      "nix-command"
      "flakes"
      # `system.nix` carries impure derivations, which force the daemon to
      # build rather than substitute. They need the feature itself and
      # `ca-derivations` under it.
      "ca-derivations"
      "impure-derivations"
    ];
  };

  phases = {
    decode = {
      script = ../../benchmark/run.py;
      after = [ "boot" ];
      description = "wire decode throughput, in a guest";
    };
    raw = {
      script = ../../benchmark/run.py;
      # After the profile phase, not only after boot: the profile phase
      # stops `pynixd.socket` to take the socket itself, and hands it back
      # in its `finally`. Alphabetical order used to provide this; the edge
      # states it, so a rename cannot break the handoff.
      after = [ "boot" "profile" ];
      description = "IsValidPath and AddTempRoot through each daemon";
    };
    profile = {
      script = ../../benchmark/run.py;
      after = [ "boot" ];
      description = "pyinstrument of pynixd under a raw pump";
    };
    system = {
      script = ../../benchmark/run.py;
      after = [ "boot" ];
      description = "a system build through each daemon";
    };
    storm = {
      script = ../../benchmark/run.py;
      after = [ "boot" ];
      description = "parallel clients through each daemon";
    };
    growth = {
      script = ../../benchmark/run.py;
      after = [ "boot" ];
      description = "one impure build, repeated; nothing may grow";
    };
  };
}
