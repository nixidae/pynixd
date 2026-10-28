# A running NixOS machine moves to pynixd and back, by `switch-to-configuration`.
#
# `nix build --file . tests.switch` boots one guest with nix-daemon alone and
# switches it through the configurations below, the way an operator does
# with `nixos-rebuild switch`. After each switch the same checks run: the
# switch exited 0, no unit failed, the right process holds each socket, and
# a user's build works. A reboot hides every fault this test is for: it
# starts each socket from the new configuration.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  vivarium,
  # Passed to vivarium's lib.nix, so it takes nixvenv from the same nixidae.
  sources,
  # Every supported Nix, by attribute name. Replace mode moves through each
  # and back: a new Nix changes nix-daemon's unit under a running pynixd.
  nixVersions,
}:

let
  vivariumLib = import (vivarium + "/lib.nix") { inherit pkgs lib sources; };

  pynixd = mode: {
    imports = [ ../../../nix/nixos/default.nix ];
    services.pynixd = {
      enable = true;
      inherit mode package;
    };
  };

  # Each step: the configuration to switch to (null is the booted one), and
  # the mode the machine is in after it. In order; each starts from the last.
  steps = [
    {
      name = "stock";
      to = false;
      mode = "stock";
    }
    {
      name = "stock-to-replace";
      to = "replace";
      mode = "replace";
    }
    {
      name = "replace-to-stock";
      to = null;
      mode = "stock";
    }
    {
      name = "stock-to-beside";
      to = "beside";
      mode = "beside";
    }
    {
      name = "beside-to-replace";
      to = "replace";
      mode = "replace";
    }
    {
      name = "replace-to-beside";
      to = "beside";
      mode = "beside";
    }
  ]
  ++ lib.mapAttrsToList (version: nix: {
    name = "to-replace-${version}";
    to = "replace-${version}";
    mode = "replace";
    nix = nix.version;
  }) nixVersions
  ++ [
    {
      name = "back-to-replace";
      to = "replace";
      mode = "replace";
    }
  ];
in
vivariumLib.mkTest {
  name = "pynixd-switch";
  backend = "uml";

  pythonPath = [
    ../../daemon/helpers
    ../../switch
  ];

  settings = {
    busybox = "${pkgs.busybox}";
    system = pkgs.stdenv.hostPlatform.system;
    # `to = false` is the booted system, checked before any switch.
    steps = map (step: {
      inherit (step) name to mode;
      nix = step.nix or null;
    }) steps;
  };

  phases = lib.listToAttrs (
    lib.imap0 (
      index: step:
      lib.nameValuePair step.name {
        script = ../../switch/step.py;
        after = if index == 0 then [ "boot" ] else [ (lib.elemAt steps (index - 1)).name ];
        description = "${step.mode} after ${if step.to == false then "boot" else "a switch"}";
      }
    ) steps
  );

  nodes.machine = {
    vivarium.memory = "1024M";
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };
    environment.systemPackages = [ pkgs.iproute2 ];
    nix.settings = {
      experimental-features = [ "nix-command" ];
      # No network; see tests/derivations/guest for the 75 seconds a
      # substituter query costs without this.
      substituters = lib.mkForce [ ];
    };
    vivarium.configurations = {
      beside = pynixd "beside";
      replace = pynixd "replace";
    }
    // lib.mapAttrs' (
      version: nix:
      lib.nameValuePair "replace-${version}" {
        imports = [ (pynixd "replace") ];
        # Above `tests.switchByNix`'s mkForce on every guest's Nix.
        nix.package = lib.mkOverride 40 nix;
      }
    ) nixVersions;
  };
}
