# The pynixd tarball on a machine with no Nix: nixidae/nixidae#1365.
#
# One guest with `nix.enable = false`, so it has no Nix database, no daemon
# and no `nix` on PATH, as a CI runner before an install step. The phase
# unpacks the tarball into /, runs its installer and does a user's work
# through pynixd. QEMU, because a runner is a VM with systemd as PID 1.
{
  pkgs,
  lib ? pkgs.lib,
  package,
  tarball,
  vivariumLib,
}:

vivariumLib.mkTest {
  name = "pynixd-installer";
  backend = "qemu";

  defaults = vivariumLib.settingsClosure;
  settings = {
    tarball = "${tarball}/${tarball.fileName}";
    python = "${package.venv}/lib/venv/bin/python";
    system = pkgs.stdenv.hostPlatform.system;
  };

  phases.install = {
    script = ../../installer/install.py;
    after = [ "boot" ];
    description = "unpack, install, and a user's build, copy, delete and substitution through pynixd";
  };

  nodes.runner = {
    vivarium.memory = "2048M";
    # The store's writes land on the root disk, and the unpacked closure is
    # 650 MB: at the default size, tar failed with ENOSPC.
    vivarium.diskSize = 4096;
    nix.enable = false;
    vivarium.nixDatabase.enable = false;
    environment.systemPackages = [
      pkgs.zstd
      pkgs.gnutar
    ];
    users.users.tester = {
      isNormalUser = true;
      uid = 1000;
    };
  };
}
