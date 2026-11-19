# How a store path's files reach another store on this machine: as a NAR
# stream, or file by file with the kernel's copy, on btrfs, XFS and ext4.
#
# A QEMU guest with one disk per filesystem. `tests/benchmark/copy_offload.py`
# does the measuring, and runs by hand on any directory too. Not a gate on
# speed: a phase fails when a copy's NAR hash differs from its source's, or a
# clone on btrfs or XFS does not share its extents.
# `nix run --file . internal.tests.copy-offload.driver -- --out <abs>`.
{
  vivariumLib,
  src,
  devEnv,
  pkgs,
}:

vivariumLib.mkTest {
  name = "pynixd-copy-offload";
  backend = "qemu";

  defaults = vivariumLib.settingsClosure;
  settings.src = "${src}";

  nodes.machine = {
    vivarium.memory = "2048M";
    vivarium.disks =
      map
        (name: {
          inherit name;
          size = 4096;
        })
        [
          "btrfs"
          "xfs"
          "ext4"
        ];
    environment.systemPackages = [
      devEnv
      pkgs.btrfs-progs
      pkgs.xfsprogs
      pkgs.e2fsprogs
    ];
  };

  phases =
    let
      script = ../../benchmark/copy_offload_phase.py;
    in
    {
      disks = {
        inherit script;
        after = [ "boot" ];
        description = "btrfs, XFS with reflinks, and ext4, one disk each";
      };
      btrfs = {
        inherit script;
        after = [ "disks" ];
        description = "both stores on btrfs";
      };
      xfs = {
        inherit script;
        after = [ "btrfs" ];
        description = "both stores on XFS";
      };
      ext4 = {
        inherit script;
        after = [ "xfs" ];
        description = "both stores on ext4, which has no reflinks";
      };
      cross = {
        inherit script;
        after = [ "ext4" ];
        description = "the source on ext4, the target on btrfs";
      };
    };
}
