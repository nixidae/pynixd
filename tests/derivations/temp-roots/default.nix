# How each way of writing temporary roots treats the event loop, on models of a slow disk.
#
# A QEMU guest, because each model needs a block device of its own: `fast` is a
# disk straight through, `slow` the same disk behind dm-delay, and `frozen` the
# fast one under `fsfreeze`. `tests/benchmark/temp_roots.py` does the measuring,
# and runs by hand on any directory too.
#
# Not a gate on speed: a phase fails only when its model does not reproduce the
# fault (inline must stall the loop there) or a roots file is wrong.
# `nix run --file . internal.tests.temp-roots.driver -- --out <abs>`.
{
  vivariumLib,
  src,
  devEnv,
  pkgs,
}:

vivariumLib.mkTest {
  name = "pynixd-temp-roots";
  backend = "qemu";

  defaults = vivariumLib.settingsClosure;
  settings.src = "${src}";

  nodes.machine = {
    vivarium.memory = "2048M";
    vivarium.disks = [
      {
        name = "bench";
        size = 4096;
      }
    ];
    boot.kernelModules = [ "dm_delay" ];
    environment.systemPackages = [
      devEnv
      pkgs.lvm2
      pkgs.e2fsprogs
      pkgs.stress-ng
    ];
  };

  phases =
    let
      script = ../../benchmark/temp_roots_phase.py;
    in
    {
      disks = {
        inherit script;
        after = [ "boot" ];
        description = "a fast and a slow filesystem on one disk";
      };
      writer = {
        inherit script;
        after = [ "disks" ];
        description = "the candidate writer's edge cases, on asyncio and uvloop";
      };
      fast = {
        inherit script;
        after = [ "writer" ];
        description = "temp roots on a fast disk";
      };
      frozen = {
        inherit script;
        after = [ "fast" ];
        description = "temp roots while the filesystem is frozen";
      };
      dirty = {
        inherit script;
        after = [ "frozen" ];
        description = "temp roots on a slow disk with dirty pages throttled";
      };
      # sg1-n3 when pynixd-0 stalled: dirty bytes low, memory PSI full 0.64-0.84 s/s,
      # IO PSI full 0.55-0.80 s/s, 31 containers OOM-killed (solidk, 2026-11-19).
      pressure = {
        inherit script;
        after = [ "dirty" ];
        description = "temp roots on a slow disk while memory is nearly gone";
      };
    };
}
