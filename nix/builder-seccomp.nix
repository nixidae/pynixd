/**
  The seccomp profile of a builder pod's sandbox container, as a Kubernetes `Localhost` profile.
  nixidae/nixidae#1560, spike.

  It allows every call. pynixd's sandbox helper installs Nix's own filter in the build (setuid and
  setgid bits, extended attributes, `no_new_privs`), and filters stack, so the build reaches exactly
  what it reaches under Nix's sandbox on a host. A profile that refuses more makes the build differ
  from Nix's: the sandbox probe measured 9 such calls under Docker's default profile, among them the
  keyrings, the NUMA memory policy and `perf_event_open`.

  A `Localhost` profile, not `Unconfined`: the Pod Security `restricted` level admits
  `RuntimeDefault` and `Localhost` only, and reads no rule of the profile. Install it under the
  kubelet's `seccomp` directory on each node.
*/
{ pkgs }:
pkgs.writeText "pynixd-builder-seccomp.json" (
  builtins.toJSON {
    defaultAction = "SCMP_ACT_ALLOW";
    architectures = [
      "SCMP_ARCH_X86_64"
      "SCMP_ARCH_X86"
      "SCMP_ARCH_X32"
      "SCMP_ARCH_AARCH64"
      "SCMP_ARCH_ARM"
    ];
    syscalls = [ ];
  }
)
