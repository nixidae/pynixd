/**
  The seccomp profile of a builder pod's sandbox container, as a Kubernetes `Localhost` profile.
  nixidae/nixidae#1560, spike.

  Docker's default profile, the one containerd and CRI-O follow, with every rule a capability gates
  removed and the calls that pynixd's sandbox helper makes in its own user namespace added. Install it
  under the kubelet's `seccomp` directory on each node.

  The output is plain OCI JSON. containerd reads a `Localhost` profile into the OCI struct
  (`contrib/seccomp/seccomp.go:40`) and drops Docker's `includes`, `excludes` and `archMap` without an
  error: Docker's file as it is would allow every call gated on CAP_SYS_ADMIN.
*/
{ pkgs }:
let
  docker = "${pkgs.docker.moby-src}/vendor/github.com/moby/profiles/seccomp/default.json";
  # `child.py`: clone with namespace flags, the namespaces, the new root and its names.
  sandbox = [
    "clone"
    "clone3"
    "unshare"
    "mount"
    "umount"
    "umount2"
    "pivot_root"
    "chroot"
    "sethostname"
    "setdomainname"
  ];
in
pkgs.runCommand "pynixd-builder-seccomp.json"
  {
    nativeBuildInputs = [ pkgs.jq ];
    sandbox = builtins.toJSON sandbox;
  }
  ''
    jq --argjson sandbox "$sandbox" '
      def ours: IN("amd64", "x86", "x32", "arm", "arm64");
      def kept:
        (.includes.caps // []) == []
        and ((.includes.arches // ["amd64"]) | any(ours))
        # Docker allows clone without namespace flags only, and refuses clone3: `sandbox` replaces both.
        and (.names | any(IN("clone", "clone3")) | not);
      {
        defaultAction,
        defaultErrnoRet,
        architectures: ["SCMP_ARCH_X86_64", "SCMP_ARCH_X86", "SCMP_ARCH_X32", "SCMP_ARCH_AARCH64", "SCMP_ARCH_ARM"],
        syscalls: ([.syscalls[] | select(kept) | del(.includes, .excludes, .comment)]
          + [{names: $sandbox, action: "SCMP_ACT_ALLOW"}])
      }' ${docker} > $out
  ''
