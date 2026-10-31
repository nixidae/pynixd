# What every job of every workflow here needs before it does anything of its
# own, as the `ghanix` options a job asks for by name.
#
# A file of its own, and one file, because nixkube learned that with two: its
# issue #33 was two workflows carrying two copies of the substituters, and
# nothing that made them agree. pynixd has one workflow today. The file is
# here so that the second one cannot start the same way.
{ lib, nixVersion }:
rec {
  /*
    The Nix every job runs with.

    cachix/install-nix-action, and not nixbuild/nix-quick-install-action,
    which is what this repository used until now. The difference is a daemon.

    **The suite is written against a machine that has one.** Fifteen tests
    talk to the store of the machine through
    `/nix/var/nix/daemon-socket/socket`, and a single-user install has no
    such socket, so they answered `System Nix daemon socket does not exist`
    and `cannot connect to socket` in run 35195978356 and in no run on a
    developer machine. A hand-started `nix daemon` covered that, and this
    covers it by installing the shape the tests assume.

    It is also what `trusted-users` is for. A daemon refuses
    `BuildDerivation` to a client outside that list, with `you are not
    privileged to build input-addressed derivations`, and 149 tests failed
    at their fixture for that reason. Issue #47.

    **And it installs the Nix this repository pins, not the newest one.**
    The suite assumes one version of Nix on the machine: the client is
    `NIX_BIN`, the daemons the tests spawn are `NIX_BIN`, and an
    `ssh-ng://` peer is whatever `nix-daemon --stdio` the login PATH
    starts. Those agree on a developer machine by construction. The action
    installed 2.35.2 beside a 2.34.8 client, and sixteen tests answered

        error: the daemon is missing the 'realisation-with-path-not-hash'
        protocol feature, needed to support content-addressing derivations

    which `RemoteStore::queryRealisation` raises at
    `remote-store.cc:528` and which exists in no 2.34 source. It reads as a
    defect of pynixd and it is a fact about two versions of Nix.
  */
  experimentalFeatures = [
    "nix-command"
    "flakes"
    "read-only-local-store"
    "ca-derivations"
    "dynamic-derivations"
    "recursive-nix"
  ];

  settings = {
    trusted-users = [
      "root"
      "runner"
    ];
    # lillecarl is the cache this repository pushes docs to. nixkube's is
    # here because the test suite names it as a substituter of its own, in
    # `tests/_conftest/nix_config.py`, so a job that cannot read it builds
    # from source what a cache already holds.
    trusted-public-keys = [
      "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
      "nixkube.cachix.org-1:H8UE0jlI9pxHexK/NhDmEoLDarJXp1WTymQrsajlh7M="
      "lillecarl.cachix.org-1:NN/LLMg7mbyvZCu32Qlo8LpSHqNw7Rr3VBCEYQvRpT0="
    ];
    substituters = [
      "https://cache.nixos.org?priority=1"
      "https://nixkube.cachix.org?priority=2"
      "https://lillecarl.cachix.org?priority=3"
    ];
  };

  # A checkout and that Nix. `job.ghanix` is ghanix's own and is stripped
  # before the YAML is written; each option enabled here puts a step at the
  # front of the job's `steps`, ahead of everything the job wrote itself.
  bootstrap = {
    checkout.enable = true;
    nix.install = {
      enable = true;
      version = nixVersion;
      inherit experimentalFeatures settings;
    };
  };

  /*
    What a job that builds in a store of its own needs on top.

    **A store Nix has diverted builds in a chroot, or it builds into the
    wrong store.** nix 2.34.8 `derivation-builder.cc:2111` forces the sandbox
    on when `storeDir != realStoreDir`, and `:2120` turns it back off, with
    `debug()` and nothing else, when the kernel namespaces are missing. The
    builder then writes to the store of the machine while Nix looks under the
    diverted directory. Ubuntu 24.04 denies an unprivileged user namespace,
    so every capability probe of every session store answered with no system
    and 39 tests failed with `No compatible store for x86_64-linux`.

    `mkMerge` and not `//`, which is shallow: an addition under `nix` would
    otherwise replace the whole install block and take the substituters with
    it, silently.
  */
  divertedStores = lib.mkMerge [
    bootstrap
    { userNamespaces.enable = true; }
  ];
}
