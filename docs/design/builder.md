# The builder: what pynixd takes from Nix's local builder

Status: accepted by User, 2026-11-04, with pynixd's own helper as the child.
It is row 6 of `daemonless.md` (op 36, nixidae/nixidae#1155). Nix 2.35.3 is
the reference; paths are under `src/libstore/`.

## What Nix does

- `makeDerivationBuilder` (`unix/build/derivation-builder.cc:1751`) picks the
  class from `sandbox`. A relocated store (`storeDir != realStoreDir`, every
  `?root=` store) always sandboxes, and fails when the kernel lacks the
  namespaces (`:1780`).
- `startBuild` (`:369`) takes a build user when `useBuildUsers` holds: a
  build-users group or `auto-allocate-uids`, and root (`unix/user-lock.cc:237`).
  It makes a 0700 directory under `build-dir` (default `<stateDir>/builds`,
  `local-store.cc:84`), picks scratch output paths, builds the environment,
  opens the log and a pseudoterminal, and starts the child.
- The environment is `initEnv` (`:692`) over `DesugaredEnv`
  (`build/derivation-env-desugar.cc`): structured attrs (`.attrs.json`,
  `.attrs.sh`), `passAsFile`, `exportReferencesGraph`.
- The Linux sandbox (`linux/build/linux-derivation-builder.cc:506`) clones
  through a helper process, because `clone` is unsafe in a threaded process.
  It unshares PID, mount, IPC and UTS, the network unless the derivation is
  fixed-output, and the user namespace when the kernel allows. The parent
  maps sandbox uid 1000 and gid 100 to the build user, or to the caller
  without one (`:430`), and writes `/etc/passwd` and `/etc/group`. The child
  (`enterChroot`, `:684`) binds the input closure and `sandbox-paths`, mounts
  the real store directory at `storeDir` (`:724-745`), then `pivot_root`.
  `setupSeccomp` (`:79`) refuses setuid and setgid modes and extended
  attributes.
- `registerOutputs` (`unix/build/derivation-builder.cc:1069`) checks that each
  output exists, canonicalises it, scans it for references to the inputs and
  the outputs (`scanForReferences`, `path-references.cc:48`), hashes
  content-addressed outputs, moves them into place under a path lock, applies
  `checkOutputs` (`build/derivation-check.cc:39`), and registers them.
- `external-derivation-builder.cc` is Nix's own seam. Nix keeps the store side
  (scratch outputs, environment, `registerOutputs`), writes `build.json`, and
  a program given that file runs the process.

## What pynixd has

- The scheduler builds for the local store through `local_store.daemon_client`
  (`scheduler.py:219`), then pulls the outputs into the same store. That pull
  is all `UPSTREAM` in `tests/functional/test_basic_config_upstream_ops.py`
  holds besides the build itself.
- `pynixd_store`: `canonicalise`, `content_hash` and `HashModulo`, `PathLock`
  (Nix's `PathLocks`, `pathlocks.cc:71`), `TempDir`, `register_valid_paths`,
  `register_drv_output`, `add_build_log`, `drv_parser`, `nar_fs`.
- Missing: the reference scanner, the environment, the output checks, the
  process isolation, the pseudoterminal and the log stream to the client,
  `max-silent-time` and `timeout`, and a `BuildResult` filled as
  `derivation-building-goal.cc` fills it.

## The proposal

1. **Op 36 joins `LocalDBStore.OWNED`.** The route is `pynixd` in test stores
   and `daemon` in a deployment: writes stay on the daemon. The scheduler
   sends a local build to `local_store.execute`. On the `pynixd` route the
   outputs register in place, so the pull and the daemon's `AddTempRoot`
   go away.
2. **Nix's split, in Python.** The store side goes in `pynixd_store/builder/`:
   `env.py` (`initEnv` and `DesugaredEnv`), `refscan.py`, `outputs.py`
   (`registerOutputs` and `checkOutputs`). The process side is one interface,
   `Sandbox`, whose methods are Nix's virtual ones: `start_child`,
   `prepare_sandbox`, `enter_chroot`, `set_user`. There is no class tree.
3. **pynixd never forks itself.** It runs aiosqlite and other threads, the
   case Nix's helper process exists for. The child is a separate executable.
   It is pynixd's own helper (below).
4. **Scope of the first version: an `_own_answer` that returns `None` for the
   rest.** Linux, sandboxed, unprivileged: the caller's uid as sandbox uid
   1000, as Nix does without build users. Input-addressed and fixed-output
   derivations. The daemon answers `builtin:` builders, `recursive-nix`,
   `__noChroot`, `uid-range`, `use-cgroups`, floating content addresses, and
   repair and check (`goals/engine.py:267` sends those whole already). Root
   and build users are a later row.
5. **Proof.** `UPSTREAM` shrinks to `SetOptions` or to nothing. A parity test
   builds one derivation on both routes and compares the output's NAR hash,
   the `ValidPaths` and `Refs` rows, and the log lines.

The guest permits this: `tester` builds into a `?root=` store in the same
test, and Nix can do that only in a user namespace.

## Recursive Nix

A `recursive-nix` derivation builds here when the store's own
`experimental-features` names the feature. Before the helper starts,
`local.build` listens on `<build dir>/.nix-socket` and sets `NIX_REMOTE` to it,
as `startDaemon` does. The instance gives the builder a `RecursiveServer`: each
connection is a `RestrictedProxy` (`pynixd/recursive.py`). The proxy answers
for the input closure and for what the builder added or built since. Each new
path goes over the helper's control socket, and the helper binds it into the
sandbox's shared store mount. When the builder exits, the listener closes, and
the outputs may then refer to the added paths.

A `builder-rpc-v0` derivation uses the same socket, with the surface of
`RecursiveSubmitted`. It builds here when the store names
`dynamic-derivations` and each output is floating. The proxy speaks the
version and features of `builderRpcV0` and serves only the operations of
Nix's allowlist (`daemon.cc:332`). The builder adds each output with
`AddToStoreScanning` (op 1001), which finds its references among the paths
the build may see, and names it with `SubmitOutput` (op 1000). When the
builder exits, `local.build` checks the submitted paths as
`checkSubmittedOutputs` does, and the store registers their realisations.
Nothing moves out of the sandbox. A check or a repair goes to the daemon.
Only Nix's master branch has the feature, so its parity test runs the
`nix` of `NIX_GIT_BIN`.

## Build users

As root with `build-users-group`, a build runs as a member of that group, as
Nix's does (`useBuildUsers`, `user-lock.cc:237`):

- pynixd locks `<nixStateDir>/userpool/<uid>` with `flock`, the file Nix
  locks, so a `nix-daemon` beside pynixd never takes the same user. When every
  user builds already, the build waits.
- The build directory is the build user's. The chroot's root and its store
  directory belong to the group, as `setupBuildChroot` makes them.
- The helper unshares as root, and pynixd writes its uid and gid maps
  (`1000` to the build user): a process inside the new namespace can map no
  uid but its own. The helper sets up the chroot as root, then becomes uid
  1000 before it runs the builder. It kills the user's processes first
  (`killUser`), and drops root's supplementary groups.
- An output, and each file in it, must belong to the build user; a hard link
  to a file of the same build passes (`canonicalisePathMetaData`).

With `auto-allocate-uids`, a build takes a slot instead, as `AutoUserLock`
does (`user-lock.cc:165`). pynixd locks `<nixStateDir>/userpool2/slot-<n>`,
and the build's uid is `start-id + n * 65536`, with no account. In the
sandbox the gid is that uid too; without the sandbox it is the group of
`build-users-group`. Without the experimental feature, or with ids Nix
asserts on, the build stays on the daemon.

A derivation that requires `uid-range` takes all 65536 ids of its slot, as
Nix's chroot builder does (`linux-derivation-builder.cc:444`):

- The builder is root in the sandbox, over the whole slot (`0 <uid> 65536`),
  and its namespace may call `setgroups`.
- The chroot's root (0755) and `/etc` belong to the slot's first uid, and
  `/etc` stays writable. The sandbox has `/sys`.
- The build needs a cgroup. pynixd gives it to the build's root, and the
  builder sees it as the root of its own cgroup namespace.
- An output belongs to the slot's first uid. A file in it may belong to any
  uid of the slot.

Without the sandbox, without `auto-allocate-uids` as root, or without the
experimental feature `cgroups`, Nix refuses the build, and pynixd leaves it to
the daemon. A build VM maps no build user yet, so as root with build users a
build uses the namespaces.

## Without the sandbox

`sandbox = false`, and `sandbox = relaxed` for a `__noChroot` or fixed-output
derivation, build without the chroot, as `makeDerivationBuilder` decides
(`uses_sandbox`). A store whose real directory is not its logical one, every
`?root=` store, always sandboxes.

- The helper unshares nothing and binds nothing. The builder runs on the host,
  in the host's build directory, as pynixd's user or as the build user itself,
  with that user's supplementary groups (`setUser`).
- No PID namespace ends what the builder leaves behind. With a build user, the
  helper kills that user's processes after the builder too (`killSandbox`). A
  build cgroup kills its processes as well, but Nix has none by default.
- The builder gets no descriptor but the standard three (`closeExtraFDs`). A
  leftover process that held a copy of the terminal would keep the build's log
  open, and the build would wait for that process.
- A fixed or impure output is copied before pynixd hashes it, because a
  leftover process may still write to it (`moveOutputToTempDir`). An output at
  its final path stays there.
- The builder's directory is `build` in the top directory, as in the sandbox.
  Nix gives it the top directory itself. The top directory holds the helper's
  job, which the build user can reach but not list or replace.

The daemon builds the rest without the sandbox: a check, a repair, a build
with an output that is valid already (Nix builds those at a fallback path and
rewrites the hashes), `recursive-nix`, and `__noChroot` under `sandbox = true`,
which Nix refuses.

## A build this store cannot run

A `BuildDerivation` that no scheduler places, as in a reverse builder's
`proxy` session, can ask for a platform this store does not have or a feature
it lacks, or arrive with `max-jobs = 0`. Nix refuses it with InputRejected and
names each reason that applies (`reject`, `derivation-building-goal.cc:289`);
pynixd sends the same text (`rejection`). Nix first offers the build to its
build hook, then to an external builder for the derivation's system. With a
machine in `builders`, pynixd leaves the build to the daemon (`hands_off`).
pynixd runs an external builder itself (`external_builder`), as Nix does,
before any of the checks above.

The scheduler sends a builder the client's options with `max-jobs = 1`, as
Nix's hook does (`build-remote.cc:97`). The client's `--max-jobs 0` means "not
here", and the builder would refuse the build if it got that value.

## A failed build with keep-failed

With `keep-failed`, a build that does not succeed keeps its build directory,
as `cleanupBuild(false)` does (`derivation-builder.cc:1697`). pynixd removes
the chroot, opens the top directory and `build` to 0755, and sends Nix's line
`note: keeping build directory "<dir>"`. A builtin keeps nothing: its
directory may hold a copy of netrc. A check whose output differs moves the
rebuilt output to `<path>.check` and names it in the error (`:1553`).

## The child: pynixd's own helper

| | bubblewrap (`bwrap`, a Nix package) | a helper of pynixd's (`python -m pynixd_store.builder.child`) |
| --- | --- | --- |
| does | namespaces, uid map, binds, `pivot_root`, `--die-with-parent`, hostname, `--seccomp FD` | the same, with `os.unshare`, `mount` and `pivot_root` through ctypes |
| lacks | cgroups, `uid-range` (one uid mapped), a build user as root | nothing by design |
| costs | one dependency; the seccomp program is still pynixd's | ~400 lines and Nix's traps (`setgroups`, the uid map handshake, `/dev/pts`) |

User chose the helper: one implementation serves the first version and the
later root and build-user row, with cgroups and `uid-range`, and with no
external dependency. It is a separate Python process, single-threaded, that
does what Nix's `startChild` and `enterChroot` do and then `execve`s the
builder.

The helper takes its whole job from one file that pynixd writes, as Nix's
`build.json` does, and nothing in the store side assumes namespaces.

On a Linux host, each store has a zygote (`zygote.py`, pynixd's end is
`helpers.py`), and the zygote forks the helper. Its imports cost 564 ms of CPU
per build in a test guest, and the fork costs 5 ms (nixidae/nixidae#1590). The
zygote is single-threaded and runs no event loop, so its fork is safe. pynixd
itself has threads and never forks.

- The first build of a store starts the zygote. A dead zygote is replaced at
  the next build.
- The helper is the zygote's child. The zygote reaps it and sends its status
  on the build's own socket. pynixd signals it through a pidfd.
- The zygote dies with pynixd, and each helper dies with the zygote.
- A build in a VM, and a darwin build, start an interpreter for the helper.

## What a build used

Each build gets a record: CPU time, peak memory, bytes read and written, and
the PSI stall times (nixidae/nixidae#1187). The scheduler stores it in the
build's observation, and a pynixd peer gets it in `BuildResult.resources`.

- **A cgroup per build**, when pynixd's own cgroup is delegated to it
  (`Delegate=yes` on the unit): `builder/cgroup.py` moves pynixd's processes
  into a `pynixd` leaf and makes a `build-*` sibling for each build. The
  helper joins that cgroup before it forks the builder. The record then counts
  every process of the build together, and `cgroup.kill` ends what is left.
  Every `usage_sample_seconds` (5 by default) pynixd also samples the
  cgroup's CPU time, current memory and IO. The samples travel in
  `BuildResources.samples`, and the observation keeps them in its own table.
- **Otherwise `wait4`** on the sandbox's pid 1: only what it reaped, memory as
  the largest single process, and no stall times. That figure includes the
  helper's interpreter, about 30 MB, which the builder inherits at the fork.
  In a cgroup the interpreter's pages stay charged to pynixd.
- **A VM build** has a cgroup in the guest. The guest's init makes it and
  gives it to the build's uid, and the helper reads it into the usage file and
  samples it on its event loop: after `unshare(CLONE_NEWPID)` the helper can
  start no thread. pynixd's libkrunfw sets `CONFIG_PSI`, which the stock x86_64
  config leaves out. The record also holds the VMM's host cost, in `host_*`
  fields: its CPU time holds the guest's vCPUs, and its memory the guest's RAM,
  so the two never add. The VMM joins a host cgroup of the build when there is
  one. Otherwise the launcher forks it and writes its `wait4` rusage, because
  libkrun ends the VMM with `_exit`.

The scheduler expects a build to peak at the median peak memory of the
derivation's recent builds, the host peak for a VM build. It leaves out each store whose available memory,
less the expected peaks of the builds it runs, is below that figure. When no
store has the room, the store with the most room goes first, and the
placement trace says why.

## darwin

On darwin pynixd builds as `DarwinDerivationBuilder` does
(`darwin/build/darwin-derivation-builder.cc`), nixidae/nixidae#1367. There is
never a chroot: the builder runs on the host, at its scratch paths, as for
`sandbox = false` on Linux. `useSandbox` picks a sandbox profile instead.

- The store side (`local.py`) refuses what `makeDerivationBuilder` refuses on
  darwin: a diverted store, and `__sandboxProfile` under `sandbox = true`. It
  then runs `getPathsInSandbox`: `sandbox-paths`, the build directory, and
  each `__impureHostDeps` path, which must be under `allowed-impure-host-deps`.
- The helper (`child.py`) does what Nix's child does after `setUser`: it
  writes the profile (`darwin.py`) and applies it with
  `sandbox_init_with_parameters`, writes `\2` to end the set-up, and starts
  the builder with `posix_spawn` and `POSIX_SPAWN_SETEXEC`. The derivation's
  system picks the slice of a universal binary.
- The full profile denies by default and opens the scratch outputs, the paths
  above, the input closure and their parent directories. A fixed-output or
  impure build adds the network rules; `__darwinAllowLocalNetworking` opens
  localhost. Without the sandbox, the minimal profile only denies setuid and
  setgid files.
- The kernel matches a rule against the real path. A store or build directory
  under a symlink (`/tmp`, `/var/folders`) breaks the profile, under Nix too.

With a build user, the helper also removes that user's System V IPC objects
before and after the build, as `cleanupSysVIPCForUser` does: darwin has no IPC
namespace, so they outlive the processes.

A `recursive-nix` builder reaches the store's socket in its build directory,
which both profiles let it connect to. Darwin has no `/proc/self/fd`, so a
socket path longer than `sun_path` binds from its directory in a short-lived
process, as Nix's `bindConnectProcHelper` does. As in Nix, a path the builder
adds stays outside the profile, which is fixed when the build starts.

Not yet: `pre-build-hook` is not run on any platform (nixidae/nixidae#1370).

`tests/functional/test_build_darwin.py` builds each case with no daemon and on
Nix's own darwin builder, and compares the two. It runs on a darwin host only.

### The darwin build VM

With `darwin_build_vm`, a sandboxed darwin build runs in a macOS guest that
starts from a snapshot (`darwin_vm.py`, nixidae/nixidae#1168). The guest runs
the same helper with the same spec, profile and scratch paths; the guest's
kernel adds what darwin's profile cannot give: its own processes, users, IPC
and macOS release.

- Three virtio-fs shares, each at its host path: the build store, read-only
  and filtered to the input closure; the host's store, filtered to the
  helper's venv, when the build store is another one; and the top directory,
  writable. darwin-virtiofsd serves them.
- A tmpfs over the store share takes what the builder writes. After the
  helper ends, the guest copies each output into the top directory, and the
  launcher moves it into the store, so `collect_outputs` reads it as after a
  host build. A virtio-fs share cannot be the upper layer of a union mount:
  it hides the layer below.
- The build directory is a tmpfs in the guest. A file made on the writable
  share costs about 3 ms: unpacking and copying 3786 files took 23 s there,
  and 5 s on a tmpfs (2.5 s on the host). With `keep-failed`, the guest
  copies a failed build's directory back.
- The guest reports as the Linux build VM does: `vm-status`, `vm-exit`, and
  the VMM's usage. A watchdog kills QEMU and the share servers when the
  launcher dies, because darwin has no parent-death signal.
- A recursive build's socket is in the build directory. The guest listens
  there and relays each connection over vsock to the host, where pynixd
  listens at the same path. A path the builder adds joins the store share's
  allowlist.
- A build without the sandbox and one for another system stay on the host
  (`choose_site`).

The snapshot is machine state: `darwin_builder.py save` with `PYNIXD=1`
makes it (`vivarium/docs/design/macos-guest-spike`). A start is ready in
about 0.35 s. `tests/functional/test_build_darwin_vm.py` builds in the guest
and compares with Nix's darwin builder; it needs `PYNIXD_DARWIN_VM_SNAPSHOT`.

## The build VM

`build_vm` on a local-socket store runs the same helper in a libkrun
microVM (nixidae/nixidae#1186): `pynixd/nix/krun` holds the launcher and
the guest's init, and `pynixd_store/builder/vm.py` the command line. Each
share mounts at its host path, so the spec needs no translation. Each store
is a read-only DAX share with an allowlist: the build store shows the input
closure, and the host's store the closure of the package's venv, whose
interpreter runs the helper. The guest's page cache keeps no copy of either.

A fixed-output or impure build stays on the namespace helper unless
`build_vm.network` is on (User, 2026-11-14, over the 2026-11-06 choice). Then
the guest gets eth0, a virtio-net NIC on a socketpair that one passt process
per build serves: passt carries the guest's sockets out through the host's,
forwards no port in, maps no host loopback, and sends DNS to the host's
resolver. The guest has fixed addresses in 10.90.35.0/24, not slirp's
10.0.2.15, which a QEMU or vivarium host has itself. The build's
`resolv.conf` names passt, and its `hosts`, `services` and CA file are
copies in the chroot, because a file in `/` is no share. libkrun's TSI was
the other way in: it switches every socket of the guest kernel over, so a
namespace in the guest would not keep a sandboxed build off the network, and
a guest's listen binds on the host. A sandboxed build gets no NIC.

On Linux, a build for a system other than the store's own stays on the host:
the guest kernel has no binfmt handler, and emulation in a VM is slow. A `recursive-nix` or `builder-rpc-v0` builder reaches the
store's socket over vsock, with no TSI: the guest's init listens at the
socket's place in the build directory and joins each connection to a host
socket in the top directory. The helper's control socket comes the same way.
A path the builder adds joins the build store's allowlist before the helper
binds it, and such a build caches no negative lookup. A `sandbox-paths` source outside both stores
is writable, as Nix binds it: a directory is a share of its own, and a file
a share of its parent whose allowlist shows the file alone, because
virtio-fs mounts only a directory.

### The build VM on a Mac

On a Mac, `build_vm` builds Linux's systems (nixidae/nixidae#1149): the same
guest on Hypervisor.framework, one microVM per build. The guest is
aarch64-linux, and Rosetta for Linux runs x86_64-linux builds. A store can
have `darwin_build_vm` too; each build goes to the VM of its system.

- The launcher is a macOS build, signed with the hypervisor entitlement. The
  kernel, the init and the helper's venv are aarch64-linux builds, which a
  Linux builder makes. `internal.krunDarwinEnv` names them; the package's
  wrapper does not yet, because pynixd for a Mac would then need a Linux
  builder to build.
- libkrunfw is a Linux library, so the launcher boots its kernel as a file:
  the arm64 Image that the aarch64-linux libkrunfw build installs.
- The build directory's image is ext4 (`mke2fs -d`), with no journal:
  mkfs.xfs builds on Linux only. The init mounts XFS or ext4.
- Rosetta's directory is a share tagged `rosetta`. libkrun answers Rosetta's
  ioctls on it, from the host's own rosetta binary, so no Apple text is in
  any source. The init registers rosetta as the x86_64 ELF handler with the
  `F` flag, so a chroot with no `/run/rosetta` still runs x86_64 binaries.
  An x86_64 build runs its vCPUs in the TSO memory model.
- The job is Linux's: the chroot, Linux's seccomp filter, and the sandbox
  always. A Mac's `sandbox` setting is for darwin builds, and Nix's Linux
  builder has its own nix.conf (NIX-DEVIATION, in `local.py`). The Mac's
  `sandbox-paths` are darwin's, so a Linux build gets Linux's default
  instead: `/bin/sh`, a static busybox.
- No passt, which is Linux-only: pynixd does not take a fixed-output or
  impure Linux build, and the store's daemon or a remote builder builds it.
- Measured on an M-series Mac: the guest boots and runs a command in 0.13 s;
  a trivial build takes 0.8 s, the Nix client included.