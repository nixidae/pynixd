# The builder: what pynixd takes from Nix's local builder

Status: proposal, 2026-11-04. It is row 6 of `daemonless.md` (op 36,
nixidae/nixidae#1155). Nix 2.35.3 is the reference; paths are under
`src/libstore/`.

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
2. **Nix's split, in Python.** The store side goes in `pynixd_store/build/`:
   `env.py` (`initEnv` and `DesugaredEnv`), `refscan.py`, `outputs.py`
   (`registerOutputs` and `checkOutputs`). The process side is one interface,
   `Sandbox`, whose methods are Nix's virtual ones: `start_child`,
   `prepare_sandbox`, `enter_chroot`, `set_user`. There is no class tree.
3. **pynixd never forks itself.** It runs aiosqlite and other threads, the
   case Nix's helper process exists for. The child is a separate executable.
   The choice of that executable is the open question below.
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

## Open question: the child

| | bubblewrap (`bwrap`, a Nix package) | a helper of pynixd's (`python -m pynixd_store.build.child`) |
| --- | --- | --- |
| does | namespaces, uid map, binds, `pivot_root`, `--die-with-parent`, hostname, `--seccomp FD` | the same, with `os.unshare`, `mount` and `pivot_root` through ctypes |
| lacks | cgroups, `uid-range` (one uid mapped), a build user as root | nothing by design |
| costs | one dependency; the seccomp program is still pynixd's | ~400 lines and Nix's traps (`setgroups`, the uid map handshake, `/dev/pts`) |

Recommendation: bubblewrap first, behind `Sandbox`. It covers all of the
first version's scope, and its gaps are the later root row. A helper of
pynixd's replaces it when that row starts.
