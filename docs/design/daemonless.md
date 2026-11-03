# pynixd without a Nix daemon: the inventory

Status: inventory, 2026-11-01, at nixidae bf7f5b93. Nothing here is
implemented. nixidae/nixidae#1155 holds the goal; this page says what stands
between pynixd and that goal.

Every fact is **read** from pynixd's source at that revision. Nothing here is
measured yet.

## Where the daemon is today

`LocalStore.ensure_daemon` (`pynixd/store/local_daemon.py`) starts a private
`nix-daemon` and connects to it over a Unix socket. An operation reaches that
daemon on one of four routes:

1. **A handler** in `pynixd/handlers/` reads the request itself. Most
   handlers then forward the bytes to a pooled daemon connection.
2. **The goal engine** (`pynixd/goals/`) answers `BuildPaths`,
   `BuildPathsWithResults` and `QueryMissing`. It plans in Python, and each
   leaf of the plan is a daemon operation.
3. **A fast path** in `LocalDBStore` (`pynixd/store/local_db.py`) reads
   `db.sqlite`. It returns `None` on a case it cannot answer.
4. **The fallback**: `DaemonStore.execute` sends every operation with no
   executor, and every `None` from a fast path, through `call()`.

Remote stores (SSH builders, reverse builders, HTTP caches) are out of scope.
They are other stores. The goal is a local store with no daemon under it.

## The operations

"Owned" means pynixd answers with no daemon. "Partly" means pynixd answers
some cases and sends the rest to the daemon.

### Reads

| op | today | the daemon's part | to own it |
| --- | --- | --- | --- |
| 1 IsValidPath | owned (SQLite; session reader first) | none while the database is active | the inactive-database case (overlay stores) |
| 26 QueryPathInfo | owned (cache, then SQLite) | none while the database is active | as above |
| 23 QueryAllValidPaths | owned (SQLite) | none | — |
| 29 QueryPathFromHashPart | owned (SQLite, hit and miss) | a hash part that is not 32 characters, which Nix refuses | — |
| 31 QueryValidPaths | owned; `substitute=true` follows the route of op 10: on `pynixd` the goal engine substitutes each missing path that is not a derivation, then SQLite answers | `substitute=true` on the daemon route of op 10 | as op 10 |
| 6 QueryReferrers | owned (SQLite, Nix's own statement) | none while the database is active | — |
| 33 QueryValidDerivers | owned (SQLite, Nix's own statement) | none while the database is active | — |
| 41 QueryDerivationOutputMap | owned (SQLite; with `ca-derivations`, the derivation file and each output's realisation) | a drv that is not valid, or whose file or closure hashes pynixd cannot read | — |
| 43 QueryRealisation | owned with `ca-derivations` (SQLite: `Realisations` by hash, or `BuildTraceV3` by path; both wire shapes) | without `ca-derivations` | — |
| 14 FindRoots | owned (the walk of `liveness.py`, validity from SQLite), censored by the handler | a daemon pynixd did not start, while pynixd is not root: that daemon may see more of `/proc` | — |
| 32 QuerySubstitutablePaths | owned: the scheduler's race over pynixd's substituter backends (a `NIX-DEVIATION`): HTTP caches, `unix://` stores, and `file://` caches pynixd can read (`LocalBinaryCacheStore`). A client's `substituters` replaces the daemon's list, and each one gets a backend on first use (`pynixd/substituters.py`) | a substituter, the client's or the daemon's, that no backend stands for: `s3://`, `ssh://`, a `file://` directory pynixd cannot read, a `unix://` store pynixd does not run | — |
| 21, 30 QuerySubstitutablePathInfo(s) | on trial: the daemon in a deployment, shadow in every test store; route `pynixd` answers what the highest-priority backend holding each path knows (`misc.cc:90`), from the same race | as op 32, and a path named with a content address | — |
| 40 QueryMissing | owned by the goal engine: validity from SQLite, availability from the race | as op 32: it then asks op 30 of the daemon | — |

### Writes to the store

Each of these registers a path in `db.sqlite`, writes the store file system,
or both. `LocalDBStore.register_valid_paths` (`pynixd_store/register.py`) writes
`ValidPaths`, `Refs` and `DerivationOutputs` as `registerValidPaths` does, with
Nix's statements, and only with `pynixd_writes` on. AddToStore, AddToStoreNar
and AddMultipleToStore call it on their `pynixd` routes. `register_drv_output`
(`pynixd_store/register.py`) writes the build trace, in `BuildTraceV3` or
`Realisations` as `registerDrvOutput` does. `add_build_log`
(`pynixd_store/build_log.py`) writes a derivation's log under `logDir`, as
`addBuildLog` does. `optimise_store` (`pynixd_store/optimise.py`) hard-links
identical files into `.links`, as `optimiseStore` does. `verify_store`
(`pynixd_store/verify.py` and the engine) cross-checks the database and the
store directory, as `verifyStore` does. `collect_garbage`
(`pynixd_store/collect.py` and `collector.py`) deletes what no root keeps, as
`collectGarbage` does.

| op | today | to own it |
| --- | --- | --- |
| 7 AddToStore | on trial: the daemon in a deployment; route `pynixd` adds the flat or NAR dump as `addToStoreFromDump` does (`local-store.cc:1168`): restore into a locked `tmp-*` directory while hashing, the path from the address and references, a temp root, then the path lock, move, NAR hash, canonical metadata, registration. A Git address, an algorithm pynixd does not know, an address Nix cannot make a path of, a name Nix refuses, a client before 1.25, repair, `auto-optimise-store` and `fsync-store-paths` go to the daemon | — |
| 39 AddToStoreNar | on trial: the daemon in a deployment; route `pynixd` imports it (`pynixd_store/add.py`): signatures, path lock, restore into a locked `tmp-*` directory, NAR and content hash, canonical metadata, registration. Repair, a Git address, `auto-optimise-store` and `fsync-store-paths` go to the daemon | — |
| 44 AddMultipleToStore | on trial: the daemon in a deployment; route `pynixd` imports each path as AddToStoreNar does, in stream order, one registration per path (`daemon.cc:505`). The first error ends the op and keeps the paths before it. A Git address goes to the daemon as AddToStoreNar for that path; repair, `auto-optimise-store` and `fsync-store-paths` send the whole op there | — |
| 37 AddSignatures | on trial: the daemon in a deployment; route `pynixd` merges the signatures into `ValidPaths.sigs` as `addSignatures` does (`local-store.cc:1583`), in Nix's set order. A path that is not valid raises, where Nix dereferences a null pointer and the worker segfaults (`NIX-DEFECT`) | — |
| 42 RegisterDrvOutput | on trial: the daemon in a deployment; route `pynixd` records the realisation as `registerDrvOutput` does (`local-store.cc:664`), in `BuildTraceV3` by the derivation's base name (Nix 2.35) or `Realisations` by its hash (Nix 2.34), in Nix's signature order. An id already present with another output path raises Nix's message. The scheduler's own registration runs the same route (`goals/ensure.py`). Without `ca-derivations` the daemon answers | — |
| 45 AddBuildLog | on trial: the daemon in a deployment; route `pynixd` writes the log as `addBuildLog` does (`local-store.cc:1645`), `<logDir>/drvs/<name[:2]>/<name[2:]>.bz2`, bzip2, a temporary file and a rename, and keeps a log already there. Admin only (`daemon.cc:1015`). The log is read whole, as Nix drains it. `logDir` is a sibling of the state, so the layout carries it (`NIX_LOG_DIR`) | — |
| 11, 49 AddTempRoot(s) | owned where pynixd can write `temproots`; else the daemon (a `NIX-DEVIATION`) | run where pynixd owns the state directory |
| 12 AddIndirectRoot | on trial: the daemon in a deployment; route `pynixd` links `gcroots/auto/<SHA-1 of the path>` to the client's path (`pynixd_store/roots.py`) | — |
| 47 AddPermRoot | on trial: the daemon in a deployment; route `pynixd` takes the session's temporary root, links the root to the store path, then adds the indirect root. It refuses a root in the store and a file that is not a link into the store, with Nix's messages | — |
| 38 NarFromPath | on trial: the daemon in a deployment; shadow in every test store, where the client gets the daemon's NAR, compared with `nar_fs.dump_path` of the store's files by SHA-256 and size; route `pynixd` streams the dump. pynixd's own reads (the HTTP cache, a copy out of the store, a substitution from a local store) take the same route through `NarReader` (`pynixd/store/nar_reader.py`); a dump of another size than the path info's fails | — |

### Builds and substitution

| op | today | to own it |
| --- | --- | --- |
| 36 BuildDerivation | scheduler sends it to a store; for the local store, the daemon builds | a builder: sandbox, user, environment, output registration |
| 9, 46 BuildPaths(WithResults) | goal engine; leaves are ops 36 and 10; repair and check modes go to the daemon whole (`engine.py:267`) | the leaves; repair and check can stay unsupported |
| 10 EnsurePath | on trial: the daemon in a deployment, `pynixd` in every test store; there `SubstitutePathGoal` fetches from the race and imports through the route of op 39. A client's own substituters take part as in op 32. A substituter no backend stands for sends the path to the daemon | that case |

### Maintenance

| op | today | to own it |
| --- | --- | --- |
| 20 CollectGarbage | on trial: the daemon in a deployment; route `pynixd` collects as Nix 2.35's `collectGarbage` does (`gc.cc:356`): the big lock held for writing, the roots socket served for the whole walk (a process that adds a temporary root meanwhile sends it there, and its answer waits while that path is deleted), then from each candidate a walk through its referrers that keeps the closure when it reaches a root, a temporary root or a live path, and otherwise deletes it referrers first. `keep-outputs` and `keep-derivations` widen the walk of the whole store only, as in 2.35. It sends each line Nix prints. `ignore-liveness` is refused for every client (`daemon.cc:774`). pynixd's own collector (op 101, `gc.py`) plans in Python and deletes on the route of op 20: on `pynixd` each group of its plan is one engine `DELETE_SPECIFIC`, and a refusal (`PathInUse`) splits the group as the daemon's does | — |
| 34 OptimiseStore | on trial: the daemon in a deployment; route `pynixd` hard-links identical files into `.links` as `optimiseStore` does (`optimise-store.cc`): each regular file and symlink hashed over its NAR, the first of a hash linked as `.links/<Nix32 NAR hash>`, a later file replaced by a hard link through a temporary link and a rename. It holds a temporary root on each path, as Nix does, so the daemon's collector does not remove one mid-walk. It reports the bytes it freed, as Nix's `printInfo` does (`optimise-store.cc:328`). Every client may run it (`daemon.cc:887`) | — |
| 35 VerifyStore | on trial: the daemon in a deployment; route `pynixd` cross-checks the database and the store directory as `verifyStore` does (`local-store.cc:1380`), under the big garbage-collector lock held for reading: a valid path the directory lost is removed from the database when its referrers can go too; with `--check-contents` each `.links` entry and each valid path is re-hashed, a modified one is an error and a missing hash is filled. It sends each line Nix prints, at Nix's level, and drops a line above the client's verbosity as `TunnelLogger::log` does (`daemon.cc:89`). A `repair` request goes to the daemon, which has the builder `repairPath` needs; an untrusted repair is refused (`daemon.cc:896`) | — |
| 19 SetOptions | owned (kept per session, sent per connection) | stops sending once nothing goes upstream |

## What the writes need first

These are not operations. Every write op above needs them, so they come
first.

- **Privilege.** The daemon runs as root and pynixd may not. Writing
  `/nix/store` and `db.sqlite`, and building as `nixbld` users, need the
  store's owner. The temp-root fallback exists because pynixd can be
  unprivileged.
- **Locks Nix honours.** While a daemon still runs beside pynixd, the two
  write one store. Nix takes the global GC lock and a `.lock` file per path,
  and SQLite's own lock. A pynixd writer must take the same ones.
- **Canonical paths.** mtime 1, no write bits, no setuid, owner root. This
  is `canonicalisePathMetaData` in Nix.
- **One registration routine.** `ValidPaths`, `Refs`, `DerivationOutputs`
  and the path-info cache in one transaction, so every write op shares it.

## Order

Reads first, writes second, builds last.

1. The read ops with a SQL answer: QueryReferrers (done), QueryValidDerivers (done),
   QueryDerivationOutputMap (done), QueryRealisation (done), and the QueryPathFromHashPart
   miss (done). No privilege, no lock, and the parity run compares each answer with
   the daemon's.
2. NarFromPath (done: a client, the HTTP cache, `transfer.py` and the
   substitution queue): a read of the file system, and every copy out of
   the store uses it. `nar_fs.restore_path`
   unpacks a NAR for the write path.
3. The registration routine (done: a differential against Nix's own rows in
   `tests/functional/test_register_valid_paths.py`), then the write ops on
   top of it, beside a running daemon. A daemon worker keeps no path-info
   cache (`pathInfoCacheSize = 0`, `daemon.cc:439`), so a pooled worker sees
   a registration by pynixd at once.
4. Substitution, through `HTTPBinaryCacheStore` and the write path.
5. Roots, the live set, and deletion.
6. The builder. This is where the experiments in nixidae/nixidae#1155 live.

## The switch

`op_routes` on a local store gives each operation one of three routes:
`daemon`, `shadow` or `pynixd` (`pynixd/op_routes.py`). An answer pynixd
gains enters `LocalDBStore.OWNED` at `daemon`, runs in `shadow` against real
traffic, and moves to `pynixd` when `pynixd_op_shadow_total` shows no
mismatch. `pynixd_op_fallthrough_total` then shows what of it still reaches
the daemon. An answer that writes has no shadow.

An answer on trial defaults to `daemon`, so a deployment keeps working as it
did. The test stores route it to `shadow` (`TEST_OP_ROUTES` in
`tests/_conftest/config.py`, and the parity and functional-suite shim
configurations), and a shadow mismatch or error fails the functional suite
(`tests/functional/conftest.py`). It moves to `pynixd` when the suites
have shown it agrees.

Writes are opt-in twice. `pynixd_writes` on the store spec is off by default,
and with it off a route to `pynixd` for an answer that writes refuses to
start. With it on, such an answer still defaults to `daemon`: only a route
set by hand takes it. The test specs turn `pynixd_writes` on. FindRoots
tidies as Nix does (a stale `gcroots/auto` link, a dead process's temporary
roots file) only with it on; its answer is the same either way.

```json
{"stores": {"local": {"type": "local-socket", "op_routes": {"QueryReferrers": "shadow"}}}}
```

The switch covers the operations that pass through `LocalDBStore.execute`.
The handlers that forward bytes take it when they gain an answer of their
own: `NarFromPath` streams pynixd's dump on the `pynixd` route, and the
daemon's on the other two. `AddToStoreNar` imports the NAR itself on the
`pynixd` route, and the test that sets that route compares the result with
the daemon's import (`tests/functional/test_add_to_store_nar_import.py`).
`AddMultipleToStore` does the same for `nix copy`
(`tests/functional/test_add_multiple_to_store_import.py`), and `AddToStore`
for an evaluation's derivations (`tests/functional/test_add_to_store_import.py`).
`AddBuildLog` records the log itself on the `pynixd` route, and the test that
sets that route reads the `.bz2` back from the store
(`tests/functional/test_add_build_log_import.py`). `OptimiseStore` hard-links
identical files on the `pynixd` route, and the test adds two paths with a
shared file, optimises, and reads the inodes back
(`tests/functional/test_optimise_store_dedup.py`). `VerifyStore` walks the
store on the `pynixd` route, and the test adds a path, deletes its files, and
checks `nix-store --verify` invalidates it
(`tests/functional/test_verify_store_invalidates.py`). `CollectGarbage` collects
on the `pynixd` route, and the test adds two paths, roots one, collects, and
reads validity and the disk back (`tests/functional/test_collect_garbage_engine.py`).
pynixd's own imports, a
substitution and the HTTP cache's upload, follow AddToStoreNar's route
(`tests/functional/test_substitution_import.py`).
