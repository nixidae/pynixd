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
| 31 QueryValidPaths | partly: `substitute=true` falls through | substitutes, then queries (`daemon.cc:377`) | substitution (below) |
| 6 QueryReferrers | owned (SQLite, Nix's own statement) | none while the database is active | — |
| 33 QueryValidDerivers | owned (SQLite, Nix's own statement) | none while the database is active | — |
| 41 QueryDerivationOutputMap | owned (SQLite; with `ca-derivations`, the derivation file and each output's realisation) | a drv that is not valid, or whose file or closure hashes pynixd cannot read | — |
| 43 QueryRealisation | owned with `ca-derivations` (SQLite: `Realisations` by hash, or `BuildTraceV3` by path; both wire shapes) | without `ca-derivations` | — |
| 14 FindRoots | owned (the walk of `liveness.py`, validity from SQLite), censored by the handler | a daemon pynixd did not start, while pynixd is not root: that daemon may see more of `/proc` | — |
| 32 QuerySubstitutablePaths | owned: the scheduler's race over pynixd's substituter backends (a `NIX-DEVIATION`) | a client that names its own substituters | — |
| 21, 30 QuerySubstitutablePathInfo(s) | daemon | asks the substituters | the same race, with the narinfo it returns |
| 40 QueryMissing | goal engine; leaves ask the daemon | — | its leaves are ops 31 and 30 |

### Writes to the store

Each of these registers a path in `db.sqlite`, writes the store file system,
or both. `LocalDBStore.register_valid_paths` (`store/register.py`) writes
`ValidPaths`, `Refs` and `DerivationOutputs` as `registerValidPaths` does, with
Nix's statements, and only with `pynixd_writes` on. AddToStore, AddToStoreNar
and AddMultipleToStore call it on their `pynixd` routes. `Realisations` has no writer in pynixd.

| op | today | to own it |
| --- | --- | --- |
| 7 AddToStore | on trial: the daemon in a deployment; route `pynixd` adds the flat or NAR dump as `addToStoreFromDump` does (`local-store.cc:1168`): restore into a locked `tmp-*` directory while hashing, the path from the address and references, a temp root, then the path lock, move, NAR hash, canonical metadata, registration. A Git address, an algorithm pynixd does not know, an address Nix cannot make a path of, a name Nix refuses, a client before 1.25, repair, `auto-optimise-store` and `fsync-store-paths` go to the daemon | — |
| 39 AddToStoreNar | on trial: the daemon in a deployment; route `pynixd` imports it (`store/add.py`): signatures, path lock, restore into a locked `tmp-*` directory, NAR and content hash, canonical metadata, registration. Repair, a Git address, `auto-optimise-store` and `fsync-store-paths` go to the daemon | — |
| 44 AddMultipleToStore | on trial: the daemon in a deployment; route `pynixd` imports each path as AddToStoreNar does, in stream order, one registration per path (`daemon.cc:505`). The first error ends the op and keeps the paths before it. A Git address goes to the daemon as AddToStoreNar for that path; repair, `auto-optimise-store` and `fsync-store-paths` send the whole op there | — |
| 37 AddSignatures | daemon | update `ValidPaths.sigs` |
| 42 RegisterDrvOutput | daemon | insert into `Realisations` |
| 45 AddBuildLog | handler forwards | write `var/log/nix/drvs/..` |
| 11, 49 AddTempRoot(s) | owned where pynixd can write `temproots`; else the daemon (a `NIX-DEVIATION`) | run where pynixd owns the state directory |
| 12 AddIndirectRoot | handler forwards | a symlink in `gcroots/auto` |
| 47 AddPermRoot | handler forwards | the symlink, plus the indirect root |
| 38 NarFromPath | on trial: the daemon in a deployment; shadow in every test store, where the client gets the daemon's NAR, compared with `nar_fs.dump_path` of the store's files by SHA-256 and size; route `pynixd` streams the dump | — |

### Builds and substitution

| op | today | to own it |
| --- | --- | --- |
| 36 BuildDerivation | scheduler sends it to a store; for the local store, the daemon builds | a builder: sandbox, user, environment, output registration |
| 9, 46 BuildPaths(WithResults) | goal engine; leaves are ops 36 and 10; repair and check modes go to the daemon whole (`engine.py:267`) | the leaves; repair and check can stay unsupported |
| 10 EnsurePath | daemon substitutes | fetch from `HTTPBinaryCacheStore`, then the write path above |

### Maintenance

| op | today | to own it |
| --- | --- | --- |
| 20 CollectGarbage | daemon deletes; pynixd plans its own (`gc.py`) and asks the daemon for the live set (`RETURN_LIVE`) | the live set (roots and temp roots) and deletion |
| 34 OptimiseStore | daemon | hard-link identical files (`.links`) |
| 35 VerifyStore | daemon | re-hash every path |
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
2. NarFromPath (done for a client; the cache proxy, `transfer.py` and the
   substitution queue still read the daemon's dump): a read of the file
   system, and every copy out of the store uses it. `nar_fs.restore_path`
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
`AddBuildLog` has none yet.
