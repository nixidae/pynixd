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
| 14 FindRoots | daemon, censored by pynixd | walks `gcroots`, `/proc`, temp roots | the root walk in Python |
| 21, 30, 32 QuerySubstitutable* | daemon | asks the substituters | `HTTPBinaryCacheStore` already reads narinfo |
| 40 QueryMissing | goal engine; leaves ask the daemon | — | its leaves are ops 31 and 30 |

### Writes to the store

Each of these registers a path in `db.sqlite`, writes the store file system,
or both. pynixd writes none of `ValidPaths`, `Refs`, `DerivationOutputs` or
`Realisations` today. It writes only tables of its own (`local_store_db.py`).

| op | today | to own it |
| --- | --- | --- |
| 7 AddToStore | handler forwards the framed NAR | unpack the NAR, hash, canonicalise, register |
| 39 AddToStoreNar | handler forwards | the same, plus signature checks against `trusted-public-keys` |
| 44 AddMultipleToStore | handler forwards | the same, many paths in one transaction |
| 37 AddSignatures | daemon | update `ValidPaths.sigs` |
| 42 RegisterDrvOutput | daemon | insert into `Realisations` |
| 45 AddBuildLog | handler forwards | write `var/log/nix/drvs/..` |
| 11, 49 AddTempRoot(s) | owned where pynixd can write `temproots`; else the daemon (a `NIX-DEVIATION`) | run where pynixd owns the state directory |
| 12 AddIndirectRoot | handler forwards | a symlink in `gcroots/auto` |
| 47 AddPermRoot | handler forwards | the symlink, plus the indirect root |
| 38 NarFromPath | handler streams the daemon's dump | dump the path to NAR (`pynixd/nar.py` writes NARs) |

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
2. NarFromPath: a read of the file system, and every copy out of the store
   uses it.
3. The registration routine, then the write ops on top of it, beside a
   running daemon.
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

```json
{"stores": {"local": {"type": "local-socket", "op_routes": {"QueryReferrers": "shadow"}}}}
```

The switch covers the operations that pass through `LocalDBStore.execute`.
The handlers that forward bytes (the `AddToStore` family, `NarFromPath`,
`AddBuildLog`) take it when they gain an answer of their own.
