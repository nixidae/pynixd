# pynixd architecture

pynixd serves the Nix daemon protocol, from version 1.35. It answers what it can from its own store engine, and
gives the rest to a Nix daemon under it. With `daemon = false` there is no daemon, and pynixd answers everything
(nixidae/nixidae#1155). It also places builds on other stores: SSH builders, reverse builders, and the stores a
client names in `builders`.

`tests/unit/test_architecture_doc.py` holds this file to the code: each file path and each class or constant
name in back quotes must exist. `GLOSSARY.md` defines the terms. The designs behind each part are in
`docs/design/`. nixidae/nixidae#1318 is the plan to restructure what this map shows.

## Packages

| package | holds | imports |
| --- | --- | --- |
| `nix-daemon-protocol` | the wire types: requests, responses, store paths, path infos, derivations | nothing of the other two |
| `pynixd-store` | the engine: Nix's `LocalStore` in Python, and the local builder | `nix-daemon-protocol` |
| `pynixd` | the server: transports, sessions, handlers, stores, the router, goals, the queue, the scheduler | both |

## One client operation

1. **A transport** accepts the connection: `pynixd/unix_server.py`, `pynixd/ssh_server.py`, or a reverse builder
   through `pynixd/reverse_server.py`. `pynixd/http_server.py` serves a binary cache and the metrics, not the
   protocol. `Server` (`pynixd/instance.py`) owns them and the task group they run in.
2. **The session** is a `DaemonProxy` (`pynixd/proxy.py`): the handshake, the client's role (`pynixd/trust.py`),
   then `DaemonProxy.dispatch` for each operation.
3. **The answer** comes from one of two registries.
   - `HANDLER_REGISTRY` (`pynixd/handlers/`): an operation that needs the session, such as roots, writes,
     builds and the admin operations. Each handler applies Nix's trust rules at Nix's point.
   - `WIRE_REGISTRY` (`nix-daemon-protocol/src/nix_daemon_protocol/wire_ops.py`, with pynixd's request types in
     `pynixd/serde/`): a request type whose `execute` holds the recipe. It calls
     `Store.execute` on the session's local store, and `Store.call` sends what has no recipe over the wire.
4. **The local store** answers or delegates (below).

A request with no recipe on a store with no daemon fails with `NoDaemonError` (`pynixd/exceptions.py`), which
names the operation.

## Stores

`Store` (`pynixd/store/base.py`) is the interface every store implements.

- `LocalDBStore` (`pynixd/store/local_db.py`) is the store pynixd serves. It holds three parts:
  - `StoreEngine` (`pynixd-store/src/pynixd_store/engine.py`): the database, the files, the locks and Nix's
    algorithms. It imports nothing of `pynixd`.
  - The daemon side, chosen by the store's mode (`StoreMode`, `pynixd/store_mode.py`): `LocalStore`
    (`pynixd/store/local_daemon.py`), the client of the daemon, or `NoDaemonStore` (`pynixd/store/no_daemon.py`)
    in the `daemonless` mode, which starts nothing and refuses what only a daemon answers.
  - `WriteRouter` (`pynixd/write_router.py`): the one place that chooses who writes, the engine or the daemon,
    and why.
  `pynixd/op_routes.py` gives each operation a route: `daemon`, `shadow` (both answer, and the two answers are
  compared), or `pynixd`.
- `DaemonStore` (`pynixd/store/daemon.py`) is a store over the wire protocol: `LocalStore`, `ExternalUnixStore`,
  the SSH stores `SSHSubprocessStore` and `SSHSocketStore`, and `ReverseStore`. `ConnectionPool`
  (`pynixd/store/pool.py`) holds their connections.
- `BinaryCacheStore` (`pynixd/store/binary_cache.py`) is a binary cache: `HTTPBinaryCacheStore`,
  `LocalBinaryCacheStore` and `S3BinaryCacheStore`.
- `LocalStoreSubstituter` (`pynixd/store/local_store_substituter.py`) is another local store, read as a
  substituter.

## Builds

A `BuildPaths` or `BuildPathsWithResults` from a client goes through these stages:

1. **Goals** (`pynixd/goals/`, described in `pynixd/goals/README.md`). `pynixd/goals/requests.py` makes the root goals of a
   request. `GoalEngine` (`pynixd/goals/engine.py`) holds the goals of one request.
   `pynixd/goals/ensure.py` makes each derived path valid: it substitutes (`pynixd/goals/substitute.py`, over
   `SubstitutionQueue` in `pynixd/substitution_queue.py`) or builds (`pynixd/goals/build_derivation.py`).
2. **The queue.** `BuildQueue` (`pynixd/build_queue.py`) holds each `QueuedBuild` once, whatever number of
   clients wait for it.
3. **Placement.** `Scheduler` (`pynixd/scheduler.py`) chooses a store for each queued build. It reads
   `pynixd/placement.py` (the slots, and what a delegation costs) and `pynixd/allocator.py` (the ranking).
   `pynixd/placement_trace.py` records each decision.
4. **Execution.** `pynixd/execution.py` gives the build its inputs on the chosen store, sends `BuildDerivation`,
   and brings the outputs back.
5. **The build itself.** On the local store, `LocalDBStore` builds with
   `pynixd-store/src/pynixd_store/builder/local.py`, or gives the build to its daemon. The local builder runs
   the sandbox helper `pynixd-store/src/pynixd_store/builder/child.py`, or an external builder through it. A
   `BuildSite` (`pynixd-store/src/pynixd_store/builder/site.py`) runs the helper: `HostSite` on the host's
   kernel, `VmSite` in a libkrun microVM (`pynixd-store/src/pynixd_store/builder/vm.py`). `choose_site` picks
   one for each build.

A sandboxed builder that asks for recursive Nix reaches a `RestrictedProxy` (`pynixd/recursive.py`).

## Maintenance

- Garbage collection: `CollectGarbage` (op 20) runs `pynixd-store/src/pynixd_store/collector.py` on the engine, or
  goes to the daemon. `PynixdCollectGarbage` (op 101) is pynixd's own collector, `pynixd/gc.py`.
  `pynixd-store/src/pynixd_store/autogc.py` collects when free space falls under `min-free`.
- `VerifyStore` (op 35) and `OptimiseStore` (op 34) are engine methods: `pynixd-store/src/pynixd_store/verify.py`
  and `pynixd-store/src/pynixd_store/optimise.py`.
- `pynixd state` (`pynixd/state.py`) reports what the instance holds.

## Experiments

An experiment is a field of `LocalSocketStoreSpec` whose annotation carries an `Experiment`
(`pynixd/experiments.py`): what its default does, its issue, and its tests. `pynixd state` lists each store's
experiments that are off their default, in the section `experiments`. `tests/unit/test_experiments.py` holds
this table to the declarations.

| switch | turns on |
| --- | --- |
| `op_routes` | an operation answered by pynixd, or by both sides and compared |
| `pynixd_writes` | pynixd writes the store |
| `daemon` | at `false`: no daemon under pynixd |
| `read_only` | a read-only store; `immutable` when its storage cannot change |
| `build_vm` | sandboxed builds in a libkrun microVM |

Nix's `external-builders` is a Nix setting that pynixd's builder follows, not an experiment of pynixd.
