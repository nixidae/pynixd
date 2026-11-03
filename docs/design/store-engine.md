# The store engine: pynixd's local store, apart from the daemon

pynixd is on the way to a full local store without a Nix daemon
(`daemonless.md`, nixidae/nixidae#1155). The result is a second
implementation of the store side of Nix. It must also serve as a platform
for experiments: a change that works here can become a proposal for Nix.
This document gives the layering that both goals need, and the steps that
reach it from the code of today.

## Where the code is today

- `LocalDBStore` is a `Store` that holds the managed daemon client
  (`LocalStore`), the engine and the router (step 5 below). The scheduler
  builds on the client, through `Store.daemon_client`. Each answer is an
  `own_*` method that takes a wire request model, checks the daemon facts
  it needs, and asks the engine.
- `StoreEngine` (`pynixd_store/engine.py`, step 2 below) holds the writes
  and the reads. Its parts are `pynixd_store/add.py` and
  `pynixd_store/register.py`. The engine is the package `pynixd-store`
  (step 6 below).
- `WriteRouter` (`write_router.py`, steps 3 and 4 below) chooses the engine
  or the daemon for the five call sites, and runs the write there: the
  handlers of ops 7, 39 and 44, the substitution queue, and the HTTP
  cache's upload. Each one makes one call with Nix's name.
- The write path reads its bytes through one `Source` (step 1, below).

Each of these works, and the suites prove it. Each one also makes the next
operation more expensive, and none of them gives an experiment a clear place
to go.

## The layers

Nix has three layers, and pynixd takes the same three.

| Nix | pynixd | holds |
| --- | --- | --- |
| `daemon.cc` | the handlers | wire decode and encode, the trust rules before `startWork` |
| `Store` interface | `StoreInterface` (new) | the operations, with Nix's names and plain types |
| `LocalStore`, `RemoteStore` | the engine, and the daemon backend | the work |

**The engine** is pynixd's local store. It has Nix's method names:
`add_to_store`, `add_to_store_from_dump`, `register_valid_paths`,
`nar_from_path`, `query_path_info`, `add_temp_root`, `collect_garbage`. Its
arguments are plain types: a store path, a `ValidPathInfo`, a byte source, a
flag. It imports no request or response model, no handler, no connection
pool. It owns the file system, the database, the locks, the temporary roots
and the settings it reads. `nar_fs`, `content_address`, `signing`, `add`,
`register`, `local_store_db`, `temp_roots`, `liveness` and the collection's
reads (`gc`, `gc_sql`) are its parts.

**The daemon backend** implements the same interface over a Nix daemon
connection. It is `DaemonStore` with the method names above.

**The router** holds both, and is the one place that chooses. For each
operation it gives the engine or the daemon backend, and the reason when it
gives the daemon. The reasons are one declared table, not three methods.
The test stores, a deployment and an experiment differ only in the table
they load. The handlers, the substitution queue and the HTTP cache call the
router's interface and never read a route.

**The handlers** stay what `daemon.cc` is: decode, check trust, call one
method, encode. Every byte that goes to a client comes from here.

## One byte source

The engine reads every dump and NAR through one async protocol, as Nix reads
a `Source` (`pynixd_store/source.py`). `read(n)` gives at most `n` bytes, and `b""`
at the end. `FramedReader` is one directly: the client's frames. `Pieces`
makes one of an async iterator: a cache, a daemon, or a decompressed upload
read on a worker thread. `Bounded` is Nix's `EnsureRead` with a cap, for a
NAR that shares its stream with the next record (op 44). `Metered` counts
the bytes. The worker-thread bridge (`BlockingReader`) is inside the engine,
where the synchronous unpacking runs.

## Experiments, and how Nix learns from them

An experiment is a variant of one engine method, behind the router like any
other route. It is off by default, and a measurement decides it.

The divergence markers stay the ledger of what pynixd does differently:
`NIX-DEFECT` where Nix is wrong, `NIX-DEVIATION` where pynixd answers
otherwise on purpose (nixidae/nixidae#412, #416). An experiment that
changes an answer gets a marker, a measurement and a test, as a deviation
does today. That ledger is the list to propose to Nix.

## The package boundary

The engine is a package of its own: `pynixd-store`
(`pynixd-store/src/pynixd_store`). It imports no module of `pynixd`. pynixd
keeps the handlers, the router, the backends for the daemon, HTTP caches and
builders, the scheduler and the collector. This is the boundary of the
standing permission to split pynixd (2026-11-02).

The engine takes its data types from the library `nix-daemon-protocol`: a
store path, a `ValidPathInfo`, a `BasicDerivation`. These are the types of
the store, and Nix's daemon sends the same ones. It takes no request or
response model. A second set of plain types would change no answer, and
every call site would convert.

## The steps

Each step changes no wire byte, and lands with every suite green, Nix's
functional suite included. Parity and the differential tests are the proof
that nothing moved.

1. The byte-source protocol and its adapters. The write path takes it. (Done.)
2. The engine class over the code of today: it owns the layout, the
   database, the settings and the temporary roots, and `store/add.py` and
   `store/register.py` become its methods. (Done: the methods have Nix's
   names, and the two modules are their parts.)
3. The router and its reason table. The five call sites call it. (Done:
   `REASONS` in `write_router.py`, first match wins.)
4. The handlers of ops 7, 39 and 44 call one router method each. (Done:
   `DaemonWrites`, `store/daemon_writes.py`, is the daemon backend with the
   engine's names. The router's `add_to_store`, `add_to_store_from_dump`,
   `add_multiple_to_store` and `import_nar` choose and run each write, and
   `WriteClient` is what a write needs of the session.)
5. The read answers (`own_*`) move into the engine with plain types, and
   `LocalDBStore` stops being a subclass of `DaemonStore`. (Done:
   `LocalDBStore` holds its daemon client and forwards to it, and
   `Store.daemon_client` names the client that builds for a store.)
6. The package split. (Done: `pynixd-store` holds the engine and its parts.)
7. The collector splits. The collection over the database and the roots
   moves into the engine; the client stream and the other stores stay in
   pynixd. (Done: `pynixd_store/gc.py` holds the weights, the disk
   pressure and `GarbagePlanner`, which reads the roots, the mirror and
   the database. `pynixd_store/gc_sql.py` holds the age rule's plan. The
   collector in `pynixd/gc.py` chooses the rule, asks the substituters,
   streams to the client and deletes. The delete is a write, so it stays
   on the daemon. The engine gets `collect_garbage` when the writes move
   to it.)

New operations go into the engine from step 2 on, not into `LocalDBStore`.
