# pynixd Glossary

This document defines technical terms and concepts used within the `pynixd` codebase.

> **Note**: This glossary is a living document. It is not exhaustive and SHOULD be expanded as new concepts are discovered, clarified, or added to the system.

## Foundational Nix Concepts

### Expression
The high-level code written in the Nix language (typically in `.nix` files). Expressions are evaluated to produce Derivations.

### Derivation (.drv)
A low-level build recipe. It is a static representation of a build task, containing paths to all required inputs (sources and other derivations), the builder script, and the intended output names.

### StorePath
A unique, immutable path within the Nix store (e.g., `/nix/store/z12...-hello-2.12`). The hash in the path is derived from the inputs used to produce the item.

### NAR (Nix Archive)
The serialization format Nix uses to represent a file system object (file, directory, or symlink) as a single byte stream. It is used for transferring paths between stores.

### Hash
Nix primarily uses SHA-256 hashes to identify store paths and ensure the integrity of the store's contents.

## pynixd Core Concepts

### Store
A polymorphic representation of a Nix store. `pynixd` can interact with local stores, remote SSH stores, S3 buckets, etc., using a unified interface.

### DAG (Directed Acyclic Graph)
The dependency graph of derivations. `pynixd`'s scheduler is "DAG-aware," meaning it understands the order in which builds must execute based on their dependencies.

### Content-Addressed (CA) Derivation
A derivation where the output path is determined by the hash of the *actual produced content* rather than the derivation's inputs. This allows for "early cutoff" optimizations.

### Realisation
The concrete mapping between a derivation output and its final `StorePath`. For CA derivations, this mapping is only known after the build is complete.

### Deferred Output
An output path that cannot be calculated before the build (common in CA derivations). These are resolved "on the fly" by the `DynamicDerivationResolver`.

### Trampolining
A Nix-native concept for "unknown output" building (common in CA derivations). It is the process where a build is intercepted because it has "deferred" dependencies that must be resolved first. Once resolved (possibly by triggering sub-builds), the original derivation is updated and re-injected into the build process. In `pynixd`:
- **`DerivationResolver`** handles pre-build resolution (deferred output, dynamic DrvWithVersion)
- **`Trampoline`** handles post-build inner-build enqueuing and DAG rewiring

## pynixd Architecture

### Python GC Liveness
`RootsTracker` reads stable roots, temporary roots, and process roots, then resolves their reference closure through SQLite.
The collector refreshes this state before planning deletions.
It asks the daemon for `RETURN_LIVE` only when Python resolution is unavailable.
The background watch refreshes Python state without daemon comparisons.
The daemon performs deletions and checks liveness again to protect roots created after planning.

### GC Delete Group
The collector sends at most 100 paths per daemon deletion request over one held connection.
Python orders the whole delete set with referrers first before dividing it into groups.
This prevents later groups from blocking earlier groups during normal deletion.
A failed request can delete some paths without returning their names.
The collector rechecks validity directly with the daemon, then splits the remaining group to isolate refusals.
Refused paths get one final retry after later groups release their references.
Timed-out groups receive no further deletion requests during that run.

### Three-Tier Execution
The pattern used to separate protocol IO from logic:
1. **Server Dispatch** (`handle`): Entry point that decodes from the client wire.
2. **Logic Hook** (`execute`): Implements optimizations and core business logic.
3. **Transport** (`call`): Low-level wire protocol implementation for upstream communication.

### Umbrella Repository
A repository that composes related projects while their package ownership and
physical source layout remain independent. For the nanopynix umbrella, this
means composing `nanopynix` and `pynixd` through explicit versioned package or
service boundaries first; it does not imply sibling-relative Python imports or
an immediate source-tree merger.

### Daemon Protocol Package
The reusable `nix_daemon_protocol` Python package. It contains the standard
Nix daemon wire codecs and protocol value models, but no daemon scheduling,
authentication, client forwarding, or pynixd-private operation definitions.
Those runtime concerns remain in `pynixd`; its private wire extensions live in
`pynixd.daemon_extensions`.

### Wire Scalar
An immutable domain value whose complete daemon-protocol representation is one
string. `StorePath`, `NARHash`, `ContentAddress`, and `DerivedPath` are wire
scalars: they subclass `str` to retain native string equality and hashing while
adding Nix-specific helpers. The protocol codec recognizes these values
directly; Pydantic support exists only for model/API boundaries rather than
making the scalar itself a Pydantic model.

### Proxy (`DaemonProxy`)
The component that manages the connection from a Nix client, handles the protocol handshake, and routes requests to the internal logic.

### Tracker (`PathTracker`)
An internal database (SQLite) that tracks which `StorePath`s are available on which `Store`, enabling locality-aware scheduling.

### Ranker / Allocator
*   **Ranker**: Scores available stores based on telemetry (CPU pressure, latency, etc.).
*   **Allocator**: Matches a pending build task to the most appropriate store based on ranks and requirements.

### Store Priority (Float Multiplier)
A per-store multiplier (`priority`, default `1.0`) applied to the telemetry score during ranking. Values > 1.0 make a store more likely to be picked; < 1.0 make it less likely. The `min_schedule_score` threshold is checked against the *raw* score before multiplication, so priority only reorders stores that are already capable. Set in store config:
```nix
{
  type = "ssh-subprocess";
  host = "builder1";
  priority = 2.0;  # preferred
}
```

### Placement Trace
The record of each scheduler decision, in `PynixdPlacementDecision` and `PynixdPlacementCandidate` of the store database (`pynixd/placement_trace.py`). A decision row holds the build, the outcome (`assigned`, `waiting`, `failed`, `cancelled`), the reason and the chosen store. One candidate row per store holds each score term, the reason a store was left out, and the expected bytes and seconds of each phase there (ship, dispatch, build, pull, slot wait). `observationId` joins the decision to what the build cost in `PynixdBuildObservation`. Settings: `placement_trace` (off by default, rows kept for `max_age`, old rows deleted in chunks every `sweep_interval`).

### Goal System
A request-scoped dependency walker for daemon operations that need to traverse derivations and store paths. `BuildPaths` and `BuildPathsWithResults` use mutating goals that may substitute or build; `QueryMissing` uses a read-only planning goal that may inspect local validity and substitution availability but must not import or build paths.

### GoalRun / GoalEngine
The current implementation creates a fresh `GoalEngine` per daemon request, which acts as the request-local goal run. It deduplicates active goals within that request only. Completed goal results are not cached globally; later requests discover current truth from the local store, substitution caches, and scheduler queues.

### Mutating Ensure Goal
`EnsureDerivedPathGoal` is the mutating coordinator for making a `DerivedPath` available locally. It may substitute paths, schedule builds through the global scheduler, resolve deferred outputs, and walk nested dynamic derivation chains.

### Read-Only Planning Goal
`QueryMissingPlanGoal` is the read-only counterpart used by `QueryMissing`. It classifies requested paths into `will_build`, `will_substitute`, and `unknown` without changing the local store. It may refresh substitution availability caches because those are scheduler-side query metadata, not local store realisation.

### Scheduler Work Lane
A typed side-effect lane owned by the singleton scheduler. Build work lives in the build lane (`BuildQueue`, builder assignment, subscribers, and `.drv` deduplication). Substitution work lives in the substitution lane (`SubstitutionQueue`, availability caches, health logs, background probes, and import deduplication).

### SubstitutionQueue
The scheduler-owned substitution lane. It exposes `can_substitute(path)` for fast availability checks, `get_substituter(path)` for selecting the highest-priority healthy source, and `substitute(path)` for deduplicated path imports into the local store.

### Build Hold
A goal request's claim on a queued build. `BuildQueue.enqueue` takes the hold under the same lock as the `.drv` deduplication, and records it both on the build (`QueuedBuild.holders`) and per request. When the request answers, `BuildQueue.release_request` drops every hold of that request in one step. A build that loses its last holder before it finishes is cancelled. A build with no holders at all (`build_derived_paths`, pynixd's own builds) is never ended this way.

### Client-Bound Build Subscription
A build log subscription tied to an active client request. Client-bound subscriptions are reference-counted per build. If the last such subscriber is explicitly removed before completion, the queued build is cancelled; internal no-client scheduler users are not cancelled by this rule.
