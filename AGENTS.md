# pynixd Development Mandates

This document defines the foundational architectural patterns and engineering standards for `pynixd`. These instructions take precedence over general defaults.

## 1. Version Control: Jujutsu (jj)
- **Tool**: Use `jj` (Jujutsu), NOT `git`.
- **Committing**: Prefer `jj commit -m "..."` to finish a task. It creates a new revision and provides a clean working copy.
- **Squashing**: If your changes are a fixup for the last commit, prefer `jj squash --use-destination-message` to keep the commit message or `jj squash -m "..."` to update the commit message
- **Paging**: Always include `--no-pager` in all `jj` commands to ensure non-interactive execution.
- **Subagents must NEVER use VCS tools** (no `jj`, no `git`, no `jj squash`, no `jj commit`, nothing). Subagents are strictly limited to reading/writing files and running validation commands (`nix build --file . checks.format checks.lint checks.types`). If a subagent cannot complete its task, it should report the error back to the primary agent and let the primary agent handle it.

## 2. Core Architectural Pattern: Request-Driven Execution
`pynixd` follows a strict three-tier execution pattern to separate protocol IO from business logic.

Important Nix protocol version support matrix:
Builder stores: >= 1.32 (nixbuild.net is 1.32)
Local stores: >= 1.35 (Lix speaks 1.35). Supported, but untested: the guest sessions run Nix >= 2.34 (`supportedNixFloor`) only.
Pynixd will adversise 1.38 support even if local_store is 1.35 and translate where appropriate

1. **Server Dispatch** (`OpRequest.handle(proxy)`): 
   - Entry point for the `DaemonProxy`.
   - Decodes the request from the client wire.
   - Delegates logic to the store: `return await proxy.local_store.execute(request)`.
   - *Streaming operations* (like `NarFromPath` or `AddToStore`) override this to handle raw byte piping.

2. **Logic Hook** (`OpRequest.execute(store, client=None, suppress_last=False)`):
   - Where the "recipe" for an operation lives.
   - Implements optimizations (SQLite fast-paths, memory caches).
   - If no optimization exists, falls back to the wire: `return await store.call(self, client=client, suppress_last=suppress_last)`.

3. **Store Executor** (`Store.execute(request, ...)`):
   - Simple polymorphic dispatcher that calls `request.execute(self, ...)`.

4. **Transport** (`Store.call(request, ...)`):
   - Low-level wire protocol implementation.
   - Handles connection pooling, protocol magic, and handshake.

## 3. Stderr & Logging
- **`StderrBuffer`**: All buffered responses MUST include a `StderrBuffer` in their `stderr` field.
- **Real-time Forwarding**: If a `ClientConn` is provided to `execute()`, logs MUST be streamed to it as they are read and NOT buffered in the response. A substituting `QueryValidPaths` sends one message per chunk it copies; buffering them grew pynixd to 10.4 GB and stalled the loop (#507). Keep messages only when the caller needs them (`keep_logs=True`), and only the kinds it needs. The terminating `LogError` is never streamed: the caller reports it.
- **`suppress_last`**: When executing sub-operations (e.g., builds within a `BuildPaths` request), intermediate `STDERR_LAST` messages MUST be suppressed to avoid confusing the client.
- **Transparency**: No-op or cached operations MUST inject a `StderrNext` message (e.g., `"pynixd: IsValidPath (SQLite hit)"`) into the buffer for transparency.

## 4. Engineering Standards
- **Validation**: before committing, the gates and the guest session:

      nix build --file . checks.format checks.lint checks.types --no-link
      nix run --file . internal.tests.guest.driver -- --out ./out

  `tests.guest` runs the five suites (unit, protocol, parity, functional,
  differential) at once, each in a vivarium container guest: 115 s for the
  session, 2 s of it booting, after evaluation and the build of a changed
  tree. The host's `nix.conf`, `/tmp` and daemons cannot reach the guests. One suite:
  `-- --out ./out --only prepare --only unit`. Evidence is in `./out`:
  `junit.xml`, `events.jsonl`, and each suite's log under
  `artifacts/<suite>/`. Agents start it through the vivarium MCP (`start`
  with `attr = "internal.tests.guest"`) and watch the `monitor` command.

  **Do not run the suites with host pytest** (`nix develop ... pytest`).
  It reads the host's config: measured on dynhetz, `use-cgroups` killed
  every managed nix-daemon, so functional and differential errored and
  the wirelog tests waited out 30 s timeouts.

  The `checks.*` derivations are gates: non-mutating, and each fails the
  build. `nix run --file . internal.fix` is the rewriter — never a gate.
  - **Subagent validation**: the `checks.*` line only. Not the suites; that is
    overkill per file change.
- **Type Safety**:
  - NEVER use string type hints (e.g., `"Store"`). Use `from __future__ import annotations` where needed for `TYPE_CHECKING` imports and forward references.
  - Use `if TYPE_CHECKING:` blocks for cross-module imports.
- **Imports**: All imports MUST be at the top of the file (or inside `if TYPE_CHECKING:` blocks). Lazy imports inside functions/methods are strictly forbidden unless they are absolutely necessary to break a circular import cycle. If you introduce a circular dependency, prefer refactoring (e.g., moving shared types to a neutral module) over lazy imports.
  - **Import ordering** (strict — ruff enforces this):
    1. `from __future__ import annotations`
    2. Standard library imports (grouped: `import X`, `from X import Y`)
    3. Third-party imports
    4. Local/pynixd imports (`from . import X`, `from ..module import Y`)
    5. `if TYPE_CHECKING:` block ( LAST among imports — only for type-only imports)
    6. Module-level constants
    7. Code (classes, functions, etc.)
  - `if TYPE_CHECKING:` blocks must contain ONLY type-only imports (imports not needed at runtime). Runtime local imports must NEVER appear after `if TYPE_CHECKING:`.
  - **Re-export pattern**: When re-exporting a name from another module (so it's accessible as `module.name` for consumers), use `from .module import Name as Name` — the `as` is required for re-export. Consolidate multiple re-exports into a single multi-line import block. Example:
    ```python
    from .constants import (
        PROTO as PROTO,
        MAGIC as MAGIC,
    )
    ```
  - **Asserts**: NEVER use `assert` statements outside of the `tests/` directory. For runtime validation, use explicit `if not cond: raise RuntimeError(...)`. To satisfy type checkers, use local variable aliasing or explicit `if cond is None` checks.
  - **Asyncio**: 
    - NEVER use `asyncio.get_event_loop()`. Use `asyncio.get_running_loop()` inside async functions. For timestamps, use `time.monotonic()` instead of `loop.time()`.
    - ALWAYS maintain a strong reference (e.g., in a class instance variable `set` or `list`) to background tasks created via `asyncio.create_task()`. Failure to do so allows the garbage collector to destroy the task mid-execution.
- **Trust**: `Role.ADMIN` is a trusted user, `Role.USER` an untrusted one (`pynixd/trust.py`, issue #56). Every upstream connection is root's, so nix-daemon applies none of its untrusted-user rules on it; pynixd applies each one in the operation's `handle`, with Nix's message and at Nix's point. An error Nix raises before `startWork` closes the session (`ClosingError`); one after it does not. Never answer a refused operation with a silent success. `execute` must always perform the actual upstream daemon operation.
- **HTTP Cache Streaming**: If a NAR transfer fails after the `200 OK` header is sent, the server MUST abruptly close the connection to signal failure to the client. Full buffering to avoid this is not supported due to memory constraints.
- **pathlib.Path**: Use pathlib.Path when dealing with any strings that aren't Nix daemon protocol related. Convert to string as late as possible if needed
- **Exception Visibility**: NEVER use bare `except Exception: pass` blocks for unexpected failures. All unexpected exceptions MUST be logged (e.g., `log.exception(...)` or `log.warning(...)`) to ensure hidden failures are visible in logs. If an exception is truly expected and should be ignored, use `contextlib.suppress(...)` with a comment explaining why.

## 5. Build Logic
Builds are the only "complex" operations in `pynixd`. They are handled via a global `BuildQueue` and a DAG-aware `Scheduler`. 
- `BuildPaths` and `BuildPathsWithResults` are decomposed into individual `BuildDerivation` requests.
- Each build executes in a spawned task, surviving client disconnects.
- Outputs are automatically pulled into the `LocalStore` upon successful completion.

## 5b. Where pynixd and Nix disagree: the two markers

**Matching the bytes of `nix-daemon` is a measure, and it is not the goal.**
The parity run puts one client against a daemon and against pynixd, and a
difference between the two recordings is how a divergence becomes visible. A
difference is not, by itself, a fault to remove. Nix is not perfect: C++
limits what its authors can do easily, and Python does not carry the same
limits.

**Every divergence gets a verdict, and "pynixd is right" is one of them.** A
comment that reads as if Nix is the specification hides the difference between
"pynixd does this because it is right" and "pynixd does this because the
parity run compares the bytes". It also gives a later reader no place to start
a correction.

There are three verdicts, and two markers:

| Verdict | Marker |
| --- | --- |
| Nix is wrong, and pynixd copies it | `NIX-DEFECT (#23):` |
| Nix is wrong, and pynixd answers correctly | `NIX-DEFECT (#23):` |
| Nix is right, and pynixd answers differently | `NIX-DEVIATION (#27):` |

### `NIX-DEFECT (#23):`, for a defect of Nix

Write the tag exactly, and give four parts:

1. the mechanism in Nix, with the file and the line, in back quotes;
2. what the mechanism gets wrong;
3. what pynixd could do instead;
4. why pynixd still copies it, or how pynixd already deviates.

Report the defect on the fork, which is `Lillecarl/nix`, and name that report
in part 4. Do not report it upstream.

### `NIX-DEVIATION (#27):`, for a decision of pynixd

Nix is right here, or neither answer is wrong, and pynixd answers differently
on purpose. Write the tag exactly, and give four parts:

1. the mechanism in Nix, with the file and the line, in back quotes;
2. what pynixd does instead;
3. why the difference is worth its cost;
4. what a reader must measure to reverse the decision.

### A divergence that makes pynixd better

**Keep it.** Do not make pynixd worse to match a byte. pynixd already
schedules, caches and removes duplicate work differently, and none of that
reaches the client. An answer that is more complete than the answer of Nix is
the same kind of decision, and it does reach the client.

Four things make such a divergence complete:

1. a measurement that states what each side answers, and why;
2. a marker of one of the two kinds above;
3. a pytest of this repository, which `### Mirror each divergence from the
   functional suite` asks for;
4. an entry of `EXEMPTIONS` in `wirelog/diff.py`, so that the parity run stops
   reporting the difference.

**Scope the exemption so that it cannot hide a difference that nobody
explained.** `EXEMPTIONS` keys on the name of a field today, and a whole field
is too wide when only some of its differences have a reason. An exemption on
`response.will_build` covers the seven differences of issue #26, and it
covers the eighth one as well, which has another cause. When a whole field is
the only key available, that is a fault of the comparison, and it is not a
reason to widen the exemption. Issue nixidae/nixidae#241 holds that work.

**Do not add a "Nix-correct mode" and a "correct-result mode".** A mode splits
every behaviour in two, and the parity run can prove only one of them. A flag
earns its place when one entry of a list has a measured cost, and the flag is
then scoped to that entry.

Issue #23 and issue #27 hold the two lists.
`tests/meta/test_nix_defect_markers.py` finds every marker and reads the two
parts that a machine can read: the tag names its tracking issue, and the
paragraph names a file of Nix.

## 6. Execution Sanity & Recovery
- **Halt on Ambiguity**: If a tool output indicates potential corruption (e.g., duplicate declarations in a `replace` output, unexpected truncations), or if you lose track of the file state relative to the VCS, **STOP immediately**. Do not attempt blind recovery (like `write_file` with partial content).
- **Verify Before Rewrite**: Before using `write_file` to "fix" a large file, you MUST have read the *entire* file in the current turn to ensure no data loss.
- **VCS Truth**: If `jj status` or `jj diff` contradicts your internal model of the changes, re-sync by reading the files from disk before taking further action. Do not guess.

## 7. Documentation & Onboarding
- **Glossary**: A comprehensive `GLOSSARY.md` is maintained in the repository root. It defines both foundational Nix terminology (NAR, StorePath, etc.) and `pynixd`-specific concepts (Trampolining, Three-Tier Execution).
- **Living Document**: The glossary is NOT exhaustive. All contributors (including AI agents) MUST expand the glossary when introducing or clarifying complex architectural patterns, specialized terminology, or non-obvious domain concepts.

## 8. Test Suite Rules

### Directory Structure
- **`tests/functional/`** — active end-to-end and integration tests
- **`tests/benchmark/`** — performance benchmarks
- **`tests/legacy/`** — old tests, do not modify

### Test Store Conventions
- All test stores MUST use the `/tmp/pynixd-stores` prefix (defined as `STORE_PREFIX` in `tests/conftest.py`).
- Use `rmtree_robust_glob(f"{STORE_PREFIX}*")` in fixtures to clean up leftover stores.
- Only a few select tests should run against the root store (`store_path=Path("/")`). Most tests should use isolated stores with the prefix.

### Test Helpers
- **`run_captured(cmd, **kwargs)`** — runs a subprocess, returns `(rc, stdout, stderr)`.
- **`run_logged(cmd, **kwargs)`** — runs a subprocess, streams output through structlog in real-time.
- Both helpers auto-set `NIX_SSHOPTS` if not already present.
- Use `env.str("NIX_BIN", "nix")` from the `environs` singleton for the Nix binary.

### Test Design
- Keep tests simple and explicit. Avoid over-engineered abstractions.
- Construct commands as plain lists so the exact invocation is visible at a glance.
- Use `pytest.fixture(autouse=True)` for per-test cleanup (store directories, etc.).

### Mirror each divergence from the functional suite

The Nix functional suite finds each place where pynixd does not match Nix.
That suite is slow, it needs a Linux builder, and it runs against a matrix of
Nix versions. It does not run on each change.

**A correction is not complete until a pytest of this repository asserts the
same thing.** The pytest is what stops the divergence from coming back with no
signal.

Name the assertion that the test stands for. Give the file and the line of the
functional suite, in the docstring of the test. A reader then finds the
upstream assertion, and also the reason that the test exists.

Copy the shape of these tests:

| Test | Assertion of the suite |
| --- | --- |
| `tests/unit/test_build_log_replay.py` | `build.sh:167` |
| `tests/unit/test_derived_path.py` | `build.sh:91` |
| `tests/unit/test_build_paths_goals.py` | `build.sh:8` |
| `tests/unit/test_keep_going_and_max_jobs.py` | `build.sh:247`, `build.sh:269` |
| `tests/unit/test_dispatch_order.py` | `build.sh:167`, `build.sh:176` |
| `tests/unit/test_ensure_child_logs.py` | `build.sh:167` |
| `tests/parity/test_wire_parity.py` | `build.sh:91` |

The functional suite proves the bytes of a run. A test of this kind proves the
decision that makes those bytes, and it runs in one second.

**A divergence that pynixd keeps needs a marker as well.** Section 5b gives
the two, and the verdict picks one: `NIX-DEFECT (#23):` when Nix is wrong,
and `NIX-DEVIATION (#27):` when Nix is not. Do not write `NIX-DEFECT` for a
place where Nix is right, because the tag then states a defect that nobody
found.

The pytest says what pynixd does. The marker says why the two differ, and it
is the register that a later reader reads to reverse the decision.

### Running Validation Commands
- **Run the suites in `tests.guest`**, not with host pytest; section 4 says why. Each suite has a guest of its own, so they run at once without sharing store paths or sockets.
- **Single host pytest only**: if you run host pytest anyway, NEVER run more than one `pytest` process at a time. Functional tests share session store paths, daemon sockets, and `/tmp/pynixd-stores` state; concurrent host runs race each other and produce misleading failures that look like real regressions.
- **NEVER pipe away output** from the `checks.*` build or from `pytest` — the full output contains failure details you need to diagnose issues.
- If the user explicitly tells you not to pipe or select on output, YOU MUST DO WHAT THEY SAY. No exceptions. Do not override their instruction with this rule's redirect-to-file fallback — they want to see the output directly.
- If output is too large for context (failing tests produce heaps of logs), you may redirect to a file: `pytest ... > /tmp/test-output.txt 2>&1`, then read specific sections. But if the user told you not to redirect, you must not redirect.
- Do NOT use `tee` when redirecting — it doubles context consumption.
- If you must limit output, use `tail -N` on the file afterwards, never pipe the command itself.
- You do NOT need to specify pytest timeout, the configured 120s is enough per test.
- **Timings in container guests** (one run): unit 13 s, protocol 3 s, parity 17 s, differential 22 s, functional 109 s. Run the session in the background and let the monitor report.

## 7. User Direction Supersedes All Rules

Every instruction in this file is a default. The user's explicit direction overrides any and all of them, always.

- If the user says "don't pipe away output", you do not pipe away output — not to a file, not to tail, not to anything. You show them the full output directly.
- If the user says run something a specific way, you run it exactly that way. You do not second-guess or substitute your judgment for theirs.
- If you're about to do something and you realize the user already told you not to, stop immediately and do what they said.
- **No self-persuasion**: If they told you to do X and the rules say Y, you do X and update the rules later if needed.

## 8. Async Task & Lifecycle Rules

Structured concurrency everywhere. `checks.lint` bans the asyncio task API (`nix/shared/ruff-anyio.toml`); `tests/` is exempt until nixidae/nixidae#1144 converts it.

- Every task runs in an `anyio` task group that some caller owns. No `asyncio.create_task`, `gather`, `wait_for`, `Future` hand-off or `shield` in new code.
- Short-lived fan-out: `async with anyio.create_task_group()` in the function that needs it.
- Long-lived loops: started into the owner's task group. `Server` owns one for its lifetime (`async with Server(...)`); `Server._spawn(fn, *args)` starts into it. `Server.start()` alone cannot run background loops.
- A long-lived loop that raises takes its owner down. A loop catches the errors it expects; an unexpected one ends the daemon, and systemd restarts it.
- Timeouts: `anyio.fail_after` / `move_on_after`. Cleanup that must finish under cancellation: `anyio.CancelScope(shield=True)`.
- A site that stays unstructured carries a comment saying why. Rare: a callback API with no async form (`loop.call_later` for write lingering), or an asyncio-only library (asyncssh).
