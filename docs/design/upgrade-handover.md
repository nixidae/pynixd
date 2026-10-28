# Upgrading pynixd without losing builds

Status: proposal, 2026-10-22. Nothing here is implemented in pynixd. The
measurements come from `tests.handover` (`tests/derivations/handover`), a
vivarium UML guest with pynixd in `replace` mode and small toy units
(`tests/handover/toy.py`). Each toy proves one thing that systemd or
nix-daemon does. None of them is pynixd.

Run it:

```sh
nix build --file . internal.tests.handover.driver   # then run its bin/ with --out <abs dir>
```

Each fact below is marked **measured** (a `[measure]` line of that session)
or **read** (source or documentation, not run). The logs are in the session's
`--out` directory: `log` holds every `[measure]` line. The numbers here are
from run 8, the last full run; runs 5 to 7 gave the same results within a
second.

## Recommendation

1. **Now: set `stopIfChanged = false` on `pynixd.service`.** One line in
   `nix/nixos/default.nix`. A switch then restarts pynixd and leaves
   `pynixd.socket` bound. Clients that connect during the switch wait in
   the backlog instead of failing. In-flight builds still die.
2. **Next: hand over on reload (approach B).** `reloadIfChanged = true`,
   and an `ExecReload=` that starts the new pynixd beside the old one. The
   old one stops accepting, finishes its sessions and builds, and exits.
   This is the only approach that keeps every kind of work: client
   sessions, queued builds, local builds, remote SSH builds and reverse
   builders. It costs a drain mode in `Server`, listener hand-over, and a
   rule for the overlap of two schedulers.
3. **Not now: the file descriptor store (approach A) and a persisted queue
   (approach C).** A works for local builds only, and needs pynixd to
   serialise every session and connection at a frame boundary. C keeps
   nothing a client can see.

The reasons are in the comparison table and the sections after it.

## What a restart does today

| Fact | How known |
| --- | --- |
| `systemctl restart pynixd` with a user's build running for a moment: the client fails with `error: pynixd: server is shutting down` (rc 1). The restart takes 2.0 s. | measured, `restart` phase |
| The build in nix-daemon dies with pynixd's connection: no builder 3 s later, the upstream journal says `interrupted by the user`, and the output is not valid after the build's whole length. | measured, `restart` phase |
| `RuntimeDirectory=pynixd` is emptied on restart: a file in `/run/pynixd` is gone. | measured, `restart` phase |
| pynixd keeps no queue on disk. Its tables in `db.sqlite` hold statistics, access times and liveness, not work. | read, `db_migrations.py` |
| `Server.close` closes the listeners, answers every session with an error (`sessions.py`), then cancels every build (`Scheduler.stop` leaves `workers()`). | read, `instance.py`, `scheduler.py` |

So a restart loses everything in flight. A client of `nix build` does not
retry: its command fails.

### Why the build dies: nix-daemon watches its client

**read**: `processConnection` in `src/libstore/daemon.cc` starts a
`MonitorFdHup` on the client's descriptor. A hang-up interrupts the worker,
which kills the build. `nix-daemon --stdio` (an `ssh-ng://` builder) runs the
same `processConnection` on stdin, so a remote build dies the same way when
its SSH channel closes.

**measured** (`hangup` phase, raw protocol in `toy.py direct`):

| Client | Builder 3 to 5 s later | Output |
| --- | --- | --- |
| closes its connection mid-build | dead; journal says `interrupted by the user` | never valid |
| forks; parent exits; child holds the fd and reads nothing for 15 s | alive | valid; the child read `ok` after 35.5 s |
| same, with a build that writes 200 000 log lines while nobody reads for 20 s | not checked | valid; result after 38.7 s |

The last row shows that an unread connection does not kill a chatty build.
It does not show whether the build stalled while nobody read.

So nix-daemon watches the descriptor, not the process. A build survives
the death of pynixd if any process still holds the connection. A new
process can then read the rest of the answer from the middle of the
operation: the toy adopted the connection after `STDERR_START_ACTIVITY`
for the build, and read the log, `STDERR_LAST` and the result.

## How `switch-to-configuration` drives pynixd

**read**, `switch-to-configuration-ng` `main.rs` `handle_modified_unit`: a
changed service with `X-StopIfChanged=true` (the NixOS default) goes into
the stop list, and so does every active `.socket` unit of the same name. The
socket is started again after activation. `stopIfChanged = false` gives a
single `restart` job instead. `reloadIfChanged = true` gives a `reload` job,
and wins over both when the unit is not also in the restart list. A changed
`.socket` unit is never restarted (`FIXME: do something?`).

**measured** (`switch-stop` and `switch-nostop` phases, a probe client
querying the daemon socket every 0.2 s during the switch):

| pynixd unit | s-t-c output | Probe queries during the switch | Switch time |
| --- | --- | --- | --- |
| default (`stopIfChanged = true`) | `stopping ... pynixd.service, pynixd.socket` and later `starting ... pynixd.socket` | 6 ok, **8 failed over 4.0 s**: `Connection reset by peer` | 8.6 s |
| `stopIfChanged = false` | `restarting ... pynixd.service` | 13 ok, **0 failed** | 8.1 s |

With the default, the daemon socket of the whole machine is closed from the
stop phase to the start phase, activation included. That window grows with
whatever else the switch does: in run 4, one reload that hung for 30 s
stretched it to 34.6 s and 99 failed queries.

In both rows the pynixd client's build had run for 0.6 s, and it died
(`server is shutting down`).

## Approach A: the file descriptor store

pynixd sends each client and upstream connection to systemd with
`FDSTORE=1`. The next pynixd gets them back in `LISTEN_FDS` and continues.

**measured** (`fdstore` phase, `toy.py fdstore`, `FileDescriptorStoreMax=4`):

| Transition | Parked build |
| --- | --- |
| `systemctl restart` | survives; the new instance adopts the fd and reads `ok` |
| `systemctl stop`, default `FileDescriptorStorePreserve` | dies 3 s later |
| `systemctl stop` then `start`, `FileDescriptorStorePreserve=yes` | survives the stopped unit; the new instance reads `ok` |
| s-t-c, default `stopIfChanged` (stop and start) | lost; the new instance found no fd and started the build again |
| s-t-c, `stopIfChanged = false` (restart) | survives; adopted, `ok` |
| s-t-c, `FileDescriptorStorePreserve=yes` (stop and start) | survives; adopted, `ok` |

So systemd carries the descriptors through an upgrade, if the unit has
`stopIfChanged = false` or `FileDescriptorStorePreserve=yes`.

What pynixd would have to write down beside each descriptor (**read**):

- A client session (`DaemonProxy`): negotiated version and features, role
  and user name, schedule mode, the last `SetOptions`, its temporary roots
  (a flock'd file per session, `temp_roots.py`, released when the old
  process dies and to be written again), and the operation in flight with
  its request.
- An upstream connection: version, features, which operation it is in,
  and the build it serves.
- Bytes already read but not parsed. pynixd reads through asyncio streams,
  so the old process must stop each reader at a frame boundary and pass its
  buffer on. The toy reads raw from a blocking socket and has no buffer.
- The queue (`QueuedBuild`): request, options, wanters, retries, failures
  per store. The new scheduler rebuilds the DAG from that.

What it cannot carry: SSH connections. `store/ssh.py` and the reverse
transport use asyncssh in process, so the encryption state dies with the
process. A remote build dies with it (`nix-daemon --stdio` sees the
hang-up). Reverse builders reconnect with backoff (`reverse_client.py`).

Cost: high. Every operation must be resumable or replayable. A client
blocked in `BuildPaths` could have its request run again by the new
process, since a build of valid paths is a no-op; most other operations
are short and could be failed. Gain: local builds and client sessions
survive. Remote builds do not.

## Approach B: hand over on reload, then drain

The new pynixd starts beside the old one and takes the listeners. The old
one stops accepting, finishes what it has, and exits.

**measured** (`switch-*` and `overlap-restart` phases, `toy.py
overlap-*`, `Type=notify`, `NotifyAccess=all`, `reloadIfChanged = true`):

1. s-t-c sends a **reload**, not a restart (`reloading the following
   units: ... toy-overlap.service`). The switch exits 0 and does not wait
   for the drain.
2. `ExecReload=` comes from the **new** unit file, after `daemon-reload`.
   So it names the new store path and carries the new `Environment=`. That
   is how the old process learns where the new binary is.
3. The reload process gets the listening socket from the old main over a
   Unix socket (`SCM_RIGHTS`), forks the new binary with it as fd 3 and
   `LISTEN_PID` set to the child, and waits for it. The child sends
   `MAINPID=<own pid>` and `READY=1`. systemd moves the main PID (1265 to
   1523) and logs `Supervising process 1523 which is not our child`. The
   unit stays `active`.
4. The old process gets `drain`, closes its listener and keeps working. Its
   in-flight client got `ok` from the old process (`A 1265 ok 1`). A new
   client got its answer from the new one (`B 1523 ok 1`). The old process
   exited by itself when its last client finished.
5. A later `systemctl stop` killed the new main normally. systemd's line
   `Unit process ... remains running after unit stopped` is a race in its
   log: the pid was gone when checked.
6. **A restart during the drain kills the old process.**
   `KillMode=control-group` covers every process of the unit. Its client got no answer
   (`overlap-restart` phase). So a `systemctl restart`, or a switch to a
   configuration that restarts rather than reloads, ends the drain.
7. **A crash of the new main is noticed, despite the warning in point 3.**
   `kill -9` of the handed-over main: 1 s later the unit is `activating`
   with `NRestarts 1`, 3 s later `active` with a new main
   (`overlap-crash` phase). The journal says `Main process exited,
   code=killed, status=9/KILL` and `Scheduled restart job`. So
   `Restart=on-failure` still works, and no watchdog is needed for this.
   **read**: the failed unit is cleaned up under `KillMode=control-group`,
   so the restart also ends a drain in progress. Not measured separately.

Why `Type=notify` with `ExecReload=`, and not `Type=notify-reload`:
`notify-reload` only sends a signal to the old main process (**read**,
`systemd.service(5)`). That process holds the old unit's command line and
environment, so it cannot know the new store path. `ExecReload=` runs from
the new unit file and knows both.

Both places to fork the new binary work: from the reload process
(`--spawn-by control`), or from the old main on request (`--spawn-by
old`). The second inherits the old process's environment, so it must be
given the new one. A trap found on the way: the inherited listener is often
fd 3 already, `dup2(3, 3)` is a no-op that keeps `O_CLOEXEC`, and the child
got `EBADF` (run 4).

What pynixd needs (**read**):

- **Listeners.** The daemon socket comes from `pynixd.socket`; it passes
  over as the toy's did. `inherited_listener` checks `LISTEN_PID`, so the
  spawner sets it in the child. pynixd binds the others itself: the reverse
  acceptor (TCP 2235 on dynhetz), HTTP (8080), SSH and metrics. The new
  process cannot bind them while the old one holds them. They pass over too,
  or bind with `SO_REUSEPORT`.
- **A drain mode in `Server`.** Today `close` errors every session and
  cancels every build. Drain is: close the listeners, stop the GC loop and
  any other periodic writer (`gc.py`; dynhetz has `gc_allow_execute`), keep
  sessions and the scheduler running until no session and no build is left,
  then `close`. With a deadline, because a session can live for hours.
- **Two schedulers for a while.** Each one counts only its own builds
  against a store's `build_limit` (`scheduler.py`, `_has_build_room`). For
  the overlap a store can get twice its limit. The new process can start
  with the old one's counts, or the old one can stop starting queued builds
  and hand them over (that is approach C, for the queue only).
- **The same derivation in both.** Locally, nix-daemon's output path locks
  make the second build wait and then find the output valid. On two
  different remote builders it can build twice.
- **Reverse builders.** Their SSH sessions stay with the old process. It
  should close each one when no build of its own runs on it; the builder then
  reconnects with backoff and registers with the new process.
- **Temporary roots.** Each process holds its own sessions' roots in its own
  flock'd files. No gap.
- **The database.** Both write `db.sqlite` in WAL mode, as several Nix
  processes already do.
- **The unit.** `NotifyAccess=all` (a child sends `MAINPID=`),
  `reloadIfChanged = true`, `ExecReload=`, and the config file still in
  `restartTriggers`: with `reloadIfChanged` a config change also reloads.

Failure modes: a restart or stop during the drain (point 6); a crash of the
new process while the old one drains (point 7: the unit comes back, the
drain ends); two schedulers overloading one builder. None loses data; each loses in-flight
work, which is today's behaviour.

## Approach C: persist the queue and resume

On SIGTERM pynixd writes the queue to disk; the next pynixd queues it again.

- The client's command fails anyway (`server is shutting down`), and a
  `nix build` does not retry. So the client gains nothing. **measured**,
  `restart` phase.
- A running build dies with its connection, unless approach A keeps the fd.
  **measured**, `hangup` phase.
- `/run/pynixd` is emptied on restart (**measured**). The file goes under
  `/var/lib/pynixd`, or `RuntimeDirectoryPreserve=restart`.
- CLAUDE.md section 5 says builds already survive a client disconnect. C
  adds survival of a pynixd restart for *queued* builds: the work completes
  and a retried `nix build` finds it done.

Cost: low (serialise `QueuedBuild` requests and options). Gain: small.

## Approach D: drain on stop

pynixd treats SIGTERM as drain, with a long `TimeoutStopSec=`.
`stopIfChanged = false` keeps the socket bound, so new clients wait in the
backlog until the new process starts (0 failed probes, **measured** above).
But the restart job, and with it `nixos-rebuild switch`, waits for the
longest build: hours, on dynhetz. And every new client waits as long.

## Comparison

| | Today | `stopIfChanged=false` alone | A: fd store | B: reload hand-over | C: persisted queue | D: drain on stop |
| --- | --- | --- | --- | --- | --- | --- |
| Client sessions in flight | lost | lost | kept, if every op resumes | kept | lost | kept |
| New clients during the switch | fail for the whole switch | wait about 2 s | wait | served by the new process | wait | wait for the drain |
| Queued builds | lost | lost | kept | kept (in the old process) | kept | kept |
| Running local builds | lost | lost | kept | kept | lost | kept |
| Running remote (SSH) builds | lost | lost | lost | kept | lost | kept |
| Reverse builder registrations | reconnect | reconnect | reconnect | stay with the old process, then move | reconnect | stay until exit |
| Switch blocks for | seconds | seconds | seconds | seconds | seconds | the longest build |
| Code in pynixd | — | none (module line) | high: per-session and per-connection state, frame-boundary hand-off | medium: drain mode, listener passing, overlap rule | low | low |
| A `systemctl restart` by hand | loses all | loses all | keeps (fd store survives restart) | loses the draining process | as C | blocks as D |
| Evidence | measured | measured | measured (toy) | measured (toy) | measured (premises) | measured (backlog only) |

## Open questions for the owner

- How long may a drain run on dynhetz before the old process is killed?
  Builds there run for hours.
- Is twice the build limit on a store acceptable for the length of a drain,
  or must the new scheduler count the old one's builds?
- B makes a config change a reload too. Is there a setting whose change must
  restart pynixd at once (for example `mode`, which moves sockets)? Such a
  setting would go into `X-Restart-Triggers` alone, outside the reload path.

## Found on the way

- pynixd adds `https://cache.nixos.org/` as a store whatever `nix.conf`
  says (`instance.py`, `_default_http_substituter_urls`). In a guest with no
  network its query delays every build.
- A build whose stores the ranking drops (score under `min_schedule_score`,
  or free disk under `min_free_bytes`, 2 GiB) stays pending with
  `waiting_slot`, and nothing triggers a new pass. On the one-CPU guest with
  a 512 MiB root, every local build waited past 90 s. The session raises the
  disk and lowers the threshold so its builds run.
