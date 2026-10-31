# Nix's functional tests, against a daemon

Nix carries 203 functional test scripts. They encode what the daemon protocol
must do. pynixd sits in the middle of that protocol, so the scripts test
pynixd as well, when a pynixd daemon answers them.

Issue #19 holds the plan and the measurements. This file gives the mode and
the commands.

## The mode, and why Nix does not run it

Nix runs the suite two ways, and neither one tests the daemon fully:

1. The plain derivation runs with `NIX_REMOTE=''`. The client speaks to the
   local store, and no daemon takes part.
2. `tests/nixos/functional/common.nix` sets `NIX_REMOTE=daemon`, and it also
   sets `isTestOnNixOS=1`. **79 of the 209 scripts call `TODO_NixOS`**, which
   skips the test under that flag. `ca/common.sh` and `dyn-drv/common.sh` call
   it, so both suites skip there.

So the ca suite and the dyn-drv suite have **no daemon coverage upstream**.

This directory makes the third mode: one store for each test, as in mode 1,
and a daemon, as in mode 2. `setup.sh` applies four patches to reach it, and
each patch names its reason in the file. A fifth patch rewrote the layout of
the store of each test, and issue nixidae/nixidae#215 removed it.

## Run it

**`nixFunctionalTests` in `default.nix` builds one program for each supported
Nix version, and that program is the way to run this.** It carries its Nix,
the test scripts of that Nix, pynixd, and every tool the scripts need. Nothing
comes off the PATH of the machine.

```sh
nix build --file . internal.nixFunctionalTests.default --out-link result
./result/bin/nanopynix-nixft-nix_2_35 all
```

`nix_2_34`, `nix_2_35` and `git` are the versions, and they are the versions
that `supportedNixFloor` in `default.nix` selects. `default` is the one of
`nixVersions.latest`. The commands are:

| command   | what it does                                           |
| --------- | ------------------------------------------------------ |
| `setup`   | prepare the suite. Run it first. It wipes the work dir. |
| `control` | run the suite against a plain `nix daemon`              |
| `pynixd`  | run the suite against pynixd                            |
| `compare` | state which tests pynixd alone fails                    |
| `all`     | all four, in that order                                 |

The stream mode reads the wire instead of the verdict of each script:

| command          | what it does                                      |
| ---------------- | ------------------------------------------------- |
| `record-control` | run against a plain `nix daemon`, and record       |
| `record-pynixd`  | run against pynixd, and record                     |
| `diff-streams`   | state which tests differ on the wire               |
| `diff-store`     | state which tests differ in the store they left   |
| `streams`        | setup, both records, and both comparisons         |

### From a darwin host: `nixft.sh`

The suite needs Linux, and a darwin host reaches Linux through the `vzrun`
builder. Use `nixft.sh` there, and give it the same commands:

```sh
./pynixd/nix/functional-tests/nixft.sh pynixd --suite ca
./pynixd/nix/functional-tests/nixft.sh --work /scratch/nixft-full all
```

**`--dump PATH` writes a file of the builder to the terminal.**
`meson test --print-errorlogs` prints the tail of the log of a failed test and
not the whole of it, so a `log.debug` line that a request writes at its start
does not reach the terminal. The whole log is at
`build/meson-logs/testlog.txt`, and no directory of the builder survives the
run, so the file has to leave in the same invocation:

```sh
./pynixd/nix/functional-tests/nixft.sh --log-level DEBUG \
    --dump build/meson-logs/testlog.txt pynixd build
```

A path with no leading `/` is relative to the work directory. Give the option
again for a second file.

**`--nix-version` picks which Nix the suite measures.** `default` is the
default: `nixVersions.latest`, because pynixd runs against the newest release.
`nix_2_34` measures the floor that `supportedNixFloor` in `default.nix` names.

```sh
./pynixd/nix/functional-tests/nixft.sh --nix-version nix_2_34 \
    --work /scratch/nixft-234 all --suite ca
```

**2.35 is the only arm that can see the gap of issue #14.** The protocol
number stopped at 1.38 and each new capability is a feature name; 2.34 offers
none of them, so a run against 2.34 measures nothing about the features that
`DrvOutput` and `UnkeyedRealisation` now hang on. 2.35 offers
`realisation-with-path-not-hash`, so it is the version that reads what pynixd
does with a peer that names it.

Give a separate `--work` for each version. The work directory holds the
checked-out test scripts of one Nix, and `setup` runs only when the build
directory is absent.

**Keep the work directory outside `$HOME`.** One run of `all --suite ca` under
each of two paths, same checkout and same suite:

| work directory      | regressions | fails in the control as well |
| ------------------- | ----------- | ---------------------------- |
| `$HOME/nixft-ca`    | 2           | 8                            |
| `/scratch/nixft-ca` | 4           | 1                            |

Seven tests that a plain `nix-daemon` passes fail under `$HOME`: `build`,
`build-cache`, `nix-copy`, `nix-shell`, `repl`, `selfref-gc` and `signatures`.
Each one builds something.

**A build user cannot reach `$HOME`.** The two directories carry these modes:

```
drwx------ builder builder  /nix/.rw-store/home
drwxrwxrwt root    root     /nix/.rw-store/scratch
```

A sandboxed build runs as `nixbld1`, which is `uid=30001 gid=30000(nixbld)`.
That user is not `builder`, it is not in the group of `builder`, and mode 700
gives "other" no execute bit, so it cannot traverse into `$HOME`. `/scratch`
is 1777, which every user may traverse.

The consequence is the reason this table is here at all: the control run is
the measuring instrument of this suite, and a broken control hides a
regression rather than reports one. Two of the four real regressions read as
"fails in both" in that run.

**The builder is disposable, and the script takes that as the rule.** It shuts
down after 60 seconds with no open connection, and the next boot makes its
disk again from nothing. No directory of it survives that, `$HOME` included:
`$HOME` and `/scratch` are two paths on one ext4 image that the host
truncates on every cold boot. So the choice of name above buys no durability,
and the table is its whole reason.

Two rules follow, and the script is both of them:

- **One invocation does the whole job.** `nixft-remote.sh` runs inside the
  builder and seeds the fetch cache, builds the runner, prepares the work
  directory and runs the command. Each of the four is a no-op when its result
  is already there. A host script that built, then prepared, then ran, left a
  half-made work directory whenever a restart landed between two calls, and
  the next call then reported something unrelated.
- **One invocation holds one connection.** The shutdown counts open
  connections, so a suite that runs inside one invocation keeps the builder
  alive for its whole length.

`nixft-remote.sh` writes `NIXFT-DONE <code>` as its last line and always exits
0. That is how the host tells a failed test from a dead worker: a run with no
such line did not finish, whatever the transport reported, and `nixft.sh`
tries it again.

The builder reads the checkout as a store path, and not as a shared
directory, so the code under test is the code the host holds. `nixft.sh`
passes that path, and it reads `nixft-remote.sh` out of it as well.

Each further argument goes to `meson test`, so `control --suite ca` runs one
suite and `control gc fetchurl` runs two tests.

`NIXFT_WORK` names the work directory, and `JOBS` gives the number of tests at
a time. The default is one.

**Match the version of the daemon to the version of the tests.** The scripts
travel with the *client*, so the suite states the version of the client. Each
program above holds one Nix and uses it for both, which is why the version is
in its name.

**The tests build derivations, so they need Linux.** On a Darwin host, build
the program in the Linux machine and then run the same store path there. The
store is shared, so one build serves both.

```sh
# On the host. `source.nix` filters the checkout into the shared store.
SRC=$(nix eval --raw --impure --expr \
    '(import ./nix/source.nix { lib = (import <nixpkgs> {}).lib; })')

# In the machine.
vzrun \
    nix build --file "$SRC" internal.nixFunctionalTests.default --no-link --print-out-paths

# `/scratch` of the machine is on a disk. `/` is a tmpfs, and the stores of the
# tests do not fit in it.
vzrun env NIXFT_WORK=/scratch/nixft-2.35 \
    /nix/store/...-nanopynix-nixft-nix_2_35/bin/nanopynix-nixft-nix_2_35 all
```

**Give the machine the fetch cache of the host, once for each start of the
machine.** The evaluation reads the lockfile and fetches the
`flake-compatish` input from the GitHub tarball endpoint. GitHub answers `429
Too Many Requests` to that endpoint after a few runs, and a token does not
raise the limit: the limiter is the anti-scraping one, and it counts the
address. `nix` then waits 69 s, 121 s, 259 s and 573 s between the tries,
which is longer than the test run.

The host already holds the answer in `~/.cache/nix`. The machine wipes its
home on a restart, so it asks GitHub again every time. The store is the one
thing the two share, so send the cache through it:

```sh
GIT_CACHE=$(nix store add-path ~/.cache/nix/tarball-cache-v2 --name nix-tarball-cache)
FETCHER=$(nix store add-path ~/.cache/nix/fetcher-cache-v4.sqlite --name nix-fetcher-cache)

vzrun sh -c "
    mkdir -p \$HOME/.cache/nix
    rm -f \$HOME/.cache/nix/fetcher-cache-v4.sqlite*
    cp -f $FETCHER \$HOME/.cache/nix/fetcher-cache-v4.sqlite
    cp -r $GIT_CACHE \$HOME/.cache/nix/tarball-cache-v2
    chmod -R u+w \$HOME/.cache/nix
"
```

Remove the `-shm` and the `-wal` file of the SQLite database as well. A
database that arrives beside the journal of a different run reads as corrupt,
and `nix` then fetches again.

## How pynixd takes the place of the daemon

`run.sh` reads `NIX_DAEMON_PACKAGE`, which is the hook Nix already has for
this: `tests/functional/package.nix` takes a `test-daemon` argument, and Nix
builds `nix-daemon-compat-tests` from it. `make-shim.sh` builds a package
whose `bin/nix` sends `daemon` to pynixd, and every other command to the real
Nix. `db-migration.sh` and `user-envs-migration.sh` call
`$NIX_DAEMON_PACKAGE/bin/nix` for ordinary commands, and `isDaemonNewer` calls
it for `daemon --version`, so a shim that answers `daemon` alone is not
enough.

**A passing test proves nothing until pynixd was in the path.** Three defects
in that one decision made the whole suite report success while the shim sent
every daemon to the real Nix. pynixd writes `pynixd-test-config.json` beside
each test store, so the `pynixd` command counts those files and fails when it
finds none.

**Compare against the control.** A test that fails in both runs is not a
defect of pynixd. Only a test that passes the control and fails through pynixd
is one. `compare.py` states that difference, and `compare` calls it.

## The stream mode

**A script says "pass" or "fail" for reasons that are not the wire.** It reads
a message, it counts the store paths on the disk, it wants a path to be dead.
The contract of pynixd is narrower: a client must not be able to tell pynixd
from `nix-daemon`, and that is a statement about the bytes.

So `streams` runs the same workload twice with a recorder between the client
and the daemon, and compares the two streams of operations. A test whose
script fails in both runs still gives an answer here.

```
client -> $NIX_DAEMON_SOCKET_PATH -> recorder -> iSocket -> the daemon
```

`make-record-shim.sh` builds the recorder into the `NIX_DAEMON_PACKAGE` place,
over an inner package. The inner package is the plain Nix in one run and the
pynixd shim in the other, so the two runs differ in the daemon and in nothing
else. The recorder starts the inner daemon as its own child, so `killDaemon`
kills one pid and both go away.

The recordings go to `$NIXFT_WORK/streams/{control,pynixd}/<suite>/<test>/
daemon-N/conn-NNNN.wire`. `<suite>` and `<test>` come from `TEST_SUITE_NAME`
and `TEST_NAME`, which meson sets, so the two runs write the same names.
`daemon-N` counts the daemons of one test, because `restartDaemon` starts a
second one.

**Some differences are on purpose, and `wirelog compare` lists each one with
its reason.** The name and the version of pynixd, the protocol version it
presents, the features it adds, the trust it reports, and the registration
time of a store path. The last one is a difference of the two runs and not of
the two daemons: they add the same path at two times.

**A build holds its temporary roots for the lifetime of the connection, and
the GC tests read that as a defect.** Issue #20 records the decision: a
long-lived daemon that holds a path is not a fault, and `max_lifetime` bounds
how long it lasts. `simple`, `gc`, `ca/gc`, `dependencies`, `build-delete`,
`gc-concurrent`, `optimise-store`, `multiple-outputs` and `selfref-gc` differ
for this reason and will keep differing.

The first run of this mode found issue nixidae/nixidae#216: `BuildPaths` answered a status
where Nix answers a constant `1`, so a failed build read as a successful one.
No script could find it, because the client of Nix reads that number and drops
it.

### The store half: `diff-store`

**A daemon can answer every request with the same bytes and still leave the
wrong store.** The wire says what the two daemons *said*; this says what they
*left*. The two measures are independent, so `streams` runs both and neither
one ends the run.

`store-state.py` reads `$TEST_ROOT/var/nix/db/db.sqlite` of every test, and
the comparison is over four tables of Nix:

| table | what a difference means |
| --- | --- |
| `ValidPaths` | a path that one engine registered and the other did not, or registered with another hash, size, deriver, content address or `ultimate` |
| `Refs` | a reference that went missing, which a `BuildResult` cannot show |
| `DerivationOutputs` | the two engines disagree on the output map of a derivation |
| `Realisations`, `RealisationsRefs` | a CA output realised differently, or realised a different number of times |

It reads the database and not a client, for three reasons. The stores are dead
when the comparison runs, so a client would have to start 200 more daemons.
The database sits under both engines, so a defect in a reader above them
cannot hide a difference. And pynixd writes its own tables into the same file
-- `PynixdSchema`, `PynixdDerivationStats`, `PynixdPathAccess` -- which are
the work of the proxy and say nothing about the store.

`registrationTime` and `sigs` are dropped. The first is a wall clock that two
runs can never agree on, and the second names the key of the store that
signed. `pynixd/tests/differential/snapshot.py` states the same two reasons.

**The snapshot is taken when each arm ends, and not at the comparison.**
`run.sh` wipes `$WORK/tmp` when the *next* run starts, so the stores of the
control run are gone before the candidate run ends. The two snapshots go to
`$NIXFT_WORK/streams/{control,pynixd}-store.json`, beside the recordings and
for the same reason.

`pynixd/tests/unit/test_store_state_reader.py` holds the negative controls: a
reader that answered "the two agree" whatever it was given would pass every
run and measure nothing.

#### Five tests this comparison cannot answer

**Some tests do not build the same thing twice, and no engine is the reason.**
A second run of one arm disagrees with the first.

The floor was measured, not guessed. Two runs of the same arm, Nix 2.34:

| control against control | `ca/duplicate-realisation-in-closure`, `ca/nix-shell` |
| --- | --- |
| pynixd against pynixd | the two above, and `ca/build` |
| control against control, `main` | `main/nix-shell`, `main/structured-attrs` |

`ca/duplicate-realisation-in-closure` builds a `current-time` derivation, so
each run makes another content address. `ca/build` registers one realisation
twice in one run and once in the next, so the row count moves.

**The other three are one cause.** Each builds an `-env` derivation, which is
a dump of the build environment, and that environment holds `NIX_BUILD_TOP`:

    .../var/nix/builds/nix-<pid>-<random>

A new string in every run, so the output holds new bytes. The store path does
not move with it, because the derivation is input-addressed: the same
derivation keeps the same name and answers a new NAR hash. The strings are
also of different lengths, which is the `nar_size` delta of a few bytes that
comes with each one.

`NOISE` in `store-state.py` holds the five, and the comparison counts them in
neither column. It prints `NOISE` for each and writes its differences to the
report anyway, with the reason beside the name: a difference nobody can see is
how a real one goes unnoticed once somebody widens the list.

**A test in this list cannot be called "same" either**: when it agrees across
the two arms, that is one draw agreeing with another.

So the summary of this mode reads `same`, `different`, `missing`, `extra` and
`noise`, and the first run over `ca` read 17, 3, 0, 0 and 3.

`missing` and `extra` are different findings and used to share one counter,
which made the full-suite run say "missing: 8" when it missed nothing. Each
`EXTRA` line carries the path count of the store, because zero paths is a
state directory pynixd created and Nix did not -- issue #42 -- and any other
number is something else.

**`NOISE` is not an exemption table.** An exemption covers a difference
somebody explained and decided to keep, under one of the two verdicts of
`CLAUDE.md` section 5b. This covers a test that measures nothing, which
needs no verdict. There is still no exemption table here -- see below.

#### What the first run found

`ca/import-from-derivation` differs in the store, and `diff-streams` calls it
identical. It is one of the four tests the wire comparison passed. That is
the whole reason this half exists, and it showed on the first run.

Half of it is fixed. pynixd registered the corrected realisation id only when
the derivation "needed" one, and an input-addressed output never does, so the
id the client reads never reached the store. Issue #40, and the store
comparison no longer reports the missing key.

What remains is an *extra* key: pynixd resolves this derivation where Nix
does not, and the upstream daemon registers a realisation for what it was
asked to build. The control store proves Nix never resolved it -- two
`DerivationOutputs` rows, and no resolved derivation. That needs a verdict
rather than a fix, and the verdict needs the table below.

`ca/build-with-garbage-path` and `ca/nix-run` are one finding, not two.
pynixd registers realisations for the `dev` and `foo` outputs of a
multi-output CA derivation, and the daemon leaves them unregistered. This is
the store-side view of the `QueryDerivationOutputMap` difference that the
wire comparison already reports, and issue #39 holds the verdict.

**This comparison has no exemption table.** `wirelog/diff.py` carries
`EXEMPTIONS` for the differences that have a reason, and nothing here does,
so a difference with a verdict reports for ever. Issue #39.

### The same mode, without the suite

`pynixd/tests/parity/test_wire_parity.py` runs one small workload the same
way, with the same recorder and the same comparison. It needs no Linux and no
builder of Nix, so it runs in the dev shell of any host and it takes eight
seconds.

It found four more defects, and each one is a difference that no script of
the suite reports:

1. `nix store gc` deleted nothing through pynixd. An idle pooled connection
   kept a worker of the daemon alive, and that worker held the temporary root
   of the path. Issue #20.
2. `QueryPathInfo` answered `sha256:<digest>` where `nix-daemon` answers the
   digest alone. The fast path of pynixd read the `narHash` column of the
   database, and that column carries the name of the algorithm. No client
   complained, because `Hash::parseAny` reads both forms. The signature of a
   path did complain: `ValidPathInfo::fingerprint` at `path-info.cc:48` puts
   the base-32 digest in the string it signs, and pynixd signed the base-16
   one, so every signature that pynixd made was false.
3. The second `nix build` of a content-addressed derivation answered
   `willBuild: [cad.drv]` to `QueryMissing`, and `nix-daemon` answers an empty
   set. `Store::queryMissing` reads `queryPartialDerivationOutputMap` at
   `misc.cc:217`, which answers from the realisation when the derivation names
   no output path. The client took a different code path after that answer, so
   every operation after it differed too.
4. `nix build --rebuild` and `--repair` both failed. The goal system of pynixd
   raised `RuntimeError` for `BuildMode.CHECK` and for `BuildMode.REPAIR`, so
   those two commands, and `nix-store --realise --check`, `--repair-path` and
   `--verify --repair`, all failed through pynixd and passed through
   `nix-daemon`. Neither mode is a build that pynixd can schedule, so the
   request goes straight to the local store now.

**The lesson is the size of the workload.** Sixty commands find what 207
scripts do not, because a script reads its own exit status and this reads the
bytes.

## The measurement

Client, scripts and daemon all Nix 2.34.8. One serial run each, on Linux,
with the relocated store layout that the suite itself sets.

| run     | OK  | FAIL | SKIP |
| ------- | --- | ---- | ---- |
| control | 159 | 12   | 36   |
| pynixd  | 158 | 13   | 36   |

**1 regression**: a test that the control passes and pynixd fails.

| test              | why                                              |
| ----------------- | ------------------------------------------------ |
| `main:store-info` | permanent, and the next section gives the reason |

**The other two regressions are gone, and issue nixidae/nixidae#246 took them.**
`ca:new-build-cmd` and `main:build` were **one assertion**, at
`build.sh:167`: `new-build-cmd.sh` sources `build.sh`, so both arrived at the
same line. The root goals of one request now enqueue their builds in the
order of `Goal::key()`, which is the order that `Worker::run` steps them, so
`-j1` over the four failing derivations of `fod-failing.nix` names x1 the way
Nix does. `dispatch_order.py` holds the reason and the measurement.

`main:multiple-outputs-substitute-failure` moved SKIP to FAIL. Issue #24
holds it: the managed upstream daemon did not start, which is a fault of the
harness rather than of the subject of the test.

`main:multiple-outputs`, `main:gc-concurrent` and `main:build-delete` come and
go between runs, and all three are the #20 family. Read a single failure of
one of them as a report of that issue, and not as a new one.

**12 tests fail in both arms.** Those are a defect of Nix or of this harness,
and not of pynixd. `main:build-remote-*` is eight of them, because the suite
has no second machine to build on here.

**The control moved from 151 to 159, and pynixd is not the reason.** The
control arm runs a plain `nix daemon` and it reaches no part of pynixd, so
read this as a measurement of a different machine on a different day. It is
`main:db-migration`, `main:fetchurl`, `main:json` and `main:tarball` that
still fail in both arms, beside the eight `main:build-remote-*` tests. The
count of tests that fail in both arms went from 20 to 12, and this run did not
record which eight moved. A control that fails hides a regression, so read a
control that improves as one that stops hiding.

### The `ca` suite alone

The whole suite takes about 25 minutes, and one suite takes about two. This
is the shorter loop, and the numbers are for `--suite ca`.

| run     | OK  | FAIL | SKIP |
| ------- | --- | ---- | ---- |
| control | 20  | 0    | 4    |
| pynixd  | 20  | 0    | 4    |

**The `ca` suite has no regression left, and no failure in either arm.**
`new-build-cmd` passed when issue nixidae/nixidae#246 gave the root goals the order of Nix,
and `ca:recursive` passes in both arms now. It failed in both before, so
pynixd did not take it.

### The same suite against Nix 2.35: 17 regressions

`--nix-version nix_2_35`, same checkout, same suite, one run of each arm:

| run     | OK  | FAIL | SKIP |
| ------- | --- | ---- | ---- |
| control | 19  | 1    | 4    |
| pynixd  | 2   | 19   | 3    |

The control is the same as at 2.34: Nix has no trouble with itself. pynixd
answers **2 of 24**, and the two that pass are
`derivation-advanced-attributes` and `derivation-json`, neither of which
builds anything. Every test that touches a realisation fails, with one
message:

```
error: the daemon is missing the 'realisation-with-path-not-hash' protocol
feature, needed to support content-addressing derivations
```

**That was issue #14, measured.** The protocol number stopped at 1.38 and
each new capability is a feature name. 2.34 offers none of them, so every
measurement of this suite before that one was taken against the one version
that cannot see the gap.

### After the claim: 4 regressions

pynixd names `realisation-with-path-not-hash` now, and every request that the
feature touches carries both shapes with a gate that picks one. Same command,
same checkout:

| run                   | OK | FAIL | SKIP | regressions |
| --------------------- | -- | ---- | ---- | ----------- |
| before the claim      | 2  | 19   | 3    | 17          |
| the claim             | 15 | 5    | 4    | 4           |
| the built-output key  | 16 | 4    | 4    | 3           |
| the early cut-off     | 17 | 3    | 4    | 2           |
| the path spelling     | 18 | 2    | 4    | 1           |

The control answers 19 OK / 1 FAIL / 4 SKIP at every one of them.

**The two arms agree now.** The one regression left is `ca:new-build-cmd`,
and it is the same regression that 2.34 has, which issue nixidae/nixidae#235 holds. So the
feature work of issue #14 costs nothing that the floor does not cost too.

Run both arms after a change to a codec. They should give the same two
numbers, and a difference between them is the report.

**The floor did not move.** The same suite against 2.34 answers control
19/1/4 and pynixd 18/2/4 after the claim, which is the number above it. Run
both arms after a change to a codec: 2.34 says the floor holds, and 2.35 says
the feature works.

### What moved, and what moved it

| correction | measured |
| ---------- | -------- |
| nixidae/nixidae#234, the empty output path off the wire | `ca:build-cache` and `ca:issue-13247` stopped dropping the connection, and each moved to the behaviour the crash was hiding |
| nixidae/nixidae#236, the options of the client on a path that is added | `ca:signatures` FAIL to OK |
| nixidae/nixidae#235, a cap for `max-jobs`, and one replay for each client | every derivation is built once, and the log of a build reaches a client once |
| do not build a derivation whose input failed | `resolved_derivation_not_stored` gone from `main:build` |
| nixidae/nixidae#237, the daemon substitutes a content-addressed output | `ca:issue-13247` FAIL to OK |
| nixidae/nixidae#226, the plan sees a realisation that a substituter holds | `ca:build-cache` FAIL to OK |

Removing the layout patch of nixidae/nixidae#215 moved `nix-channel` from FAIL to OK in the
control run, so that patch was breaking a test of Nix on its own.
`nested-sandboxing` still fails, and its cause is not the layout.

The count was 40 before issues nixidae/nixidae#217, nixidae/nixidae#218, nixidae/nixidae#219, nixidae/nixidae#221, nixidae/nixidae#222, nixidae/nixidae#223 and nixidae/nixidae#224.
Issues #20 and nixidae/nixidae#214 then took it from 29 to 9, in five steps: 29, 19, 15,
11, 9. Each one is below.

`compare` reports no "other change" now. Three tests moved from SKIP to FAIL
in an earlier run -- `local-overlay-store:delete-duplicate`,
`local-overlay-store:stale-file-handle` and
`main:multiple-outputs-substitute-failure` -- and all three skip again, in
both runs. Issue #22 holds the first two, and the cause is not proven: the
run that reported them differs from this one in the corrections of #20 and
nixidae/nixidae#214, and in nothing that touches the shape of a store.

### A regression that stays

`main:store-info` reads the version out of `nix store info`, and it greps for
`Version: 2.34.8`. pynixd answers `Version: pynixd-0.1.0`, because it is not
Nix and it says so in the handshake. `wirelog compare` holds the same
difference as the `handshake.nix_version` exemption, and that exemption is on
purpose.

So this test counts as a regression in every run, and no correction is
planned. The GC tests above are the other permanent group.

### A log line that stays different

`nix-build` on a dynamic derivation writes the warning "Ignoring dynamic
derivation ... while querying missing paths" **twice** with `nix-daemon`, and
**once** with pynixd. Nothing else of that command differs.

Nix walks the request twice. The client calls `printMissing` at
`shared.cc:57`, which is one `QueryMissing` on the wire, and `Worker::run` at
`worker.cc:340` calls `store.queryMissing` again inside the daemon to warm
the cache of the substituters. The second walk answers the same question, and
each walk writes the warning. pynixd answers the request and runs no worker,
so it walks once.

**pynixd keeps one walk.** A second walk of the closure buys one log line, and
pynixd already holds the answer in its substitution queue. Writing the line a
second time without the walk would be a line that no work produced. The
marker at `pynixd/goals/query_missing.py:209` names the defect of Nix, and
issue #23 tracks the list of such markers.

No script of the suite reads this line, so no test moves either way. Issue
nixidae/nixidae#228 holds the measurement.

### The 12 control failures

**A failure here is a failure of Nix or of this harness, and not of pynixd.**
`compare` puts these under "FAILS IN BOTH" and keeps them out of the answer.
Report a genuine defect of Nix to `github/lillecarl/nix`.

- **8 `build-remote-*`** need a remote builder.
- **1 `db-migration`** — the script states its own condition: "This assumes
  that the `daemon` package is older than the `client` one". Both are 2.34.8
  here.
- **3 others** — `json`, `tarball` and `fetchurl`.

The control arm reads `159 OK, 36 SKIP, 12 FAIL` of 207 tests against Nix
2.34.8.

**The list held 20 names before, and eight of them pass now.** The four
recursive-nix tests are four of the eight: `main:recursive`, `ca:recursive`,
`dyn-drv:recursive-mod-json` and `dyn-drv:dep-built-drv-2`. The other four are
`structured-attrs`, `shell`, `formatter` and `nix-profile`. The shell that the
suite now has in its store is what the last group needed, which is the defect
that "A harness defect that hid a whole fixture" states below; `chroot-store`
and `nested-sandboxing` left the list for that reason already.

Measure this list again before you read a name in it. A name that leaves it
quietly turns a real regression into an expected failure.

### A number that hid a defect

`main:placeholders` passed through pynixd in every run before this one, and it
was never passing. pynixd answered `BuildPaths` with a number that said
"failed", `RemoteStore::buildPaths` of Nix reads that number and drops it, and
`placeholders.sh` reads the exit status alone. Issue nixidae/nixidae#216 corrected the answer
and the test moved to FAIL, with a real builder error under it. Issue nixidae/nixidae#217
holds that defect.

Measured both ways: with the correction the test fails, and with the
correction reverted and nothing else changed it reports `Ok: 1`.

### One test, and three defects under it

`main:placeholders` passes now. Reaching that took three corrections, and
each one made the next one visible. This is the shape of the work here, so it
is worth stating once:

1. **nixidae/nixidae#217.** pynixd sent the wanted outputs of a derivation and dropped the
   others. The daemon rewrites `builtins.placeholder <name>` for each output
   the derivation names, so `${placeholder "bin"}` reached the builder as a
   path that is not there. `main:placeholders` passes with this corrected.
2. **nixidae/nixidae#218.** An already-valid derived path answered with no realisation, so
   `nix build --json` wrote no `outputs` key. `main:build` read `null` at
   line 23.
3. **nixidae/nixidae#219.** The request held its derived paths in a set, so the answers came
   back sorted. `main:build` reads them by position at line 8. That test
   passed in one work directory and failed in another, with no change but the
   hash part of two store paths.

`main:build` reaches line 91 now, and it started at line 8. A defect in this
project hides the next one, so a test that moves forward is progress even
when it still fails.

### The same test, and four more defects

`main:build` reached line 167 after those three, and it reaches line 247 now.
Issue nixidae/nixidae#235 holds the four, and each one is a decision of the root-goal loop
that Nix takes differently:

1. **A substituter counted as a builder.** `_build_slots` returned the number
   of goals when any store was not the local one, so `max-jobs` reached the
   loop in almost no configuration. Almost every configuration holds a binary
   cache. The instrumented line reads `slots=4, goals=4, max_build_jobs=1,
   stores=["http-cache.nixos.org", "local"]`.
2. **The goals ran in the order of the request.**
   `DerivationBuildingGoal::key()` at `derivation-building-goal.cc:54` builds
   `"dd$" + name + "$" + path`, and `goal.hh:604` states the rule: `aardvark`
   runs before `baboon`. The client sends the four derivations of
   `fod-failing.nix` in store-path order, so pynixd built x3 where the test
   asserts x1.
3. **"2 dependencies failed" where Nix says 1.** `Goal::amDone` at
   `goal.cc:242` drops every waitee that is left as soon as one fails and
   `keep-going` is off, so the number is 1 whatever number would have failed.
4. **A goal that the request left behind was still awaited, and reported.**
   `Worker::run` leaves its loop when `topGoals` is empty, so that goal never
   reaches `amDone` and `entry-points.cc:93` skips it.

### `main:build` passes, and one order is left

`main:build` reaches the end now. Three more decisions of the root-goal loop
made that happen, and all three came from one line: `_build_slots` of
`goals/requests.py` limited the root goals by `max-jobs`.

**`max-jobs` limits the builders of the machine, and not the goals.**
`Worker::waitForBuildSlot` at `worker.cc:261` counts `getNrLocalBuilds()`
against `settings.maxBuildJobs`. Nix makes every goal of the request first and
holds the builders after that, so `-j1` runs one builder and not one goal.
pynixd holds the builders in `Scheduler._local_slot_is_full`, which reads the
same option of the same client, so the limit in the goal loop was a second one
on the wrong thing.

A request answers when its goals answer, so a goal that waited for a slot held
every goal behind it. Three failures had that shape:

1. **`build.sh:247` named the wrong derivation.** With `-j2` the loop took the
   first two goals by name, which are `depends-on-fail` and `depends-on-slow`,
   so `fast-fail` was never a root of its own.
2. **`build.sh:269` hung for the whole 300 s cap.** `slow` blocks on a fifo
   that nothing opens, and the loop waited for it before it started
   `fast-fail`. Issue nixidae/nixidae#235 holds the two pieces that this needed beside the
   line: a goal that the request leaves behind goes quiet, and a build that no
   goal waits for ends.
3. **`build.sh:279` held an answer that Nix does not send.** `nix build
   fast-fail^out depends-on-fail^out` names two derivations, and the second
   has the first as an input. `Worker::removeGoal` at `worker.cc:173` clears
   `topGoals` at the first failure, so the goal of the waiter never reaches
   `amDone` and `entry-points.cc:93` skips it. pynixd answered for both.
   `GoalResult.failing_derivation` names the build that really failed and
   passes through each dependency failure, so a root that names another root
   of the same request reports nothing.

**One difference is left, and it is an order.** `build.sh:167` builds x1, x2,
x3 and x4 of `fod-failing.nix` with `-j1`. All of x1, x2 and x3 give the wrong
hash, and x4 has x2 and x3 as inputs. The test asserts one `error:` line, and
that the line names x1.

Nix names x1 because it makes the four goals together and then gives the one
builder slot to the first of them in the order of `Worker::awake`, which
`_goal_order` states. pynixd makes its goals as coroutines that prepare at
their own speed, so the derivation that reaches the build queue first takes
the slot, and that is x2 or x3 as often as x1. A `DEBUG` run measured the
three `build_enqueued` lines at 12 ms from the first to the last, and
`build_waits_for_a_local_slot` came 0.4 ms after the second one: the scheduler
gives the slot away before the other candidates exist.

The count of `error:` lines has the same cause. x1 has no goal that waits for
it, so Nix writes one block. x2 and x3 each have x4, so
`_tell_the_client_it_failed` writes the reason and the client writes it again,
which is the defect that the `NIX-DEFECT (#23)` note in that method states.

The whole suite reads `156 OK, 36 SKIP, 15 FAIL` against this pynixd, and
`compare` names the same three regressions as the table above: `main:build`,
`ca:new-build-cmd` and `main:store-info`. **The first two are one assertion.**
`ca/new-build-cmd.sh` is `source ./build.sh` with `NIX_TESTS_CA_BY_DEFAULT=1`,
so both stop at `build.sh:167`, and the line they stop at moved from 247 to
167 with this work.

`ca:new-build-cmd` stopped at `build.sh:167` before this work as well,
measured with a shim that names the build of the commit before it. The limit
that `_build_slots` held gave x1 the slot in the `main` suite and not in the
`ca` suite, where pynixd resolves each derivation before it sends it. So the
order was already the answer there, and this work makes the two suites agree
rather than making one of them worse.

`main:store-info` is the third, and it stands apart. `store-info.sh:69` reads
the version of `nix store info` and compares it against `nix daemon
--version`. The shim sends `--version` to the real Nix on purpose, because the
suite asks that question for a number and pynixd answers the version of the
protocol, so the two answers disagree by construction: `Version: 2.34.8`
against `pynixd-0.1.0`. It fails with the earlier build too.

**The correction would be a barrier, so pynixd does not take it.** Every root
goal would have to reach "ready to build" before any build starts. That delays
the first build of every request by the preparation of the slowest goal of it,
and it decides only which of several equally doomed builds reports first. The
result of the request is the same either way, so this is a recorded deviation
and not a defect to correct.

### A harness defect that hid a whole fixture

`tests/functional/meson.build:17` runs `find_program('bash')` and writes the
answer into `config.nix` as `shell`. Every `mkDerivation` of the suite uses
that value as its builder. The runner named no bash in `runtimeInputs`, so
the first one on PATH was `/run/current-system/sw/bin/bash` of the builder
machine, and no sandbox can reach that path.

`cancelled-builds` of `main:build` was therefore degenerate **in both arms**.
`slow` must sleep 10 s so that `fast-fail` can fail while it runs; with no
shell it died at once with "executing '/run/current-system/sw/bin/bash': No
such file or directory", and the test asked which of two instant failures
landed first. The control arm won that race by milliseconds, so the fixture
reported nothing about either daemon.

This is the second time the harness, and not pynixd, decided what a test
said. The `$HOME` table above is the first. Read a control failure as a
question about the harness before you read it as a defect of Nix.

`main:multiple-outputs` is a `#20` difference now, and not one of the three
above. `nix store delete --ignore-liveness` cannot delete a path that a temp
root holds: `collectGarbage` of Nix reads the temp roots whatever that option
says. Add it to the list of GC tests above.

### The `ca` suite, and one id

pynixd resolves a derivation before it sends it, and the daemon then hashes a
different ATerm. Each realisation came back under that other hash, so the
client asked for the original id and found nothing. Nine of sixteen failures
were two assertions of Nix, and both stopped the program:

```
Assertion 'thisRealisation' failed at built-path.cc:122      (5 tests)
Assertion 'maybeOutputPath' failed at nix-build.cc:730       (4 tests)
```

Issue nixidae/nixidae#221 gives each id its original hash again, as Nix does at
`derivation-goal.cc:193-236`. Both assertions are gone, and each test that
they killed reaches a later line now.

Issue nixidae/nixidae#222 is the other half. `Derivation::shouldResolve` at
`derivations.cc:1129` states which derivations Nix resolves before it builds
them, and pynixd asked a narrower question: a deferred output alone. A
floating content-addressed output therefore went unresolved, so the builder
read a `DownstreamPlaceholder` as a path and the input was not in `inputSrcs`.

The two together took the `ca` regressions from 14 to 10.

Issue nixidae/nixidae#223 is the third defect. pynixd sends a resolved derivation, and it
sent the path of the original derivation with it. `DerivationBuildingGoal` of
the daemon prefers the derivation on the disk whenever that path is valid, at
`derivation-building-goal.cc:1239`, so the daemon read the original derivation
and answered under the id of that one. pynixd writes the resolved derivation
to the store now, as `derivation-resolution-goal.cc` of Nix does, and names
that path in the request. `Store.add_text_to_store` and
`Connection.call_with_payload` are the two parts that this needed.

Issue nixidae/nixidae#224 is the fourth. A floating content-addressed output takes its path
from the build, so the derivation names no path for that output, and pynixd
built the derivation again. `DerivationGoal::checkPathValidity` at
`derivation-goal.cc:405` reads the store instead: `sha256:<hash>!<name>` maps
to a path, and a valid path there ends the goal with no build.
`EnsureDerivedPathGoal` asks that question first now. `ca:build` needs it,
because `testGC` builds with `-j0` after a garbage collection, so a second
build is not allowed.

| `ca` suite    | OK  | FAIL | SKIP |
| ------------- | --- | ---- | ---- |
| before nixidae/nixidae#223   | 5   | 16   | 3    |
| with nixidae/nixidae#223     | 7   | 14   | 3    |
| with nixidae/nixidae#224     | 11  | 9    | 4    |

`ca:build` passes. So do `build-with-garbage-path`,
`duplicate-realisation-in-closure`, `nix-copy`, `nix-shell`, `selfref-gc` and
`why-depends`.

## The store of each test

**The suite uses a relocated store, and pynixd serves one.** `common/vars.sh`
exports `NIX_STORE_DIR=$TEST_ROOT/store` and `NIX_STATE_DIR=$TEST_ROOT/var/nix`,
so the store path and the directory on disk are the same and the state sits
beside it. `make-shim.sh` reads both names out of the environment of the test
and writes them into the configuration of pynixd.

Nix moves a store the other way as well. `--store <root>` puts the files at
`<root>/nix/store` and moves no store path, so `builtins.storeDir` still
answers `/nix/store`. pynixd served that shape alone until issue nixidae/nixidae#215, and a
fifth patch here rewrote the layout of the suite to match. **That patch
changed the tests to fit pynixd**, and it was the one patch of the five that
worked around pynixd rather than around the harness. It is gone, with the two
script rewrites under it.

`pynixd/store_layout.py` holds both shapes now, and `tests/unit/test_store_layout.py`
states the difference.

## The safety limit

`run.sh` starts a watchdog, and the watchdog ends the run above 1500
processes. Patch 1 of `setup.sh` gives the reason: a daemon that inherits
`NIX_REMOTE=daemon` opens itself as its store, and each worker forks another
worker. One test reached 16,451 processes in under two minutes.
