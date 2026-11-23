# Builds in Kubernetes pods

Status: design, 2026-11-23. The mechanism works end to end on bookmark
`spike/pod-build` (not landed). nixidae/nixidae#1560 holds the measurements
as they were taken. "Open decisions" at the end lists what this page does not
settle.

Each fact is marked **measured** (a named vivarium run) or **read** (source or
documentation, not run). Numbers come from QEMU guests. UML numbers are
slower and mislead, so this page does not use them.

## The goal

pynixd builds a derivation in a pod of its own on a Kubernetes cluster. The
pod:

- exists for one build, and nothing else runs in it;
- runs as a user with no capability, under a seccomp profile;
- gives way to every ordinary pod, so build capacity costs the cluster nothing
  it needs for other work;
- reads its inputs from the node, in place, with no copy;
- cannot add any path to central's store but the outputs of its own
  derivation.

Central is the pynixd that schedules the build. It is a pod in the cluster
(nixkube's `pynixd-0`) or a host outside it.

## The shape

```
central (pynixd)                         build pod (one per build)
-----------------                        -------------------------
scheduler places the build on
a kubernetes-build-pods store
  |
  |- adds the farm root path   --------> nixkube CSI volume at /nix:
  |  (inputs + template paths)           one read-only bind per closure path
  |- makes a key, grants it the
  |  derivation's outputs
  |- creates the Pod, then the  -------> container "pynixd": pynixd build-one
  |  Secret (job + key) it owns            store /build, lower store /
  |                                        dials central over SSH
  |<-------- SSH, per-build key ---------  reads nothing it lacks
  |                                        builds through the sibling sandbox
  |                                      container "sandbox": the namespace
  |                                        helper, Localhost seccomp profile
  |<-------- outputs (AddToStoreNar) ----
  |- follows the pod's log  <----------- stdout of "pynixd"
  |- reads the result from the  <------- termination message
  |  container's termination message
  |- deletes the pod
```

### Placement

`BuildPodStore` (`store/build_pods.py`) is a scheduled store with slots, like
any builder. The scheduler places on it as on any other store. It holds no
paths and answers no store operation. The executor sees the store type and
calls `build_in_pod` (`execution.py`) in place of a connection.

The pod is a `Pod`, not a `Job`. Central retries a build itself, and a Job
would add a controller between central and the placement. Kueue is the case
for Jobs later.

### The job and the key

For each build, central generates an ed25519 key. It grants the key one
derivation (`build_grant.py`) and writes a Secret with the job
(`build_job.py`: central's address, its host key, and the `BuildDerivation`
request in wire form) and the private key. The Secret has the pod as its
owner, so it goes when the pod goes. Central revokes the key when the build
ends, whatever the outcome.

The pod is a client only. It dials central's SSH listener, as user `pod`,
with that key. Central never connects to the pod.

A granted session may run only the reads, the adds, the temporary roots and
`BuildPathsWithResults` (`GRANTED_OPS`). Any other operation closes the
session. An add succeeds only for:

- a static output path of the derivation; or
- a content-addressed path whose address names it (`is_content_addressed`),
  as for any untrusted client (`pathInfoIsUntrusted`, `local-store.cc:1048`,
  read).

The session skips signature checks for the add: the grant checks the name
against the address, and the store checks the files against the address.
**Measured** (run `rogue1`): a pod that adds a path that is not an output gets
"may not add … it is not an output", and the path is not valid on central.

### The result

The pod writes its `BuildResult` in wire form to `/dev/termination-log`.
Central reads it from the container's status. The kubelet keeps 4096 bytes
of a termination message, so `result_message` cuts the error text to fit. A
pod that ends with no result, or one that does not decode, is the pod's fault,
not the build's: the build goes back to the queue.

Central checks the result (`checked_result`): every realised output is an
output of the derivation, each static output is its own path, and a success
realises each floating output. Then it checks that each output is valid in
its own store.

### Priority and preemption

nixkube's `buildPods.enable` creates the PriorityClass `pynixd-build`, value
-1, `preemptionPolicy: Never`. The scheduler preempts a build pod for any
ordinary pod (default 0), the kubelet evicts it first, and a build pod never
preempts anything.

A build pod that Kubernetes removes for its own reasons ends with a
`DisruptionTarget` condition, or the kubelet's `Evicted`. Central reads that
reason, raises `PodDisrupted`, and queues the build again with no store
failure. **Measured** (run `preempt4`): a preempted build is queued again and
then passes.

Trap: a pod can be gone before central reads it. **Measured**: preempted at
23:05:10, killed at 23:05:11, read by central at 23:05:23 as "deleted". So
central watches each pod from its creation, and keeps the reason from each
change it sees.

## The sandbox

The pod has two containers on one `emptyDir` at `/build`:

- **pynixd** runs `pynixd build-one`. It holds the pod's store and its
  database, talks to central, and runs `build()` of pynixd's own builder
  (`pynixd_store/builder`).
- **sandbox** runs the namespace helper (`pynixd_store.builder.sibling run`).
  `SiblingSite` (`builder/site.py`) hands it each build through files and a
  control socket in the build's top directory. The helper makes the build's
  user, mount, network, IPC and UTS namespaces, as in Nix.

Both containers run as uid 1000 with every capability dropped. The pod's
`fsGroup` is 1000, so that user reads the job Secret (mode 0400).

The default seccomp profile refuses every `unshare` (**measured**). The
sandbox container names the profile `nixkube/pynixd-builder.json`, which
nixkube's node DaemonSet installs under the kubelet's `seccomp` directory.
The profile allows every call (`nix/builder-seccomp.nix`). The helper
installs Nix's own filter in the build, and filters stack, so the build
reaches what it reaches under Nix's sandbox on a host. Docker's default
profile refuses 9 calls that Nix's sandbox allows, among them the keyrings
and `perf_event_open` (**measured**, sandbox probe). It is `Localhost`, not
`Unconfined`, because PSA `restricted` admits only `RuntimeDefault` and
`Localhost`. The pynixd container keeps `RuntimeDefault`.

A container's `/proc` is masked, so a fresh `proc` mount fails with EPERM
(**measured**). Two modes:

- **Plain pod**: the helper makes no PID namespace and binds the container's
  `/proc`. It passes PSA `restricted`.
- **`hostUsers: false` and `procMount: Unmasked`**: the helper probes
  `fresh_proc_allowed` once and then makes a PID namespace and a fresh `/proc`,
  as Nix does. It needs PSA `baseline`.

`kubernetes_sandbox_probe.py` builds the same probe derivation under Nix,
under pynixd's helper on a host, and in each pod mode. It names each known
difference from Nix. **Measured** (run `qprobe1`): on QEMU the pods match Nix
on every system call the probe tries.

## Inputs

A build pod gets its inputs in one of two ways. `BuildPodStoreSpec.farm`
selects one.

### Copy in

With no farm, `build-one` streams each input from central over SSH into
`/build`. It works on any cluster. **Measured** (run `qbench1`, 16 CPUs): a
build pod costs about 5.4 s fixed plus 11 ms per MiB of input closure.
Busybox took 5.8 s, pynixd's closure (378 MiB) 9.7 s. A pod that runs `true`
starts in 1.4 s.

### A farm of one path per build (nixkube)

With `farm.volume`, central gives each pod a farm:

1. Central adds a text file, `pynixd-build-inputs`, to its own store. Its
   lines and references are the build's inputs plus every store path the pod
   template names (`template_store_paths`). The same inputs give the same
   path. Central holds a temporary root on it while the pod lives.
2. The pod's CSI volume asks nixkube for that path, with
   `presentation: farm`, under each of the store's systems.
3. The node realises the path from central, its substituter
   (`ssh-ng://nix@pynixd`). nixkube mounts a tmpfs at the volume's
   `nix/store`, binds each closure path read-only into it, and writes the
   closure's database. The pod mounts the volume as its whole `/nix`.
4. The pod's pynixd opens the farm as the **lower store** of its own store in
   `/build` (`lower_store = "/"`, `LowerStoreEngine`). The farm's paths are
   valid in the pod's store, their files stay in the farm, and writes go to
   `/build`. No OverlayFS takes part.
5. The helper binds each input from the farm into the build
   (`StoreEngine.files_of`).

**Measured** (run `nkcentral3`, nixkube's cluster test on QEMU): central built
a busybox derivation in a farm pod. The pod's lower store held 174 paths, the
closure of the farm root, and it copied 0 bytes of input from central. On the
pynixd test's stand-in farm the same build copied 39,552,408 bytes on the
copy-in pod and 0 on the farm pod (run `provk-qfull4`).

What the farm costs instead: one realisation of each path on each node, and
one bind per path. Binding pynixd's closure (132 paths) took 4.3 ms
(**measured**, `qbench1`).

At start, central refuses a farm store whose template has no CSI volume of
that name, or that names a store path central and its caches cannot get.

## Recursive Nix

The builder's socket is in its build directory, on the shared volume, so the
pynixd container serves it with no relay. The restricted proxy (`recursive.py`)
answers it.

- A path the builder adds is content-addressed, so the grant takes it when it
  reaches central as part of an output's closure.
- A nested build runs at central, not in the pod (`builds_at`). The pod sends
  the derivations to central, central builds them as for any untrusted client,
  and the pod copies the outputs back. The pod's key could not add another
  derivation's outputs anyway.
- A floating content-addressed output is known only from the result; central
  takes it from there.

**Measured** (run `provk-qfull6`): the three cases (an added path, a nested
build, a floating output) pass on the copy-in pod and on the stand-in farm
pod.

## Traps found

- A Nix client on the pod's Unix socket cannot build: `UDSRemoteStore` is a
  `LocalFSStore` and reads a `.drv` and its inputs from one directory, and the
  pod keeps them in two. `build-one` drives pynixd in process instead.
- Nix takes `HOME` only when the user owns it, and asyncssh needs a user name.
  A pod image has no passwd entry for uid 1000, so `build-one` makes both.
- Nix 2.35 keys realisations by path (`BuildTraceV3`), 2.34 by hash
  (`Realisations`). Each table exists only with `ca-derivations`. The lower
  store asks the farm only for a keying whose table it holds, and the job's
  result is written in the shape its empty feature set reads (`for_the_wire`).
- PSA `restricted` and `baseline` both refuse a `hostPath` store. A CSI store
  passes `restricted` (dry run, **measured**).
- A seccomp profile that names an `includes`/`excludes` filter silently loses
  it in containerd (`contrib/seccomp/seccomp.go:40`, read). The profile is
  plain OCI.

## Simplified after the spike

Agreed 2026-11-23, done before landing:

1. One watch per pod (`BuildPodWatch`). The spike watched each pod three
   times: for its end reason, for its container's start, and for its end. One
   task now keeps what it saw. The store reads the log once the container
   ran, and the end once it is known. A watch that fails ends the build as
   the pod's fault, so a build never waits on a dead watch.
2. `files_of` takes the place of the store directory in `RestrictionContext`
   and in each build site's `endpoint`. A map of the inputs was the first
   plan, and it is wrong: a path the builder adds during the build may be in
   the lower store, so the restriction resolves it when it arrives.
3. `BuildPodStore.arm(...)` holds the farm checks.
4. The recursive proxy's nested build reads outputs through
   `realised_outputs()`.
5. nixkube's standalone farm pod check is gone; the test through central
   covers it.

## Open decisions

1. **Which ways to build in Kubernetes stay.** Three exist: nixkube's
   `BuilderManager` (long-lived builder Jobs), pynixd's provisioning pools
   (long-lived pods that register through the reverse acceptor; kinds nested
   and krun), and these build pods. Recommendation: build pods replace
   `BuilderManager` once nixkube renders their template. Keep the krun pool
   for builds that need a VM until a build pod can run one.
2. **Copy-in stays for clusters without nixkube.** Recommendation: yes. It is
   the same code with the farm off, and the only path on a cluster without the
   CSI driver.
3. **nixkube renders the template.** Today the only farm template is the
   test's (`nixkube/nix/uml/build-pod-store.nix`). Recommendation: a
   `nixkube.buildPods` option renders the pod (image, containers, seccomp
   profile, PriorityClass, CSI volume) and central's store spec, so an operator
   writes neither.
4. **Where nested builds wait.** A nested build can wait for the slot its own
   parent holds (nixidae/nixidae#1576). It needs a rule before build pods take
   recursive builds in production.
5. **builder-rpc-v0.** The pod serves the restricted proxy. The protocol itself
   needs a Nix client in the builder that speaks it. Untested.
6. **Node requirements.** The profile must be on each node (nixkube installs
   it). A node that forbids unprivileged user namespaces (Ubuntu 23.10 and
   later with the AppArmor restriction, or `max_user_namespaces=0`) cannot run
   the sandbox container. Recommendation: nixkube probes this per node and
   labels the node, and the template selects on the label.
7. **Pods left behind.** A pod outlives a central that dies; it ends at
   `activeDeadlineSeconds`. Recommendation: central deletes pods with its build
   label at start, as nothing it did not start in this run can report to it.
8. **The fixed cost.** About 5.4 s of a build pod is not the build. The gaps
   are pod creation to pynixd's first log line, the managed Nix daemon start
   (about 0.9 s; a farm pod runs none), SSH (0.6 s), and pod shutdown to the
   result. Each is a lever; none is measured alone.
