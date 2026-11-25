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
  |  Secret (job + key) it owns            store /nix, on the pod's emptyDir
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

The pod has two containers on one `emptyDir` at `/build`, and its store on
another (see the farm, below):

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
   (`ssh-ng://nix@pynixd`). The volume names the pod's emptyDir `store`
   (`storeVolume`). nixkube binds that emptyDir as the volume's `nix`,
   private, binds each closure path read-only into its `store`, and writes
   the closure's database. The pod mounts the volume as its whole `/nix`.
4. The pod's pynixd uses `/nix` as its own store (`store_path = "/"`). The
   farm's paths are valid in nixkube's database, their files stay the node's,
   and outputs go to the emptyDir. No OverlayFS takes part.
5. The helper binds each input from the farm into the build
   (`StoreEngine.files_of`).

The outputs count against the pod's ephemeral storage, and the inputs do not.
The kubelet's disk walk crosses no other `st_dev`, and a bind keeps its
source's, so an input bind at the emptyDir's own path would count. nixkube's
binds attach at the volume's path in its own namespace, and its bind of the
emptyDir is private, so none propagates there. A read-only farm on a tmpfs
(`lower_store = "/"`) is the alternative, and it puts the pod's writes in
`/build`, which forces the sandbox: a relocated store always sandboxes.

**Measured** (run `nkdisk4`, nixkube's cluster test on QEMU): central built a
busybox derivation in a farm pod. The farm held 174 paths, the closure of the
farm root, and the pod copied 0 bytes of input from central. On a probe pod
with a 4 MiB output the kubelet counted 8,548,352 bytes on the emptyDir; the
closure is 39,499,458 bytes. The rest is Nix's database, with its `reserved`
file (8 MiB apparent size), a fixed cost per pod. The kubelet counts only an emptyDir that a
container mounts (0 bytes unmounted, measured), so the template mounts it
read-only as well. On the pynixd test's stand-in farm the same build copied
39,552,408 bytes on the copy-in pod and 0 on the farm pod (run
`provk-qfull4`).

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

Answered 2026-11-23. Every item is decided.

1. **Which ways to build in Kubernetes stay. Decided: build pods, eventually.**
   Three exist: nixkube's `BuilderManager` (long-lived builder Jobs), pynixd's
   provisioning pools (long-lived pods that register through the reverse
   acceptor; kinds nested and krun), and these build pods. Build pods replace
   `BuilderManager` once nixkube renders their template. The krun pool stays
   for builds that need a VM until a build pod can run one.
2. **Copy-in for clusters without nixkube. Decided: keep it, priced as a
   store that holds no paths.** The scheduler counts the whole input closure
   as bytes to send to a copy-in store. Copy-in is the
   mode with the farm off: central sends each input over SSH into the pod's
   empty store. It needs no CSI driver, so it is the only mode on a cluster
   without nixkube. It is the same code as the farm mode, less the farm
   volume. The cost: each build copies its whole input closure (39.5 MB for
   the test's input, run `provk-qfull4`). Removing it makes build pods
   nixkube-only.
3. **nixkube renders the template. Decided.** A `nixkube.buildPods` option
   renders the pod (image, containers, seccomp profile, PriorityClass, CSI
   volume) and central's store spec, so an operator writes neither.
   Done (`nixkube/kubenix/build-pods.nix`): one store and one template per
   system of `nixkube.systems`, `buildpods-<system>`, with a node selector
   for that architecture and that system's pynixd. `buildPods.builds` turns
   the stores off and keeps the classes and the profile. The settings merge
   at option-default priority, so an operator's settings win key by key. The
   pods run nixkube's scratch image: `/nix` is the farm. The test sets only
   features, the deadline and the log level (`nix/uml/build-pod-store.nix`).
4. **Where nested builds wait. Decided: priority by depth, at both layers.**
   A nested build can wait for the slot its own parent holds
   (nixidae/nixidae#1576). Two layers can make it wait:
   - Central's slots. A nested build goes ahead of every waiting build of a
     smaller depth. A parent that waits on a nested build gives its slot back.
     The cluster, not the slot count, limits how many pods run.
   - The cluster. One PriorityClass per depth, all of them below 0, so every
     ordinary pod stays ahead. A deeper build pod is scheduled before a
     shallower one. With `preemptionPolicy: PreemptLowerPriority`, a nested
     pod can evict a shallower build pod on a full cluster. Only build pods are
     below it, so no ordinary pod is at risk. The evicted build requeues
     (`PodDisrupted`). If the evicted pod is the nested pod's own parent, the
     parent runs again and finds the nested output at central.
   Done, both layers. Phase `pod-nested` gives a one-slot store an outer
   build whose nested build needs that store. It passes (run `provk-nest2`):
   the outer pod ran as `pynixd-build`, the nested pod as
   `pynixd-build-nested`. Negative control (run `provk-negnest1`): with the
   slot release off, the outer build waits until the phase's 420 s
   deadline, and no nested pod starts.
   The eviction: phase `pod-evicted` gives a store pods that request 1400Mi,
   so the node has room for one. The nested pod preempts its own outer pod.
   The outer pod's session then lets go of the nested build, and central
   cancelled it: the outer build's next pod asked again and was preempted
   again, every 12 s until the deadline (run `provk-evict2`). Now the outer
   build holds its nested builds until it ends (`BuildQueue`,
   `_parent_lives_locked`). Run `provk-evict3`: one preemption, then the
   outer build passes in 31 s.
5. **builder-rpc-v0.** Recursive Nix is tested: run `provk-clean1`, phase
   `pod-recursive` (`kubernetes_pod_recursive.py`), on the copy-in pod and on
   the stand-in farm pod. The cases are a path the builder adds, a nested
   build, and a floating output. Not tested: recursive Nix in a pod on
   nixkube's real farm, and the builder-rpc-v0 protocol, which needs a Nix
   client in the builder that speaks it. Decided: the recursive test probes
   enough of builder-rpc-v0 for now.
6. **Node requirements. Decided: nixkube probes and labels each node.**
   Decided fallback for a node without the label: a second template with no
   sandbox container and Nix's sandbox off. The pod is then the only boundary.
   In farm mode the pod already sees only the input closure and the template's
   paths, and its build directory is its own. A NetworkPolicy limits its
   egress to central's SSH port. The cost: the builder sees the template's
   paths and the pod's `/proc`, and the NetworkPolicy needs a CNI that enforces
   it. The profile must be on each node (nixkube installs it). Ubuntu 23.10
   and later with the AppArmor restriction, or `max_user_namespaces=0`, forbid
   unprivileged user namespaces.
   Revised (User, 2026-11-23): keep the sandbox where AppArmor is the only
   obstacle. The restriction lifts for a process whose AppArmor profile grants
   `userns`. So nixkube loads such a profile on each node that restricts, and
   the build pod names it. That is Ubuntu's default, so most nodes keep
   Nix's sandbox and build alike. The template without the sandbox is opt-in
   and off by default, for a node with user namespaces off entirely. The node
   DaemonSet probes: it reads `max_user_namespaces`,
   `apparmor_restrict_unprivileged_userns` and `unprivileged_userns_clone`,
   tries `unshare(CLONE_NEWUSER)` as an unprivileged uid, and labels the node.
   Its process is not a build pod's container, so the label can be wrong at
   the edges. A build pod that still fails is an infrastructure error, and
   central builds elsewhere. The NetworkPolicy applies to every build pod.
   Measured (QEMU node, kernel 7.2.9, AppArmor built in and off): the
   kubelet rejects a pod that names an AppArmor profile on a node without
   AppArmor ("Cannot enforce AppArmor: AppArmor is not enabled on the host").
   NRI cannot set the profile either: its container adjustment has no field
   for it. So one template cannot serve both kinds of node. The plan: a label
   per node (`userns`, `apparmor` or `none`), and one template per kind with
   a node selector on it. Decided (User): one store, not one per kind. The
   store holds the templates by kind, and central picks one for each pod from
   the labels of the Ready nodes. Central then needs to list nodes.
   Revised again (User, 2026-11-23): no AppArmor profile of nixkube's own.
   Upstream AppArmor lets a confined process make a user namespace unless its
   profile mediates them (`security/apparmor/task.c`, `aa_profile_ns_perm`).
   containerd's default container profile is written to ABI 3.0, which has no
   `userns` rule (`contrib/apparmor/template.go`). Ubuntu's restriction is an
   Ubuntu kernel patch, and it acts on unconfined processes. So a pod that
   names no AppArmor profile is likely to keep the sandbox on Ubuntu. This is
   not measured on an Ubuntu kernel. The probe settles it per node: the
   DaemonSet's `userns-probe` container (`nixkube-userns-probe`) runs with
   the sandbox container's security context (one option,
   `buildPods.sandboxSecurityContext`). It makes the helper's namespaces,
   maps its uid, mounts and sets a host name inside, and labels the node
   `nixkube/userns` `true` or `false` (the error in `nixkube/userns-error`).
   The template selects `nixkube/userns=true`. Two kinds remain: the sandbox
   template, and the opt-in template without it for nodes labelled `false`.
   Built (2026-11-23): `buildPods.unsandboxed`, off by default, adds
   `buildpods-<system>-unsandboxed` second in each store's list. It selects
   `nixkube/userns=false`, sets `sandbox = false`, and keeps the sandbox
   container with the pod's RuntimeDefault seccomp profile. The build runs
   there, not beside pynixd (User): it cannot read the job's key, write the
   termination message or reach pynixd's process. It shares the pod's
   network and host name. The sandbox container runs one build, so the
   runner kills every other process in it once the helper exits. Without
   that, a process the builder leaves holds the terminal open and the build
   waits for the pod's deadline. Next (User): measure whether NRI can make
   the builder container itself Nix's sandbox. The runtime then makes the
   namespaces, and a node labelled `false` keeps the sandbox.
   Measured (2026-11-23, QEMU node, containerd 2.4.1, NRI v0.12.3, runc;
   nixkube's `nriprobe` phase). A second plugin changes one container of a
   pod, and a control container in the same pod keeps each result apart:
   - New network, IPC, UTS and cgroup namespaces: applied. The network
     namespace holds `lo` alone, up, with 127.0.0.1, and a bind works. That
     is Nix's network.
   - A seccomp policy of the plugin's own: applied. It refuses a setuid
     chmod with EPERM and lets a plain chmod through, as Nix's filter does.
   - Mount removal: `/etc/hosts`, `/etc/hostname`, `/etc/resolv.conf`, the
     termination log and the service account token are all gone.
   - Env removal: not applied. NRI v0.12.3 drops the removal of a variable
     that no plugin set, so all eight `KUBERNETES_*` stayed. containerd/nri
     193a7fdfa4 fixes it, after v0.12.5. NRI's `args` replace the process,
     so the runner can start the builder with an empty environment anyway.
   - Host name: the new UTS namespace keeps the node's name (`cp`). NRI has
     no field for it, and Nix sets `localhost`. Setting it needs
     CAP_SYS_ADMIN over that namespace: an OCI hook can, the container
     cannot. Not measured.
   - No user namespace: NRI has no field for uid maps. On a node labelled
     `false` the build then runs as a build user in the host's user
     namespace, as Nix does without `auto-allocate-uids`.
   - containerd's `default_validator` is off unless set. An operator who
     sets `reject_namespace_adjustment` or `reject_custom_seccomp_adjustment`
     refuses this route, and `required_plugins` can refuse a pod that the
     plugin did not see.
   Decided (User, 2026-11-23): build this sandbox for nodes labelled
   `false`, in this shape. The pod has two containers. The controller talks
   to central. The runner runs the build, and its own process is the
   builder: no runner process and no chroot. The design:
   - Central writes the builder, its arguments and its environment into the
     runner container's spec. One pod runs one build, so central knows them
     when it writes the pod.
   - The runner's image is empty. Its root holds the farm at `/nix` and what
     the runtime supplies (`/proc`, `/dev`, `/dev/shm`). The build sees no
     store path it did not declare.
   - The NRI plugin adds new network, IPC, UTS and cgroup namespaces and
     Nix's seccomp filter. It removes the kubelet mounts, and an OCI hook
     sets the host name to `localhost`.
   - The runner runs as a build uid with no capabilities.
   - The controller is a native sidecar that starts first and fills the
     build directory. The runner waits for its startup probe.
   - The controller reads the log and the exit status through the API, with
     RBAC on its own pod. A cancel deletes the pod, and `max-silent-time`
     acts on the log stream.
   Measured (2026-11-23, same node, `nriprobe`'s second pod): a controller
   as a native sidecar with a startup probe, and a builder on the empty
   `scratch` image as uid 30001. The builder starts only after the
   controller fills the shared directory. The plugin's createContainer hook
   (the node's `hostname localhost`) sets the host name, and the runtime
   runs it before it drops capabilities. The namespaces and the seccomp
   filter apply as in the first pod. The builder's root holds `dev etc nix
   proc run sys tmp` and its volumes; the guest binds all of `/nix/store`
   into every container, so the store part of its view is not measured.
   Measured (2026-11-23, run `nrip5`, a builder that exits 7):
   - The kubelet sends SIGTERM to the sidecar 1 s after the builder exits.
     The sidecar then had the pod's grace period (30 s): it ran 10 s more,
     and its exit code and termination message reach
     `initContainerStatuses`. The pod is `Failed`.
   - The sidecar cannot read the builder's output. As uid 1000 with no
     capabilities, it fails to list a 0700 directory, read a 0600 file and
     chmod either. The builder (uid 30001) cannot write a 0644 file of
     uid 1000.
   - Kubernetes expands `$(name)` in `command`, `args` and each `env` value
     when `name` is a variable of the container. `$$` gives `$`, and an
     unknown name stays as written. Central escapes each `$` as `$$`.
   - The runtime adds `HOME=/` and `HOSTNAME=<pod>` (the spec overrides
     both) and the eight `KUBERNETES_*`. A variable named `a-b.c` is
     accepted.
   - The other order works too: a plain controller container whose
     postStart hook waits for its own file holds back the next container.
   Decided (User, 2026-11-23), from these measurements:
   - The environment: nixkube's containerd carries the NRI fix, and the
     plugin removes the `KUBERNETES_*` and `HOSTNAME`. Central writes the
     builder's argv and environment with each `$` escaped. On a containerd
     without the fix the build sees those nine variables.
   - The builder runs as the controller's uid, in its own container. The
     controller then reads and canonicalises each output. The runner does
     not mount the database.
   - The controller is the first plain container, not a sidecar. Its
     postStart hook waits until it has filled the build directory. It ends
     when its work is done, and it reads the builder's exit code through
     the API.
7. **Pods left behind. Decided: ownership, and a sweep at start.** A pod
   outlives a central that dies; it ends at `activeDeadlineSeconds`.
   - Ownership: central in the cluster makes its own Pod the owner of each
     build pod. When central's Pod goes, Kubernetes deletes the build pods,
     even if central never comes back. A new Pod with the same name has a new
     UID, so it owns none of the old ones.
   - Ownership does not cover a central process that restarts in the same Pod
     (a container restart keeps the UID), or a central outside the cluster,
     which has no object to own anything. In both cases the old pods cannot
     report: their keys were in the old process. So central also deletes pods
     with its build label at start.
   Done: the sweep (`BuildPodStore.sweep`). Each pod carries its store's id
   and its process's run id as labels, and the sweep deletes the store's pods
   of every other run. It runs in the background until the API server
   answers: in server start it failed while the cluster came up (run
   `provk-left1`). A server that a hand-over started sweeps nothing, as the
   old process still drains its builds. Phase `pod-left`.
   Done: the owner reference. Central reads its Pod's name, UID and namespace
   from the downward API (`PYNIXD_POD_NAME`, `PYNIXD_POD_UID`,
   `PYNIXD_POD_NAMESPACE`; nixkube's central sets them). Settings refuse a
   namespace other than the build pods': Kubernetes counts an owner in
   another namespace as absent, and deletes what it owns. Phase `pod-owner`
   deletes a Pod that stands in for central's, and the garbage collector
   deletes its build pod.
8. **The fixed cost. Decided: measure each part, and state the cost.** About
   5.4 s of a build pod is not the build. At least 1 to 2 s of that is
   Kubernetes itself (a `true` pod starts in 1.4 s). The other gaps are
   pod creation to pynixd's first log line, the managed Nix daemon start
   (about 0.9 s; a farm pod runs none), SSH (0.6 s), and pod shutdown to the
   result. The store gives the fixed cost to the
   scheduler's time-aware ranking (`placement.py`), so a small build goes to
   central or to a long-lived builder. The operator documentation (nixkube's
   `docs/build-pods.md`) states the cost and says to send small builds elsewhere.

   **Measured** (run `provk-secs1`, QEMU, three builds each). The pod and
   central each write one line of seconds to the build log. For the busybox
   input (37 MiB), 5.8 s in all:

   | part | seconds | who pays it |
   | --- | --- | --- |
   | create to container running | 1.6 | Kubernetes: scheduling, volumes, container start |
   | container start to first log line | 1.1 | Python's start in the pod (1.5 s from the process start) |
   | server start | 1.0 | pynixd in the pod |
   | inputs, with the SSH connection | 0.26 | central to the pod |
   | build | 0.68 | the build |
   | last log line to result | 1.00 | the kubelet |

   The inputs grow with the closure: 1.5 s for 210 MiB, 3.3 s for 378 MiB.
   The last part is 1.00 s in every run: the kubelet checks a followed log
   for the container's end once a second, and reports the pod's status on a
   one-second relist (read, not measured alone). The levers, largest first:
   the result over the pod's SSH session in place of the termination
   message (1 s); a pod without a server start (1 s); a faster Python start.
   The store learns its fixed cost from each pod (`BuildPodStore.setup_seconds`),
   and the build history records the pod's build alone.
