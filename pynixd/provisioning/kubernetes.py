"""Kubernetes pods as builders: one pod per machine. nixidae/nixidae#1560.

The template is a pod manifest. The provider adds the name, the tags and the
join ticket, and nothing else: how pynixd reaches the pod (an image, a nixkube
store volume, the node's store) is the template's. The ticket goes in a Secret
the pod owns, mounted as a file the pod's pynixd reads as
`reverse_initiator.join_ticket`, so it shows in no pod spec and goes when the
pod goes.

A label value holds 63 characters of a small alphabet, and a tag value need
not fit. So one label marks the provider's pods and an annotation holds the
tags as JSON; `machines` filters by those.
"""

from __future__ import annotations

import contextlib
import copy
import datetime
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Final

import anyio

# httpx imports httpcore inside its transport's `__init__`, so kr8s' first client would load 178 modules on
# the event loop: 100-124 ms, measured 2026-11-21.
import httpcore  # noqa: F401 -- imported for that load, before the loop runs
import httpx
import kr8s
import kr8s.asyncio
from kr8s.asyncio.objects import Pod, Secret

from .provider import Machine, MachineState, NoCapacity, ProviderError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping

    from ..config import KubernetesSettings, KubernetesTemplateSettings

MANAGED_LABEL: Final = "app.kubernetes.io/managed-by"
MANAGED_VALUE: Final = "pynixd"
TAGS_ANNOTATION: Final = "pynixd.nixidae.org/tags"
TICKET_KEY: Final = "ticket"
TICKET_VOLUME: Final = "pynixd-join-ticket"
_NAME_PREFIX: Final = "pynixd-"
_NAME_MAX: Final = 63
_HASH_CHARS: Final = 10

type Manifest = dict[str, Any]
"""A Kubernetes object as JSON: its schema is the API server's, not Python's."""


def pod_name(tags: Mapping[str, str]) -> str:
    """A DNS-1123 label from the tags alone, so a launch whose answer was lost creates no second pod."""
    ordered = dict(sorted(tags.items()))
    digest = hashlib.sha256(json.dumps(ordered).encode()).hexdigest()[:_HASH_CHARS]
    readable = re.sub(r"[^a-z0-9-]+", "-", "-".join(ordered.values()).lower()).strip("-")
    room = _NAME_MAX - len(_NAME_PREFIX) - _HASH_CHARS - 1
    stem = readable[:room].strip("-")
    return f"{_NAME_PREFIX}{stem}-{digest}" if stem else f"{_NAME_PREFIX}{digest}"


def pod_manifest(template: KubernetesTemplateSettings, name: str, tags: Mapping[str, str]) -> Manifest:
    """The template's pod with its name, tags and the ticket volume. The template itself is left as it was."""
    pod: Manifest = copy.deepcopy(template.pod)
    pod["apiVersion"] = "v1"
    pod["kind"] = "Pod"
    metadata = pod.setdefault("metadata", {})
    metadata["name"] = name
    metadata.pop("generateName", None)
    metadata.setdefault("labels", {})[MANAGED_LABEL] = MANAGED_VALUE
    metadata.setdefault("annotations", {})[TAGS_ANNOTATION] = json.dumps(dict(sorted(tags.items())))
    spec = pod.setdefault("spec", {})
    # A builder that exits has ended; the provisioner launches the next one.
    spec.setdefault("restartPolicy", "Never")
    spec.setdefault("volumes", []).append({"name": TICKET_VOLUME, "secret": {"secretName": name, "defaultMode": 0o400}})
    containers: list[Manifest] = spec.get("containers") or []
    target = next((c for c in containers if c.get("name") == template.container), None)
    if template.container is None and containers:
        target = containers[0]
    if target is None:
        raise ValueError(f"kubernetes: the template has no container {template.container!r}")
    target.setdefault("volumeMounts", []).append(
        {"name": TICKET_VOLUME, "mountPath": str(template.ticket_dir), "readOnly": True}
    )
    return pod


def secret_manifest(name: str, pod_uid: str, user_data: str) -> Manifest:
    """The ticket, owned by the pod: deleting the pod deletes it."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "labels": {MANAGED_LABEL: MANAGED_VALUE},
            "ownerReferences": [{"apiVersion": "v1", "kind": "Pod", "name": name, "uid": pod_uid}],
        },
        "type": "Opaque",
        "stringData": {TICKET_KEY: user_data},
    }


def _epoch(timestamp: str) -> float:
    return datetime.datetime.fromisoformat(timestamp).timestamp()


def _failure(status: Manifest) -> str:
    """Why a failed pod failed: the pod's reason, else its first terminated container's."""
    for container in status.get("containerStatuses") or []:
        terminated = (container.get("state") or {}).get("terminated")
        if terminated:
            reason = terminated.get("reason") or "exited"
            return f"container {container.get('name')} {reason} with exit {terminated.get('exitCode')}"
    return str(status.get("reason") or status.get("message") or "the pod failed")


def machine(pod: Manifest) -> Machine | None:
    """A machine from a pod; `None` for a pod with no tags."""
    metadata: Manifest = pod.get("metadata") or {}
    try:
        tags = json.loads((metadata.get("annotations") or {})[TAGS_ANNOTATION])
    except (KeyError, ValueError):
        return None
    if not isinstance(tags, dict):
        return None
    status: Manifest = pod.get("status") or {}
    phase = status.get("phase")
    error = None
    if phase == "Running":
        state = MachineState.RUNNING
    elif phase in {"Succeeded", "Failed"}:
        state = MachineState.ENDED
        error = None if phase == "Succeeded" else _failure(status)
    else:
        state = MachineState.PENDING
    # An eviction or a node drain sets it to the end of the grace period: the builder's notice.
    deletion = metadata.get("deletionTimestamp")
    return Machine(
        provider_id=str(metadata["name"]),
        state=state,
        tags={str(key): str(value) for key, value in tags.items()},
        notice=None if deletion is None else _epoch(str(deletion)),
        error=error,
    )


def _status_code(error: kr8s.ServerError) -> int | None:
    return None if error.response is None else error.response.status_code


def _quota(error: kr8s.ServerError) -> bool:
    """A refusal by a ResourceQuota: there is no room for a builder now, not a broken template."""
    return _status_code(error) == HTTPStatus.FORBIDDEN and "exceeded quota" in str(error)


_FAILURES: Final = (kr8s.ServerError, kr8s.NotFoundError, kr8s.ConnectionClosedError, httpx.HTTPError, OSError)
"""What a kr8s call raises when the API server fails it, refuses it or is not there. kr8s raises httpx's own
errors for a transport failure. A 403 on `/version` comes while a new cluster's RBAC is not yet in place."""


REQUEST_SECONDS: Final = 30.0
"""How long one call waits for the API server. kr8s sets no timeout, so an unanswered request would hold the
provisioner's loop for ever."""


@contextlib.contextmanager
def failing(what: str, seconds: float = REQUEST_SECONDS) -> Iterator[None]:
    """A failed or unanswered call as `ProviderError`, which the provisioner retries; any other exception ends
    pynixd."""
    try:
        with anyio.fail_after(seconds):
            yield
    except TimeoutError as error:
        raise ProviderError(f"kubernetes: {what}: no answer in {seconds:g} s") from error
    except _FAILURES as error:
        raise ProviderError(f"kubernetes: {what}: {error}") from error


class KubernetesProvider:
    name = "kubernetes"

    def __init__(self, settings: KubernetesSettings) -> None:
        self._settings = settings
        self._api: kr8s.asyncio.Api | None = None

    async def _client(self) -> kr8s.asyncio.Api:
        # kr8s reads the kubeconfig's TLS files on the event loop at each session, 0.16 ms: nixidae/nixidae#1562.
        if self._api is None:
            with failing("opening the API"):
                try:
                    self._api = await kr8s.asyncio.api(
                        kubeconfig=None if self._settings.kubeconfig is None else str(self._settings.kubeconfig),
                        context=self._settings.context,
                        namespace=self._settings.namespace,
                    )
                except ValueError as error:
                    # kr8s' word for no kubeconfig and no service account: a node not yet set up.
                    raise ProviderError(f"kubernetes: no API: {error}") from error
        return self._api

    def _template(self, name: str) -> KubernetesTemplateSettings:
        try:
            return self._settings.templates[name]
        except KeyError:
            raise ProviderError(f"kubernetes: no template {name!r}") from None

    async def launch(self, template: str, tags: Mapping[str, str], user_data: str) -> Machine:
        spec = self._template(template)
        api = await self._client()
        name = pod_name(tags)
        pod = Pod(pod_manifest(spec, name, tags), api=api)
        with failing(f"creating pod {name}"):
            try:
                await pod.create()
            except kr8s.ServerError as error:
                if _quota(error):
                    raise NoCapacity(f"kubernetes: {error}") from error
                if _status_code(error) != HTTPStatus.CONFLICT:
                    raise
                # A launch whose answer was lost made it: the same tags name the same pod.
                pod = await Pod.get(name, namespace=self._settings.namespace, api=api)
        try:
            with failing(f"creating secret {name}"):
                try:
                    await Secret(secret_manifest(name, str(pod.raw["metadata"]["uid"]), user_data), api=api).create()
                except kr8s.ServerError as error:
                    if _status_code(error) != HTTPStatus.CONFLICT:
                        raise
        except ProviderError:
            # The provisioner records a failed launch with no pod to end, and a pod without its Secret waits
            # in Pending for ever.
            with contextlib.suppress(ProviderError):
                await self.terminate(name)
            raise
        found = machine(pod.raw)
        if found is None:
            raise ProviderError(f"kubernetes: pod {name} lost its tags")
        return found

    async def terminate(self, provider_id: str) -> None:
        api = await self._client()
        with failing(f"deleting pod {provider_id}"):
            try:
                pod = await Pod.get(provider_id, namespace=self._settings.namespace, api=api)
                await pod.delete(propagation_policy="Background")
            except kr8s.NotFoundError:
                return
            except kr8s.ServerError as error:
                if _status_code(error) != HTTPStatus.NOT_FOUND:
                    raise

    async def machines(self, tags: Mapping[str, str]) -> list[Machine]:
        api = await self._client()
        with failing("listing pods"):
            pods = [
                pod
                async for pod in api.get(
                    "pods",
                    namespace=self._settings.namespace,
                    label_selector={MANAGED_LABEL: MANAGED_VALUE},
                    raw=True,
                )
            ]
        found = [one for pod in pods if isinstance(pod, dict) and (one := machine(pod)) is not None]
        return [one for one in found if all(one.tags.get(key) == value for key, value in tags.items())]

    async def close(self) -> None:
        # kr8s keeps one client per argument set for the thread and has no close.
        self._api = None

    def build_pod_template(self, template: str) -> KubernetesTemplateSettings:
        """The template build pods of a store start from; `ProviderError` for a name the settings lack."""
        return self._template(template)

    async def start_build_pod(
        self,
        template: str,
        name: str,
        owner: BuildPodOwner,
        job: Mapping[str, str],
        farm: FarmVolume | None = None,
    ) -> None:
        """A pod for one build of *owner*, with *job* in a Secret it owns (nixidae/nixidae#1560, spike).

        The pod has no tags, so `machines` and the provisioner never see it.
        """
        spec = self._template(template)
        api = await self._client()
        pod = Pod(build_pod_manifest(spec, name, owner, farm), api=api)
        with failing(f"creating build pod {name}"):
            try:
                await pod.create()
            except kr8s.ServerError as error:
                if _quota(error):
                    raise NoCapacity(f"kubernetes: {error}") from error
                raise
        try:
            with failing(f"creating secret {name}"):
                await Secret(job_secret_manifest(name, str(pod.raw["metadata"]["uid"]), job), api=api).create()
        except ProviderError:
            with contextlib.suppress(ProviderError):
                await self.terminate(name)
            raise

    async def delete_left_build_pods(self, owner: BuildPodOwner) -> list[str]:
        """Delete the build pods of *owner*'s store that another run made, and name them. Their Secrets go
        with them."""
        api = await self._client()
        with failing(f"deleting the build pods earlier runs of store {owner.store} left"):
            names: list[str] = []
            async for pod in api.get(Pod, namespace=self._settings.namespace, label_selector=owner.others()):
                if not isinstance(pod, Pod):
                    raise ProviderError(f"kubernetes: listing pods answered {type(pod).__name__}")
                names.append(pod.name)
        for name in names:
            await self.terminate(name)
        return names

    async def watch_build_pod(self, name: str, watch: BuildPodWatch) -> None:
        """Keep *watch* up to date with build pod *name* from its creation, until the pod ended or is gone.

        No limit of its own: a pod waits for room and builds for as long as its deadline lets it. A watch the
        API server closes starts again from a new read.
        """
        api = await self._client()
        with failing(f"watching build pod {name}", seconds=math.inf):
            while not watch.ended.is_set():
                try:
                    pod = await Pod.get(name, namespace=self._settings.namespace, api=api)
                except kr8s.NotFoundError:
                    watch.gone()
                    return
                watch.saw(pod.raw)
                # From the read's resourceVersion: no change between the read and the watch is lost.
                async for event, changed in pod.watch():
                    watch.saw(changed.raw)
                    if event == "DELETED":
                        watch.gone()
                    if watch.ended.is_set():
                        return

    async def follow_build_pod(self, name: str, container: str) -> AsyncIterator[str]:
        """The container's log lines, until the container ends. Only for a container that started: a pod that
        waits for room, as a low-priority pod does on a full cluster, answers a log request with 400."""
        api = await self._client()
        # As long as the build: the pod's `activeDeadlineSeconds` bounds it.
        with failing(f"reading the log of build pod {name}", seconds=math.inf):
            pod = await Pod.get(name, namespace=self._settings.namespace, api=api)
            async for line in pod.logs(container=container, follow=True):
                yield line


BUILD_LABEL: Final = "pynixd.nixidae.org/build"
BUILD_STORE_LABEL: Final = "pynixd.nixidae.org/build-store"
BUILD_RUN_LABEL: Final = "pynixd.nixidae.org/build-run"
_LABEL_VALUE: Final = re.compile(r"[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?")
JOB_VOLUME: Final = "pynixd-job"
JOB_DIR: Final = "/run/pynixd-job"


def build_store_label(store_id: str) -> str:
    """The label value that marks a store's build pods: the store's id, or a digest of an id no label can hold."""
    if _LABEL_VALUE.fullmatch(store_id):
        return store_id
    return hashlib.sha256(store_id.encode()).hexdigest()[:_HASH_CHARS]


@dataclass(frozen=True, slots=True)
class BuildPodOwner:
    """Who made a build pod: a store of one central, in one run of its process. A label holds each."""

    store: str
    run: str
    """New in each process: a pod of another run can never report, as its key died with that process."""

    def labels(self) -> dict[str, str]:
        return {
            MANAGED_LABEL: MANAGED_VALUE,
            BUILD_LABEL: "true",
            BUILD_STORE_LABEL: build_store_label(self.store),
            BUILD_RUN_LABEL: self.run,
        }

    def others(self) -> str:
        """The selector of the store's pods that another run made."""
        mine = self.labels()
        run = mine.pop(BUILD_RUN_LABEL)
        return ",".join([*(f"{key}={value}" for key, value in mine.items()), f"{BUILD_RUN_LABEL}!={run}"])


@dataclass(frozen=True, slots=True)
class BuildPodEnd:
    exit_code: int | None
    """None when the container never ran, or the pod went before the kubelet wrote it."""
    message: str
    """The container's termination message: the build's result, or empty."""
    cause: str
    """Why the pod ended: `PreemptionByScheduler`, `Evicted`, `DeadlineExceeded`, or the container's reason."""

    @property
    def disrupted(self) -> bool:
        return self.cause in DISRUPTIONS


DISRUPTIONS: Final = frozenset(
    {
        "PreemptionByScheduler",
        "EvictionByEvictionAPI",
        "DeletionByTaintManager",
        "TerminationByKubelet",
        "DeletionByPodGC",
        "Evicted",
    }
)
"""Why Kubernetes takes a pod for its own reasons: the `DisruptionTarget` reasons, and the kubelet's
node-pressure `Evicted`. A build pod ended so did nothing wrong."""


@dataclass(slots=True)
class BuildPodWatch:
    """What one watch of a build pod has seen (`KubernetesProvider.watch_build_pod`).

    The reason for an end is kept from each change, not read at the end: a pod can be gone by then. Measured:
    the scheduler preempted a build pod at 23:05:10, the kubelet killed it at 23:05:11, and a read at 23:05:23
    found nothing.
    """

    container: str
    settled: anyio.Event = field(default_factory=anyio.Event)
    """The container started, or the pod ended without it: its log can be read, or never will be."""
    ended: anyio.Event = field(default_factory=anyio.Event)
    ran: bool = False
    end: BuildPodEnd | None = None
    cause: str | None = None
    """The last reason seen for the pod's end (`pod_end`)."""

    def saw(self, pod: Manifest) -> None:
        """One state of the pod. The log ends when the container exits, and the kubelet writes the container's
        end after that (measured: a crash read as still running), so the end waits for that state."""
        if self.ended.is_set():
            return
        ending = pod_end(pod)
        self.cause = ending or self.cause
        if _started(pod, self.container):
            self.ran = True
            self.settled.set()
        terminated = _terminated(pod, self.container)
        if terminated is None and ending is None:
            return
        exit_code = (terminated or {}).get("exitCode")
        self._end(
            BuildPodEnd(
                exit_code=exit_code if isinstance(exit_code, int) else None,
                message=str((terminated or {}).get("message") or ""),
                cause=ending or str((terminated or {}).get("reason") or "exited"),
            )
        )

    def gone(self) -> None:
        self._end(BuildPodEnd(exit_code=None, message="", cause=self.cause or "deleted"))

    def lost(self, error: str) -> None:
        """The watch failed: nothing more will be known of the pod."""
        self._end(BuildPodEnd(exit_code=None, message="", cause=self.cause or f"watch failed: {error}"))

    def _end(self, end: BuildPodEnd) -> None:
        if self.ended.is_set():
            return
        self.end = end
        self.settled.set()
        self.ended.set()


def _container_state(pod: Manifest, container: str) -> Manifest:
    for status in (pod.get("status") or {}).get("containerStatuses") or []:
        if status.get("name") == container:
            state: Manifest = status.get("state") or {}
            return state
    return {}


def _terminated(pod: Manifest, container: str) -> Manifest | None:
    terminated: Manifest | None = _container_state(pod, container).get("terminated")
    return terminated


def _started(pod: Manifest, container: str) -> bool:
    return bool({"running", "terminated"} & _container_state(pod, container).keys())


def pod_end(pod: Manifest) -> str | None:
    """Why the pod ended or is ending, or None while it may still run.

    The scheduler and the kubelet mark a pod they remove with a `DisruptionTarget` condition before they
    delete it: its reason says preemption or eviction. A pod past `activeDeadlineSeconds` fails with
    `DeadlineExceeded` and never runs a container that waits.
    """
    status: Manifest = pod.get("status") or {}
    for condition in status.get("conditions") or []:
        if condition.get("type") == "DisruptionTarget" and condition.get("status") == "True":
            return str(condition.get("reason") or "DisruptionTarget")
    if status.get("phase") in {"Failed", "Succeeded"}:
        return str(status.get("reason") or status.get("phase"))
    return None


_STORE_PATH: Final = re.compile(r"/nix/store/[0-9a-df-np-sv-z]{32}-[A-Za-z0-9+\-._?=]+")
FARM_PRESENTATION: Final = {"presentation": "farm"}
"""nixkube's CSI attribute for one read-only bind per closure path (`nixkube/pkgs/nixkube/src/presentation.py`)."""


def template_store_paths(template: KubernetesTemplateSettings) -> frozenset[str]:
    """Every store path the template's pod names: what its containers run, and what a farm `/nix` must hold.

    nixkube reads a pod the same way (`extract_store_paths`). A path inside another names that one.
    """
    return frozenset(_STORE_PATH.findall(json.dumps(template.pod)))


def farm_volume(pod: Manifest, volume: str) -> Manifest:
    """The pod's CSI volume *volume*; `ValueError` when it has none of that name."""
    for entry in (pod.get("spec") or {}).get("volumes") or []:
        if entry.get("name") == volume and "csi" in entry:
            found: Manifest = entry
            return found
    raise ValueError(f"kubernetes: the template has no CSI volume {volume!r} for a build's farm")


@dataclass(frozen=True, slots=True)
class FarmVolume:
    """A build's farm: the template's CSI volume *volume* points at *root* for each node system of *systems*."""

    volume: str
    root: str
    systems: frozenset[str]


def build_pod_manifest(
    template: KubernetesTemplateSettings,
    name: str,
    owner: BuildPodOwner,
    farm: FarmVolume | None = None,
) -> Manifest:
    """The template's pod for one build of *owner*: no tags, *owner*'s labels, and the job Secret at `JOB_DIR`
    in its container.

    With *farm*, its CSI volume asks nixkube for a farm of the build's root path. nixkube keys the path by the
    node's system, not the derivation's: an i686 build on an x86_64 node reads the x86_64 key.
    """
    pod: Manifest = copy.deepcopy(template.pod)
    if farm is not None:
        csi = farm_volume(pod, farm.volume)["csi"]
        csi["volumeAttributes"] = {
            **(csi.get("volumeAttributes") or {}),
            **dict.fromkeys(sorted(farm.systems), farm.root),
            **FARM_PRESENTATION,
        }
    pod["apiVersion"] = "v1"
    pod["kind"] = "Pod"
    metadata = pod.setdefault("metadata", {})
    metadata["name"] = name
    metadata.pop("generateName", None)
    metadata.setdefault("labels", {}).update(owner.labels())
    spec = pod.setdefault("spec", {})
    spec["restartPolicy"] = "Never"
    spec.setdefault("volumes", []).append({"name": JOB_VOLUME, "secret": {"secretName": name, "defaultMode": 0o400}})
    containers: list[Manifest] = spec.get("containers") or []
    target = next((c for c in containers if c.get("name") == template.container), None)
    if template.container is None and containers:
        target = containers[0]
    if target is None:
        raise ValueError(f"kubernetes: the template has no container {template.container!r}")
    target.setdefault("volumeMounts", []).append({"name": JOB_VOLUME, "mountPath": JOB_DIR, "readOnly": True})
    return pod


def job_secret_manifest(name: str, pod_uid: str, job: Mapping[str, str]) -> Manifest:
    manifest = secret_manifest(name, pod_uid, "")
    manifest["stringData"] = dict(job)
    return manifest
