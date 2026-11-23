"""A store whose every build is a Kubernetes pod of its own, which dials central and ends. nixidae/nixidae#1560, spike.

The scheduler places a build here like on any builder, up to the spec's
slots. No connection goes from central to a pod: the executor creates the
pod (`Executor.build_in_pod`), grants the pod's key the derivation
(`build_grant.py`), follows the pod's log, and reads the `BuildResult` from
the pod's termination message (`pod_build.py`). The pod has added the
outputs to central's store by then.
"""

from __future__ import annotations

import contextlib
import secrets
from typing import TYPE_CHECKING, Any, Final

import anyio
import asyncssh
import structlog

from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest, QueryValidPathsResponse
from nix_daemon_protocol.store_path import StorePath

from ..build_grant import BuildGrant
from ..build_job import JOB_NAME, KEY_NAME, Job, decode_response, encode
from ..build_result import failure, result_succeeded
from ..exceptions import InfrastructureError
from ..provisioning.kubernetes import BuildPodOwner, BuildPodWatch, FarmVolume, farm_volume, template_store_paths
from ..provisioning.provider import ProviderError
from .scheduled import ProbeState, ScheduledStore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from contextlib import AbstractAsyncContextManager

    from nix_daemon_protocol.build_derivation import BuildDerivationRequest, BuildDerivationResponse
    from nix_daemon_protocol.set_options import SetOptionsRequest
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd_store.drv_parser import Derivation

    from ..build_grant import Grants
    from ..config import BuildPodStoreSpec
    from ..connection import ClientConn, Connection
    from ..provisioning.kubernetes import KubernetesProvider
    from .base import Store

log = structlog.get_logger(__name__)

_DELETE_SECONDS: Final = 30.0
_SWEEP_RETRY_SECONDS: Final = 10.0
_ENDED_SECONDS: Final = 60.0
"""How long the kubelet may take to write a container's end after its log ended."""


FARM_ROOT_NAME: Final = "pynixd-build-inputs"
"""The name of the text path whose closure is a build pod's farm (`BuildPodFarmSpec`)."""


def farm_references(request: BuildDerivationRequest, template_paths: frozenset[str]) -> frozenset[str]:
    """What a build pod's farm must hold: the build's inputs, and every store path its template names."""
    inputs = {str(StorePath(str(path)).with_store_prefix()) for path in request.derivation.input_srcs}
    return frozenset(inputs) | template_paths


def farm_text(references: frozenset[str]) -> str:
    """The farm root's text: its references, one a line, so the same inputs make the same path."""
    return "".join(f"{path}\n" for path in sorted(references))


class PodDisrupted(InfrastructureError):
    """Kubernetes took the pod for another, by preemption or eviction: the build goes back, and no store failed."""


def checked_result(request: BuildDerivationRequest, response: BuildDerivationResponse) -> BuildDerivationResponse:
    """The pod's result, or a failure when it realises what the derivation does not have, or too little.

    A pod says which path each output became, and central records that as the
    realisation. The grant let the pod add only the static outputs and
    content-addressed paths, so a floating output's path is one of those; this
    holds the names, and a static output to its own path. A success must name
    each floating output: central's check that the outputs arrived reads the
    paths from here, and with none it checks nothing.
    """
    static = request.derivation.output_paths()
    realised = response.result.realised_outputs()
    if result_succeeded(response.result):
        floating = {name for name in request.derivation.outputs if static.get(name, StorePath("")) == StorePath("")}
        if missing := sorted(name for name in floating if realised.get(name) is None or not realised[name].out_path):
            return response.model_copy(
                update={"result": failure(f"a build pod built, and realised no path for {', '.join(missing)}")}
            )
    for name, realisation in realised.items():
        if name not in request.derivation.outputs:
            return response.model_copy(update={"result": failure(f"a build pod realised {name}, no output of it")})
        known = static.get(name, StorePath(""))
        if realisation.out_path is None or known == StorePath(""):
            continue
        if StorePath(str(realisation.out_path)).with_store_prefix() != known:
            return response.model_copy(
                update={"result": failure(f"a build pod realised {name} as {realisation.out_path}, not {known}")}
            )
    return response


class BuildPodStore(ScheduledStore):
    def __init__(self, spec: BuildPodStoreSpec) -> None:
        super().__init__(spec)
        self.spec = spec
        self.grants: Grants | None = None
        """The context's; `Server` sets it, as the stores exist before the context does."""
        self.central_host_key: str | None = None
        """Central's SSH host key, which the pod pins; `Server` sets it when the SSH listener starts."""
        self.kubernetes: KubernetesProvider | None = None
        self.owner = BuildPodOwner(store=str(self.store_id), run=secrets.token_hex(4))
        self.template_paths: frozenset[str] = frozenset()
        """The store paths the template names, which each farm holds; `Server` reads them when it arms the store."""

    async def sweep(self) -> None:
        """Delete the store's pods an earlier process left, once the API server answers.

        Their keys died with that process, so none can report. A server that a hand-over started never sweeps,
        as the old process still drains its builds. Pods of this run carry its own label, so a late sweep
        keeps them.
        """
        if self.kubernetes is None:
            return
        while True:
            try:
                left = await self.kubernetes.delete_left_build_pods(self.owner)
            except ProviderError as error:
                # The API server may come up after central (a cluster that runs central), and says nothing when.
                log.info("build_pods_sweep_waits", store_id=self.store_id, error=str(error))
                await anyio.sleep(_SWEEP_RETRY_SECONDS)
                continue
            if left:
                log.info("build_pods_left_deleted", store_id=self.store_id, pods=left)
            return

    async def arm(self, grants: Grants, host_key: str, kubernetes: KubernetesProvider, local: Store) -> None:
        """Take what the server has only once it runs: the grants, central's SSH host key, the provider.

        A store whose pods take a farm refuses to start here, not at its first build: a template without the
        farm's volume, or a store path the template names that *local* cannot give a node, fails every build.
        """
        self.grants = grants
        self.central_host_key = host_key
        self.kubernetes = kubernetes
        if self.spec.farm is None:
            return
        template = kubernetes.build_pod_template(self.spec.template)
        farm_volume(template.pod, self.spec.farm.volume)
        paths = template_store_paths(template)
        # `substitute`: central fetches what its caches hold, as a node asks central for it next.
        valid = await local.execute(
            QueryValidPathsRequest(paths={StorePath(path) for path in paths}, substitute=1), client=None
        )
        held = {str(StorePath(str(path)).with_store_prefix()) for path in valid.paths}
        if missing := sorted(paths - held):
            raise RuntimeError(
                f"store {self.store_id}: its template names {', '.join(missing)}, which a farm needs"
                " and central's store and caches lack"
            )
        self.template_paths = paths

    @property
    def daemon_client(self) -> BuildPodStore:
        # What the scheduler places on; `Executor.build_in_pod` takes it before any connection is asked for.
        return self

    async def start(self, sync_paths: bool = True) -> None:
        del sync_paths
        await self.probe()

    async def probe(self) -> None:
        self.probe_state = ProbeState.PROBED
        self._probe_event.set()

    def _holds_nothing(self) -> InfrastructureError:
        # A pod's store lives only as long as its build; central has none to ask.
        return InfrastructureError(f"{self.store_id}: a build pod store holds no paths")

    async def create_conn(self) -> Connection:
        raise self._holds_nothing()

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:  # noqa: ANN401 -- the base class's signature
        raise self._holds_nothing()

    async def execute(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        skip_probe: bool = False,
    ) -> Any:  # noqa: ANN401 -- the base class's signature
        if isinstance(request, QueryValidPathsRequest):
            return await self.query_valid_paths(request)
        raise self._holds_nothing()

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        return None

    async def is_valid_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:  # noqa: ANN401 -- the base class's
        raise self._holds_nothing()

    async def query_path_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:  # noqa: ANN401 -- the base class's
        raise self._holds_nothing()

    async def query_path_from_hash_part(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:  # noqa: ANN401 -- the base class's
        raise self._holds_nothing()

    async def query_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:  # noqa: ANN401 -- the base class's
        # A copy-in pod starts empty and central sends it every input, so the placement prices the whole
        # closure. A farm pod reads its inputs from the node, which central does not see: its price stays unknown.
        if self.spec.farm is None:
            return QueryValidPathsResponse(paths=set())
        raise self._holds_nothing()

    async def nar_from_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:  # noqa: ANN401 -- the base class's
        raise self._holds_nothing()

    async def answer_handshake(self) -> None:
        # Each pod dials central; there is nothing here to answer.
        return

    def build_conn(self, options: SetOptionsRequest | None = None) -> AbstractAsyncContextManager[Connection]:
        raise InfrastructureError(f"{self.store_id}: a build pod store has no connection")

    def transfer_conn(self, options: SetOptionsRequest | None = None) -> AbstractAsyncContextManager[Connection]:
        raise InfrastructureError(f"{self.store_id}: a build pod store has no connection")

    async def _watch(self, kubernetes: KubernetesProvider, name: str, watch: BuildPodWatch) -> None:
        try:
            await kubernetes.watch_build_pod(name, watch)
        except ProviderError as error:
            # The build goes back to the queue: with no watch, nothing says how the pod ended.
            log.warning("build_pod_watch_failed", pod=name, error=str(error))
            watch.lost(str(error))

    async def build(
        self,
        request: BuildDerivationRequest,
        on_line: Callable[[str], Awaitable[None]],
        farm_root: str | None = None,
        depth: int = 0,
    ) -> BuildDerivationResponse:
        """Build *request* in a pod; `InfrastructureError` when the pod, not the build, failed.

        *farm_root*, with the spec's `farm`, is the path the pod's `/nix` is a farm of: central holds it valid.
        *depth* is the build's in recursive Nix, which picks the pod's PriorityClass.
        """
        if self.grants is None or self.central_host_key is None or self.kubernetes is None:
            raise InfrastructureError(f"{self.store_id}: no SSH listener or no Kubernetes settings for build pods")
        # A floating output has no path yet: the grant takes it as a content-addressed path.
        paths = request.derivation.output_paths()
        outputs = frozenset(str(path) for path in paths.values() if path != StorePath(""))
        key = asyncssh.generate_private_key("ssh-ed25519")
        public = key.export_public_key().decode()
        name = f"pynixd-build-{StorePath(str(request.drv_path)).hash_part()[:12]}-{secrets.token_hex(3)}"
        job = Job(
            central_host=self.spec.central_host,
            central_port=self.spec.central_port,
            central_host_key=self.central_host_key,
            request=await encode(request),
        )
        self.grants.grant(public, BuildGrant(drv_path=str(request.drv_path), outputs=outputs))
        try:
            secret = {JOB_NAME: job.model_dump_json(), KEY_NAME: key.export_private_key().decode()}
            farm = (
                FarmVolume(volume=self.spec.farm.volume, root=farm_root, systems=frozenset(self.spec.systems or ()))
                if self.spec.farm is not None and farm_root is not None
                else None
            )
            priority = self.spec.priority_class(depth)
            await self.kubernetes.start_build_pod(
                self.spec.template, name, self.owner, secret, farm, priority_class=priority
            )
            log.info("build_pod_started", pod=name, drv=str(request.drv_path), depth=depth, priority_class=priority)
            watch = BuildPodWatch(self.spec.container)
            async with anyio.create_task_group() as watching:
                watching.start_soon(self._watch, self.kubernetes, name, watch)
                await watch.settled.wait()
                if watch.ran:
                    try:
                        async for line in self.kubernetes.follow_build_pod(name, self.spec.container):
                            await on_line(line)
                    except ProviderError as error:
                        # The log of a pod that is preempted or deleted breaks off: how the pod ended says why.
                        log.warning("build_pod_log_ended", pod=name, error=str(error))
                with anyio.move_on_after(_ENDED_SECONDS):
                    await watch.ended.wait()
                watching.cancel_scope.cancel()
            ended = watch.end
            if ended is None:
                raise InfrastructureError(f"build pod {name}: its log ended while its container ran")
            if ended.exit_code != 0 or not ended.message:
                fault = PodDisrupted if ended.disrupted else InfrastructureError
                raise fault(f"build pod {name}: {ended.cause} with exit {ended.exit_code} and no result")
            try:
                return checked_result(request, await decode_response(ended.message))
            except (ValueError, EOFError) as error:
                raise InfrastructureError(f"build pod {name}: its result does not read: {error}") from error
        except ProviderError as error:
            raise InfrastructureError(str(error)) from error
        finally:
            self.grants.revoke(public)
            # A pod left behind ends at its template's `activeDeadlineSeconds`.
            with (
                anyio.CancelScope(shield=True),
                anyio.move_on_after(_DELETE_SECONDS),
                contextlib.suppress(ProviderError),
            ):
                await self.kubernetes.terminate(name)
