"""Recursive Nix: the daemon a sandboxed builder reaches, restricted as `RestrictedStore` (`restricted-store.cc`).

`startDaemon` (`derivation-builder.cc:1153`) runs `processConnection` with `NotTrusted` and `Recursive` over a
store that answers only for the input closure and what the builder added since. `RestrictedProxy` is that
session: pynixd's own op handling, with each `RestrictedStore` override applied before or after it.

A `builder-rpc-v0` build gets `RecursiveSubmitted` instead (`unix-derivation-builder.cc:693`): the version and
features of `builderRpcV0`, the operations of `daemon.cc:332` alone, and `SubmitOutput` for its outputs.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import anyio
import structlog
from anyio.abc import SocketListener

from nix_daemon_protocol.add_indirect_root import AddIndirectRootRequest, AddIndirectRootResponse
from nix_daemon_protocol.add_signatures import AddSignaturesRequest
from nix_daemon_protocol.add_temp_root import AddTempRootRequest, AddTempRootResponse
from nix_daemon_protocol.add_temp_roots import AddTempRootsRequest, AddTempRootsResponse
from nix_daemon_protocol.add_to_store_scanning import AddToStoreScanningRequest, AddToStoreScanningResponse
from nix_daemon_protocol.build_derivation import BuildDerivationRequest
from nix_daemon_protocol.build_paths import BuildPathsRequest, BuildPathsResponse
from nix_daemon_protocol.build_paths_with_results import BuildPathsWithResultsRequest, BuildPathsWithResultsResponse
from nix_daemon_protocol.build_result import BuildMode
from nix_daemon_protocol.collect_garbage import CollectGarbageRequest, CollectGarbageResponse
from nix_daemon_protocol.constants import (
    FEATURE_ADD_TO_STORE_SCANNING,
    FEATURE_DISABLE_SET_OPTIONS,
    FEATURE_REALISATION_WITH_PATH,
    FEATURE_SUBMIT_OUTPUT,
)
from nix_daemon_protocol.ensure_path import EnsurePathRequest, EnsurePathResponse
from nix_daemon_protocol.find_roots import FindRootsRequest, FindRootsResponse
from nix_daemon_protocol.is_valid_path import IsValidPathRequest, IsValidPathResponse
from nix_daemon_protocol.nar_from_path import NarFromPathRequest
from nix_daemon_protocol.optimise_store import OptimiseStoreRequest, OptimiseStoreResponse
from nix_daemon_protocol.query_all_valid_paths import QueryAllValidPathsRequest, QueryAllValidPathsResponse
from nix_daemon_protocol.query_derivation_output_map import QueryDerivationOutputMapRequest
from nix_daemon_protocol.query_missing import QueryMissingRequest, QueryMissingResponse
from nix_daemon_protocol.query_path_from_hash_part import QueryPathFromHashPartRequest
from nix_daemon_protocol.query_path_info import QueryPathInfoRequest, QueryPathInfoResponse
from nix_daemon_protocol.query_path_infos import QueryPathInfosRequest, QueryPathInfosResponse
from nix_daemon_protocol.query_realisation import QueryRealisationRequest, QueryRealisationResponse
from nix_daemon_protocol.query_referrers import QueryReferrersRequest, QueryReferrersResponse
from nix_daemon_protocol.query_substitutable_path_info import QuerySubstitutablePathInfoRequest
from nix_daemon_protocol.query_substitutable_path_infos import QuerySubstitutablePathInfosRequest
from nix_daemon_protocol.query_substitutable_paths import QuerySubstitutablePathsRequest
from nix_daemon_protocol.query_valid_derivers import QueryValidDeriversRequest, QueryValidDeriversResponse
from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest
from nix_daemon_protocol.register_drv_output import RegisterDrvOutputRequest
from nix_daemon_protocol.set_options import SetOptionsRequest, SetOptionsResponse
from nix_daemon_protocol.store_dir import store_dir
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.submit_output import SubmitOutputRequest, SubmitOutputResponse
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.verify_store import VerifyStoreRequest, VerifyStoreResponse
from nix_daemon_protocol.wire_ops import WIRE_REGISTRY
from pynixd_store.builder.restriction import AddDependencyError, SubmitError
from pynixd_store.content_address import parse_method_algo
from pynixd_store.hintfmt import magenta

from .exceptions import BackendError, ClosingError, PynixdError
from .goals.engine import _build_failure_message
from .goals.results import result_succeeded
from .handlers.nar_from_path import NarFromPathHandler
from .proxy import BUILDER_ONLY_OPS, DaemonProxy
from .serde.auth import Role
from .serde.context import ReadContext, RequestContext
from .store import LocalDBStore
from .unix_socket import as_link
from .wire import FramedReader, UnixNixReader, UnixNixWriter

if TYPE_CHECKING:
    import socket
    from collections.abc import AsyncIterator

    from anyio.abc import SocketStream

    from nix_daemon_protocol.wire_ops import WireRequest, WireResponse
    from pynixd_store.builder.restriction import RecursiveServer, RestrictionContext
    from pynixd_store.engine import StoreEngine

    from .context import PynixdContext
    from .wire import NixReader, NixWriter

log = structlog.get_logger(__name__)

STORE_NAME = "local"
"""What `Store::unsupported` names the restricted store, measured on Nix 2.35.3 (`local-store.cc:452`)."""

_ADDS = frozenset({7, 39, 44})
"""AddToStore, AddToStoreNar, AddMultipleToStore: each path they add joins the build (`restricted-store.cc:203`)."""

_PASSED = frozenset({45, 47})
"""AddBuildLog and AddPermRoot: an untrusted client gets Nix's refusal from the handler already."""

_SUBMITTING_OPS = _ADDS | {
    AddToStoreScanningRequest.op,
    SubmitOutputRequest.op,
    AddTempRootRequest.op,
    IsValidPathRequest.op,
}
"""The allowlist of `RecursiveSubmitted` (`daemon.cc:332`)."""

BUILDER_RPC_FEATURES = frozenset(
    {FEATURE_REALISATION_WITH_PATH, FEATURE_DISABLE_SET_OPTIONS, FEATURE_ADD_TO_STORE_SCANNING, FEATURE_SUBMIT_OUTPUT}
)
"""The features of `WorkerProto::builderRpcV0` (`worker-protocol.cc:42`)."""


class RestrictedError(PynixdError):
    """An error `RestrictedStore` throws after `startWork`: the session goes on."""


def _unsupported(op: str) -> RestrictedError:
    """`Store::unsupported` (`store-api.hh:950`)."""
    return RestrictedError(f"operation {_quoted(op)} is not supported by store {_quoted(STORE_NAME)}")


def _quoted(text: str) -> str:
    return f"'{magenta(text)}'"


def _path_part(derived: str) -> str:
    """`pathPartOfReq`: the store path a derived path starts from."""
    return derived.split("!", 1)[0]


def _path_of(derived: str) -> StorePath | None:
    """The store path a derived path starts from, or `None` when its text names no path of this store."""
    try:
        return StorePath.in_store(_path_part(derived), store_dir())
    except ValueError:
        return None


class RestrictedProxy(DaemonProxy):
    """One recursive-nix client of a build: an untrusted session that reaches only *restriction*."""

    def __init__(self, reader: NixReader, writer: NixWriter, ctx: PynixdContext, restriction: RestrictionContext):
        # `processConnection` watches no hangup in recursive mode (`daemon.cc:1016`).
        super().__init__(reader, writer, ctx, role=Role.USER, username="recursive-nix", transport="recursive")
        self.restriction = restriction
        self._adding: list[str] | None = None

    pynixd_features = frozenset()
    """A builder is no pynixd peer."""

    @property
    def repairs_adds(self) -> bool:
        """A builder that repairs its outputs adds them over the paths it repairs."""
        return self.restriction.repairing

    def honourable_features(self) -> frozenset[str]:
        """`builderRpcV0` names its own features (`daemon.cc:1170`); recursive mode names those of the stores."""
        if self.restriction.submitting:
            return BUILDER_RPC_FEATURES
        return super().honourable_features()

    def server_features(self, honourable: frozenset[str]) -> set[str]:
        """The protocol features alone: none of pynixd's own operations."""
        return set(honourable)

    async def add_temp_root(self, path: str | StorePath) -> None:
        """Roots *path* for the session, and notes it when an add op is running: the add ops root each path."""
        if self._adding is not None:
            self._adding.append(str(path))
        await super().add_temp_root(path)

    async def dispatch(self, op_num: int) -> WireResponse | None:
        if self.restriction.submitting:
            if op_num not in _SUBMITTING_OPS:
                # `daemon.cc:345`, before `startWork`.
                raise ClosingError(f"Operation {magenta(str(op_num))} not allowed inside derivation")
            if op_num == SubmitOutputRequest.op:
                return self._submit(await SubmitOutputRequest.from_reader(self._read_context()))
            if op_num == AddToStoreScanningRequest.op:
                return await self._add_scanning(await AddToStoreScanningRequest.from_reader(self._read_context()))
        elif op_num in BUILDER_ONLY_OPS:
            # `daemon.cc:353`: `SubmitOutput` is `RecursiveSubmitted`'s alone, and `Recursive` of Nix 2.35 has no
            # `AddToStoreScanning`.
            raise ClosingError(f"invalid operation {op_num}")
        if op_num in _ADDS:
            return await self._add(op_num)
        if op_num in _PASSED:
            return await super().dispatch(op_num)
        if op_num == NarFromPathRequest.op:
            req = await NarFromPathRequest.from_reader(self._read_context())
            if not self.restriction.is_allowed(req.path):
                # NIX-DEVIATION (#27) of `NarFromPathHandler`: Nix throws after `stopWork` and closes the session.
                raise ClosingError(f"cannot dump unknown path {_quoted(str(req.path))} in recursive Nix")
            await NarFromPathHandler().serve(self._request_context(), req)
            return None
        if op_num == BuildDerivationRequest.op:
            req_drv = await BuildDerivationRequest.from_reader(self._read_context())
            outputs = req_drv.derivation.outputs.values()
            if not (outputs and all(output.is_ca for output in outputs)):
                raise RestrictedError("you are not privileged to build input-addressed derivations")
            raise _unsupported("buildDerivation")
        wire_cls = WIRE_REGISTRY.get(op_num)
        if wire_cls is None or wire_cls.is_extension:
            # `performOp` (`daemon.cc:1007`): an operation Nix does not know ends the session.
            raise ClosingError(f"invalid operation {op_num}")
        return await self.execute(await wire_cls.from_reader(self._read_context()))

    async def _add(self, op_num: int) -> WireResponse | None:
        """`RestrictedStore::addToStore` and `addToStoreFromDump`: the store adds the path, then the build does."""
        self._adding = []
        try:
            response = await super().dispatch(op_num)
        finally:
            added, self._adding = self._adding, None
            # `addMultipleToStore` adds each path in turn: those before an error joined already.
            with anyio.CancelScope(shield=True):
                for path in added:
                    if await self._engine.is_valid_path_uncached(path):
                        await self._add_dependency(StorePath(path))
        return response

    def _submit(self, request: SubmitOutputRequest) -> SubmitOutputResponse:
        """`SubmitOutput` (`daemon.cc:1113`)."""
        try:
            self.restriction.submit_output(request.path, request.output)
        except SubmitError as e:
            raise RestrictedError(str(e)) from e
        return SubmitOutputResponse(value=1)

    async def _add_scanning(self, request: AddToStoreScanningRequest) -> AddToStoreScanningResponse:
        """`AddToStoreScanning` (`daemon.cc:1081`) over `RestrictedStore::addToStoreScanning` (`:294`).

        The candidates are what the build may see (`queryAllValidPaths`), and the new path joins it.
        """
        if FEATURE_ADD_TO_STORE_SCANNING not in self.standard_features:
            raise ClosingError("Adding to store with scanning was requested, but not supported in negotiated protocol")
        frames = FramedReader(self.r)
        try:
            method, algo = parse_method_algo(str(request.cam), (await self._engine.settings()).experimental_features)
            path = await self._engine.add_to_store_from_dump(
                request.path_name,
                method,
                algo,
                frozenset(str(p) for p in self.restriction.allowed),
                frames,
                root=self.add_temp_root,
                repair=self.restriction.repairing,
                scan=True,
            )
        except ValueError as e:
            raise RestrictedError(str(e)) from e
        finally:
            await frames.ensure_eof()
        self._engine.invalidate_path_info_cache_for(StorePath(path))
        await self._add_dependency(StorePath(path))
        found = await self._path_info(QueryPathInfoRequest(path=StorePath(path)))
        if not isinstance(found, QueryPathInfoResponse) or found.info is None:
            raise RestrictedError(f"path '{magenta(path)}' is not valid")
        return AddToStoreScanningResponse(info=ValidPathInfo(path=StorePath(path), info=found.info))

    def _knows(self, derived: str) -> bool:
        """Whether the build may see the store path a derived path starts from."""
        path = _path_of(derived)
        return path is not None and self.restriction.is_allowed(path)

    async def _add_dependency(self, path: StorePath) -> None:
        try:
            await self.restriction.add_dependency(path)
        except AddDependencyError as e:
            raise RestrictedError(str(e)) from e

    @property
    def _engine(self) -> StoreEngine:
        store = self.local_store
        if not isinstance(store, LocalDBStore):
            raise TypeError("recursive Nix needs pynixd's own store")
        return store.engine

    async def _next(self, request: WireRequest) -> WireResponse:
        """The answer of the store behind the restriction (`next`)."""
        response: WireResponse = await super().execute(request)
        return response

    def _read_context(self) -> ReadContext:
        return ReadContext(reader=self.r, version=self.version, features=self.standard_features)

    def _request_context(self) -> RequestContext:
        return RequestContext(proxy=self, role=self.role, version=self.version, username=self.username)

    async def execute(self, request: WireRequest) -> WireResponse:  # type: ignore[override]  # one request type
        allowed = self.restriction.is_allowed
        match request:
            case SetOptionsRequest():
                # `daemon.cc:787`: recursive mode applies none of the settings.
                return SetOptionsResponse()
            case IsValidPathRequest() if not allowed(request.path):
                return IsValidPathResponse.fast(valid=False)
            case QueryValidPathsRequest():
                # `substitutePaths` finds nothing to do: an allowed path is valid already.
                paths = {p for p in request.paths if allowed(p)}
                return await self._next(request.model_copy(update={"paths": paths, "substitute": 0}))
            case QueryPathInfoRequest():
                return await self._path_info(request)
            case QueryPathInfosRequest():
                return await self._path_infos(request)
            case QueryReferrersRequest():
                return QueryReferrersResponse()
            case QueryValidDeriversRequest():
                return QueryValidDeriversResponse()
            case QueryAllValidPathsRequest():
                return QueryAllValidPathsResponse(paths=self.restriction.allowed)
            case QueryDerivationOutputMapRequest() if not allowed(request.path):
                raise RestrictedError(
                    f"cannot query output map for unknown path {_quoted(str(request.path))} in recursive Nix"
                )
            case QueryPathFromHashPartRequest():
                raise RestrictedError("queryPathFromHashPart")
            case RegisterDrvOutputRequest():
                raise RestrictedError("registerDrvOutput")
            case AddSignaturesRequest():
                raise _unsupported("addSignatures")
            case EnsurePathRequest():
                if not allowed(request.path):
                    raise RestrictedError(
                        f"cannot substitute unknown path {_quoted(str(request.path))} in recursive Nix"
                    )
                return EnsurePathResponse(value=1)
            case BuildPathsRequest() | BuildPathsWithResultsRequest():
                return await self._build(request)
            case QueryMissingRequest():
                return await self._missing(request)
            case QueryRealisationRequest():
                return await self._realisation(request)
            case AddTempRootRequest():
                return AddTempRootResponse(value=1)
            case AddTempRootsRequest():
                return AddTempRootsResponse(value=1)
            case AddIndirectRootRequest():
                return AddIndirectRootResponse(value=1)
            case FindRootsRequest():
                return FindRootsResponse()
            case CollectGarbageRequest():
                if request.ignore_liveness:
                    raise RestrictedError("you are not allowed to ignore liveness")
                return CollectGarbageResponse(paths_deleted=set(), bytes_freed=0, obsolete=0)
            case OptimiseStoreRequest():
                return OptimiseStoreResponse(value=1)
            case VerifyStoreRequest():
                if request.repair:
                    raise RestrictedError("you are not privileged to repair paths")
                return VerifyStoreResponse(value=0)
            case (
                IsValidPathRequest()
                | QueryDerivationOutputMapRequest()
                | QuerySubstitutablePathsRequest()
                | QuerySubstitutablePathInfoRequest()
                | QuerySubstitutablePathInfosRequest()
            ):
                return await self._next(request)
        raise ClosingError(f"invalid operation {request.op}")

    async def _path_info(self, request: QueryPathInfoRequest) -> WireResponse:
        """`queryPathInfoUncached` (`restricted-store.cc:179`): the impure fields censored."""
        if not self.restriction.is_allowed(request.path):
            return QueryPathInfoResponse(valid=False)
        response = await self._next(request)
        if not isinstance(response, QueryPathInfoResponse) or response.info is None:
            return response
        info = response.info.model_copy(
            update={"deriver": None, "registration_time": 0, "ultimate": False, "sigs": set()}
        )
        return QueryPathInfoResponse(valid=response.valid, info=info)

    async def _path_infos(self, request: QueryPathInfosRequest) -> QueryPathInfosResponse:
        """`Store::queryPathInfos` of Determinate Nix: `queryPathInfo` for each path, so the same censoring."""
        infos: list[ValidPathInfo] = []
        for path in request.paths:
            response = await self._path_info(QueryPathInfoRequest(path=path))
            if isinstance(response, QueryPathInfoResponse) and response.valid and response.info is not None:
                infos.append(ValidPathInfo(path=path, info=response.info))
        return QueryPathInfosResponse(infos=infos)

    async def _build(self, request: BuildPathsRequest | BuildPathsWithResultsRequest) -> WireResponse:
        """`buildPathsWithResults` (`restricted-store.cc:248`): the closure of what it built joins the build."""
        if request.build_mode == BuildMode.REPAIR:
            # `daemon.cc:554`, before `startWork`.
            raise ClosingError("repairing is not allowed because you are not in 'trusted-users'")
        if request.build_mode not in (BuildMode.NORMAL, BuildMode.CHECK):
            raise ClosingError("Invalid build mode")
        if request.build_mode != BuildMode.NORMAL:
            raise RestrictedError("unsupported build mode")
        for derived in request.derived_paths:
            if not self._knows(str(derived)):
                text = str(derived).replace("!", "^")
                raise RestrictedError(f"cannot build {_quoted(text)} in recursive Nix because path is unknown")
        results = await self._next(
            BuildPathsWithResultsRequest(derived_paths=request.derived_paths, build_mode=request.build_mode)
        )
        if not isinstance(results, BuildPathsWithResultsResponse):
            raise TypeError(f"BuildPathsWithResults answered {type(results).__name__}")
        built: set[str] = set()
        for keyed in results.results:
            if not result_succeeded(keyed.result):
                continue
            by_name = keyed.result.built_outputs_by_name or {}
            for name, unkeyed in by_name.items():
                built.add(str(unkeyed.out_path))
                self.restriction.added_drv_outputs.add(f"{_path_part(str(keyed.path))}^{name}")
            for key, realisation in (keyed.result.built_outputs or {}).items():
                if realisation.out_path is not None:
                    built.add(str(realisation.out_path))
                self.restriction.added_drv_outputs.add(str(key))
        for path in sorted(await self._engine.query_closure(sorted(built))):
            await self._add_dependency(path)
        if isinstance(request, BuildPathsWithResultsRequest):
            return results
        # `buildPaths` (`restricted-store.cc:240`) throws the first failure.
        failed = [item for item in results.results if not result_succeeded(item.result)]
        if failed:
            raise BackendError(_build_failure_message(failed))
        return BuildPathsResponse(value=1)

    async def _missing(self, request: QueryMissingRequest) -> WireResponse:
        """`queryMissing` (`restricted-store.cc:283`): an unknown path is unknown, and the rest is asked."""
        allowed = {d for d in request.derived_paths if self._knows(str(d))}
        unknown = {StorePath(_path_part(str(d))) for d in request.derived_paths - allowed}
        response = await self._next(request.model_copy(update={"derived_paths": allowed}))
        if not isinstance(response, QueryMissingResponse):
            raise TypeError(f"QueryMissing answered {type(response).__name__}")
        return response.model_copy(update={"unknown": response.unknown | unknown})

    async def _realisation(self, request: QueryRealisationRequest) -> WireResponse:
        """`queryRealisationUncached` (`restricted-store.cc:226`).

        NIX-DEFECT (#23): `restricted-store.cc:251` answers `callback(nullptr)` for an output the build may not
        see, then goes on to ask the next store, with no `return`: the second answer fails the assertion of
        `Callback::operator()`. pynixd answers "not found" alone; Lillecarl/nix#394 holds the report.
        """
        if request.keyed_drv_output is not None:
            key = f"{request.keyed_drv_output.drv_path}^{request.keyed_drv_output.output_name}"
        else:
            key = str(request.drv_output)
        if key not in self.restriction.added_drv_outputs:
            return QueryRealisationResponse()
        return await self._next(request)


def restricted_server(ctx: PynixdContext) -> RecursiveServer:
    """The `RecursiveServer` pynixd's sandbox hands each recursive-nix build."""

    @contextlib.asynccontextmanager
    async def serve(listener: socket.socket, restriction: RestrictionContext) -> AsyncIterator[None]:
        async def session(accepted: SocketStream) -> None:
            link = await as_link(accepted)
            try:
                proxy = RestrictedProxy(
                    UnixNixReader(link, identifier="recursive"),
                    UnixNixWriter(link, identifier="recursive"),
                    ctx,
                    restriction,
                )
                await proxy.run()
            except Exception:
                log.exception("recursive_session_failed")
            finally:
                anyio.notify_closing(link.sock)
                await link.stream.aclose()

        async with await SocketListener.from_socket(listener) as accepting, anyio.create_task_group() as tg:
            tg.start_soon(accepting.serve, session, tg)
            try:
                yield
            finally:
                # `stopDaemon` (`derivation-builder.cc:1217`): the builder has exited, and each client with it.
                tg.cancel_scope.cancel()

    return serve
