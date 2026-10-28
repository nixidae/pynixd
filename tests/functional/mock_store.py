"""Mock Store implementations for unit and functional tests."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import anyio
import structlog

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.query_all_valid_paths import (
    QueryAllValidPathsRequest,
    QueryAllValidPathsResponse,
)
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_message import WireModel
from nix_daemon_protocol.wire_time import Time
from pynixd.config import StoreSpecBase
from pynixd.psi import CpuUtil
from pynixd.serde import (
    BuildDerivationRequest,
    QueryClosureWithInfoRequest,
    QueryClosureWithInfoResponse,
    QueryValidPathsRequest,
    QueryValidPathsResponse,
    StorePath,
)
from pynixd.store.daemon import DaemonStore

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd.connection import ClientConn, Connection
    from pynixd.drv_parser import Derivation
    from pynixd.store_path import StorePath
    from pynixd.wire import NixReader, NixWriter


log = structlog.get_logger(__name__)


class MockConnection:
    """Mock connection that doesn't actually connect to anything.

    Delegates all `call()` invocations directly back to the `MockStore`
    that created it. This allows tests to simulate the protocol handshake
    and operation execution without any network/IO overhead.
    """

    def __init__(self, store: MockStore) -> None:
        # We don't call super().__init__ because we don't have real R/W pairs.
        # Instead, we initialize the fields expected by the base class.
        self.store = store
        self.id = f"mock-{store.store_id}"
        self.version = 0x125  # Simulates a modern Nix protocol version (e.g. 1.37)
        self.nix_version = store.nix_version
        self.features: set[str] = set()
        self.connected = True
        self.dirty = False
        self.op_log: list[str] = []
        # `ConnectionPool._too_old` reads this on every `acquire`, so a stand-in
        # for `Connection` has to carry it. `Connection.__init__` sets it from
        # `time.monotonic()`, and this does the same rather than picking a
        # constant, so a pool with a `max_lifetime` retires a mock the way it
        # retires a real one.
        self.opened_at: float = time.monotonic()
        # `ConnectionPool.get_or_create_conn` reads it when it reuses an idle one.
        self.applied_options = None
        # `ConnectionPool.waiting_on_upstream` reads it, and a mock call
        # finishes before anything could scrape it.
        self.current_op: tuple[str, float] | None = None

        # Dummy reader/writer to avoid AttributeErrors on .identifier
        class DummyRW:
            def __init__(self, rw_id: str):
                self.identifier = rw_id
                # The transport counters of `NixReader`/`NixWriter`, which
                # the pool sums. A mock moves no bytes, so both stay zero.
                self.bytes_read = 0
                self.bytes_written = 0
                self.last_read_at = time.monotonic()

            def peer_closed(self) -> bool:
                return False

            def framed(self) -> DummyRW:
                return self

            def write(self, *args: object, **kwargs: object) -> None:
                pass

            def write_uint64(self, *args: object, **kwargs: object) -> None:
                pass

            def write_string(self, *args: object, **kwargs: object) -> None:
                pass

            def write_string_set(self, *args: object, **kwargs: object) -> None:
                pass

            async def is_dirty(self) -> bool:
                return False

            async def drain(self) -> None:
                pass

            async def finalize(self) -> None:
                pass

        self.r = cast("NixReader", DummyRW(f"{self.id}-r"))
        self.w = cast("NixWriter", DummyRW(f"{self.id}-w"))

    async def __aenter__(self) -> MockConnection:
        """Simulate the entry into a connection context (e.g. acquiring from pool)."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Simulate connection release."""

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
    ) -> Any:
        """Forward all calls to the parent MockStore's execute_mock method."""
        return await self.store.execute_mock(request)

    def close(self) -> None:
        self.connected = False


class MockStore(DaemonStore):
    """Store with pre-recorded or dynamically generated responses.

    Useful for testing complex orchestration logic (like build decomposition
    or the build scheduler) without spawning real Nix processes.
    Also tracks statistics like current CPU utilization to test how the
    scheduler reacts to fleet saturation.
    """

    def __init__(
        self,
        store_id: str | StoreId,
        feature_matrix: dict[str, set[str]] | None = None,
        cpu_utilization: float = 0.0,
    ) -> None:
        spec = StoreSpecBase(
            store_id=StoreId(store_id),
            feature_matrix=feature_matrix,
            probe=False,
        )
        super().__init__(spec)
        self._mock_known_paths: set[StorePath] = set()
        self.store_path = Path(f"/mock/{store_id}")
        self.nix_version = "pynixd-mock-2.18.1"
        # responses: Maps Request type -> fixed Response object
        self.responses: dict[type[WireRequest], Any] = {}
        # call_handlers: Maps Request type -> async handler function
        self.call_handlers: dict[type[WireRequest], Any] = {}

        self.build_blockers: dict[str, anyio.Event] = {}
        self.cpu_utilization_val = cpu_utilization

    @property
    def cpu_util(self) -> CpuUtil | None:
        """Mocked CPU utilization."""

        return CpuUtil(utilization=self.cpu_utilization_val, cores=1.0, throttled_pct=0.0)

    @property
    def pressure(self) -> float | None:
        return self.cpu_utilization_val

    def set_cpu_utilization(self, val: float) -> None:
        self.cpu_utilization_val = val

    def block_build(self, drv_path: str | StorePath, blocker: anyio.Event | None = None) -> anyio.Event:
        """Create or use an event that will block builds of this drv_path."""
        event = blocker or anyio.Event()
        self.build_blockers[str(drv_path)] = event
        return event

    def unblock_build(self, drv_path: str | StorePath) -> None:
        """Signal the event to allow blocked builds of this drv_path to proceed."""
        drv_str = str(drv_path)
        if drv_str in self.build_blockers:
            self.build_blockers[drv_str].set()

    async def create_conn(self) -> Connection:
        """Return a MockConnection that delegates back to us."""
        return cast("Connection", MockConnection(self))

    async def execute_mock(self, request: WireRequest) -> Any:
        """Internal dispatcher for the MockConnection."""
        req_type = type(request)

        if isinstance(request, BuildDerivationRequest):
            drv_path = str(request.drv_path)
            if drv_path in self.build_blockers:
                log.debug("mock_build_blocking", store_id=self.store_id, drv_path=drv_path)
                await self.build_blockers[drv_path].wait()
                log.debug("mock_build_resuming", store_id=self.store_id, drv_path=drv_path)

        if req_type in self.call_handlers:
            return await self.call_handlers[req_type](request)

        if req_type in self.responses:
            return self.responses[req_type]

        # Dynamic defaults for common queries
        if isinstance(request, QueryValidPathsRequest):
            return QueryValidPathsResponse(paths=request.paths)

        if req_type is QueryAllValidPathsRequest:
            return QueryAllValidPathsResponse(paths=set(self._mock_known_paths))
        if isinstance(request, QueryClosureWithInfoRequest):
            # Just return some dummy info for everything

            infos = [
                ValidPathInfo(
                    path=StorePath(path=str(p)),
                    info=UnkeyedValidPathInfo(
                        deriver=None,
                        nar_hash=NARHash(hash="0000000000000000000000000000000000000000000000000000000000000000"),
                        nar_size=1024,
                        references=set(),
                        registration_time=Time(ts=0),
                        ultimate=True,
                        sigs=set(),
                        ca=ContentAddress(value=""),
                    ),
                )
                for p in request.paths
            ]
            return QueryClosureWithInfoResponse(infos=infos)

        log.warning(
            "mock_store_no_response",
            store_id=self.store_id,
            request=req_type.__name__,
        )
        raise NotImplementedError(
            f"MockStore {self.store_id} has no response for {req_type.__name__}. "
            f"Update your test setup to provide a mock response.",
        )

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        """Read a .drv file from the mock filesystem."""
        from pynixd.drv_parser import read_drv_file

        return await read_drv_file(drv_store_path, self.layout)

    async def execute(  # type: ignore[override]
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        """Always use the dynamic mock logic, bypassing request.execute()."""
        if isinstance(request, WireModel):
            return await self.call(request, client=client, suppress_last=suppress_last)
        raise TypeError(f"MockStore only supports WireRequest, got {type(request).__name__}")
