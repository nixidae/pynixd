"""
Base Store ABC for pynixd.

Defines the minimal contract that every store backend must fulfill:
signing keys, operation execution, and lifecycle management.  Daemon-specific logic (connection pooling, probing,
feature matrices, circuit breaking) lives in DaemonStore.
"""

from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, Self, TypeVar, cast

import anyio
import structlog
from cachetools import TTLCache

from ..background import Background
from ..trust import TrustPolicy

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping, Set as AbstractSet
    from types import TracebackType

    from nix_daemon_protocol.ids import StoreId
    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from nix_daemon_protocol.wire_ops import WireRequest

    from ..config import StoreSpecBase
    from ..connection import ClientConn, Connection
    from ..drv_parser import Derivation
    from ..signing import SecretKey
    from ..store_path import StorePath


log = structlog.get_logger(__name__)

_F = TypeVar("_F", bound=Callable[..., Any])


class _TaggedExecutor(Protocol):
    """A method that `Store.executor` has tagged with its operation."""

    _pynixd_op: int


class Store(ABC):
    """Minimal store contract.

    Subclasses implement create_conn() and the operation executors.
    Daemon-backed stores extend DaemonStore, which adds connection
    pooling, probing, and circuit-breaking.
    """

    _executors: ClassVar[dict[int, str]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for name, method in cls.__dict__.items():
            if callable(method) and hasattr(method, "_pynixd_op"):
                cls._executors[method._pynixd_op] = name  # type: ignore[union-attr]

    def __init__(self, spec: StoreSpecBase) -> None:
        """Initialize store identity, feature matrix, signing keys, and path info cache."""
        if spec.store_id is None:
            raise RuntimeError("store_id must be set on the spec before Store construction")
        self.store_id: StoreId = spec.store_id
        self.priority = spec.priority
        self.no_schedule = spec.no_schedule
        self.gc_defer = spec.gc_defer
        """Here and not in `DaemonStore`: an HTTP binary cache is the store
        this flag exists for, and it speaks no worker protocol."""
        self._feature_matrix: dict[str, set[str]] | None = spec._effective_feature_matrix()
        self._signing_keys: dict[str, SecretKey] = {}
        self.path_info_cache = cast(
            "TTLCache[str, ValidPathInfo, float]",
            TTLCache(
                maxsize=10000,
                ttl=300,
            ),
        )
        self._started: bool = False
        self._background = Background(f"store {self.store_id}")
        self._running: contextlib.AbstractAsyncContextManager[None] | None = None

    @property
    def feature_matrix(self) -> dict[str, set[str]]:
        if self._feature_matrix is not None:
            return self._feature_matrix
        return {}

    @property
    def features(self) -> AbstractSet[str]:
        """The worker-protocol features this store advertises.

        Empty here, and `DaemonStore` overrides it with what the handshake
        returned. A store that speaks no worker protocol -- an HTTP binary
        cache -- advertises nothing, and that is the honest answer rather
        than a missing attribute.

        `DaemonProxy.honourable_features` unions these: one store with a
        feature is enough for pynixd to name it. That store is kept out by
        `no_schedule`, not by the absence of this property.
        """
        return frozenset()

    @property
    def is_healthy(self) -> bool:
        return True

    @property
    def in_flight(self) -> int:
        return 0

    async def trust_policy(self) -> TrustPolicy:
        """Who may use this store and who is trusted. Nix's defaults here;
        `LocalStore` reads what its Nix reads."""
        return TrustPolicy()

    # ── Lifecycle ───────────────────────────────────────────────────

    @abstractmethod
    async def start(self, sync_paths: bool = True) -> None:
        """Explicitly start the store and ensure it is ready for operations."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Close all resources."""
        ...

    async def __aenter__(self) -> Self:
        if self._running is not None:
            raise RuntimeError(f"store {self.store_id} is already open")
        running = self._run()
        await running.__aenter__()
        self._running = running
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        running, self._running = self._running, None
        if running is None:
            raise RuntimeError(f"{type(self).__name__}.__aexit__ without __aenter__")
        return await running.__aexit__(exc_type, exc_val, exc_tb)

    @contextlib.asynccontextmanager
    async def _run(self) -> AsyncIterator[None]:
        """The store's lifetime: its background loops live in this block.

        A loop that raises leaves `async with store` as an `ExceptionGroup`,
        and the store closes.
        """
        async with self._background.running():
            try:
                await self.start()
                yield
            finally:
                # Shielded: a failed loop has cancelled this scope. The loops
                # stop before the connections they use close.
                with anyio.CancelScope(shield=True):
                    await self._background.aclose()
                    await self.close()

    # ── Connection / execution ──────────────────────────────────────

    @abstractmethod
    async def create_conn(self) -> Connection:
        """Create transport, construct Connection, and connect it."""
        ...

    @abstractmethod
    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        """Send an operation to this store."""
        ...

    @abstractmethod
    async def execute(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        """Execute an operation on this store."""
        ...

    @abstractmethod
    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        """Fetch and parse a .drv file from this store."""
        ...

    async def add_text_to_store(self, name: str, text: str, references: AbstractSet[str]) -> str:
        """Put a text file in this store, and answer the path it took.

        `Store::addTextToStore` of Nix, which `writeDerivation` uses to put a
        resolved derivation in the store. A store that cannot do this raises,
        and the caller then keeps the derivation it has.
        """
        raise NotImplementedError(f"{type(self).__name__} cannot add a text file to its store")

    async def retire_idle_connections(self) -> int:
        """Close each connection to this store that nobody uses now.

        A garbage collection calls this first. A worker of the daemon holds a
        temporary root for each path that it took, and an idle connection
        keeps that worker alive, so the collector frees nothing that passed
        through pynixd. A store that pools no connection holds no such root
        and answers zero. Issue #20.
        """
        return 0

    # ── Signing ─────────────────────────────────────────────────────

    @property
    def signing_keys(self) -> Mapping[str, SecretKey]:
        """Read-only mapping of signing keys configured on this store."""
        return self._signing_keys

    @property
    def signing_key_names(self) -> list[str]:
        """List of signing key names configured on this store."""
        return list(self._signing_keys.keys())

    def get_signing_key(self, name: str) -> SecretKey:
        """Get a signing key by name."""
        key = self._signing_keys.get(name)
        if key is None:
            raise KeyError(f"Signing key '{name}' not found")
        return key

    # ── Path info cache ──────────────────────────────────────────────

    def add_path_info(self, info: ValidPathInfo) -> None:
        """Cache a single ValidPathInfo entry."""
        self.path_info_cache[str(info.path)] = info

    def add_path_infos(self, infos: Iterable[ValidPathInfo]) -> None:
        """Cache multiple ValidPathInfo entries."""
        for info in infos:
            self.path_info_cache[str(info.path)] = info

    def get_path_info(self, path: object) -> ValidPathInfo | None:
        """Retrieve a cached ValidPathInfo entry, or None."""
        return self.path_info_cache.get(str(path))

    def forget_path_info(self, path: object) -> None:
        """Drop the cached entry for *path*, because something changed it.

        **A cache that nothing invalidates answers with the past.** This one
        has a 300 s TTL, and an operation that changes what `QueryPathInfo`
        returns has to call this or the store keeps answering with the value
        from before its own write. `nix-daemon` holds no such cache and cannot
        have the fault, so every case of it is a divergence.

        `AddSignatures` and `SignPathInfo` are the operations that do it
        today: `nix store sign` used to succeed and `nix path-info` then
        reported no signature for the next five minutes.
        """
        self.path_info_cache.pop(str(path), None)

    # ── Executor infrastructure ─────────────────────────────────────

    @classmethod
    def _register_executor(cls, op: int, name: str) -> None:
        """Register a method as the executor for an operation."""
        cls._executors[op] = name

    @staticmethod
    def executor(op: int) -> Callable[[_F], _F]:
        """Decorator: register a method as the executor for an operation."""

        def decorator(method: _F) -> _F:
            cast("_TaggedExecutor", method)._pynixd_op = op
            return method

        return decorator

    # ── Common read operations ──────────────────────────────────────

    @executor(op=1)
    @abstractmethod
    async def is_valid_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """IsValidPath (op 1)."""
        raise NotImplementedError

    @executor(op=26)
    @abstractmethod
    async def query_path_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathInfo (op 26)."""
        raise NotImplementedError

    @executor(op=29)
    @abstractmethod
    async def query_path_from_hash_part(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathFromHashPart (op 29)."""
        raise NotImplementedError

    @executor(op=31)
    @abstractmethod
    async def query_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryValidPaths (op 31)."""
        raise NotImplementedError

    @executor(op=38)
    @abstractmethod
    async def nar_from_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """NarFromPath (op 38)."""
        raise NotImplementedError


def get_current_system() -> str:
    """Return the current system identifier (e.g., x86_64-linux)."""
    import platform

    machine = platform.machine()
    system = platform.system().lower()
    if system == "darwin":
        system = "darwin"
    return f"{machine}-{system}"


# Populate Store._executors from methods decorated with @executor (__init_subclass__
# only fires for subclasses, not the base class itself).
for _name, _method in Store.__dict__.items():
    if callable(_method) and (op := getattr(_method, "_pynixd_op", None)) is not None:
        Store._executors[op] = _name
