"""An operation that changes a path's info must not leave the old info cached.

`Store.path_info_cache` holds a `ValidPathInfo` for 300 s and
`LocalDBStore.query_path_info` answers from it before it reads SQLite.
Nothing invalidated it, so an operation that changed what `QueryPathInfo`
returns left the store answering with the value from before its own write.

Measured on `tests/functional/test_admin_ops.py::test_add_signatures_via_store`:
`nix store sign` returned 0, and `nix path-info --json` on the next line
reported no signature. Three client sessions, in order:

    QueryValidPaths, QueryMissing, AddMultipleToStore, BuildPathsWithResults
    QueryPathInfo, AddSignatures, QueryMissing        <- `nix store sign`
    QueryPathInfo, QueryMissing                       <- `nix path-info`

`AddMultipleToStore` cached the unsigned info in the first session and the
third read it back. `nix-daemon` holds no such cache and cannot have the
fault, so every case of it is a divergence from Nix.

**These tests are here and not only in `tests/functional/`, because no gate
runs that suite.** `checks.pynixd` builds the unit suite, so an assertion here
is one CI enforces. Issue #29 holds the other half.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.collect_garbage import CollectGarbageRequest, CollectGarbageResponse
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.protocol import GCAction
from nix_daemon_protocol.signature import Signature
from pynixd.config import LocalSocketStoreSpec
from pynixd.serde import (
    AddSignaturesRequest,
    ContentAddress,
    NARHash,
    SignPathInfoRequest,
    StorePath,
    Time,
    UnkeyedValidPathInfo,
    ValidPathInfo,
)
from pynixd.store.daemon import DaemonStore
from pynixd.store.local_db import LocalDBStore

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.connection import ClientConn

PATH = "/nix/store/00000000000000000000000000000001-thing"


def _info(*signatures: str, path: str = PATH) -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(path=path),
        info=UnkeyedValidPathInfo(
            deriver=StorePath(path=""),
            nar_hash=NARHash("0" * 64),
            references=set(),
            registration_time=Time(0),
            nar_size=1,
            ultimate=False,
            sigs={Signature(name=n, signature="x") for n in signatures},
            ca=ContentAddress(""),
        ),
    )


class RecordingStore(DaemonStore):
    """A `DaemonStore` whose wire is a list, so no daemon has to exist."""

    def __init__(self) -> None:
        # `Store.__init__` wants a spec, and every field this test reads comes
        # from the base class rather than the spec, so a minimal one does.
        from nix_daemon_protocol.ids import StoreId
        from pynixd.config import LocalSocketStoreSpec

        super().__init__(LocalSocketStoreSpec(store_id=StoreId("test")))
        self.sent: list[str] = []
        # Paths the fake daemon reports as deleted, for `collect_garbage`.
        self.deleted: set[StorePath] = set()
        # `features` is a read-only property over `_features`, which the
        # handshake fills in. No handshake runs here, so this writes it.
        self._features: set[str] = set()

    async def create_conn(self) -> Any:
        raise AssertionError("no test here opens a connection")

    async def call(
        self,
        request: Any,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        del client, suppress_last, raise_on_error, skip_probe
        self.sent.append(type(request).__name__)
        if isinstance(request, CollectGarbageRequest):
            return CollectGarbageResponse(paths_deleted=set(self.deleted), bytes_freed=0, obsolete=0)
        return None


@pytest.fixture
def store() -> RecordingStore:
    return RecordingStore()


def test_the_cache_answers_before_anything_changes_it(store: RecordingStore) -> None:
    """The premise: a cached entry is what `QueryPathInfo` would return."""
    store.add_path_info(_info("old"))

    cached = store.get_path_info(PATH)

    assert cached is not None
    assert {sig.name for sig in cached.info.sigs} == {"old"}


@pytest.mark.anyio
async def test_add_signatures_forgets_the_cached_info(store: RecordingStore) -> None:
    """`nix store sign` reached the daemon and pynixd kept answering with the past."""
    store.add_path_info(_info("old"))

    await store.add_signatures(AddSignaturesRequest(path=StorePath(path=PATH), sigs=set()))

    assert store.sent == ["AddSignaturesRequest"], "it must still reach the daemon"
    assert store.get_path_info(PATH) is None


@pytest.mark.anyio
async def test_sign_path_info_forgets_it_when_pynixd_signs(store: RecordingStore) -> None:
    """The decompose path calls `self.call` directly and skips `add_signatures`."""
    store.add_path_info(_info("old"))

    await store.sign_path_info(SignPathInfoRequest(info=_info()))

    assert store.sent == ["AddSignaturesRequest"]
    assert store.get_path_info(PATH) is None


@pytest.mark.anyio
async def test_sign_path_info_forgets_it_when_the_daemon_signs(store: RecordingStore) -> None:
    """And the branch that relays `SignPathInfo` upstream changes it just as much."""
    store._features = {"SignPathInfo"}  # noqa: SLF001 -- see RecordingStore.__init__
    store.add_path_info(_info("old"))

    await store.sign_path_info(SignPathInfoRequest(info=_info()))

    assert store.sent == ["SignPathInfoRequest"]
    assert store.get_path_info(PATH) is None


def test_forgetting_a_path_that_was_never_cached_is_not_an_error(store: RecordingStore) -> None:
    """A miss is the common case: most paths are never in the cache."""
    store.forget_path_info(PATH)

    assert store.get_path_info(PATH) is None


@pytest.mark.anyio
async def test_collect_garbage_forgets_only_what_it_deleted(store: RecordingStore) -> None:
    """A collected path stayed cached for 300 s and answered `valid=True`.

    Deletion is `CollectGarbage` in every shape: `nix store delete` is
    `DELETE_SPECIFIC` and `nix-collect-garbage` is `DELETE_DEAD`. The live
    paths keep their entries; only the deleted ones are forgotten.
    """
    store.add_path_info(_info("old"))
    other = "/nix/store/00000000000000000000000000000002-thing"
    store.add_path_info(_info("old", path=other))
    store.deleted = {StorePath(path=PATH)}

    await store.collect_garbage(
        CollectGarbageRequest(
            action=GCAction.DELETE_DEAD,
            paths_to_delete=set(),
            ignore_liveness=0,
            max_freed=0,
            obsolete1=0,
            obsolete2=0,
            obsolete3=0,
        )
    )

    assert store.sent == ["CollectGarbageRequest"], "it must still reach the daemon"
    assert store.get_path_info(PATH) is None
    assert store.get_path_info(other) is not None


def test_an_engine_write_forgets_what_the_daemon_client_cached(tmp_path: Path) -> None:
    """`LocalDBStore` and its daemon client hold one cache.

    The engine forgets a path it wrote through `LocalDBStore`. The daemon
    client answers `QueryPathInfo` on the daemon route from its own cache. Two
    caches would let that route answer with the info from before the write.
    """
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=tmp_path))
    store.daemon_client.add_path_info(_info("old"))

    store.engine.invalidate_path_info_cache_for(PATH)

    assert store.daemon_client.get_path_info(PATH) is None
