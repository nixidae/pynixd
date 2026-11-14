"""pynixd signs the outputs it pulls from a builder with its own `secret-key-files`.

A client of sg1's pynixd builds there and copies the outputs back. Nix copies
a hook build's outputs unsigned (`build-remote.cc:432`), so every output an
sg1 node or a GX10 built reached the client with no signature, and the copy
failed: "lacks a signature by a trusted key". pynixd's own sandbox builds
were signed, so only the builds it placed elsewhere failed.
"""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING

import nacl.signing
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from nix_daemon_protocol.signature import Signature
from pynixd.build_queue import BuildQueue
from pynixd.config import LocalSocketStoreSpec
from pynixd.serde import (
    BuildDerivationResponse,
    BuildResult,
    BuildResultStatus,
    ContentAddress,
    NARHash,
    StorePath,
    Time,
    UnkeyedValidPathInfo,
    ValidPathInfo,
)
from pynixd.store.local_db import LocalDBStore
from pynixd_store.add import ImportSettings
from pynixd_store.signing import PublicKey, check_signatures
from tests._conftest.execution import FakeSandbox, Streamed, fake_executor, queued
from tests._conftest.placement import FakeStore

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

OUTPUT = "/nix/store/00000000000000000000000000000001-out"
BUILDER = StoreId("sg1-n1")


async def _collect(store: FakeStore, stores: dict[StoreId, FakeStore]) -> FakeSandbox:
    queue = BuildQueue()
    build = await queued(queue, outputs={"out": OUTPUT})
    sandbox = FakeSandbox()
    executor = fake_executor(queue, stores, here=sandbox, stream=Streamed())
    await executor.collect_outputs(
        build, store, BuildDerivationResponse(result=BuildResult(status=BuildResultStatus.BUILT))
    )
    return sandbox


class TestTheExecutor:
    @pytest.mark.anyio
    async def test_signs_the_outputs_of_a_configured_builder(self) -> None:
        store = FakeStore(BUILDER)

        sandbox = await _collect(store, {BUILDER: store})

        assert sandbox.signed == [{StorePath(OUTPUT)}]

    @pytest.mark.anyio
    async def test_signs_nothing_a_client_s_own_builder_built(self) -> None:
        """A client could name any builder, and get any content signed with pynixd's key."""
        store = FakeStore(BUILDER, session_only=True)

        sandbox = await _collect(store, {BUILDER: store})

        assert sandbox.signed == []

    @pytest.mark.anyio
    async def test_signs_nothing_the_local_builder_built(self) -> None:
        local = FakeStore(LOCAL_STORE_ID)

        sandbox = await _collect(local, {LOCAL_STORE_ID: local})

        assert sandbox.signed == []


def _key(tmp_path: Path, name: str) -> tuple[Path, PublicKey]:
    signing = nacl.signing.SigningKey.generate()
    raw = bytes(signing) + bytes(signing.verify_key)
    file = tmp_path / name
    file.write_text(f"{name}:{base64.b64encode(raw).decode()}\n")
    public = PublicKey.parse(f"{name}:{base64.b64encode(bytes(signing.verify_key)).decode()}")
    return file, public


def _info(path: str) -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(path=path),
        info=UnkeyedValidPathInfo(
            deriver=StorePath(path=""),
            nar_hash=NARHash("0" * 64),
            references=set(),
            registration_time=Time(0),
            nar_size=1,
            ultimate=False,
            sigs=set(),
            ca=ContentAddress(""),
        ),
    )


class _Store:
    """A `LocalDBStore` whose engine answers from a dict and whose router records signatures."""

    def __init__(self, tmp_path: Path, key_files: Iterable[Path], valid: Iterable[str]) -> None:
        self.store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=tmp_path / "store"))
        self.infos = {path: _info(path) for path in valid}
        self.added: dict[str, set[Signature]] = {}
        settings = ImportSettings(secret_key_files=tuple(str(file) for file in key_files))

        async def settings_() -> ImportSettings:
            return settings

        async def query_path_info(path: str) -> ValidPathInfo | None:
            return self.infos.get(path)

        async def add_signatures(path: str, sigs: Iterable[Signature]) -> None:
            self.added.setdefault(path, set()).update(sigs)

        self.store.engine.settings = settings_  # type: ignore[method-assign]  # a stand-in for the store's nix.conf
        self.store.engine.query_path_info = query_path_info  # type: ignore[method-assign,assignment]  # no database
        self.store.router.add_signatures = add_signatures  # type: ignore[method-assign,assignment]  # recorded

    def vouched(self, path: str, keys: dict[str, PublicKey]) -> int:
        info = self.infos[path].model_copy(deep=True)
        info.info.sigs = self.added.get(path, set())
        return check_signatures(info, keys, "/nix/store")


class TestSignPulled:
    @pytest.mark.anyio
    async def test_signs_each_output_with_each_secret_key(self, tmp_path: Path) -> None:
        (file_a, public_a), (file_b, public_b) = _key(tmp_path, "pynixd-a"), _key(tmp_path, "pynixd-b")
        store = _Store(tmp_path, [file_a, file_b], [OUTPUT])

        await store.store.sign_pulled([StorePath(OUTPUT)])

        assert store.vouched(OUTPUT, {"pynixd-a": public_a}) == 1
        assert store.vouched(OUTPUT, {"pynixd-a": public_a, "pynixd-b": public_b}) == 2

    @pytest.mark.anyio
    async def test_signs_nothing_without_a_secret_key(self, tmp_path: Path) -> None:
        store = _Store(tmp_path, [], [OUTPUT])

        await store.store.sign_pulled([StorePath(OUTPUT)])

        assert store.added == {}

    @pytest.mark.anyio
    async def test_refuses_an_output_the_pull_left_invalid(self, tmp_path: Path) -> None:
        file, _public = _key(tmp_path, "pynixd-a")
        store = _Store(tmp_path, [file], [])

        with pytest.raises(Exception, match="is not valid after the pull"):
            await store.store.sign_pulled([StorePath(OUTPUT)])

    @pytest.mark.anyio
    async def test_fails_on_a_key_file_it_cannot_read(self, tmp_path: Path) -> None:
        """As a local build fails on one (`Store::signPathInfo`)."""
        store = _Store(tmp_path, [tmp_path / "missing"], [OUTPUT])

        with pytest.raises(FileNotFoundError):
            await store.store.sign_pulled([StorePath(OUTPUT)])
