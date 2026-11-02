"""pynixd's answer to `QueryDerivationOutputMap`, from the store database.

Nix starts from `DerivationOutputs` (`local-store.cc:922`). Without
`ca-derivations` that is the answer (`store-api.cc:471`). With it, each output
of the derivation takes its realisation, and an output with neither maps to
the empty path (`outputs-query.cc:119`). Nix master keys the realisation by
the derivation path, Nix 2.34 by the hash of the output (`store-api.cc:406`
of 2.34).
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.constants import FEATURE_REALISATION_WITH_PATH
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_derivation_output_map import QueryDerivationOutputMapRequest
from pynixd import drv_hash
from pynixd.config import LocalSocketStoreSpec
from pynixd.local_store_db import LocalStoreDB
from pynixd.serde import StorePath
from pynixd.store.local_db import LocalDBStore
from pynixd.store_layout import StoreLayout
from pynixd.trust import TrustPolicy

if TYPE_CHECKING:
    from pathlib import Path

    from nix_daemon_protocol.query_derivation_output_map import QueryDerivationOutputMapResponse

HELLO_DRV = "/nix/store/00000000000000000000000000000001-hello.drv"
EMPTY_DRV = "/nix/store/00000000000000000000000000000002-empty.drv"
HELLO = "/nix/store/00000000000000000000000000000003-hello"
DOC = "/nix/store/00000000000000000000000000000004-hello-doc"
GONE_DRV = "/nix/store/00000000000000000000000000000005-gone.drv"
REALISED = "/nix/store/00000000000000000000000000000006-hello"
OUT_HASH = "ab" * 32


@dataclass
class _Output:
    output_name: str


@dataclass
class _Derivation:
    outputs: list[_Output] = field(default_factory=lambda: [_Output("out"), _Output("doc"), _Output("dev")])


def _store_db(root: Path) -> None:
    """`HELLO_DRV` has two rows and a realisation of `out`; `EMPTY_DRV` has no row; `GONE_DRV` is not valid."""
    db_path = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        conn.execute("CREATE TABLE DerivationOutputs (drv INTEGER NOT NULL, id TEXT NOT NULL, path TEXT NOT NULL)")
        for path_id, path in ((1, HELLO_DRV), (2, EMPTY_DRV), (6, REALISED)):
            conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (?, ?, 0)", (path_id, path))
        conn.executemany(
            "INSERT INTO DerivationOutputs (drv, id, path) VALUES (?, ?, ?)",
            [(1, "out", HELLO), (1, "doc", DOC), (99, "out", HELLO)],
        )
        conn.execute(
            "CREATE TABLE Realisations (id INTEGER PRIMARY KEY, drvPath TEXT NOT NULL, outputName TEXT NOT NULL, "
            "outputPath INTEGER NOT NULL, signatures TEXT)",
        )
        conn.execute(
            "INSERT INTO Realisations (drvPath, outputName, outputPath) VALUES (?, 'out', 6)", (f"sha256:{OUT_HASH}",)
        )
        conn.execute(
            "CREATE TABLE BuildTraceV3 (id INTEGER PRIMARY KEY, drvPath TEXT NOT NULL, outputName TEXT NOT NULL, "
            "outputPath TEXT NOT NULL, signatures TEXT)",
        )
        conn.execute(
            "INSERT INTO BuildTraceV3 (drvPath, outputName, outputPath) VALUES (?, 'out', ?)",
            (HELLO_DRV.removeprefix("/nix/store/"), REALISED),
        )


async def _ask(
    root: Path, drv: str, *features: str, reads_paths: bool = False, parsed: _Derivation | None = None
) -> dict[str, str] | None:
    """The map pynixd answers, as text; `None` where the daemon answers."""
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=root))
    store.daemon_client._trust_policy = TrustPolicy(experimental_features=features)
    store.daemon_client._features = {FEATURE_REALISATION_WITH_PATH} if reads_paths else set()

    async def read_derivation(_path: StorePath | str) -> Any:
        return parsed

    store.read_derivation = read_derivation  # type: ignore[method-assign, assignment]  # a stand-in that holds only the output names
    async with await LocalStoreDB.open(StoreLayout.chroot(root)) as store.db:
        answer: QueryDerivationOutputMapResponse | None = await store.own_query_derivation_output_map(
            QueryDerivationOutputMapRequest(path=StorePath(path=drv))
        )
    return None if answer is None else {name: str(path) for name, path in answer.items.items()}


@pytest.mark.anyio
class TestWithoutCaDerivations:
    async def test_every_output_the_table_holds(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _ask(tmp_path, HELLO_DRV) == {"out": HELLO, "doc": DOC}

    async def test_a_valid_derivation_with_no_row(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _ask(tmp_path, EMPTY_DRV) == {}

    async def test_a_derivation_that_is_not_valid_goes_to_the_daemon(self, tmp_path: Path) -> None:
        """`queryValidPathId` throws "path is not valid"; the daemon says so with Nix's words."""
        _store_db(tmp_path)
        assert await _ask(tmp_path, GONE_DRV) is None


@pytest.mark.anyio
class TestWithCaDerivations:
    """`out` takes its realisation, `doc` keeps its row, and `dev` has neither."""

    async def test_realisations_by_path(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        answer = await _ask(tmp_path, HELLO_DRV, "ca-derivations", reads_paths=True, parsed=_Derivation())
        assert answer == {"out": REALISED, "doc": DOC, "dev": ""}

    async def test_realisations_by_hash(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        async def output_hashes(_parsed: Any, _read: Any) -> dict[str, str]:
            return {"out": OUT_HASH, "doc": "cd" * 32, "dev": "ef" * 32}

        monkeypatch.setattr(drv_hash, "output_hashes", output_hashes)
        _store_db(tmp_path)
        answer = await _ask(tmp_path, HELLO_DRV, "ca-derivations", parsed=_Derivation())
        assert answer == {"out": REALISED, "doc": DOC, "dev": ""}

    async def test_no_hashes_goes_to_the_daemon(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A missing input derivation: Nix's `staticOutputHashes` throws, and the daemon says so."""

        async def output_hashes(_parsed: Any, _read: Any) -> None:
            return None

        monkeypatch.setattr(drv_hash, "output_hashes", output_hashes)
        _store_db(tmp_path)
        assert await _ask(tmp_path, HELLO_DRV, "ca-derivations", parsed=_Derivation()) is None

    async def test_an_unreadable_derivation_goes_to_the_daemon(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        assert await _ask(tmp_path, HELLO_DRV, "ca-derivations", reads_paths=True, parsed=None) is None
