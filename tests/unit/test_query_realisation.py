"""pynixd's answer to `QueryRealisation`, from the store database.

Nix 2.34 reads `Realisations`, keyed by the hash of the derivation
(`local-store.cc:377` of 2.34). Nix master reads `BuildTraceV3`, keyed by the
derivation path without the store directory (`local-store.cc:407`). The answer
carries both wire shapes, as the daemon's does after `answer_in_both_shapes`.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.constants import FEATURE_REALISATION_WITH_PATH
from nix_daemon_protocol.drv_output import DrvOutput
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.keyed_drv_output import KeyedDrvOutput
from nix_daemon_protocol.query_realisation import QueryRealisationRequest
from pynixd.config import LocalSocketStoreSpec
from pynixd.exceptions import BackendError
from pynixd.local_store_db import LocalStoreDB
from pynixd.serde import StorePath
from pynixd.store.local_db import LocalDBStore
from pynixd.store_layout import StoreLayout
from pynixd.trust import TrustPolicy

if TYPE_CHECKING:
    from pathlib import Path

    from nix_daemon_protocol.query_realisation import QueryRealisationResponse

HELLO_DRV = "/nix/store/00000000000000000000000000000001-hello.drv"
HELLO = "/nix/store/00000000000000000000000000000002-hello"
DRV_HASH = "sha256:" + "ab" * 32
SIGNATURES = "zeta:c2ln a-b:c2ln a:c2ln"
IN_NIX_ORDER = ["a:c2ln", "a-b:c2ln", "zeta:c2ln"]
"""By key name, then signature: "a" before "a-b", which plain text order reverses."""


def _store_db(root: Path) -> None:
    db_path = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (7, ?, 0)", (HELLO,))
        conn.execute(
            "CREATE TABLE Realisations (id INTEGER PRIMARY KEY, drvPath TEXT NOT NULL, outputName TEXT NOT NULL, "
            "outputPath INTEGER NOT NULL, signatures TEXT)",
        )
        conn.execute(
            "INSERT INTO Realisations (drvPath, outputName, outputPath, signatures) VALUES (?, 'out', 7, ?)",
            (DRV_HASH, SIGNATURES),
        )
        conn.execute(
            "CREATE TABLE BuildTraceV3 (id INTEGER PRIMARY KEY, drvPath TEXT NOT NULL, outputName TEXT NOT NULL, "
            "outputPath TEXT NOT NULL, signatures TEXT)",
        )
        conn.execute(
            "INSERT INTO BuildTraceV3 (drvPath, outputName, outputPath, signatures) VALUES (?, 'out', ?, ?)",
            (HELLO_DRV.removeprefix("/nix/store/"), HELLO, SIGNATURES),
        )


async def _ask(
    root: Path, request: QueryRealisationRequest, *, reads_paths: bool, features: tuple[str, ...] = ("ca-derivations",)
) -> QueryRealisationResponse | None:
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=root))
    store._trust_policy = TrustPolicy(experimental_features=features)
    store._features = {FEATURE_REALISATION_WITH_PATH} if reads_paths else set()
    async with await LocalStoreDB.open(StoreLayout.chroot(root)) as store.db:
        answer: QueryRealisationResponse | None = await store.own_query_realisation(request)
    return answer


def _by_hash(output: str = "out") -> QueryRealisationRequest:
    return QueryRealisationRequest(drv_output=DrvOutput(f"{DRV_HASH}!{output}"))


def _by_path(output: str = "out") -> QueryRealisationRequest:
    return QueryRealisationRequest(
        keyed_drv_output=KeyedDrvOutput(drv_path=StorePath(path=HELLO_DRV), output_name=output)
    )


@pytest.mark.anyio
class TestQueryRealisation:
    async def test_by_hash_answers_both_shapes(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        answer = await _ask(tmp_path, _by_hash(), reads_paths=False)
        assert answer is not None
        [realisation] = answer.realisations
        assert realisation.id == DrvOutput(f"{DRV_HASH}!out")
        assert realisation.out_path == StorePath(path=HELLO)
        assert realisation.signatures == IN_NIX_ORDER
        assert answer.present == 1
        assert answer.realisation is not None
        assert str(answer.realisation.out_path) == HELLO

    async def test_by_path_answers_both_shapes(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        answer = await _ask(tmp_path, _by_path(), reads_paths=True)
        assert answer is not None
        assert answer.present == 1
        assert answer.realisation is not None
        assert str(answer.realisation.out_path) == HELLO
        assert answer.realisation.signatures == set(IN_NIX_ORDER)
        [realisation] = answer.realisations
        assert realisation.out_path == StorePath(path=HELLO)

    @pytest.mark.parametrize("reads_paths", [False, True])
    async def test_an_output_with_no_realisation(self, tmp_path: Path, reads_paths: bool) -> None:
        _store_db(tmp_path)
        request = _by_path("doc") if reads_paths else _by_hash("doc")
        answer = await _ask(tmp_path, request, reads_paths=reads_paths)
        assert answer is not None
        assert answer.realisations == []
        assert answer.present == 0
        assert answer.realisation is None

    async def test_without_ca_derivations_the_daemon_answers(self, tmp_path: Path) -> None:
        """Nix 2.34 answers nothing there, and Nix master has no statement to run."""
        _store_db(tmp_path)
        assert await _ask(tmp_path, _by_hash(), reads_paths=False, features=()) is None

    async def test_a_hash_cannot_ask_a_store_that_reads_paths(self, tmp_path: Path) -> None:
        """The daemon route refuses the same: no index maps the hash to the path."""
        _store_db(tmp_path)
        with pytest.raises(BackendError, match="cannot be recovered"):
            await _ask(tmp_path, _by_hash(), reads_paths=True)
