"""pynixd imports a NAR on AddToStoreNar's `pynixd` route as `LocalStore::addToStore` does (`local-store.cc:1046`).

`nix-store --import` sends op 39 (`export-import.cc:89`; `nix copy` sends op
44), so the export of store A goes into store B on the `pynixd` route and
store C on the daemon route. Rows and files must agree. The refusals use a raw
client, which can send what Nix's client never does.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.add_to_store_nar import AddToStoreNarRequest, AddToStoreNarResponse
from nix_daemon_protocol.nar_hash import NARHash
from pynixd.exceptions import BackendError
from pynixd.serde.context import ReadContext, WriteContext
from pynixd_store.engine import StoreEngine
from pynixd_store.nar_fs import dump_path
from tests._conftest.import_lab import (
    IA,
    NIX_STORE,
    OUTPUTS,
    REFS,
    VALID_PATHS,
    client,
    files,
    info_of,
    lab,
    nar_of,
    real,
    rows,
    run,
    server,
    valid,
)
from tests.conftest import NIX_BIN

if TYPE_CHECKING:
    from pathlib import Path

    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from pynixd.connection import Connection


async def _import(a: Path, root: Path, socket: Path, paths: list[str]) -> None:
    exported = (await anyio.run_process([NIX_STORE, "--store", str(a), "--export", *paths], check=True)).stdout
    await anyio.run_process(
        [NIX_STORE, "--store", f"unix://{socket}?root={root}", "--import"], input=exported, check=True
    )


async def test_pynixd_imports_as_the_daemon_does(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    a, order, self_ca = await lab(tmp_path)
    assert str(rows(a, f"select ca from ValidPaths where path = '{self_ca}'")[0][0]).startswith("fixed:r:sha256:")
    imported: list[str] = []

    add_to_store = StoreEngine.add_to_store

    async def counting(engine: StoreEngine, info: ValidPathInfo, *args: Any, **kwargs: Any) -> None:
        imported.append(str(info.path))
        await add_to_store(engine, info, *args, **kwargs)

    monkeypatch.setattr(StoreEngine, "add_to_store", counting)

    b, c = tmp_path / "b", tmp_path / "c"
    async with server(b, tmp_path / "b.sock", "pynixd"):
        await _import(a, b, tmp_path / "b.sock", order)
    # `exportPaths` orders the paths itself (`export-import.cc:39`).
    assert sorted(imported) == sorted(order), "pynixd imported each path, none went to the daemon"
    imported.clear()
    async with server(c, tmp_path / "c.sock", "daemon"):
        await _import(a, c, tmp_path / "c.sock", order)
    assert imported == []

    for query in (VALID_PATHS, REFS, OUTPUTS):
        assert rows(b, query) == rows(c, query)
    assert len(rows(b, VALID_PATHS)) == len(order)
    for path in order:
        assert files(b, path) == files(c, path), path
        assert not (real(b, path).parent / f"{real(b, path).name}.lock").exists()
    assert not list((b / "nix" / "store").glob("tmp-*"))
    await run(str(NIX_BIN), "store", "verify", "--store", str(b), "--no-trust", *order)


async def _send(conn: Connection, info: ValidPathInfo, nar: bytes) -> None:
    """One AddToStoreNar; raises `BackendError` with the server's message."""
    await AddToStoreNarRequest(info=info, repair=0, dont_check_sigs=0).to_writer(WriteContext.from_conn(conn))
    framed = conn.w.framed()
    framed.write(nar)
    await framed.finalize()
    await AddToStoreNarResponse.from_reader(ReadContext.from_conn(conn, raise_on_error=True))


_REFUSALS = [
    pytest.param({"nar_size": 1}, "size mismatch importing path", id="size"),
    pytest.param({"nar_hash": NARHash(hash="0" * 64)}, "hash mismatch importing path", id="hash"),
]


@pytest.mark.parametrize(("changes", "message"), _REFUSALS)
async def test_a_wrong_nar_is_refused_and_the_session_goes_on(
    tmp_path: Path, changes: dict[str, Any], message: str
) -> None:
    a, order, _ = await lab(tmp_path)
    flat = next(p for p in order if p.endswith("-flat"))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, "pynixd"), client(socket) as conn:
        with pytest.raises(BackendError, match=message):
            await _send(conn, info_of(a, flat, **changes), nar_of(a, flat))
        assert not await valid(conn, flat)
        await _send(conn, info_of(a, flat), nar_of(a, flat))
        assert await valid(conn, flat)


async def test_a_false_content_hash_is_refused(tmp_path: Path) -> None:
    """The NAR matches its hash, and the address names other bytes: `ca hash mismatch` (`local-store.cc:1125`)."""
    a, order, _ = await lab(tmp_path)
    flat = next(p for p in order if p.endswith("-flat"))
    other = tmp_path / "other"
    other.write_bytes(b"other\n")  # noqa: ASYNC240 -- test setup
    nar = b"".join(dump_path(other))
    info = info_of(a, flat, nar_hash=NARHash(hash=hashlib.sha256(nar).hexdigest()), nar_size=len(nar))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, "pynixd"), client(socket) as conn:
        with pytest.raises(BackendError, match="ca hash mismatch importing path"):
            await _send(conn, info, nar)
        assert not await valid(conn, flat)


async def test_an_unsigned_path_is_refused_and_a_signed_one_is_not(tmp_path: Path) -> None:
    a, _, _ = await lab(tmp_path)
    nix = str(NIX_BIN)
    secret = tmp_path / "sk"
    secret.write_text(await run(nix, "key", "generate-secret", "--key-name", "test-1"))  # noqa: ASYNC240 -- test setup
    public = (
        (await anyio.run_process([nix, "key", "convert-secret-to-public"], input=secret.read_bytes(), check=True))
        .stdout.decode()
        .strip()
    )  # noqa: ASYNC240 -- test setup
    tree = next(p for (p, *_) in rows(a, VALID_PATHS) if p.endswith("-tree"))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with (
        server(b, socket, "pynixd", require_sigs=True, trusted_public_keys=(public,)),
        client(socket) as conn,
    ):
        # Content-addressed: no key needed.
        await _send(conn, info_of(a, tree), nar_of(a, tree))
        with pytest.raises(BackendError, match="lacks a signature by a trusted key"):
            await _send(conn, info_of(a, IA), nar_of(a, IA))
        assert not await valid(conn, IA)
        await run(nix, "store", "sign", "--store", str(a), "--key-file", str(secret), IA)
        await _send(conn, info_of(a, IA), nar_of(a, IA))
        assert await valid(conn, IA)
        # Valid already: the NAR is read and dropped, and nothing changes.
        await _send(conn, info_of(a, IA), nar_of(a, IA))
        assert await valid(conn, IA)
