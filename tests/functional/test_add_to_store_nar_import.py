"""pynixd imports a NAR on AddToStoreNar's `pynixd` route as `LocalStore::addToStore` does (`local-store.cc:1046`).

Nix fills store A: a source tree, a flat file, derivations, an input-addressed
path that refers to itself, and its content-addressed copy, whose address
hashes the NAR modulo the self-reference. `nix-store --import` sends op 39
(`export-import.cc:89`; `nix copy` sends op 44), so the export of A goes into
store B on the `pynixd` route and store C on the daemon route. Rows and files
must agree. The refusals use a raw client, which can send what Nix's client never does.
"""

from __future__ import annotations

import graphlib
import hashlib
import json
import os
import sqlite3
import stat
from contextlib import asynccontextmanager, closing
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.add_to_store_nar import AddToStoreNarRequest, AddToStoreNarResponse
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd import Server
from pynixd.connection import Connection
from pynixd.exceptions import BackendError
from pynixd.nar_fs import dump_path
from pynixd.serde import IsValidPathRequest, StorePath
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.store import LocalDBStore
from pynixd.store.add import add_nar
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

_VALID_PATHS = "select path, hash, deriver, narSize, ultimate, sigs, ca from ValidPaths order by path"
_REFS = (
    "select a.path, b.path from Refs join ValidPaths a on a.id = referrer join ValidPaths b on b.id = reference "
    "order by a.path, b.path"
)
_OUTPUTS = (
    "select v.path, d.id, d.path from DerivationOutputs d join ValidPaths v on v.id = d.drv order by v.path, d.id"
)
_EXPR = """
let src = builtins.toFile "src" "hello"; in
derivation { name = "base"; system = "x86_64-linux"; builder = "/bin/sh"; args = [ src ]; outputs = [ "out" "doc" ]; }
"""
_IA = "/nix/store/1111111111111111111111111111111a-ia"
#: `--import` and `--register-validity` have no `nix` equivalent.
_NIX_STORE = str(NIX_BIN.with_name("nix-store"))


async def _run(*args: str, stdin: bytes | None = None) -> str:
    result = await anyio.run_process(list(args), input=stdin, check=True)
    return result.stdout.decode().strip()


def _rows(root: Path, query: str) -> list[tuple[Any, ...]]:
    db = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    with closing(sqlite3.connect(f"file:{db}?mode=ro", uri=True)) as conn:
        return conn.execute(query).fetchall()


def _real(root: Path, path: str) -> Path:
    return root / "nix" / "store" / path.rsplit("/", 1)[-1]


async def _lab(tmp_path: Path) -> tuple[Path, list[str], str]:
    """Store A, its paths with references first, and the content-addressed path that refers to itself."""
    nix = str(NIX_BIN)
    a = tmp_path / "a"
    tree = tmp_path / "tree"
    (tree / "bin").mkdir(parents=True)
    (tree / "bin" / "hello").write_bytes(b"#!/bin/sh\necho hi\n")  # noqa: ASYNC240 -- test setup
    (tree / "bin" / "hello").chmod(0o755)
    (tree / "link").symlink_to("bin/hello")
    flat = tmp_path / "flat"
    flat.write_bytes(b"flat\n")  # noqa: ASYNC240 -- test setup
    tree_path = await _run(nix, "store", "add-path", "--store", str(a), str(tree))
    await _run(nix, "store", "add-file", "--store", str(a), str(flat))
    await _run(nix, "eval", "--store", str(a), "--raw", "--expr", f"({_EXPR}).drvPath")

    # An input-addressed path that names itself and the tree, registered by hand.
    ia = _real(a, _IA)
    ia.write_text(f"{_IA} {tree_path}\n")  # noqa: ASYNC240 -- test setup
    nar = b"".join(dump_path(ia))
    reginfo = f"{_IA}\n{hashlib.sha256(nar).hexdigest()}\n{len(nar)}\n\n2\n{_IA}\n{tree_path}\n"
    await _run(nix, "store", "info", "--store", str(a))
    await _run(_NIX_STORE, "--store", str(a), "--register-validity", "--hash-given", stdin=reginfo.encode())
    rewrites = json.loads(await _run(nix, "store", "make-content-addressed", "--store", str(a), "--json", _IA))
    self_ca = rewrites["rewrites"][_IA]

    graph: dict[str, set[str]] = {}
    for referrer, reference in _rows(a, _REFS):
        graph.setdefault(referrer, set()).add(reference)
    for path, *_ in _rows(a, _VALID_PATHS):
        graph.setdefault(path, set())
    others = {p: refs.difference((p,)) for p, refs in graph.items()}
    sorter = graphlib.TopologicalSorter[str](others)
    order = list(sorter.static_order())
    return a, order, self_ca


async def _import(a: Path, root: Path, socket: Path, paths: list[str]) -> None:
    exported = (await anyio.run_process([_NIX_STORE, "--store", str(a), "--export", *paths], check=True)).stdout
    await anyio.run_process(
        [_NIX_STORE, "--store", f"unix://{socket}?root={root}", "--import"], input=exported, check=True
    )


@asynccontextmanager
async def _server(root: Path, socket: Path, route: str, **config: Any) -> AsyncIterator[LocalDBStore]:
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=(), **config),
        op_routes={"AddToStoreNar": route},
    )
    store = LocalDBStore(spec)
    async with Server(stores={StoreId("local"): store}, unix_path=socket, ssh_port=None, http_port=None):
        yield store


def _files(root: Path, path: str) -> list[tuple[Any, ...]]:
    """Each file of *path*: its name, type, mode, mtime, owner, and what it holds."""
    out = []
    top = _real(root, path)
    for at in [top, *sorted(top.rglob("*"))] if top.is_dir() else [top]:
        st = at.lstat()
        held = (
            os.readlink(at) if at.is_symlink() else hashlib.sha256(at.read_bytes()).hexdigest() if at.is_file() else ""
        )
        out.append(
            (
                str(at.relative_to(top.parent)),
                stat.S_IFMT(st.st_mode),
                stat.S_IMODE(st.st_mode),
                int(st.st_mtime),
                st.st_uid,
                held,
            )
        )
    return out


async def test_pynixd_imports_as_the_daemon_does(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    a, order, self_ca = await _lab(tmp_path)
    assert str(_rows(a, f"select ca from ValidPaths where path = '{self_ca}'")[0][0]).startswith("fixed:r:sha256:")
    imported: list[str] = []

    async def counting(store: LocalDBStore, info: ValidPathInfo, *args: Any, **kwargs: Any) -> None:
        imported.append(str(info.path))
        await add_nar(store, info, *args, **kwargs)

    monkeypatch.setattr("pynixd.handlers.add_to_store_nar.add_nar", counting)

    b, c = tmp_path / "b", tmp_path / "c"
    async with _server(b, tmp_path / "b.sock", "pynixd"):
        await _import(a, b, tmp_path / "b.sock", order)
    # `exportPaths` orders the paths itself (`export-import.cc:39`).
    assert sorted(imported) == sorted(order), "pynixd imported each path, none went to the daemon"
    imported.clear()
    async with _server(c, tmp_path / "c.sock", "daemon"):
        await _import(a, c, tmp_path / "c.sock", order)
    assert imported == []

    for query in (_VALID_PATHS, _REFS, _OUTPUTS):
        assert _rows(b, query) == _rows(c, query)
    assert len(_rows(b, _VALID_PATHS)) == len(order)
    for path in order:
        assert _files(b, path) == _files(c, path), path
        assert not (_real(b, path).parent / f"{_real(b, path).name}.lock").exists()
    assert not list((b / "nix" / "store").glob("tmp-*"))
    await _run(str(NIX_BIN), "store", "verify", "--store", str(b), "--no-trust", *order)


def _info_of(a: Path, path: str, **changes: Any) -> ValidPathInfo:
    [(_, nar_hash, deriver, nar_size, ultimate, sigs, ca)] = _rows(
        a, f"select path, hash, deriver, narSize, ultimate, sigs, ca from ValidPaths where path = '{path}'"
    )
    refs = {StorePath(path=r) for (p, r) in _rows(a, _REFS) if p == path}
    unkeyed = UnkeyedValidPathInfo(
        deriver=StorePath(path=deriver or ""),
        nar_hash=NARHash(hash=nar_hash.removeprefix("sha256:")),
        references=refs,
        registration_time=Time(ts=0),
        nar_size=nar_size,
        ultimate=bool(ultimate),
        sigs={Signature(s) for s in (sigs or "").split()},
        ca=ContentAddress(value=ca or ""),
    )
    return ValidPathInfo(path=StorePath(path=path), info=unkeyed.model_copy(update=changes))


@asynccontextmanager
async def _client(socket: Path) -> AsyncIterator[Connection]:
    link = await open_unix_connection(socket)
    conn = Connection(UnixNixReader(link, identifier="test"), UnixNixWriter(link, identifier="test"), "test-import")
    await conn.connect()
    try:
        yield conn
    finally:
        await conn.close()


async def _send(conn: Connection, info: ValidPathInfo, nar: bytes) -> None:
    """One AddToStoreNar; raises `BackendError` with the server's message."""
    await AddToStoreNarRequest(info=info, repair=0, dont_check_sigs=0).to_writer(WriteContext.from_conn(conn))
    framed = conn.w.framed()
    framed.write(nar)
    await framed.finalize()
    await AddToStoreNarResponse.from_reader(ReadContext.from_conn(conn, raise_on_error=True))


async def _valid(conn: Connection, path: str) -> bool:
    resp = await conn.call(IsValidPathRequest(path=StorePath(path=path)), raise_on_error=True)
    return bool(resp.valid)


def _nar(a: Path, path: str) -> bytes:
    return b"".join(dump_path(_real(a, path)))


_REFUSALS = [
    pytest.param({"nar_size": 1}, "size mismatch importing path", id="size"),
    pytest.param({"nar_hash": NARHash(hash="0" * 64)}, "hash mismatch importing path", id="hash"),
]


@pytest.mark.parametrize(("changes", "message"), _REFUSALS)
async def test_a_wrong_nar_is_refused_and_the_session_goes_on(
    tmp_path: Path, changes: dict[str, Any], message: str
) -> None:
    a, order, _ = await _lab(tmp_path)
    flat = next(p for p in order if p.endswith("-flat"))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with _server(b, socket, "pynixd"), _client(socket) as conn:
        with pytest.raises(BackendError, match=message):
            await _send(conn, _info_of(a, flat, **changes), _nar(a, flat))
        assert not await _valid(conn, flat)
        await _send(conn, _info_of(a, flat), _nar(a, flat))
        assert await _valid(conn, flat)


async def test_a_false_content_hash_is_refused(tmp_path: Path) -> None:
    """The NAR matches its hash, and the address names other bytes: `ca hash mismatch` (`local-store.cc:1125`)."""
    a, order, _ = await _lab(tmp_path)
    flat = next(p for p in order if p.endswith("-flat"))
    other = tmp_path / "other"
    other.write_bytes(b"other\n")  # noqa: ASYNC240 -- test setup
    nar = b"".join(dump_path(other))
    info = _info_of(a, flat, nar_hash=NARHash(hash=hashlib.sha256(nar).hexdigest()), nar_size=len(nar))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with _server(b, socket, "pynixd"), _client(socket) as conn:
        with pytest.raises(BackendError, match="ca hash mismatch importing path"):
            await _send(conn, info, nar)
        assert not await _valid(conn, flat)


async def test_an_unsigned_path_is_refused_and_a_signed_one_is_not(tmp_path: Path) -> None:
    a, _, _ = await _lab(tmp_path)
    nix = str(NIX_BIN)
    secret = tmp_path / "sk"
    secret.write_text(await _run(nix, "key", "generate-secret", "--key-name", "test-1"))  # noqa: ASYNC240 -- test setup
    public = (
        (await anyio.run_process([nix, "key", "convert-secret-to-public"], input=secret.read_bytes(), check=True))
        .stdout.decode()
        .strip()
    )  # noqa: ASYNC240 -- test setup
    tree = next(p for (p, *_) in _rows(a, _VALID_PATHS) if p.endswith("-tree"))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with (
        _server(b, socket, "pynixd", require_sigs=True, trusted_public_keys=(public,)),
        _client(socket) as conn,
    ):
        # Content-addressed: no key needed.
        await _send(conn, _info_of(a, tree), _nar(a, tree))
        with pytest.raises(BackendError, match="lacks a signature by a trusted key"):
            await _send(conn, _info_of(a, _IA), _nar(a, _IA))
        assert not await _valid(conn, _IA)
        await _run(nix, "store", "sign", "--store", str(a), "--key-file", str(secret), _IA)
        await _send(conn, _info_of(a, _IA), _nar(a, _IA))
        assert await _valid(conn, _IA)
        # Valid already: the NAR is read and dropped, and nothing changes.
        await _send(conn, _info_of(a, _IA), _nar(a, _IA))
        assert await _valid(conn, _IA)
