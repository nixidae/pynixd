"""Store A for the import tests of AddToStore (op 7), AddToStoreNar (op 39) and AddMultipleToStore (op 44), and a raw client.

Nix fills store A: a source tree, a flat file, derivations, an input-addressed
path that refers to itself, and its content-addressed copy, whose address
hashes the NAR modulo the self-reference.
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
from aiohttp import web

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd import Server
from pynixd.config import HTTPBinaryCacheSpec
from pynixd.connection import Connection
from pynixd.serde import IsValidPathRequest, StorePath
from pynixd.store import LocalDBStore, Store
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from pynixd_store.nar_fs import dump_path
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

VALID_PATHS = "select path, hash, deriver, narSize, ultimate, sigs, ca from ValidPaths order by path"
REFS = (
    "select a.path, b.path from Refs join ValidPaths a on a.id = referrer join ValidPaths b on b.id = reference "
    "order by a.path, b.path"
)
OUTPUTS = "select v.path, d.id, d.path from DerivationOutputs d join ValidPaths v on v.id = d.drv order by v.path, d.id"
EXPR = """
let src = builtins.toFile "src" "hello"; in
derivation { name = "base"; system = "x86_64-linux"; builder = "/bin/sh"; args = [ src ]; outputs = [ "out" "doc" ]; }
"""
#: Its env holds `__json` and the outputs, and no `name`.
STRUCTURED = """
derivation { name = "structured"; system = "x86_64-linux"; builder = "/bin/sh"; __structuredAttrs = true; }
"""
IA = "/nix/store/1111111111111111111111111111111a-ia"
#: `--import` and `--register-validity` have no `nix` equivalent.
NIX_STORE = str(NIX_BIN.with_name("nix-store"))
IMPORT_OPS = ("AddToStore", "AddToStoreNar", "AddMultipleToStore")


async def run(*args: str, stdin: bytes | None = None) -> str:
    result = await anyio.run_process(list(args), input=stdin, check=True)
    return result.stdout.decode().strip()


def rows(root: Path, query: str) -> list[tuple[Any, ...]]:
    db = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    with closing(sqlite3.connect(f"file:{db}?mode=ro", uri=True)) as conn:
        return conn.execute(query).fetchall()


def real(root: Path, path: str) -> Path:
    return root / "nix" / "store" / path.rsplit("/", 1)[-1]


async def lab(tmp_path: Path) -> tuple[Path, list[str], str]:
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
    tree_path = await run(nix, "store", "add-path", "--store", str(a), str(tree))
    await run(nix, "store", "add-file", "--store", str(a), str(flat))
    await run(nix, "eval", "--store", str(a), "--raw", "--expr", f"({EXPR}).drvPath")
    await run(nix, "eval", "--store", str(a), "--raw", "--expr", f"({STRUCTURED}).drvPath")

    # An input-addressed path that names itself and the tree, registered by hand.
    ia = real(a, IA)
    ia.write_text(f"{IA} {tree_path}\n")  # noqa: ASYNC240 -- test setup
    nar = b"".join(dump_path(ia))
    reginfo = f"{IA}\n{hashlib.sha256(nar).hexdigest()}\n{len(nar)}\n\n2\n{IA}\n{tree_path}\n"
    await run(nix, "store", "info", "--store", str(a))
    await run(NIX_STORE, "--store", str(a), "--register-validity", "--hash-given", stdin=reginfo.encode())
    rewrites = json.loads(await run(nix, "store", "make-content-addressed", "--store", str(a), "--json", IA))
    self_ca = rewrites["rewrites"][IA]

    graph: dict[str, set[str]] = {}
    for referrer, reference in rows(a, REFS):
        graph.setdefault(referrer, set()).add(reference)
    for path, *_ in rows(a, VALID_PATHS):
        graph.setdefault(path, set())
    others = {p: refs.difference((p,)) for p, refs in graph.items()}
    sorter = graphlib.TopologicalSorter[str](others)
    order = list(sorter.static_order())
    return a, order, self_ca


@asynccontextmanager
async def server(
    root: Path,
    socket: Path,
    route: str,
    *,
    upstream: str | None = None,
    daemon: bool | None = None,
    **config: Any,
) -> AsyncIterator[LocalDBStore]:
    """A local store whose import ops take *route*; *upstream* is a binary cache pynixd substitutes from.

    `daemon=False` runs the store with no Nix daemon.
    """
    # `make_test_spec` reads an explicit `daemon` as the caller's choice, so it is passed only when given.
    spec_args: dict[str, Any] = {} if daemon is None else {"daemon": daemon}
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=(), **config),
        op_routes=dict.fromkeys(IMPORT_OPS, route),
        **spec_args,
    )
    store = LocalDBStore(spec)
    stores: dict[StoreId, Store] = {StoreId("local"): store}
    if upstream is not None:
        cache = HTTPBinaryCacheSpec(store_id=StoreId("upstream"), url=upstream, trusted=True)
        stores[StoreId("upstream")] = cache.to_store("upstream")
    async with Server(stores=stores, unix_path=socket, ssh_port=None, http_port=None):
        yield store


@asynccontextmanager
async def serve_directory(directory: Path) -> AsyncIterator[str]:
    """Serve *directory* over HTTP, as a binary cache `nix copy --to file://` wrote, and give its URL."""
    app = web.Application()
    app.router.add_static("/", directory)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        host, port = runner.addresses[0][:2]
        yield f"http://{host}:{port}/"
    finally:
        await runner.cleanup()


def files(root: Path, path: str) -> list[tuple[Any, ...]]:
    """Each file of *path*: its name, type, mode, mtime, owner, and what it holds."""
    out = []
    top = real(root, path)
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


def info_of(a: Path, path: str, **changes: Any) -> ValidPathInfo:
    [(_, nar_hash, deriver, nar_size, ultimate, sigs, ca)] = rows(
        a, f"select path, hash, deriver, narSize, ultimate, sigs, ca from ValidPaths where path = '{path}'"
    )
    refs = {StorePath(path=r) for (p, r) in rows(a, REFS) if p == path}
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
async def client(socket: Path) -> AsyncIterator[Connection]:
    link = await open_unix_connection(socket)
    conn = Connection(UnixNixReader(link, identifier="test"), UnixNixWriter(link, identifier="test"), "test-import")
    await conn.connect()
    try:
        yield conn
    finally:
        await conn.close()


async def valid(conn: Connection, path: str) -> bool:
    resp = await conn.call(IsValidPathRequest(path=StorePath(path=path)), raise_on_error=True)
    return bool(resp.valid)


def nar_of(a: Path, path: str) -> bytes:
    return b"".join(dump_path(real(a, path)))
