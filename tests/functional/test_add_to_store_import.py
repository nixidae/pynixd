"""pynixd adds AddToStore's dump on its `pynixd` route as `LocalStore::addToStoreFromDump` does (`local-store.cc:1168`).

An evaluation writes its derivations and `builtins.toFile` through op 7, as
text with references (`remote-store.cc:addCAToStore`). `nix store add`
hashes its source in the client and sends op 39 (`Store::addToStore`), so
it covers the other modes only through the import of op 39. The same
commands go to store B on the `pynixd` route and store C on the daemon
route, and rows and files must agree. A raw client sends op 7 in the flat
and NAR modes, and the refusals.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.add_to_store import AddToStoreRequest, AddToStoreResponse
from nix_daemon_protocol.content_address import ContentAddress
from pynixd.exceptions import BackendError
from pynixd.nar_fs import dump_path
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.store.engine import StoreEngine
from tests._conftest.import_lab import (
    EXPR,
    OUTPUTS,
    REFS,
    STRUCTURED,
    VALID_PATHS,
    client,
    files,
    rows,
    run,
    server,
    valid,
)
from tests.conftest import NIX_BIN

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.connection import Connection

_ROUTES = pytest.mark.parametrize("route", ["pynixd", "daemon"])


@pytest.fixture
def added(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The paths pynixd's engine added for op 7."""
    seen: list[str] = []
    add = StoreEngine.add_to_store_from_dump

    async def counting(engine: StoreEngine, *args: Any, **kwargs: Any) -> str:
        path = await add(engine, *args, **kwargs)
        seen.append(path)
        return path

    monkeypatch.setattr(StoreEngine, "add_to_store_from_dump", counting)
    return seen


def _inputs(tmp_path: Path) -> tuple[Path, Path]:
    tree = tmp_path / "tree"
    (tree / "bin").mkdir(parents=True)
    (tree / "bin" / "hello").write_bytes(b"#!/bin/sh\necho hi\n")  # noqa: ASYNC240 -- test setup
    (tree / "bin" / "hello").chmod(0o755)
    (tree / "link").symlink_to("bin/hello")
    flat = tmp_path / "flat"
    flat.write_bytes(b"flat\n")  # noqa: ASYNC240 -- test setup
    flat.chmod(0o755)
    return tree, flat


async def _add_all(store: str, tree: Path, flat: Path) -> list[str]:
    nix = str(NIX_BIN)
    out = []
    for mode, algo, source in (
        ("nar", "sha256", tree),
        ("nar", "sha512", tree),
        ("flat", "sha256", flat),
        ("flat", "sha1", flat),
        ("text", "sha256", flat),
    ):
        out.append(await run(nix, "store", "add", "--store", store, "--mode", mode, "--hash-algo", algo, str(source)))
    for expr in (EXPR, STRUCTURED):
        out.append(await run(nix, "eval", "--store", store, "--raw", "--expr", f"({expr}).drvPath"))
    return out


async def test_nix_store_add_and_eval_add_as_the_daemon_does(tmp_path: Path, added: list[str]) -> None:
    tree, flat = _inputs(tmp_path)
    b, c = tmp_path / "b", tmp_path / "c"
    printed = {}
    for root, route in ((b, "pynixd"), (c, "daemon")):
        socket = tmp_path / f"{route}.sock"
        async with server(root, socket, route):
            store = f"unix://{socket}?root={root}"
            printed[route] = await _add_all(store, tree, flat)
            # A second add of every path finds it valid and writes nothing.
            assert await _add_all(store, tree, flat) == printed[route]
        if route == "pynixd":
            text = {p for (p, *_, ca) in rows(root, VALID_PATHS) if ca.startswith("text:")}
            drvs = {p for p in text if p.endswith(".drv")}
            assert drvs, "the evaluation wrote derivations"
            assert drvs <= set(added) <= text, "pynixd added each path op 7 sent, none went to the daemon"
            added.clear()
    assert added == []
    assert printed["pynixd"] == printed["daemon"]

    for query in (VALID_PATHS, REFS, OUTPUTS):
        assert rows(b, query) == rows(c, query)
    paths = [p for (p, *_) in rows(b, VALID_PATHS)]
    for path in paths:
        assert files(b, path) == files(c, path), path
        assert not (b / "nix" / "store" / f"{path.rsplit('/', 1)[-1]}.lock").exists()
    assert not list((b / "nix" / "store").glob("tmp-*"))
    await run(str(NIX_BIN), "store", "verify", "--store", str(b), "--no-trust", *paths)


async def _send(conn: Connection, name: str, cam: str, dump: bytes) -> AddToStoreResponse:
    """One AddToStore with *dump* in one frame; raises `BackendError`."""
    request = AddToStoreRequest(path_name=name, cam=ContentAddress(value=cam), references=set(), repair=0)
    await request.to_writer(WriteContext.from_conn(conn))
    framed = conn.w.framed()
    framed.write(dump)
    await framed.finalize()
    return await AddToStoreResponse.from_reader(ReadContext.from_conn(conn, raise_on_error=True))


async def test_each_mode_of_a_raw_client_adds_as_the_daemon_does(tmp_path: Path, added: list[str]) -> None:
    tree, flat = _inputs(tmp_path)
    nar = b"".join(dump_path(tree))
    sends = [
        ("tree", "fixed:r:sha256", nar),
        ("tree", "fixed:r:sha512", nar),
        ("tree", "fixed:r:md5", nar),
        ("flat", "fixed:sha256", flat.read_bytes()),  # noqa: ASYNC240 -- test setup
        ("flat", "fixed:sha1", flat.read_bytes()),  # noqa: ASYNC240 -- test setup
        ("note", "text:sha256", b"a note\n"),
    ]
    answers = {}
    b, c = tmp_path / "b", tmp_path / "c"
    for root, route in ((b, "pynixd"), (c, "daemon")):
        socket = tmp_path / f"{route}.sock"
        async with server(root, socket, route), client(socket) as conn:
            answers[route] = [
                (await _send(conn, name, cam, dump)).info.model_dump(exclude={"info": {"registration_time"}})
                for name, cam, dump in sends
            ]
    assert len(added) == len(sends)
    assert answers["pynixd"] == answers["daemon"]
    for query in (VALID_PATHS, REFS):
        assert rows(b, query) == rows(c, query)
    for path, *_ in rows(b, VALID_PATHS):
        assert files(b, path) == files(c, path), path


@_ROUTES
async def test_a_dump_that_is_not_a_nar_is_refused_and_the_session_goes_on(tmp_path: Path, route: str) -> None:
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, route), client(socket) as conn:
        with pytest.raises(BackendError, match=re.escape("input doesn't look like a Nix archive")):
            await _send(conn, "bad", "fixed:r:sha256", b"not a nar")
        resp = await _send(conn, "good", "fixed:sha256", b"good\n")
        assert await valid(conn, str(resp.info.path))
    assert not list((b / "nix" / "store").glob("tmp-*"))


async def test_a_git_dump_goes_to_the_daemon(tmp_path: Path, added: list[str]) -> None:
    """pynixd does not hash a Git address (`git::dumpHash`), so the daemon adds it."""
    _, flat = _inputs(tmp_path)
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, "pynixd", experimental_features=("git-hashing",)):
        path = await run(
            str(NIX_BIN), "store", "add", "--store", f"unix://{socket}?root={b}",
            "--extra-experimental-features", "git-hashing", "--mode", "git", "--hash-algo", "sha1", str(flat),
        )  # fmt: skip
    assert added == []
    assert str(rows(b, f"select ca from ValidPaths where path = '{path}'")[0][0]).startswith("fixed:git:sha1:")
