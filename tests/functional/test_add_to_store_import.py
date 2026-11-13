"""pynixd adds AddToStore's dump on its `pynixd` route as `LocalStore::addToStoreFromDump` does (`local-store.cc:1151`).

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

import anyio
import pytest

from nix_daemon_protocol.add_to_store import AddToStoreRequest, AddToStoreResponse
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.store_path import StorePath
from pynixd.exceptions import BackendError
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.store.daemon_writes import DaemonWrites
from pynixd_store.content_address import text_needs_sha256
from pynixd_store.engine import StoreEngine
from pynixd_store.hintfmt import magenta, missing_feature
from pynixd_store.nar_fs import dump_path
from tests._conftest.import_lab import (
    EXPR,
    OUTPUTS,
    REFS,
    STRUCTURED,
    VALID_PATHS,
    client,
    files,
    real,
    rows,
    run,
    server,
    valid,
)
from tests.conftest import NIX_BIN

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from pynixd.connection import Connection

_ROUTES = pytest.mark.parametrize("route", ["pynixd", "daemon"])
_GIT = ("git-hashing",)


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


async def _send(
    conn: Connection, name: str, cam: str, dump: bytes, *, repair: int = 0, references: Iterable[str] = ()
) -> AddToStoreResponse:
    """One AddToStore with *dump* in one frame; raises `BackendError`."""
    request = AddToStoreRequest(
        path_name=name,
        cam=ContentAddress(value=cam),
        references={StorePath(ref) for ref in references},
        repair=repair,
    )
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


async def test_a_dump_the_store_holds_is_answered_without_the_daemon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nix answers op 7 for a valid path from its database and writes nothing (`local-store.cc:1151`).

    On the daemon route pynixd does the same, and the daemon sees only the first add. An evaluation sends one
    op 7 for each derivation file, and most are valid: 1420 for one `firefox.drvPath` (nixidae/nixidae#419).
    """
    forwarded: list[str] = []
    forward = DaemonWrites.add_to_store_from_dump

    async def counting(writes: DaemonWrites, name: str, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401 -- the forward's own answer
        forwarded.append(name)
        return await forward(writes, name, *args, **kwargs)

    monkeypatch.setattr(DaemonWrites, "add_to_store_from_dump", counting)
    tree, flat = _inputs(tmp_path)
    sends = [
        ("tree", "fixed:r:sha256", b"".join(dump_path(tree))),
        ("flat", "fixed:sha256", flat.read_bytes()),  # noqa: ASYNC240 -- test setup
        ("note", "text:sha256", b"a note\n"),
    ]
    root, socket = tmp_path / "c", tmp_path / "daemon.sock"
    async with server(root, socket, "daemon"), client(socket) as conn:
        first = [(await _send(conn, name, cam, dump)).info.model_dump() for name, cam, dump in sends]
        assert forwarded == [name for name, *_ in sends]
        forwarded.clear()
        again = [(await _send(conn, name, cam, dump)).info.model_dump() for name, cam, dump in sends]
    assert forwarded == []
    assert again == first


def _corrupt(root: Path, path: str) -> None:
    """A changed file, as a disk or a hand edit leaves it."""
    target = real(root, path)
    if target.is_dir():
        target.chmod(0o755)
        (target / "bin").chmod(0o755)
        target = target / "bin" / "hello"
    target.chmod(0o644)
    target.write_bytes(b"corrupt\n")


async def test_a_repair_restores_a_corrupted_dump_as_the_daemon_does(tmp_path: Path, added: list[str]) -> None:
    """Op 7 with repair restores a valid path (`local-store.cc:1249`); without repair it keeps the path as it is."""
    tree, flat = _inputs(tmp_path)
    sends = [
        ("tree", "fixed:r:sha256", b"".join(dump_path(tree))),
        ("flat", "fixed:sha256", flat.read_bytes()),  # noqa: ASYNC240 -- test setup
        ("note", "text:sha256", b"a note\n"),
    ]
    b, c = tmp_path / "b", tmp_path / "c"
    for root, route in ((b, "pynixd"), (c, "daemon")):
        socket = tmp_path / f"{route}.sock"
        async with server(root, socket, route), client(socket) as conn:
            paths = [str((await _send(conn, *send)).info.path) for send in sends]
            good = {path: files(root, path) for path in paths}
            for path in paths:
                _corrupt(root, path)
            corrupt = {path: files(root, path) for path in paths}
            for send in sends:
                await _send(conn, *send)
            assert {path: files(root, path) for path in paths} == corrupt
            for send in sends:
                await _send(conn, *send, repair=1)
            assert {path: files(root, path) for path in paths} == good
        if route == "pynixd":
            assert added == paths * 3, "pynixd took each send, none went to the daemon"
            added.clear()
    assert added == []
    for query in (VALID_PATHS, REFS):
        assert rows(b, query) == rows(c, query)
    for path in paths:
        assert files(b, path) == files(c, path), path
    assert not list((b / "nix" / "store").glob("tmp-*"))


@_ROUTES
async def test_a_dump_that_is_not_a_nar_is_refused_and_the_session_goes_on(tmp_path: Path, route: str) -> None:
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, route), client(socket) as conn:
        with pytest.raises(BackendError, match=re.escape("input doesn't look like a Nix archive")):
            await _send(conn, "bad", "fixed:r:sha256", b"not a nar")
        resp = await _send(conn, "good", "fixed:sha256", b"good\n")
        assert await valid(conn, str(resp.info.path))
    assert not list((b / "nix" / "store").glob("tmp-*"))


_REFUSED_DUMPS = [
    # `ContentAddressMethod::parseWithAlgo` (`content-address.cc:203`), before the `FramedSource`.
    ("f", "sha256"),
    ("f", "other:sha256"),
    ("f", "fixed:md4"),
    ("f", "fixed:r:blake3"),
    ("f", "text:"),
    ("f", "text"),
    ("f", "fixed"),
    ("f", "fixed:r"),
    # `addToStoreFromDump`, after the hash: `checkName` (`path.cc:12`) as `makeStorePath` makes the path.
    ("bad name", "fixed:sha256"),
    (".", "fixed:sha256"),
    ("..-f", "fixed:sha256"),
    ("f" * 212, "fixed:sha256"),
    ("bad name", "text:sha256"),
]


async def _refusals(tmp_path: Path, route: str, daemon: bool | None, sends: list[tuple[str, str]]) -> list[str]:
    """What the store answers each of *sends*, a flat dump `x`; the session goes on after each.

    A send named `f` with `fixed:sha256` refers to a valid path.
    """
    answers = []
    tmp_path.mkdir(exist_ok=True)
    root, socket = tmp_path / route, tmp_path / f"{route}.sock"
    async with server(root, socket, route, daemon=daemon), client(socket) as conn:
        good = str((await _send(conn, "good", "fixed:sha256", b"good")).info.path)
        for name, cam in sends:
            references = [good] if (name, cam) == ("f", "fixed:sha256") else []
            with pytest.raises(BackendError) as refused:
                await _send(conn, name, cam, b"x", references=references)
            answers.append(str(refused.value))
            assert await valid(conn, good)
    return answers


_DEFERRED = (
    'Derive([("out","","","")],[],[],"x86_64-linux","/bin/sh",["-c","true"],'
    '[("builder","/bin/sh"),("name","x"),("out",""),("system","x86_64-linux")])'
)


async def test_a_deferred_output_warns_the_client_as_the_daemon_does(tmp_path: Path) -> None:
    """`checkInvariants` warns while it registers the `.drv` (`derivations.cc:1334`), and the client prints it."""
    drv = tmp_path / "x.drv"
    drv.write_text(_DEFERRED)  # noqa: ASYNC240 -- test setup
    warned = {}
    for route in ("pynixd", "daemon"):
        root, socket = tmp_path / route, tmp_path / f"{route}.sock"
        async with server(root, socket, route):
            store = f"unix://{socket}?root={root}"
            added = await anyio.run_process(
                [str(NIX_BIN), "store", "add", "--store", store, "--mode", "text", str(drv)], check=True
            )
        warned[route] = [line for line in added.stderr.decode().splitlines() if "deferred output" in line]
    assert warned["pynixd"], warned
    assert warned["pynixd"] == warned["daemon"]


async def test_a_dump_nix_refuses_is_refused_in_nix_words_with_no_daemon(tmp_path: Path) -> None:
    sends = [*_REFUSED_DUMPS, ("f", "fixed:sha256")]
    there = await _refusals(tmp_path, "daemon", None, sends)
    here = await _refusals(tmp_path, "pynixd", False, sends)
    assert here == there, {"pynixd": here, "daemon": there}


async def test_a_text_dump_hashed_with_sha1_is_refused_where_the_daemon_aborts(tmp_path: Path) -> None:
    """NIX-DEFECT (#23): `makeFixedOutputPathFromCA` asserts SHA-256 (`store-dir-config.cc:137`).

    The daemon's worker aborts, and the client reads a closed connection.
    pynixd refuses in the words of the client's own check (`remote-store.cc:363`).
    """
    (there,) = await _refusals(tmp_path, "daemon", None, [("f", "text:sha1")])
    assert there.startswith("IncompleteReadError"), there
    for route, daemon in (("pynixd", False), ("pynixd", None)):
        assert await _refusals(tmp_path / f"{daemon}", route, daemon, [("f", "text:sha1")]) == [
            text_needs_sha256("f", "sha1")
        ]


_BLAKE3 = ("blake3-hashes",)


async def test_a_blake3_dump_adds_as_the_daemon_does(tmp_path: Path, added: list[str]) -> None:
    """With `blake3-hashes`, op 7 hashes a dump with BLAKE3, and op 39 imports a NAR with a BLAKE3 address."""
    tree, flat = _inputs(tmp_path)
    sends = [
        ("tree", "fixed:r:blake3", b"".join(dump_path(tree))),
        ("flat", "fixed:blake3", flat.read_bytes()),  # noqa: ASYNC240 -- test setup
    ]
    nix = str(NIX_BIN)
    answers, printed = {}, {}
    b, c = tmp_path / "b", tmp_path / "c"
    for root, route in ((b, "pynixd"), (c, "daemon")):
        socket = tmp_path / f"{route}.sock"
        async with server(root, socket, route, experimental_features=_BLAKE3):
            async with client(socket) as conn:
                answers[route] = [
                    (await _send(conn, name, cam, dump)).info.model_dump(exclude={"info": {"registration_time"}})
                    for name, cam, dump in sends
                ]
            store = f"unix://{socket}?root={root}"
            add = (nix, "store", "add", "--store", store, "--extra-experimental-features", "blake3-hashes")
            printed[route] = [
                await run(*add, "--mode", mode, "--hash-algo", "blake3", str(source))
                for mode, source in (("nar", tree / "bin"), ("flat", flat))
            ]
    assert len(added) == len(sends)
    assert answers["pynixd"] == answers["daemon"]
    assert printed["pynixd"] == printed["daemon"]
    for query in (VALID_PATHS, REFS):
        assert rows(b, query) == rows(c, query)
    paths = [p for (p, *_) in rows(b, VALID_PATHS)]
    for path in paths:
        assert files(b, path) == files(c, path), path


@pytest.mark.parametrize("daemon", [None, False])
async def test_a_blake3_dump_without_blake3_hashes_is_refused_in_nix_words(tmp_path: Path, daemon: bool | None) -> None:
    """`parseHashAlgoOpt` requires the feature (`hash.cc:475`). The daemon route reads a reset (nixidae/nixidae#1270)."""
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, "pynixd", daemon=daemon), client(socket) as conn:
        with pytest.raises(BackendError, match=re.escape(missing_feature("blake3-hashes"))):
            await _send(conn, "flat", "fixed:blake3", b"x")
        resp = await _send(conn, "good", "fixed:sha256", b"good")
        assert await valid(conn, str(resp.info.path))


async def _git_add(store: str, algo: str, source: Path) -> str:
    return await run(
        str(NIX_BIN), "store", "add", "--store", store, "--extra-experimental-features", "git-hashing",
        "--mode", "git", "--hash-algo", algo, str(source),
    )  # fmt: skip


async def test_a_git_dump_adds_as_the_daemon_does(tmp_path: Path, added: list[str]) -> None:
    """With `git-hashing`, op 7 hashes a Git dump's tree with `git::dumpHash` (`local-store.cc:1238`).

    `nix store add --mode git` sends op 39, whose import checks the address the same way (`local-store.cc:1113`).
    """
    tree, flat = _inputs(tmp_path)
    sends = [
        ("tree", "fixed:git:sha1", b"".join(dump_path(tree))),
        ("tree", "fixed:git:sha256", b"".join(dump_path(tree))),
        ("flat", "fixed:git:sha1", b"".join(dump_path(flat))),
    ]
    nix = str(NIX_BIN)
    answers, printed = {}, {}
    b, c = tmp_path / "b", tmp_path / "c"
    for root, route in ((b, "pynixd"), (c, "daemon")):
        socket = tmp_path / f"{route}.sock"
        async with server(root, socket, route, experimental_features=_GIT):
            async with client(socket) as conn:
                answers[route] = [
                    (await _send(conn, name, cam, dump)).info.model_dump(exclude={"info": {"registration_time"}})
                    for name, cam, dump in sends
                ]
            store = f"unix://{socket}?root={root}"
            printed[route] = [
                await _git_add(store, algo, source) for algo, source in (("sha1", tree / "bin"), ("sha256", flat))
            ]
    assert len(added) == len(sends)
    assert answers["pynixd"] == answers["daemon"]
    assert printed["pynixd"] == printed["daemon"]
    for query in (VALID_PATHS, REFS):
        assert rows(b, query) == rows(c, query)
    paths = [p for (p, *_) in rows(b, VALID_PATHS)]
    assert len(paths) == len(sends) + 2
    for path in paths:
        assert files(b, path) == files(c, path), path
    await run(
        nix, "store", "verify", "--store", str(b), "--extra-experimental-features", "git-hashing", "--no-trust", *paths
    )


@_ROUTES
async def test_a_git_dump_without_git_hashing_is_refused_in_nix_words(tmp_path: Path, route: str) -> None:
    """The daemon's `ContentAddressMethod::parse` requires the feature (`content-address.cc:17`); a client's is ignored."""
    tree, _ = _inputs(tmp_path)
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, route), client(socket) as conn:
        with pytest.raises(
            BackendError, match=re.escape(f"experimental Nix feature '{magenta('git-hashing')}' is disabled")
        ):
            await _send(conn, "tree", "fixed:git:sha1", b"".join(dump_path(tree)))
        resp = await _send(conn, "good", "fixed:sha256", b"good\n")
        assert await valid(conn, str(resp.info.path))


@_ROUTES
async def test_a_refused_upload_larger_than_a_socket_buffer_says_why(
    tmp_path_factory: pytest.TempPathFactory, route: str
) -> None:
    """The daemon refuses a git dump before it reads it, and closes; the client still reads why.

    Nix's `FramedSink` reads the daemon's answer before each frame it sends
    (`remote-store.cc:849`). pynixd read only after the whole upload: 16 MiB
    outgrew the socket buffer, the send broke on the closed socket, and the
    client read `RuntimeError: unable to perform operation on <UnixTransport closed=True ...>`.
    """
    # Not `tmp_path`: the parameter puts a `[` in its name, and Nix cannot parse that in a `unix://` URL.
    tmp_path = tmp_path_factory.mktemp("refused")
    big = tmp_path / "big"
    big.write_bytes(b"x" * (16 << 20))  # noqa: ASYNC240 -- test setup
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, route):
        result = await anyio.run_process(
            [
                *(str(NIX_BIN), "store", "add", "--store", f"unix://{socket}", "--mode", "git", "--hash-algo", "sha1"),
                *("--extra-experimental-features", "git-hashing nix-command", str(big)),
            ],
            check=False,
        )
    assert result.returncode == 1
    assert "experimental Nix feature 'git-hashing' is disabled" in result.stderr.decode()
