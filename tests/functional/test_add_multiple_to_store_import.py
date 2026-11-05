"""pynixd imports AddMultipleToStore's paths on its `pynixd` route as the loop of `daemon.cc:505` does.

`nix copy` sends op 44 (`remote-store.cc:508`): store A goes into store B on
the `pynixd` route and store C on the daemon route, and rows and files must
agree. The paths share one framed stream, so a raw client tests what lines
them up: frames that hold the end of one NAR and the start of the next path's
info, and a valid path whose NAR is skipped unread (`EnsureRead`). Each
refusal runs on both routes; the daemon's answer is the reference.
"""

from __future__ import annotations

import re
import struct
from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.add_multiple_to_store import AddMultipleToStoreRequest, AddMultipleToStoreResponse
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from pynixd.exceptions import BackendError
from pynixd.serde.context import ReadContext, WriteContext
from pynixd_store.engine import StoreEngine
from pynixd_store.hintfmt import magenta
from tests._conftest.import_lab import (
    IA,
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
from tests.conftest import NIX_BIN, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from pynixd.connection import Connection

_ROUTES = pytest.mark.parametrize("route", ["pynixd", "daemon"])


@pytest.fixture
def imported(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The paths pynixd's engine imported for op 44."""
    seen: list[str] = []
    add_to_store = StoreEngine.add_to_store

    async def counting(engine: StoreEngine, info: ValidPathInfo, *args: Any, **kwargs: Any) -> None:
        seen.append(str(info.path))
        await add_to_store(engine, info, *args, **kwargs)

    monkeypatch.setattr(StoreEngine, "add_to_store", counting)
    return seen


async def test_nix_copy_imports_as_the_daemon_does(tmp_path: Path, imported: list[str]) -> None:
    a, order, _ = await lab(tmp_path)
    assert any(p.endswith("-structured.drv") for p in order)
    b, c = tmp_path / "b", tmp_path / "c"
    nix = str(NIX_BIN)
    for root, route in ((b, "pynixd"), (c, "daemon")):
        socket = tmp_path / f"{route}.sock"
        async with server(root, socket, route):
            await run(nix, "copy", "--from", str(a), "--to", f"unix://{socket}?root={root}", "--no-check-sigs", *order)
        if route == "pynixd":
            assert sorted(imported) == sorted(order), "pynixd imported each path, none went to the daemon"
            imported.clear()
    assert imported == []

    for query in (VALID_PATHS, REFS, OUTPUTS):
        assert rows(b, query) == rows(c, query)
    assert len(rows(b, VALID_PATHS)) == len(order)
    for path in order:
        assert files(b, path) == files(c, path), path
        assert not (real(b, path).parent / f"{real(b, path).name}.lock").exists()
    assert not list((b / "nix" / "store").glob("tmp-*"))
    await run(nix, "store", "verify", "--store", str(b), "--no-trust", *order)


def _corrupt(root: Path, flat: str, tree: str) -> None:
    """A changed file and a changed file under a directory, as a disk or a hand edit leaves them."""
    target = real(root, flat)
    target.chmod(0o644)
    target.write_bytes(b"corrupt\n")
    hello = real(root, tree) / "bin" / "hello"
    hello.parent.chmod(0o755)
    hello.chmod(0o755)
    hello.write_bytes(b"#!/bin/sh\necho corrupt\n")


async def test_a_repair_restores_a_corrupted_path_as_the_daemon_does(tmp_path: Path, imported: list[str]) -> None:
    """Op 44 with repair: `addToStore` restores a valid path (`local-store.cc:1054`).

    A raw client sends it: `nix copy --repair` drops a valid path before it sends (`store-api.cc:1018`).
    """
    a, order, _ = await lab(tmp_path)
    flat = next(p for p in order if p.endswith("-flat"))
    tree = next(p for p in order if p.endswith("-tree"))
    b, c = tmp_path / "b", tmp_path / "c"
    nix = str(NIX_BIN)
    for root, route in ((b, "pynixd"), (c, "daemon")):
        socket = tmp_path / f"{route}.sock"
        url = f"unix://{socket}?root={root}"
        async with server(root, socket, route):
            await run(nix, "copy", "--from", str(a), "--to", url, "--no-check-sigs", *order)
            _corrupt(root, flat, tree)
            imported.clear()
            async with client(socket) as conn:
                await _send(conn, _items(a, [flat, tree]), repair=1)
        if route == "pynixd":
            assert sorted(imported) == sorted([flat, tree]), "pynixd repaired each path, none went to the daemon"
    assert imported == []

    for query in (VALID_PATHS, REFS, OUTPUTS):
        assert rows(b, query) == rows(c, query)
    for path in (flat, tree):
        assert files(b, path) == files(c, path) == files(a, path), path
    assert not list((b / "nix" / "store").glob("tmp-*"))
    await run_subproc([nix, "store", "verify", "--store", str(b), "--no-trust", flat, tree])


async def _send(
    conn: Connection,
    items: list[tuple[ValidPathInfo, bytes]],
    *,
    frame: int = 0,
    dont_check_sigs: int = 0,
    repair: int = 0,
) -> None:
    """One AddMultipleToStore, in frames of *frame* bytes (0: one frame); raises `BackendError`."""
    payload = struct.pack("<Q", len(items))
    for info, nar in items:
        payload += await info.bytes_wire() + nar
    await AddMultipleToStoreRequest(repair=repair, dont_check_sigs=dont_check_sigs).to_writer(
        WriteContext.from_conn(conn)
    )
    framed = conn.w.framed()
    step = frame or len(payload)
    for at in range(0, len(payload), step):
        framed.write(payload[at : at + step])
    await framed.finalize()
    await AddMultipleToStoreResponse.from_reader(ReadContext.from_conn(conn, raise_on_error=True))


def _items(a: Path, paths: list[str]) -> list[tuple[ValidPathInfo, bytes]]:
    return [(info_of(a, p), nar_of(a, p)) for p in paths]


@pytest.mark.parametrize("frame", [0, 7], ids=["one-frame", "7-byte-frames"])
async def test_paths_that_share_frames_are_each_imported(tmp_path: Path, imported: list[str], frame: int) -> None:
    a, order, _ = await lab(tmp_path)
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, "pynixd"), client(socket) as conn:
        await _send(conn, _items(a, order), frame=frame)
        for path in order:
            assert await valid(conn, path), path
    assert imported == order
    await run(str(NIX_BIN), "store", "verify", "--store", str(b), "--no-trust", *order)


async def test_a_valid_path_is_skipped_unread(tmp_path: Path, imported: list[str]) -> None:
    a, order, _ = await lab(tmp_path)
    flat = next(p for p in order if p.endswith("-flat"))
    tree = next(p for p in order if p.endswith("-tree"))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, "pynixd"), client(socket) as conn:
        await _send(conn, _items(a, [flat]))
        await _send(conn, _items(a, [flat, tree]))
        assert await valid(conn, tree)
    assert imported == [flat, flat, tree]


@_ROUTES
async def test_an_error_ends_the_op_and_keeps_the_paths_before_it(tmp_path: Path, route: str) -> None:
    a, order, _ = await lab(tmp_path)
    flat = next(p for p in order if p.endswith("-flat"))
    tree = next(p for p in order if p.endswith("-tree"))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    items = [*_items(a, [flat]), (info_of(a, tree, nar_hash=NARHash(hash="0" * 64)), nar_of(a, tree)), *_items(a, [IA])]
    async with server(b, socket, route), client(socket) as conn:
        with pytest.raises(BackendError, match=re.escape(f"hash mismatch importing path '{magenta(tree)}';\n")):
            await _send(conn, items)
        assert await valid(conn, flat)
        assert not await valid(conn, tree)
        assert not await valid(conn, IA)
        await _send(conn, _items(a, [tree, IA]))
        assert await valid(conn, IA)


@_ROUTES
async def test_a_reference_must_come_first(tmp_path: Path, route: str) -> None:
    """`registerValidPaths` reads each reference's id (`local-store.cc:986`)."""
    a, order, _ = await lab(tmp_path)
    tree = next(p for p in order if p.endswith("-tree"))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, route), client(socket) as conn:
        with pytest.raises(BackendError, match=re.escape(f"path '{magenta(tree)}' is not valid")):
            await _send(conn, _items(a, [IA, tree]))
        assert not await valid(conn, IA)
        assert not await valid(conn, tree)


@_ROUTES
async def test_an_unsigned_path_is_refused_unless_the_client_says_not_to_check(tmp_path: Path, route: str) -> None:
    a, order, _ = await lab(tmp_path)
    tree = next(p for p in order if p.endswith("-tree"))
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, route, require_sigs=True, trusted_public_keys=()), client(socket) as conn:
        with pytest.raises(
            BackendError,
            match=re.escape(f"cannot add path '{magenta(IA)}' because it lacks a signature by a trusted key"),
        ):
            await _send(conn, _items(a, [tree, IA]))
        # Content-addressed: no key needed, and it came before the refusal.
        assert await valid(conn, tree)
        assert not await valid(conn, IA)
        await _send(conn, _items(a, [IA]), dont_check_sigs=1)
        assert await valid(conn, IA)


async def _git_lab(tmp_path: Path) -> tuple[Path, list[str]]:
    """Store A, and a flat path, a Git-addressed tree and the plain tree, in that order."""
    a, order, _ = await lab(tmp_path)
    git = await run(
        str(NIX_BIN), "store", "add", "--store", str(a), "--extra-experimental-features", "git-hashing",
        "--mode", "git", "--hash-algo", "sha1", str(tmp_path / "tree"),
    )  # fmt: skip
    assert str(rows(a, f"select ca from ValidPaths where path = '{git}'")[0][0]).startswith("fixed:git:")
    flat = next(p for p in order if p.endswith("-flat"))
    tree = next(p for p in order if p.endswith("-tree"))
    return a, [flat, git, tree]


async def test_a_git_address_is_imported_as_the_daemon_does(tmp_path: Path, imported: list[str]) -> None:
    """With `git-hashing`, `addToStore` checks a Git address with `git::dumpHash` (`local-store.cc:1113`)."""
    a, paths = await _git_lab(tmp_path)
    b, c = tmp_path / "b", tmp_path / "c"
    for root, route in ((b, "pynixd"), (c, "daemon")):
        socket = tmp_path / f"{route}.sock"
        async with server(root, socket, route, experimental_features=("git-hashing",)), client(socket) as conn:
            await _send(conn, _items(a, paths))
        if route == "pynixd":
            assert imported == paths, "pynixd imported each path, none went to the daemon"
            imported.clear()
    assert imported == []
    for query in (VALID_PATHS, REFS):
        assert rows(b, query) == rows(c, query)
    for path in paths:
        assert files(b, path) == files(c, path) == files(a, path), path


@_ROUTES
async def test_a_wrong_git_address_is_refused_as_the_daemon_refuses_it(tmp_path: Path, route: str) -> None:
    a, [flat, git, _] = await _git_lab(tmp_path)
    wrong = ContentAddress(value="fixed:git:sha1:" + "0" * 32)
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, route, experimental_features=("git-hashing",)), client(socket) as conn:
        with pytest.raises(BackendError) as refused:
            await _send(conn, [*_items(a, [flat]), (info_of(a, git, ca=wrong), nar_of(a, git))])
        assert await valid(conn, flat)
        assert not await valid(conn, git)
    assert str(refused.value).startswith(f"ca hash mismatch importing path '{magenta(git)}';\n  specified: ")


@_ROUTES
async def test_a_git_address_without_git_hashing_is_refused(tmp_path: Path, route: str) -> None:
    """pynixd's route sends the Git path to the daemon, which refuses it in Nix's words."""
    a, [flat, git, tree] = await _git_lab(tmp_path)
    b, socket = tmp_path / "b", tmp_path / "b.sock"
    async with server(b, socket, route), client(socket) as conn:
        with pytest.raises(
            BackendError, match=re.escape(f"experimental Nix feature '{magenta('git-hashing')}' is disabled")
        ):
            await _send(conn, _items(a, [flat, git, tree]))
        assert await valid(conn, flat)
        assert not await valid(conn, git)
