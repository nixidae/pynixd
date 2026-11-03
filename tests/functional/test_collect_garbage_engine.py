"""`nix store gc` against pynixd deletes what no root keeps (op 20).

CollectGarbage (op 20) runs `LocalStore::collectGarbage`. On the `pynixd` route
the engine takes the big lock, walks from each store path through its
referrers, and deletes a closure no root reaches. This adds two paths, roots
one, collects, and reads validity and the disk back: `rc == 0` alone proves
nothing about the engine.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from tests.conftest import CLIENT_BIN, run_subproc, unix_session_uri

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd import Server


async def _add_path(uri: str, directory: Path, content: bytes) -> str:
    directory.mkdir()
    (directory / "foo").write_bytes(content)
    rc, out, _stderr, both = await run_subproc(
        [str(CLIENT_BIN), "store", "add-path", "--store", uri, str(directory)],
    )
    assert rc == 0, f"add-path failed:\n{both}"
    return out.strip().splitlines()[-1]


async def _is_valid(uri: str, path: str) -> bool:
    rc, _out, _stderr, _both = await run_subproc(
        [str(CLIENT_BIN), "path-info", "--store", uri, path],
        expected_retcode=None,
    )
    return rc == 0


async def test_gc_deletes_what_no_root_keeps(pynixd_server: Server, tmp_path: Path) -> None:
    uri = unix_session_uri(pynixd_server)
    kept = await _add_path(uri, tmp_path / f"kept-{os.getpid()}", b"kept\n")
    dead = await _add_path(uri, tmp_path / f"dead-{os.getpid()}", b"dead\n")
    rc, _out, _stderr, both = await run_subproc(
        [str(CLIENT_BIN.parent / "nix-store"), "--store", uri, "--add-root", str(tmp_path / "root"), "-r", kept],
    )
    assert rc == 0, f"add-root failed:\n{both}"

    rc, _out, _stderr, both = await run_subproc([str(CLIENT_BIN), "store", "gc", "--store", uri])
    assert rc == 0, f"gc failed:\n{both}"

    real_store = pynixd_server.local_store.daemon_client.store_path / "nix" / "store"
    assert f"deleting '{dead}'" in both
    assert not await _is_valid(uri, dead)
    assert not (real_store / dead.rsplit("/", 1)[-1]).exists()
    assert await _is_valid(uri, kept)
    assert (real_store / kept.rsplit("/", 1)[-1]).exists()


async def test_delete_refuses_a_path_a_root_keeps(pynixd_server: Server, tmp_path: Path) -> None:
    """`gc.cc:784`: a named path that is still alive fails the request, and nothing goes."""
    uri = unix_session_uri(pynixd_server)
    kept = await _add_path(uri, tmp_path / f"kept-{os.getpid()}", b"kept\n")
    rc, _out, _stderr, both = await run_subproc(
        [str(CLIENT_BIN.parent / "nix-store"), "--store", uri, "--add-root", str(tmp_path / "root"), "-r", kept],
    )
    assert rc == 0, f"add-root failed:\n{both}"

    rc, _out, _stderr, both = await run_subproc(
        [str(CLIENT_BIN), "store", "delete", "--store", uri, kept], expected_retcode=None
    )

    assert rc != 0
    assert "since it is still alive" in both
    assert await _is_valid(uri, kept)
