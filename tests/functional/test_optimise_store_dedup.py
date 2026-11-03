"""`nix store optimise` against pynixd deduplicates store files through op 34.

OptimiseStore (op 34) runs `LocalStore::optimiseStore`. On the `pynixd` route
the engine hard-links identical files into `.links`. This adds two store paths
whose inner file has the same content, optimises, and checks the two files
share one inode (the proof of a hard link) and that `.links` holds the entry.
`rc == 0` alone proves nothing about the engine, so this reads the inodes.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import structlog

from tests.conftest import CLIENT_BIN, run_subproc, unix_session_uri

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd import Server

log = structlog.get_logger(__name__)

CONTENT = f"optimise-probe-{os.getpid()}\n".encode()


async def _add_path(uri: str, directory: Path) -> str:
    rc, out, _stderr, both = await run_subproc(
        [str(CLIENT_BIN), "store", "add-path", "--store", uri, str(directory)],
    )
    assert rc == 0, f"add-path failed:\n{both}"
    return out.strip().splitlines()[-1]


async def test_optimise_deduplicates_a_shared_file(pynixd_server: Server, tmp_path: Path) -> None:
    uri = unix_session_uri(pynixd_server)

    # Two directories with different names but the same inner file. They become
    # two store paths whose `foo` holds identical content.
    paths = []
    for name in (f"pkg1-{os.getpid()}", f"pkg2-{os.getpid()}"):
        directory = tmp_path / name
        directory.mkdir()
        (directory / "foo").write_bytes(CONTENT)
        paths.append(await _add_path(uri, directory))

    rc, _stdout, _stderr, both = await run_subproc(
        [str(CLIENT_BIN), "store", "optimise", "--store", uri],
    )
    assert rc == 0, f"optimise failed:\n{both}"

    store_root = pynixd_server.local_store.daemon_client.store_path
    store_dir = store_root / "nix" / "store"
    first, second = (store_dir / path.rsplit("/", 1)[-1] / "foo" for path in paths)

    assert first.stat().st_ino == second.stat().st_ino, "the shared file was not hard-linked"
    # Three names for one inode: the two store files and the `.links` entry that
    # `optimiseStore` made. A direct scan of `.links` races a shared store's GC.
    assert first.stat().st_nlink >= 3, "the two files and the `.links` entry should share the inode"
