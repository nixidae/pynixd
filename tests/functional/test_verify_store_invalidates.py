"""`nix-store --verify` against pynixd invalidates a path the store lost (op 35).

VerifyStore (op 35) runs `LocalStore::verifyStore`. On the `pynixd` route the
engine walks the database against the store directory: a valid path whose files
are gone, with no valid referrer, is removed from the database. This adds a
path, deletes its files, verifies, and checks the path is no longer valid.
`rc == 0` alone proves nothing about the engine, so this reads validity.
"""

from __future__ import annotations

import os
import shutil
import stat
from typing import TYPE_CHECKING

import structlog

from tests.conftest import CLIENT_BIN, run_subproc, unix_session_uri

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd import Server

log = structlog.get_logger(__name__)

CONTENT = f"verify-probe-{os.getpid()}\n".encode()


def _remove_readonly_tree(root: Path) -> None:
    """A store path is read-only; make every node writable, then remove it."""
    for dirpath, _dirnames, filenames in os.walk(root):
        os.chmod(dirpath, 0o755)
        for name in filenames:
            path = os.path.join(dirpath, name)
            if not stat.S_ISLNK(os.lstat(path).st_mode):
                os.chmod(path, 0o644)
    shutil.rmtree(root)


async def _add_path(uri: str, directory: Path) -> str:
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


async def test_verify_invalidates_a_disappeared_path(pynixd_server: Server, tmp_path: Path) -> None:
    uri = unix_session_uri(pynixd_server)

    # A sentinel stays on disk: verify walks the whole store, so it proves the
    # walk invalidates only the path that is gone, not every path.
    sentinel_dir = tmp_path / f"keep-{os.getpid()}"
    sentinel_dir.mkdir()
    (sentinel_dir / "foo").write_bytes(b"keep\n")
    sentinel = await _add_path(uri, sentinel_dir)

    directory = tmp_path / f"pkg-{os.getpid()}"
    directory.mkdir()
    (directory / "foo").write_bytes(CONTENT)
    path = await _add_path(uri, directory)
    assert await _is_valid(uri, path), "the added path should start valid"

    store_root = pynixd_server.local_store.daemon_client.store_path
    on_disk = store_root / "nix" / "store" / path.rsplit("/", 1)[-1]
    _remove_readonly_tree(on_disk)

    rc, _stdout, _stderr, both = await run_subproc(
        [str(CLIENT_BIN.parent / "nix-store"), "--verify", "--store", uri],
    )
    assert rc == 0, f"verify failed:\n{both}"

    # The engine removed the row for the path whose files disappeared, and kept
    # the sentinel, whose files are still there.
    assert not await _is_valid(uri, path), "the disappeared path should no longer be valid"
    assert await _is_valid(uri, sentinel), "a path still on disk must stay valid"
