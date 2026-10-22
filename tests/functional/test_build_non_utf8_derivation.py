"""A derivation whose strings are not UTF-8 builds through pynixd, as through Nix.

Nix treats derivation strings as bytes (`derivations.cc:224-275`, `:533-561`).
Measured on dynhetz: `weird = "a\\xffb\\xc3(";` builds through the stock
nix-daemon and the output holds those five bytes. Through pynixd the request
failed with "UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff", from
`read_derivation`. nixidae/nixidae#1010.

Raw subprocess pipes, as in `test_build_non_utf8_log.py`: `run_subproc`
decodes strict UTF-8.
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

from tests.conftest import CLIENT_BIN, DEFAULT_SSH_OPTS, server_uri

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd import Server

WEIRD = b"a\xffb\xc3("
REPRODUCER = (
    b"derivation {\n"
    b'  name = "non-utf8";\n'
    b"  system = builtins.currentSystem;\n"
    b'  builder = "/bin/sh";\n'
    b'  args = [ "-c" "printf %s \\"$weird\\" > $out" ];\n'
    b'  weird = "' + WEIRD + b'";\n'
    b"}\n"
)


async def _nix(*args: str, store: Path) -> tuple[int, bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        str(CLIENT_BIN),
        *args,
        env=os.environ.copy() | {"NIX_STATE_DIR": str(store / "var/nix"), "NIX_SSHOPTS": DEFAULT_SSH_OPTS},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode or 0, stdout, stderr


async def test_pynixd_as_the_store_builds_it(pynixd_server: Server, tmp_path: Path) -> None:
    """pynixd reads the `.drv` itself here: the road that raised."""
    uri = server_uri(pynixd_server)
    nix_file = tmp_path / "non-utf8.nix"
    nix_file.write_bytes(REPRODUCER)

    rc, stdout, stderr = await _nix(
        "build", "--store", uri, "--file", str(nix_file), "--no-link", "--print-out-paths", store=tmp_path
    )
    assert rc == 0, stderr.decode("utf-8", errors="replace")

    rc, content, stderr = await _nix("store", "cat", "--store", uri, stdout.strip().decode(), store=tmp_path)
    assert rc == 0, stderr.decode("utf-8", errors="replace")
    assert content == WEIRD


async def test_pynixd_as_a_builder_builds_it(pynixd_server: Server, tmp_path: Path) -> None:
    """`BuildDerivation` carries the env value on the wire."""
    store = tmp_path / "client"
    store.mkdir()
    nix_file = tmp_path / "non-utf8.nix"
    nix_file.write_bytes(REPRODUCER)

    rc, stdout, stderr = await _nix(
        "build",
        "--store",
        str(store),
        "--builders",
        f"{server_uri(pynixd_server)} x86_64-linux",
        "--max-jobs",
        "0",
        "--file",
        str(nix_file),
        "--no-link",
        "--print-out-paths",
        store=store,
    )
    assert rc == 0, stderr.decode("utf-8", errors="replace")

    rc, content, stderr = await _nix("store", "cat", "--store", str(store), stdout.strip().decode(), store=store)
    assert rc == 0, stderr.decode("utf-8", errors="replace")
    assert content == WEIRD
