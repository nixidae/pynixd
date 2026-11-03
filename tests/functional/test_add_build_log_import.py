"""`nix store copy-log` into pynixd records the build log through op 45.

copy-log reads a derivation's log from one store and calls `addBuildLog` on
the other (`store-copy-log.cc:32`). Against pynixd that is AddBuildLog (op
45). On the `pynixd` route the engine writes the log; this reads the `.bz2`
back from the store and checks it is the source log.
"""

from __future__ import annotations

import bz2
import os
from typing import TYPE_CHECKING

import structlog

from tests.conftest import CLIENT_BIN, run_subproc, unix_session_uri

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd import Server

log = structlog.get_logger(__name__)

MARKER = f"build-log-probe-{os.getpid()}"
DERIVATION = (
    "derivation { "
    'name = "build-log-probe"; '
    "system = builtins.currentSystem; "
    'builder = "/bin/sh"; '
    f'args = [ "-c" "echo {MARKER}; echo x > $out" ]; '
    "}"
)


async def test_copy_log_into_pynixd_records_the_build_log(pynixd_server: Server, tmp_path: Path) -> None:
    source = f"local?root={tmp_path / 'src'}"

    rc, out, _stderr, both = await run_subproc(
        [
            str(CLIENT_BIN),
            "build",
            "--impure",
            "--store",
            source,
            "--expr",
            DERIVATION,
            "--no-link",
            "--print-out-paths",
        ],
    )
    assert rc == 0, f"source build failed:\n{both}"
    out_path = out.strip().splitlines()[-1]

    # The source daemon wrote the log during the build; read its `.bz2` whole.
    source_logs = sorted((tmp_path / "src" / "nix" / "var" / "log" / "nix" / "drvs").glob("*/*.bz2"))
    assert len(source_logs) == 1, f"expected one build log under the source store, found {source_logs}"
    source_bytes = bz2.decompress(source_logs[0].read_bytes())
    assert MARKER.encode() in source_bytes, f"the source log misses the marker: {source_bytes!r}"

    uri = unix_session_uri(pynixd_server)
    rc, _stdout, _stderr, both = await run_subproc(
        [str(CLIENT_BIN), "store", "copy-log", "--from", source, "--to", uri, out_path],
    )
    assert rc == 0, f"copy-log into pynixd failed:\n{both}"

    # op 45 wrote pynixd's own `.bz2`. The session store is shared and already
    # holds other logs, so find this one by its content. Compare the content,
    # not the compressed bytes: Python's bzip2 and Nix's libarchive may frame a
    # stream differently.
    store_root = pynixd_server.local_store.daemon_client.store_path
    recorded = [
        bz2.decompress(path.read_bytes())
        for path in (store_root / "nix" / "var" / "log" / "nix" / "drvs").glob("*/*.bz2")
    ]
    assert source_bytes in recorded, f"op 45 did not record the build log; marker {MARKER} absent"
