"""Tests for copying store paths between stores."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tests._conftest.constants import HELLO
from tests.conftest import (
    CLIENT_BIN,
    run_subproc,
    server_uri,
)

if TYPE_CHECKING:
    from pynixd import Server


async def test_copy(pynixd_server: Server) -> None:
    """Copy paths between two stores via UDS.

    Store operations triggered:
    - AddMultipleToStore: Adds multiple paths to store
    - QueryValidPaths: Queries valid paths
    - RegisterDrvOutput: Registers derivation output
    """

    await run_subproc([CLIENT_BIN, "build", *HELLO, "--no-link"])
    await run_subproc(
        [
            CLIENT_BIN,
            "copy",
            "--from",
            "daemon",
            "--to",
            server_uri(pynixd_server),
            *HELLO,
        ],
    )
