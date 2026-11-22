"""A VictoriaTraces server for the tracing tests (nixidae/nixidae#1570).

One server per test, on a free port: the functional suite runs workers at once, and a shared server would mix
their spans. The server is a child of the test, and it ends with it.
"""

from __future__ import annotations

import contextlib
import os
import subprocess
from typing import TYPE_CHECKING

import anyio
import httpx
from anyio.abc import SocketAttribute

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from anyio.abc import Process

_START_SECONDS = 30.0
_STOP_SECONDS = 10.0


def _binary() -> str:
    """`VICTORIATRACES_BIN`, which the dev shell and the guest set; a missing server fails, never skips."""
    binary = os.environ.get("VICTORIATRACES_BIN")
    if binary is None:
        raise RuntimeError("VICTORIATRACES_BIN is unset: the dev shell and the test guests set it")
    return binary


async def _free_port() -> int:
    async with await anyio.create_tcp_listener(local_host="127.0.0.1", local_port=0) as listener:
        return int(listener.extra(SocketAttribute.local_port))


async def _ready(url: str, server: Process) -> None:
    async with httpx.AsyncClient(base_url=url, timeout=2) as client:
        # The server sends no notice when it listens, so this asks until it answers.
        with anyio.fail_after(_START_SECONDS):
            while True:
                if server.returncode is not None:
                    raise RuntimeError(f"victoria-traces exited with {server.returncode}")
                with contextlib.suppress(httpx.TransportError):  # not listening yet
                    if (await client.get("/health")).is_success:
                        return
                await anyio.sleep(0.05)


@contextlib.asynccontextmanager
async def victoriatraces(data: Path) -> AsyncIterator[str]:
    """A running server; yields its base URL. Its log is `victoria-traces.log` in *data*."""
    port = await _free_port()
    log = await anyio.open_file(data / "victoria-traces.log", "wb")
    async with log:
        server = await anyio.open_process(
            [
                _binary(),
                f"-storageDataPath={data / 'storage'}",
                f"-httpListenAddr=127.0.0.1:{port}",
                # A span is searchable at once, not 30 s after it arrived (nixpkgs' `otlp-ingestion` test sets it too).
                "-search.latencyOffset=0s",
            ],
            stdin=subprocess.DEVNULL,
            stdout=log.wrapped,
            stderr=subprocess.STDOUT,
        )
    url = f"http://127.0.0.1:{port}"
    try:
        await _ready(url, server)
        yield url
    finally:
        with anyio.CancelScope(shield=True):
            server.terminate()
            with anyio.move_on_after(_STOP_SECONDS):
                await server.wait()
            if server.returncode is None:
                server.kill()
                await server.wait()
