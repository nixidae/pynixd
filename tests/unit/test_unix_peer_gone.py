"""A client that closes before pynixd takes its connection is no error.

darwin forgets the peer of an accepted Unix socket once the peer closes:
`getpeername` fails with EINVAL, and anyio's `SocketStream.from_socket`
raised `ValueError: the socket must be connected`. pynixd logged
`unix_client_accept_failed` with a traceback when `pynixd-install` probed
the socket on a macOS runner. Linux keeps the peer, so there the link
opens and reads end-of-file.
"""

from __future__ import annotations

import socket
import sys
import tempfile
from pathlib import Path

import anyio
import pytest

from pynixd.unix_socket import PeerGoneError, as_link


async def test_a_peer_that_closed_first_is_gone_or_reads_end_of_file() -> None:
    # Under the real /tmp: `sun_path` holds 104 bytes on darwin, and pytest's tmp_path is longer.
    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        path = Path(directory) / "s"
        async with await anyio.create_unix_listener(path) as listener:
            with socket.socket(socket.AF_UNIX) as client:
                client.connect(str(path))
            accepted = await listener.accept()
            if sys.platform == "darwin":
                with pytest.raises(PeerGoneError):
                    await as_link(accepted)
                return
            link = await as_link(accepted)
            try:
                assert await link.stream.receive() == b""
            except anyio.EndOfStream:
                pass
            finally:
                await link.stream.aclose()
