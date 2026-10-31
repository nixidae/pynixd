"""A Unix link and the raw stream at its other end, for the wire classes that read and write a socket."""

from __future__ import annotations

import contextlib
import socket
from typing import TYPE_CHECKING

from anyio.abc import UNIXSocketStream

from pynixd.unix_socket import UnixLink, link_socket

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@contextlib.asynccontextmanager
async def socket_pair() -> AsyncIterator[tuple[UnixLink, UNIXSocketStream]]:
    ours, theirs = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    link = await link_socket(ours)
    peer = await UNIXSocketStream.from_socket(theirs)
    try:
        yield link, peer
    finally:
        await link.stream.aclose()
        await peer.aclose()


@contextlib.asynccontextmanager
async def fed(data: bytes) -> AsyncIterator[UnixLink]:
    """A link that holds *data* and then its end, as a peer that sent it and closed."""
    async with socket_pair() as (ours, theirs):
        await theirs.send(data)
        await theirs.send_eof()
        yield ours
