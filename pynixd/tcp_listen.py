"""TCP listening sockets that pynixd binds itself, so a hand-over can pass them on.

asyncssh and aiohttp each bind from a host and a port and hide the sockets. Both also take a socket that is
already listening (`sock=`, `SockSite`), so pynixd binds as asyncio's `create_server` does and gives them that.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Final

import structlog

log = structlog.get_logger(__name__)

BACKLOG: Final = 100
"""asyncio's default for `create_server`."""


@dataclass(frozen=True, slots=True)
class _Address:
    family: socket.AddressFamily
    host: str
    sockaddr: tuple[str, int] | tuple[str, int, int, int]


def _addresses(host: str, port: int) -> list[_Address]:
    # `""` is every interface, one socket per family, as asyncio reads it (`base_events.py`, `create_server`).
    infos = socket.getaddrinfo(host or None, port, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
    addresses: dict[tuple[socket.AddressFamily, str], _Address] = {}
    for family, _type, _proto, _name, sockaddr in infos:
        if family == socket.AF_INET:
            address = _Address(family, str(sockaddr[0]), (str(sockaddr[0]), port))
        elif family == socket.AF_INET6 and len(sockaddr) == 4:
            address = _Address(family, str(sockaddr[0]), (str(sockaddr[0]), port, int(sockaddr[2]), int(sockaddr[3])))
        else:
            continue
        addresses.setdefault((family, address.host), address)
    return list(addresses.values())


def bind_tcp(host: str, port: int) -> list[socket.socket]:
    """Listen on *host* and *port*: each address of *host*, `SO_REUSEADDR`, and IPv6 sockets v6 only."""
    sockets: list[socket.socket] = []
    try:
        for address in _addresses(host, port):
            sock = socket.socket(address.family, socket.SOCK_STREAM)
            sockets.append(sock)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if address.family == socket.AF_INET6:
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            sock.bind(address.sockaddr)
            sock.listen(BACKLOG)
            sock.setblocking(False)
    except OSError:
        for sock in sockets:
            sock.close()
        raise
    return sockets


def adopt_tcp(handed: list[socket.socket], host: str, port: int) -> list[socket.socket]:
    """*handed*, when they listen where the settings say; else close them and bind anew.

    A hand-over carries the old settings' sockets. A changed host or port binds the new ones, which fails while
    the old process still holds the same port under another address.
    """
    if handed and port != 0:
        wanted = {(address.family, address.host, port) for address in _addresses(host, port)}
        have = {(sock.family, *sock.getsockname()[:2]) for sock in handed}
        if have == wanted:
            for sock in handed:
                sock.setblocking(False)
            return handed
    for sock in handed:
        log.info("handover_listener_rebound", address=sock.getsockname())
        sock.close()
    return bind_tcp(host, port)
