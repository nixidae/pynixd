"""Readiness for systemd's `Type=notify`: one datagram, and no dependency.

`Type=simple` calls the unit active when the process starts. pynixd binds
its listeners about two seconds later, and in `replace` mode the daemon
socket is one of them, so a unit ordered after pynixd found no daemon.
Measured in the daemon guest session: active at 8.0s, first log line at
9.8s, and `prepare` saw no listener on the socket.
"""

from __future__ import annotations

import os
import socket


def notify(state: str) -> bool:
    """Send `state` to the service manager, if one asked for it.

    Returns whether anything was sent. Outside systemd, or under a unit that
    is not `Type=notify`, `NOTIFY_SOCKET` is unset and this does nothing.
    """
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return False
    # An abstract socket is written with a leading `@`.
    target = "\0" + address[1:] if address.startswith("@") else address
    # No SOCK_CLOEXEC: darwin has none, and Python's sockets are non-inheritable already (PEP 446).
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
        sock.connect(target)
        sock.sendall(state.encode())
    return True
