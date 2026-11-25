"""The zygote: `python -m pynixd_store.builder.zygote REQUESTS_FD PARENT_PID`, which forks a sandbox helper per build.

The helper's imports took 564 ms of CPU in a test guest, against 5 ms for a fork of a process that holds them
(nixidae/nixidae#1590). pynixd runs threads and never forks itself; this process is single-threaded and runs no event
loop, so its fork is safe. It runs on `selectors`: a forked child starts its own anyio loop.

`helpers.py` holds pynixd's end. A request is a JSON line on *REQUESTS_FD* that carries the build's descriptors. The
answer goes on the build's own socket: the helper's pid with a pidfd, then its wait status. pynixd signals the helper
through the pidfd, which no later process can take over.

The helper's parent is this process, and this process' parent is pynixd. Each dies with its parent
(`PR_SET_PDEATHSIG`), so a killed pynixd leaves no build behind.
"""

from __future__ import annotations

import contextlib
import gc
import json
import os
import selectors
import signal
import socket
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final, NoReturn

from ..add import delete_path
from . import child

if TYPE_CHECKING:
    from .spec import ForkRequest

REQUEST_LIMIT: Final = 1 << 16
FDS_LIMIT: Final = 4


@dataclass(frozen=True, slots=True)
class _Running:
    """A helper this process has forked and not reaped."""

    pid: int
    pidfd: int
    answers: socket.socket


def _close_all_but(keep: set[int]) -> None:
    for name in os.listdir(child.FD_DIR):
        fd = int(name)
        if fd not in keep:
            with contextlib.suppress(OSError):
                os.close(fd)


def _become_helper(spec_file: str, zygote: int, terminal: int, status_fd: int, control_fd: int | None) -> NoReturn:
    """Run the helper in this forked process, as `python -m pynixd_store.builder.child` does, and never return."""
    try:
        # An ignored signal stays ignored across the builder's exec.
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        os.dup2(terminal, 1)
        os.dup2(terminal, 2)
        # The other builds' descriptors are the zygote's alone.
        _close_all_but({0, 1, 2, status_fd} | ({control_fd} if control_fd is not None else set()))
        control = None if control_fd is None else socket.socket(fileno=control_fd)
        child.run(spec_file, status_fd, control, parent=zygote)
    except BaseException:  # noqa: BLE001 -- nothing above this frame may run in the fork, SystemExit included
        os.write(2, traceback.format_exc().encode(errors="surrogateescape"))
    os._exit(1)


def _remove(paths: list[str], zygote: int) -> NoReturn:
    """Delete *paths* in this forked process, and never return."""
    try:
        child.die_with(zygote)
        for path in paths:
            delete_path(Path(path))
    except BaseException:  # noqa: BLE001 -- nothing above this frame may run in the fork, SystemExit included
        os.write(2, traceback.format_exc().encode(errors="surrogateescape"))
        os._exit(1)
    os._exit(0)


def _start(request: ForkRequest, fds: list[int]) -> _Running | None:
    """Fork a helper for *request*; `None` when the kernel refuses, which the build's socket is told."""
    answers_fd, *others = fds
    answers = socket.socket(fileno=answers_fd)
    zygote = os.getpid()
    try:
        pid = os.fork()
        if pid == 0:
            if "remove" in request:
                _remove(request["remove"], zygote)
            terminal, status_fd, *control = others
            _become_helper(request["spec"], zygote, terminal, status_fd, control[0] if control else None)
    except OSError as e:
        with contextlib.suppress(OSError):
            answers.sendall(json.dumps({"error": f"starting the sandbox helper: {e.strerror}"}).encode() + b"\n")
        answers.close()
        return None
    finally:
        # Held here, they keep the build's log and status pipe from ending.
        for fd in others:
            os.close(fd)
    pidfd = os.pidfd_open(pid)
    with contextlib.suppress(OSError):
        socket.send_fds(answers, [json.dumps({"pid": pid}).encode() + b"\n"], [pidfd])
    return _Running(pid, pidfd, answers)


def _reap(helper: _Running) -> None:
    _pid, status = os.waitpid(helper.pid, 0)
    with contextlib.suppress(OSError):
        helper.answers.sendall(json.dumps({"status": status}).encode() + b"\n")
    helper.answers.close()
    os.close(helper.pidfd)


def serve(requests: socket.socket) -> None:
    """Fork a helper for each request, and reap each, until pynixd closes its end."""
    with selectors.DefaultSelector() as selector:
        selector.register(requests, selectors.EVENT_READ)
        while True:
            for key, _events in selector.select():
                if isinstance(key.data, _Running):
                    selector.unregister(key.fileobj)
                    _reap(key.data)
                    continue
                # A message that carries descriptors ends a read, so one read is one request and its descriptors.
                data, fds, _flags, _address = socket.recv_fds(
                    requests, REQUEST_LIMIT, FDS_LIMIT, socket.MSG_CMSG_CLOEXEC
                )
                if not data:
                    return
                request: ForkRequest = json.loads(data)
                if (helper := _start(request, fds)) is not None:
                    selector.register(helper.pidfd, selectors.EVENT_READ, helper)


def main() -> None:
    requests_fd, parent = int(sys.argv[1]), int(sys.argv[2])
    child.die_with(parent)
    # pynixd's terminal signals are pynixd's: it ends its builds and closes the socket.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    with socket.socket(fileno=requests_fd) as requests:
        # Every object so far is shared by every helper; a collection in one would copy their pages.
        gc.collect()
        gc.freeze()
        serve(requests)


if __name__ == "__main__":
    main()
