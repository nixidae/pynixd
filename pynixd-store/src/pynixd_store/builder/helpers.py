"""Start sandbox helpers: pynixd's end of the zygote (`zygote.py`), and the process either start gives."""

from __future__ import annotations

import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
from typing import TYPE_CHECKING, Final, Protocol, Self

import anyio
import structlog
from anyio.abc import UNIXSocketStream

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType

    from anyio.abc import Process

    from .spec import ForkRequest

log = structlog.get_logger(__name__)

START_SECONDS: Final = 60.0
"""How long the zygote may take to fork. Its first request waits for its imports."""
STOP_SECONDS: Final = 5.0
ANSWER_LIMIT: Final = 4096


class HelperProcess(Protocol):
    """A running helper: an anyio `Process`, or one the zygote forked."""

    @property
    def pid(self) -> int: ...
    @property
    def returncode(self) -> int | None: ...
    def kill(self) -> None: ...
    def send_signal(self, signal: signal.Signals, /) -> None: ...
    async def wait(self) -> int: ...


class HelperStartError(OSError):
    """The zygote could not fork a helper: what `OSError` is to a process that does not start."""


class ForkedHelper:
    """A helper the zygote forked. Its parent is the zygote, which reaps it and sends its status here."""

    def __init__(self, pid: int, pidfd: int, answers: UNIXSocketStream) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self._pidfd = pidfd
        self._answers = answers
        self._lock = anyio.Lock()

    def kill(self) -> None:
        self.send_signal(signal.SIGKILL)

    def send_signal(self, signal_: signal.Signals, /) -> None:
        if self.returncode is not None:
            raise ProcessLookupError
        signal.pidfd_send_signal(self._pidfd, signal_)

    async def wait(self) -> int:
        async with self._lock:
            if self.returncode is None:
                status = await self._status()
                self.returncode = status
                os.close(self._pidfd)
                await self._answers.aclose()
            return self.returncode

    async def _status(self) -> int:
        try:
            answer = await self._answers.receive(ANSWER_LIMIT)
        except (anyio.EndOfStream, anyio.BrokenResourceError):
            # The zygote died, and the helper with it (`PR_SET_PDEATHSIG`).
            return -signal.SIGKILL
        return os.waitstatus_to_exitcode(json.loads(answer)["status"])


class Helpers:
    """The zygote of one store, started by its first build. Enter it for as long as builds may start."""

    def __init__(self) -> None:
        self._process: Process | None = None
        self._requests: UNIXSocketStream | None = None
        self._lock = anyio.Lock()
        self._open = False

    async def __aenter__(self) -> Self:
        self._open = True
        return self

    async def __aexit__(
        self, kind: type[BaseException] | None, error: BaseException | None, traceback: TracebackType | None
    ) -> None:
        with anyio.CancelScope(shield=True):
            async with self._lock:
                self._open = False
                await self._stop()

    @property
    def zygote_pid(self) -> int | None:
        return None if self._process is None else self._process.pid

    async def start(self, spec_file: Path, terminal: int, status_fd: int, control: int | None) -> ForkedHelper:
        """Fork a helper for the job in *spec_file*, on *terminal*. The descriptors stay the caller's to close."""
        # A cancelled start would leave a helper that nothing waits for.
        with anyio.CancelScope(shield=True):
            helper = await self._start_in_time(spec_file, terminal, status_fd, control)
        return helper

    async def _start_in_time(self, spec_file: Path, terminal: int, status_fd: int, control: int | None) -> ForkedHelper:
        try:
            with anyio.fail_after(START_SECONDS):
                return await self._start(spec_file, terminal, status_fd, control)
        except TimeoutError:
            async with self._lock:
                await self._stop()
            raise HelperStartError("starting the sandbox helper: the zygote does not answer") from None
        except (anyio.EndOfStream, anyio.BrokenResourceError):
            raise HelperStartError("starting the sandbox helper: the zygote died") from None

    async def _start(self, spec_file: Path, terminal: int, status_fd: int, control: int | None) -> ForkedHelper:
        ours, theirs = socket.socketpair()
        answers = await UNIXSocketStream.from_socket(ours)
        try:
            with theirs:
                fds = [theirs.fileno(), terminal, status_fd, *(() if control is None else (control,))]
                await self._send({"spec": str(spec_file)}, fds)
            answer, pidfds = await answers.receive_fds(ANSWER_LIMIT, 1)
            started = json.loads(answer)
            if "error" in started:
                raise HelperStartError(started["error"])
            return ForkedHelper(started["pid"], pidfds[0], answers)
        except BaseException:
            await answers.aclose()
            raise

    async def _send(self, request: ForkRequest, fds: list[int]) -> None:
        message = json.dumps(request).encode()
        async with self._lock:
            try:
                await (await self._running()).send_fds(message, fds)
            except anyio.BrokenResourceError:
                # It died since the last build, so it never read this one.
                log.warning("sandbox_zygote_died")
                await self._stop()
                await (await self._running()).send_fds(message, fds)

    async def _running(self) -> UNIXSocketStream:
        if not self._open:
            raise RuntimeError("a helper starts only while its `Helpers` is entered")
        if self._requests is not None and self._process is not None and self._process.returncode is None:
            return self._requests
        await self._stop()
        ours, theirs = socket.socketpair()
        with theirs:
            self._process = await anyio.open_process(
                [
                    *(sys.executable, "-m", "pynixd_store.builder.zygote"),
                    *(str(theirs.fileno()), str(os.getpid())),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=None,
                pass_fds=(theirs.fileno(),),
            )
        log.debug("sandbox_zygote_started", pid=self._process.pid)
        self._requests = await UNIXSocketStream.from_socket(ours)
        return self._requests

    async def _stop(self) -> None:
        """Close the zygote's socket, which ends it and every helper it still holds."""
        requests, self._requests = self._requests, None
        process, self._process = self._process, None
        if requests is not None:
            await requests.aclose()
        if process is not None:
            with anyio.move_on_after(STOP_SECONDS):
                await process.wait()
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
            log.debug("sandbox_zygote_stopped", pid=process.pid, returncode=process.returncode)
