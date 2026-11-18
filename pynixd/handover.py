"""Hand the listeners to a new pynixd on reload, and let the old one drain.

`pynixd handover`, the unit's `ExecReload=`, runs from the new unit file: it knows the new store path and the new
settings, and the old main knows neither. It asks the old main for its listening sockets over the control socket
(`SCM_RIGHTS`), starts the successor with them, and waits for its ready byte. Then it tells the old main to drain.
On any failure before that, the old main keeps everything. docs/design/upgrade-handover.md, nixidae/nixidae#1519.

The control protocol runs between two versions of pynixd, so each message carries `protocol` and ignores
fields it does not know.
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol

import anyio
import anyio.to_thread
import structlog
from anyio.abc import SocketAttribute, SocketListener, UNIXSocketStream
from anyio.streams.buffered import BufferedByteReceiveStream
from pydantic import BaseModel, ConfigDict, ValidationError

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd_store.db_migrations import TARGET_VERSION

if TYPE_CHECKING:
    from collections.abc import Mapping, MutableMapping, Sequence
    from pathlib import Path

    from .config import PynixdSettings
    from .unix_server import Spawn

log = structlog.get_logger(__name__)

PROTOCOL: Final = 1
LISTEN_FDS_ENV: Final = "PYNIXD_LISTEN_FDS"
"""The listeners a successor inherits, as `Inherited` JSON. Not `LISTEN_FDS`: `LISTEN_PID` needs the child's pid
before exec, which a spawner in Python does not have."""
READY_FD_ENV: Final = "PYNIXD_READY_FD"
READY: Final = b"ready\n"
MESSAGE_LIMIT: Final = 65536
MAX_FDS: Final = 64
READY_TIMEOUT: Final = 300.0
"""A start opens the stores and binds every listener: 2 s in a guest. The limit is for a successor that hangs."""
STOP_TIMEOUT: Final = 30.0


class ListenerName(StrEnum):
    UNIX = "unix"
    SSH = "ssh"
    REVERSE = "reverse"
    HTTP = "http"
    HTTPS = "https"
    HANDOVER = "handover"


class _Message(BaseModel):
    model_config = ConfigDict(extra="ignore")
    protocol: int = PROTOCOL


class Offer(_Message):
    """From `pynixd handover`: what the new binary and its settings are. `None` is a field an older one lacks."""

    schema_version: int
    unix_path: str | None = None
    local_store: str | None = None

    @classmethod
    def of(cls, settings: PynixdSettings) -> Offer:
        return cls(
            schema_version=TARGET_VERSION,
            unix_path=str(settings.unix_path) if settings.unix_path else "",
            local_store=local_store_key(settings),
        )


def local_store_key(settings: PynixdSettings) -> str:
    """The local store's settings, as text two versions compare. A field one version adds reads as a change."""
    spec = settings.stores.get(str(LOCAL_STORE_ID))
    return "" if spec is None else spec.model_dump_json(exclude={"settings"})


class Answer(_Message):
    """From the old main: its listeners, in the order of the fds that come with it, or why it refuses."""

    pid: int
    refused: str | None = None
    listeners: dict[str, int] = {}
    unix_owns_path: bool = False


class Command(_Message):
    drain: bool


class Drained(_Message):
    pid: int


class Inherited(BaseModel):
    """The listeners a successor got from its predecessor, by name."""

    fds: dict[str, list[int]] = {}
    unix_owns_path: bool = False

    @classmethod
    def from_env(cls, environ: MutableMapping[str, str]) -> Inherited | None:
        """Take the variable out of *environ*, so no child of this process reads it."""
        raw = environ.pop(LISTEN_FDS_ENV, None)
        if raw is None:
            return None
        inherited = cls.model_validate_json(raw)
        for fds in inherited.fds.values():
            for fd in fds:
                os.set_inheritable(fd, False)
        return inherited

    def take(self, name: ListenerName) -> list[socket.socket]:
        return [socket.socket(fileno=fd) for fd in self.fds.pop(name, [])]

    def close_rest(self) -> None:
        """Close what no listener took: a listener the new settings dropped."""
        for name, fds in self.fds.items():
            log.info("handover_listener_dropped", listener=name)
            for fd in fds:
                os.close(fd)
        self.fds.clear()


def take_ready_fd(environ: MutableMapping[str, str]) -> int | None:
    raw = environ.pop(READY_FD_ENV, None)
    if raw is None:
        return None
    fd = int(raw)
    os.set_inheritable(fd, False)
    return fd


def say_ready(fd: int) -> None:
    try:
        os.write(fd, READY)
    finally:
        os.close(fd)


def raw_socket(listener: SocketListener) -> socket.socket:
    return listener.extra(SocketAttribute.raw_socket)


class Handing(Protocol):
    """What the old main answers on the control socket. `instance.Server` implements it."""

    def handover_refusal(self, offer: Offer) -> str | None: ...

    def listener_sockets(self) -> Mapping[ListenerName, Sequence[socket.socket]]: ...

    def unix_owns_path(self) -> bool: ...

    async def drain(self) -> None: ...


async def _read_line(stream: BufferedByteReceiveStream) -> bytes:
    return await stream.receive_until(b"\n", MESSAGE_LIMIT)


async def _send(stream: UNIXSocketStream, message: _Message, fds: Sequence[int] = ()) -> None:
    data = message.model_dump_json().encode() + b"\n"
    if fds:
        await stream.send_fds(data, fds)
    else:
        await stream.send(data)


class HandoverControl:
    """The old main's side: one hand-over at a time, on a socket only root reaches."""

    def __init__(self, listener: SocketListener, path: Path, *, owns_path: bool) -> None:
        self.listener = listener
        self.path = path
        self.owns_path = owns_path
        self._lock = anyio.Lock()
        self._accepting = anyio.CancelScope()
        self._accept_ended = anyio.Event()
        self._accept_started = False

    @classmethod
    async def bind(cls, path: Path, inherited: list[socket.socket]) -> HandoverControl:
        for sock in inherited:
            if sock.getsockname() == str(path):
                return cls(await SocketListener.from_socket(sock), path, owns_path=True)
            sock.close()
        # `create_unix_listener` removes a stale socket at the path first.
        listener = await anyio.create_unix_listener(path, mode=0o600)
        return cls(listener, path, owns_path=True)

    def serve(self, spawn: Spawn, server: Handing) -> None:
        self._accept_started = True
        spawn(self._accept_loop, spawn, server)

    async def _accept_loop(self, spawn: Spawn, server: Handing) -> None:
        try:
            with self._accepting:
                while True:
                    try:
                        stream = await self.listener.accept()
                    except anyio.BrokenResourceError as ex:
                        log.warning("handover_accept_failed", error=str(ex.__cause__ or ex))
                        await anyio.sleep(1)
                        continue
                    spawn(self._handle, stream, server)
        finally:
            self._accept_ended.set()

    async def _handle(self, stream: UNIXSocketStream, server: Handing) -> None:
        # An error here must not reach the owner's task group: the daemon serves on.
        async with stream:
            try:
                async with self._lock:
                    await self._hand_over(stream, server)
            except (anyio.EndOfStream, anyio.BrokenResourceError, anyio.IncompleteRead, ValidationError) as ex:
                log.warning("handover_aborted", error=repr(ex))
            except Exception:
                log.exception("handover_failed")

    async def _hand_over(self, stream: UNIXSocketStream, server: Handing) -> None:
        reader = BufferedByteReceiveStream(stream)
        offer = Offer.model_validate_json(await _read_line(reader))
        refused = server.handover_refusal(offer)
        if refused is not None:
            log.warning("handover_refused", reason=refused)
            await _send(stream, Answer(pid=os.getpid(), refused=refused))
            return
        sockets = server.listener_sockets()
        listeners = {str(name): len(socks) for name, socks in sockets.items()}
        fds = [sock.fileno() for socks in sockets.values() for sock in socks]
        log.info("handover_offered", listeners=listeners)
        await _send(stream, Answer(pid=os.getpid(), listeners=listeners, unix_owns_path=server.unix_owns_path()), fds)
        command = Command.model_validate_json(await _read_line(reader))
        if not command.drain:
            log.warning("handover_abandoned")
            return
        await server.drain()
        await _send(stream, Drained(pid=os.getpid()))

    async def aclose(self, *, unlink: bool) -> None:
        self._accepting.cancel()
        if self._accept_started:
            await self._accept_ended.wait()
        await self.listener.aclose()
        if unlink and self.owns_path:
            with contextlib.suppress(FileNotFoundError):
                await anyio.Path(self.path).unlink()


class HandoverError(Exception):
    """The hand-over failed, and the old main keeps serving."""


class Refused(Exception):
    """The old main cannot overlap with a successor."""


async def restart_instead(unit: str | None, reason: str) -> int:
    """Fall back to today's restart, so the new configuration still applies."""
    if unit is None:
        print(f"pynixd handover: {reason}; no --unit to restart", file=sys.stderr)
        return 1
    print(f"pynixd handover: {reason}; restarting {unit} instead", file=sys.stderr)
    # `--no-block`: this reload job holds the unit, and a blocking restart would wait on itself.
    result = await anyio.run_process(
        ["systemctl", "--no-block", "restart", unit], check=False, stdout=None, stderr=None
    )
    return result.returncode


async def _offer(path: Path, offer: Offer) -> tuple[UNIXSocketStream, BufferedByteReceiveStream, Answer, list[int]]:
    try:
        stream = await anyio.connect_unix(path)
    except (FileNotFoundError, ConnectionRefusedError) as ex:
        raise Refused(f"no pynixd answers on {path}: {ex.strerror}") from ex
    await _send(stream, offer)
    data, fds = await stream.receive_fds(MESSAGE_LIMIT, MAX_FDS)
    buffered = BufferedByteReceiveStream(stream)
    if b"\n" not in data:
        data += await _read_line(buffered)
    answer = Answer.model_validate_json(data.split(b"\n", 1)[0])
    if answer.refused is not None:
        for fd in fds:
            os.close(fd)
        await stream.aclose()
        raise Refused(f"pynixd {answer.pid} refuses: {answer.refused}")
    if sum(answer.listeners.values()) != len(fds):
        for fd in fds:
            os.close(fd)
        await stream.aclose()
        raise HandoverError(f"pynixd {answer.pid} named {sum(answer.listeners.values())} listeners and sent {len(fds)}")
    return stream, buffered, answer, fds


def _inherited(answer: Answer, fds: list[int]) -> Inherited:
    by_name: dict[str, list[int]] = {}
    rest = iter(fds)
    for name, count in answer.listeners.items():
        by_name[name] = [next(rest) for _ in range(count)]
    return Inherited(fds=by_name, unix_owns_path=answer.unix_owns_path)


async def _wait_ready(ready: socket.socket, timeout: float) -> None:
    stream = await UNIXSocketStream.from_socket(ready)
    async with stream:
        with anyio.fail_after(timeout):
            try:
                data = await stream.receive(len(READY))
            except anyio.EndOfStream:
                raise HandoverError("the successor exited before it was ready") from None
    if data != READY:
        raise HandoverError(f"the successor said {data!r}, not ready")


async def _stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            await anyio.to_thread.run_sync(lambda: process.wait(STOP_TIMEOUT))
        except subprocess.TimeoutExpired:
            process.kill()
            await anyio.to_thread.run_sync(lambda: process.wait())


async def hand_over(
    path: Path, command: Sequence[str], offer: Offer, *, unit: str | None, timeout: float = READY_TIMEOUT
) -> int:
    """Start *command* with the old main's listeners, then drain the old main. The exit status for `ExecReload=`."""
    try:
        stream, reader, answer, fds = await _offer(path, offer)
    except Refused as ex:
        return await restart_instead(unit, str(ex))
    async with stream:
        ours, theirs = socket.socketpair()
        env = dict(os.environ)
        env[LISTEN_FDS_ENV] = _inherited(answer, fds).model_dump_json()
        env[READY_FD_ENV] = str(theirs.fileno())
        try:
            # Not `anyio.open_process`: asyncio's transport kills a child still running when its loop closes,
            # and the successor must outlive this process. Measured: SIGKILL as `handover` exited.
            # stdout and stderr inherited: the successor's logs go where the unit's go.
            process = subprocess.Popen(  # noqa: ASYNC220 -- returns at once; the successor is not waited on
                list(command), stdin=subprocess.DEVNULL, env=env, pass_fds=[*fds, theirs.fileno()]
            )
        finally:
            theirs.close()
            for fd in fds:
                os.close(fd)
        try:
            await _wait_ready(ours, timeout)
        except (HandoverError, TimeoutError) as ex:
            await _stop(process)
            await _send(stream, Command(drain=False))
            print(
                f"pynixd handover: {ex or 'the successor was not ready in time'}; {answer.pid} keeps serving",
                file=sys.stderr,
            )
            return 1
        await _send(stream, Command(drain=True))
        drained = Drained.model_validate_json(await _read_line(reader))
    print(f'{{"old": {drained.pid}, "new": {process.pid}}}')
    return 0
