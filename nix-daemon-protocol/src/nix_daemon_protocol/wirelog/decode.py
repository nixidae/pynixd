"""Read a recording, and give the operations that it holds.

A recording is bytes. This module finds the handshake and each operation in
it, and it uses the codecs of this package to do that. A recording that it
cannot read is a finding: the file says what the daemon really sent, so the
fault is in the codecs, and `Session.problem` names the operation and the
offset.

## How the two directions divide into operations

The request stream divides exactly. Each request starts with the number of
its operation, and that number gives the type, and the type reads its own
fields. So the decoder knows where each request ends.

The response stream does not divide that way. To find the end of a response
the decoder would have to read the type of every response, and two of them
are not types at all: `NarFromPath` answers with a NAR, and the model of
`AddToStore` covers its header alone.

So the decoder divides the responses by **when the bytes arrived**, which the
recording states. The protocol is synchronous: a client does not send request
N+1 until it has read response N. Response N is therefore every byte the
daemon sent between the start of request N and the start of request N+1.

That rule also holds for the five operations that carry a framed source. The
daemon writes log messages while the client is still sending frames, and
those bytes fall inside the window of the operation that they belong to.

## What a response holds

Log messages come first, and then the payload. `logs.read_stream` reads the
log messages and stops after `STDERR_LAST`, or after an error. What remains
is the payload, and the decoder keeps it as bytes. A comparison of two
recordings compares those bytes, so a NAR and a `ValidPathInfo` both work and
neither needs a model.

Issue nixidae/nixidae#214.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..constants import STDERR_LAST, WORKER_MAGIC_1, WORKER_MAGIC_2, proto
from ..context import ReadContext
from ..io import BytesReader, BytesWriter
from ..logs import LogError, LogNext, read_stream
from ..store_dir import reading_store_dir
from ..wire_message import WireModel
from ..wire_ops import WIRE_REGISTRY, WireResponse
from .framing import Chunk, Direction, read_recording

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

# The operations whose request carries a framed source: a length and that many
# raw bytes, again and again, and a length of zero at the end. `FramedSource`
# in `src/libutil/include/nix/util/serialise.hh` of Nix, and the five callers
# in `src/libstore/daemon.cc`. The model of each one covers the header alone,
# so the decoder skips the frames itself.
FRAMED_REQUESTS: frozenset[int] = frozenset({7, 39, 44, 45, 1001})


@dataclass(frozen=True)
class Handshake:
    """What the client and the daemon agreed before the first operation."""

    server_version: int
    client_version: int
    negotiated: int
    client_features: frozenset[str]
    server_features: frozenset[str]
    nix_version: str | None
    trusted: int | None


@dataclass(frozen=True)
class Operation:
    """One request, and everything the daemon said before the next request."""

    index: int
    op: int
    name: str
    request_body: bytes
    """The bytes after the operation number, and before the next request."""

    response_payload: bytes
    """The bytes after `STDERR_LAST`. Empty when the daemon reported an error."""

    logs: tuple[str, ...]
    """The text of each `STDERR_NEXT`. `diff._compare_logs` reads these."""

    error: str | None
    """The message of `STDERR_ERROR`, or None."""

    request_fields: dict[str, str] | None = None
    """The request as named fields, or None when the model does not cover it.

    **A request is compared byte by byte without this, and some of those bytes
    belong to the run rather than to the answer.** `AddIndirectRoot` carries
    the `result` symbolic link that `nix build` makes under
    `nix-build-<pid>-<random>`, so two runs differ there in every pair. A raw
    comparison can say only "byte 39 differs", and `EXEMPTIONS` has no name to
    reach.

    A replacement inside the raw bytes could not answer it either: a string on
    the wire carries an 8-byte length, and two pids of different width give
    two lengths. So the comparison has to read the decoded value. Issue nixidae/nixidae#241.
    """

    response_fields: dict[str, str] | None = None
    """The payload as named fields, or None when no model covers it.

    A comparison of the raw payload can say only "byte 224 differs". The
    registration time of a store path lives at byte 224 of one answer and at
    byte 96 of another, and it differs in every pair of runs, because the two
    runs add the path at two times. A name is what makes an answer like that
    readable, and what lets `EXEMPTIONS` reach one field of it.

    None for an answer that no model covers: `NarFromPath` sends a NAR, and
    the model of `AddToStore` covers its header alone.
    """


@dataclass
class Session:
    """One connection of a recording, as operations."""

    source: Path
    handshake: Handshake | None = None
    operations: list[Operation] = field(default_factory=list)
    problem: str | None = None
    """Why the decoder stopped early, or None when it read the whole file."""


class _Quiet:
    """A logger that says nothing.

    A body that no model covers is the ordinary case here, and the decoder
    tries every model to find out which. `deserialization_scope` writes the
    whole traceback of each failure through `ctx.logger`, so without this the
    report of a run holds one traceback for every NAR that passed.
    """

    def __getattr__(self, name: str) -> Callable[..., None]:
        def nothing(*_args: object, **_kwargs: object) -> None:
            return None

        return nothing


_QUIET = _Quiet()


def _flatten(value: object, prefix: str, out: dict[str, str]) -> None:
    """Every leaf of a model, under its dotted name."""
    if isinstance(value, WireModel):
        for name in type(value).model_fields:
            if name == "logs":
                # The stream of log messages, which the decoder already read
                # into `Operation.logs`. `diff._compare_logs` reads it there,
                # so a copy here would report each difference twice.
                continue
            _flatten(getattr(value, name), f"{prefix}.{name}" if prefix else name, out)
        return
    if isinstance(value, (set, frozenset)):
        out[prefix] = repr(sorted(str(item) for item in value))
        return
    out[prefix] = repr(value)


async def _decode_request(op: int, body: bytes, version: int) -> dict[str, str] | None:
    """The request of one operation as named fields, or None.

    `body` starts after the 8-byte operation code, which is where the model of
    a request starts as well. A framed request carries its source after the
    model, and `remaining()` is what says so.
    """
    request_cls = WIRE_REGISTRY.get(op)
    if request_cls is None:
        return None

    reader = BytesReader(body, identifier="wirelog:request")
    try:
        model = await request_cls.from_reader(
            ReadContext(reader=reader, version=version, logger=_QUIET),  # type: ignore[arg-type]  # see _Quiet
        )
    except (EOFError, ConnectionError, ValueError, TypeError, KeyError):
        return None
    if reader.remaining():
        # The model covers a part of the request only, so its names would
        # describe a part and hide the rest. A framed request is that case.
        return None

    out: dict[str, str] = {}
    _flatten(model, "", out)
    return out


async def _decode_body(op: int, payload: bytes, version: int) -> dict[str, str] | None:
    """The payload of one answer as named fields, or None.

    The model of a response reads the log stream first, and `payload` starts
    after that stream. So this puts one `STDERR_LAST` in front of the payload,
    which is the shortest stream that ends at once.
    """
    response_cls = getattr(WIRE_REGISTRY[op], "response_type", None)
    if response_cls is None or not (isinstance(response_cls, type) and issubclass(response_cls, WireResponse)):
        return None

    head = BytesWriter("wirelog")
    head.write_uint64(STDERR_LAST)
    reader = BytesReader(head.get_bytes() + payload, identifier="wirelog:body")
    try:
        model = await response_cls.from_reader(
            ReadContext(reader=reader, version=version, logger=_QUIET),  # type: ignore[arg-type]  # see _Quiet
        )
    except (EOFError, ConnectionError, ValueError, TypeError, KeyError):
        return None
    if reader.remaining():
        # The model covers a part of the answer only, so its names would
        # describe a part and hide the rest. `AddToStore` is that case.
        return None

    out: dict[str, str] = {}
    _flatten(model, "", out)
    return out


async def _read_handshake(client: BytesReader, server: BytesReader) -> Handshake:
    magic = await client.read_uint64()
    if magic != WORKER_MAGIC_1:
        raise ValueError(f"the client sent {magic:#x}, and a handshake starts with {WORKER_MAGIC_1:#x}")
    reply = await server.read_uint64()
    if reply != WORKER_MAGIC_2:
        raise ValueError(f"the daemon sent {reply:#x}, and a handshake answers {WORKER_MAGIC_2:#x}")

    server_version = await server.read_uint64()
    client_version = await client.read_uint64()
    negotiated = min(server_version, client_version)

    client_features: frozenset[str] = frozenset()
    server_features: frozenset[str] = frozenset()
    if negotiated >= proto(1, 38):
        client_features = frozenset(await _read_string_set(client))
        server_features = frozenset(await _read_string_set(server))

    if await client.read_uint64():  # sendCpu
        await client.read_uint64()  # cpuAffinity, which the daemon ignores
    await client.read_uint64()  # reserveSpace, which the daemon ignores

    nix_version = None
    trusted = None
    if client_version >= proto(1, 33):
        nix_version = (await server.read_bytes()).decode()
        if client_version >= proto(1, 35):
            trusted = await server.read_uint64()
    await server.read_uint64()  # STDERR_LAST

    return Handshake(
        server_version=server_version,
        client_version=client_version,
        negotiated=negotiated,
        client_features=client_features,
        server_features=server_features,
        nix_version=nix_version,
        trusted=trusted,
    )


async def _read_string_set(reader: BytesReader) -> set[str]:
    count = await reader.read_uint64()
    return {(await reader.read_bytes()).decode() for _ in range(count)}


async def _skip_framed_source(reader: BytesReader) -> None:
    while True:
        length = await reader.read_uint64()
        if not length:
            return
        # The frames of a framed source are raw, and not padded to eight.
        await reader.readexactly(length)


def _chunk_index_at(offsets: list[tuple[int, int]], position: int) -> int:
    """The index in the recording of the chunk that holds a client offset."""
    found = 0
    for start, index in offsets:
        if start > position:
            break
        found = index
    return found


def _server_window(chunks: list[Chunk], after: int, before: int | None) -> bytes:
    return b"".join(
        chunk.data
        for index, chunk in enumerate(chunks)
        if chunk.direction is Direction.SERVER and index > after and (before is None or index < before)
    )


async def decode(path: Path) -> Session:
    """Read one recording into a handshake and a list of operations.

    The store directory comes out of the recording, because the paths on the
    wire do. A recording of Nix's functional suite names that test's own
    store, and reading it against this process's store refuses every path.
    Issue #37.
    """
    recording = read_recording(path)
    with reading_store_dir(recording.store_dir):
        return await _decode(recording.chunks, path)


async def _decode(chunks: list[Chunk], path: Path) -> Session:
    session = Session(source=path)

    client_bytes = bytearray()
    client_offsets: list[tuple[int, int]] = []
    for index, chunk in enumerate(chunks):
        if chunk.direction is Direction.CLIENT:
            client_offsets.append((len(client_bytes), index))
            client_bytes.extend(chunk.data)

    client = BytesReader(bytes(client_bytes), identifier=f"{path}:client")
    server_all = b"".join(chunk.data for chunk in chunks if chunk.direction is Direction.SERVER)
    server = BytesReader(server_all, identifier=f"{path}:server")

    try:
        session.handshake = await _read_handshake(client, server)
    except (EOFError, ValueError) as exc:
        session.problem = f"the handshake did not decode: {exc}"
        return session

    version = session.handshake.negotiated

    # Where each request starts in the client stream. The op loop needs the
    # start of request N+1 to close the window of response N, so it reads the
    # whole request stream first and divides the responses after.
    starts: list[tuple[int, int, bytes]] = []  # (offset, op, body)
    while True:
        offset = client.tell()
        try:
            op = await client.read_uint64()
        except EOFError:
            break
        request_cls = WIRE_REGISTRY.get(op)
        if request_cls is None:
            session.problem = f"operation {op} at offset {offset} of the request stream is not in the registry"
            break
        try:
            await request_cls.from_reader(ReadContext(reader=client, version=version))
            if op in FRAMED_REQUESTS:
                await _skip_framed_source(client)
        except (EOFError, ValueError) as exc:
            session.problem = f"{request_cls.name} at offset {offset} of the request stream did not decode: {exc}"
            break
        starts.append((offset, op, bytes(client_bytes[offset + 8 : client.tell()])))

    for index, (offset, op, body) in enumerate(starts):
        after = _chunk_index_at(client_offsets, offset)
        before = _chunk_index_at(client_offsets, starts[index + 1][0]) if index + 1 < len(starts) else None
        window = _server_window(chunks, after, before)

        logs: list[str] = []
        error: str | None = None
        reader = BytesReader(window, identifier=f"{path}:response:{index}")
        try:
            async for message in read_stream(ReadContext(reader=reader, version=version)):
                if isinstance(message, LogNext):
                    logs.append(str(message.text))
                elif isinstance(message, LogError):
                    error = str(message.msg)
        except (EOFError, ConnectionError, ValueError) as exc:
            # The daemon may have closed while the client waited, and then the
            # window holds no `STDERR_LAST`. Keep what arrived.
            session.problem = session.problem or f"the response of operation {index} did not decode: {exc}"

        request_cls = WIRE_REGISTRY[op]
        payload = reader.remaining()
        session.operations.append(
            Operation(
                index=index,
                op=op,
                name=request_cls.name,
                request_body=body,
                request_fields=await _decode_request(op, body, version),
                response_payload=payload,
                logs=tuple(logs),
                error=error,
                response_fields=await _decode_body(op, payload, version),
            )
        )

    return session
