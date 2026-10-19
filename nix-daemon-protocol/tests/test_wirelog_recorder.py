"""The recorder copies bytes, and writes what it copies.

The recorder sits between a Nix client and a daemon, so a defect in it shows
as a defect in pynixd. These tests use a server that speaks no protocol at
all, because the recorder must not care what the bytes mean. Issue nixidae/nixidae#214.
"""

from __future__ import annotations

import ast
import shutil
import tempfile
from pathlib import Path

import anyio
import pytest

from nix_daemon_protocol.wirelog import Direction, Recorder, one_direction, read_chunks
from nix_daemon_protocol.wirelog.framing import HEADER, encode_chunk, encode_header

STORE_DIR = "/nix/store"
"""What the recorder writes into the header of each recording here. The
recorder never reads it -- the caller passes it -- so any absolute path
would do, and this is the one a reader expects."""

RECORDER_SOURCE = Path(Recorder.__module__.replace(".", "/")).name


@pytest.fixture
def short_path():
    """A directory whose name fits in `sun_path`, which is 104 bytes here."""
    path = Path(tempfile.mkdtemp(prefix="/tmp/nixwire-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


async def upper_server(path: Path, ready: anyio.Event) -> None:
    """A server that speaks no protocol: it answers each read in upper case."""
    listener = await anyio.create_unix_listener(path)
    ready.set()

    async def handle(stream) -> None:
        async with stream:
            while True:
                try:
                    data = await stream.receive(4096)
                except (anyio.EndOfStream, anyio.ClosedResourceError):
                    return
                await stream.send(data.upper())

    async with listener:
        await listener.serve(handle)


async def record_exchange(root: Path, sends: list[bytes]) -> list[Path]:
    """Send each item through the recorder, and give back the recordings."""
    upstream = root / "up.sock"
    front = root / "front.sock"
    out = root / "rec"
    recorder = Recorder(listen=front, connect=upstream, out_dir=out, store_dir=STORE_DIR)
    ready = anyio.Event()

    async with anyio.create_task_group() as group:
        group.start_soon(upper_server, upstream, ready)
        await ready.wait()
        group.start_soon(recorder.serve)
        await recorder.wait_started()

        async with await anyio.connect_unix(front) as client:
            for item in sends:
                await client.send(item)
                # The echo is one send for one receive, so one read gets it.
                assert await client.receive(4096) == item.upper()

        # The recorder writes under a lock, and the last write lands after the
        # client closes. Wait for the file rather than guess a delay.
        with anyio.fail_after(5):
            while not list(out.glob("conn-*.wire")):
                await anyio.sleep(0.01)
        group.cancel_scope.cancel()

    return sorted(out.glob("conn-*.wire"))


@pytest.mark.anyio
async def test_the_recording_holds_both_directions(short_path):
    files = await record_exchange(short_path, [b"hello", b"world"])
    assert len(files) == 1
    chunks = read_chunks(files[0])
    assert one_direction(chunks, Direction.CLIENT) == b"helloworld"
    assert one_direction(chunks, Direction.SERVER) == b"HELLOWORLD"


@pytest.mark.anyio
async def test_the_recording_keeps_the_order_between_the_directions(short_path):
    files = await record_exchange(short_path, [b"one", b"two"])
    chunks = read_chunks(files[0])
    assert [(chunk.direction, chunk.data) for chunk in chunks] == [
        (Direction.CLIENT, b"one"),
        (Direction.SERVER, b"ONE"),
        (Direction.CLIENT, b"two"),
        (Direction.SERVER, b"TWO"),
    ]


@pytest.mark.anyio
async def test_bytes_that_no_protocol_explains_go_through(short_path):
    """The recorder must not need to understand the stream.

    An operation this package does not know, a NAR, a truncated message: each
    one is bytes, and each one must reach the recording whole.
    """
    payload = bytes(range(256)) * 4
    files = await record_exchange(short_path, [payload])
    chunks = read_chunks(files[0])
    assert one_direction(chunks, Direction.CLIENT) == payload


@pytest.mark.anyio
async def test_each_connection_gets_its_own_file(short_path):
    upstream = short_path / "up.sock"
    front = short_path / "front.sock"
    out = short_path / "rec"
    recorder = Recorder(listen=front, connect=upstream, out_dir=out, store_dir=STORE_DIR)
    ready = anyio.Event()

    async with anyio.create_task_group() as group:
        group.start_soon(upper_server, upstream, ready)
        await ready.wait()
        group.start_soon(recorder.serve)
        await recorder.wait_started()

        for word in (b"first", b"second", b"third"):
            async with await anyio.connect_unix(front) as client:
                await client.send(word)
                assert await client.receive(4096) == word.upper()

        with anyio.fail_after(5):
            while len(list(out.glob("conn-*.wire"))) < 3:
                await anyio.sleep(0.01)
        group.cancel_scope.cancel()

    files = sorted(out.glob("conn-*.wire"))
    assert [path.name for path in files] == ["conn-0000.wire", "conn-0001.wire", "conn-0002.wire"]
    assert [one_direction(read_chunks(path), Direction.CLIENT) for path in files] == [
        b"first",
        b"second",
        b"third",
    ]


def test_a_recording_that_a_killed_run_cut_short_still_reads(short_path):
    """The watchdog of the harness kills a run, and the file then ends anywhere."""
    whole = (
        encode_header(STORE_DIR)
        + encode_chunk(Direction.CLIENT, 1, b"kept")
        + encode_chunk(Direction.SERVER, 2, b"lost")
    )
    cut = whole[: -len(b"lost") - HEADER.size // 2]
    path = short_path / "cut.wire"
    path.write_bytes(cut)
    chunks = read_chunks(path)
    assert [(chunk.direction, chunk.data) for chunk in chunks] == [(Direction.CLIENT, b"kept")]


def test_a_file_that_is_not_a_recording_is_refused(short_path):
    path = short_path / "other.bin"
    path.write_bytes(b"not a recording")
    with pytest.raises(ValueError, match="not a recording"):
        read_chunks(path)


def test_the_recorder_reads_no_codec_of_this_package():
    """The rule that keeps a recording honest.

    A recorder that decodes can hold a defect of the codecs, and the recording
    then measures the recorder rather than the daemon. So this module may
    import `framing`, which is the file format, and no other module of
    `nix_daemon_protocol`.
    """
    source = Path(__file__).parent.parent / "src" / "nix_daemon_protocol" / "wirelog" / "recorder.py"
    tree = ast.parse(source.read_text())

    reached: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            # A relative import inside the package, or an absolute one.
            if node.level:
                reached.add(node.module.split(".")[0])
            elif node.module.startswith("nix_daemon_protocol"):
                reached.add(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("nix_daemon_protocol"):
                    reached.add(alias.name)

    assert reached == {"framing"}, f"{RECORDER_SOURCE} reads {sorted(reached)}, and it may read framing alone"
