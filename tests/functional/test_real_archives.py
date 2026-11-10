"""pynixd's NAR walkers and `.drv` parser against what Nix itself wrote.

The unit tests build their archives with pynixd's own `write_nar`, so a
shape that writer never makes reaches no walker there. Here the archives
come from `nix store dump-path` over `hello`'s closure, and the
derivations from the `.drv` closure Nix instantiates for it.
"""

from __future__ import annotations

import random

import anyio

from pynixd.nar import NarForwarder, nar_size, parse_nar, write_nar
from pynixd.wire import BytesReader, BytesWriter, stream_parse_nar
from pynixd_store.drv_parser import parse_drv
from tests._conftest.config import NIX_BIN
from tests._conftest.constants import HELLO


async def _nix(*args: str) -> bytes:
    return (await anyio.run_process([str(NIX_BIN), *args])).stdout


async def _closure(*flags: str) -> list[str]:
    out = await _nix("path-info", "--recursive", *flags, *HELLO)
    return sorted(out.decode().split())


def _forwarded(data: bytes, rng: random.Random) -> bytes:
    forwarder = NarForwarder()
    out: list[bytes] = []
    offset = 0
    while offset < len(data):
        step = rng.randint(1, 4096)
        out.extend(forwarder.feed(data[offset : offset + step]))
        offset += step
    assert forwarder.complete
    return b"".join(out)


async def test_every_walker_reads_each_archive_of_a_real_closure_whole() -> None:
    paths = await _closure()
    assert len(paths) > 1, paths
    rng = random.Random(42)
    for path in paths:
        data = await _nix("store", "dump-path", path)
        assert nar_size(data + b"TRAILING") == len(data), path
        assert _forwarded(data + b"TRAILING", rng) == data, path
        dst = BytesWriter()
        await stream_parse_nar(BytesReader(data + b"TRAILING"), dst)
        assert dst.get_bytes() == data, path
        assert write_nar(parse_nar(data)) == data, path


async def test_each_derivation_nix_wrote_serializes_to_its_own_bytes() -> None:
    drvs = [p for p in await _closure("--derivation") if p.endswith(".drv")]
    assert len(drvs) > 10, drvs
    for drv in drvs:
        raw = await anyio.Path(drv).read_text()
        assert parse_drv(raw).serialize() == raw, drv
