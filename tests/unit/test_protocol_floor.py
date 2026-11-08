"""pynixd refuses a client below protocol 1.35 at the handshake, in the words of `daemon.cc:1048`.

Nix admits a client from 1.18 (`worker-protocol.cc:35`). Below 1.25 such a
client sends AddToStore and AddToStoreNar unframed, and no handler here reads
that, so the floor is the NIX-DEVIATION at `MINIMUM_PROTOCOL`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from pynixd import wire
from pynixd.constants import proto
from pynixd.proxy import DaemonProxy


class _Reader:
    def __init__(self, words: list[int]) -> None:
        self.words = words

    async def read_uint64(self) -> int:
        if not self.words:
            raise EOFError
        return self.words.pop(0)


async def _nothing() -> None:
    return None


def _proxy(client_version: int) -> SimpleNamespace:
    return SimpleNamespace(
        r=_Reader([wire.WORKER_MAGIC_1, client_version]),
        w=SimpleNamespace(write_uint64=lambda _value: None, drain=_nothing),
        version=0,
    )


async def test_a_client_below_the_floor_is_refused() -> None:
    proxy = _proxy(proto(1, 34))
    with pytest.raises(ValueError, match="the Nix client version is too old"):
        await DaemonProxy.handshake(cast("DaemonProxy", proxy))


async def test_a_client_at_the_floor_goes_on() -> None:
    """Lix speaks 1.35. The handshake reads on past the version, here into the end of the stream."""
    proxy = _proxy(proto(1, 35))
    with pytest.raises(EOFError):
        await DaemonProxy.handshake(cast("DaemonProxy", proxy))
    assert proxy.version == proto(1, 35)
