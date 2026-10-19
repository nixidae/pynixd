"""Every NAR loop counts its own bytes, so a transfer is never invisible.

Before these series only AddMultipleToStore (op 44) was counted. A pod could
receive a closure one path at a time, or serve one to every node that started,
and `pynixd_nar_forward_bytes_total` stayed flat throughout.

That also makes the split the discriminator. Given a push whose op nobody
recorded, whichever receive counter moved names the loop it took -- which is
the question nixidae/nixidae#507 turns on.

The counters live on the handlers, not in `pynixd.wire`, because the op is
known at the handler. These tests therefore drive the wire helpers with the
same `on_bytes` the handlers pass, which is what the handlers can be wrong
about: a helper that never calls it leaves the series flat with no failure
anywhere.
"""

from __future__ import annotations

from pynixd import metrics
from pynixd.wire import BytesReader, BytesWriter, forward_framed, forward_raw

_FRAME = 32 * 1024
_FRAMES = 64
_CHUNK = 1024 * 1024


def _framed_payload(frames: int = _FRAMES, size: int = _FRAME) -> bytes:
    out = BytesWriter()
    for i in range(frames):
        out.write_uint64(size)
        out.write(bytes([i % 256]) * size)
    out.write_uint64(0)
    return out.get_bytes()


def _value(counter) -> float:  # noqa: ANN001 - a prometheus_client Counter
    return counter._value.get()  # type: ignore[attr-defined]


async def test_a_single_path_add_counts_its_bytes() -> None:
    """op 7 and op 39 land on `pynixd_nar_add_bytes_total`, not on forward."""
    before_add = _value(metrics.NAR_ADD_BYTES)
    before_forward = _value(metrics.NAR_FORWARD_BYTES)

    await forward_framed(
        BytesReader(_framed_payload()),
        BytesWriter(),
        chunk_size=_CHUNK,
        on_bytes=metrics.NAR_ADD_BYTES.inc,
    )

    assert _value(metrics.NAR_ADD_BYTES) - before_add == _FRAMES * _FRAME
    # The op 44 counter is the discriminator, so a single-path add must leave
    # it alone. A shared counter would answer "which loop" with "either".
    assert _value(metrics.NAR_FORWARD_BYTES) == before_forward


async def test_serving_a_nar_counts_its_bytes() -> None:
    """op 38 lands on `pynixd_nar_serve_bytes_total`."""
    before_serve = _value(metrics.NAR_SERVE_BYTES)
    before_add = _value(metrics.NAR_ADD_BYTES)
    size = _CHUNK * 4

    await forward_raw(
        BytesReader(b"\0" * size),
        BytesWriter(),
        size,
        chunk_size=_CHUNK,
        on_bytes=metrics.NAR_SERVE_BYTES.inc,
    )

    assert _value(metrics.NAR_SERVE_BYTES) - before_serve == size
    assert _value(metrics.NAR_ADD_BYTES) == before_add


async def test_the_counter_moves_during_the_transfer() -> None:
    """Not once at the end: a dashboard has to see a long transfer progressing.

    A push that only reports on completion looks identical to a push that has
    hung, which is the state nixidae/nixidae#507 needed to tell apart.
    """
    seen: list[float] = []

    def record(n: int) -> None:
        seen.append(n)

    await forward_raw(
        BytesReader(b"\0" * (_CHUNK * 4)),
        BytesWriter(),
        _CHUNK * 4,
        chunk_size=_CHUNK,
        on_bytes=record,
    )

    assert len(seen) == 4, f"reported {len(seen)} times for 4 chunks"
    assert sum(seen) == _CHUNK * 4
