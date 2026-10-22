"""A derivation is bytes, and pynixd carries every one of them.

Nix reads and writes derivation strings as bytes (`derivations.cc:224-275`,
`:533-561`). An env value holding 0xff and a broken UTF-8 sequence built
under nix-daemon, and pynixd failed the request with "UnicodeDecodeError:
'utf-8' codec can't decode byte 0xff". `nix-instantiate` wrote the text
below, from `weird = "a\\xffb\\xc3(";`. nixidae/nixidae#1010.
"""

from __future__ import annotations

import binascii

import pytest

from nix_daemon_protocol.constants import PROTOCOL_VERSION
from pynixd.drv_hash import output_hashes
from pynixd.drv_parser import drv_bytes, drv_text, parse_drv
from pynixd.goals.resolution import _make_output_path
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode, StorePath
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.wire import BytesReader, BytesWriter

DRV_PATH = "/nix/store/6fza8wr7ydvdgcmgnwh4pa6w4dhw809r-non-utf8.drv"
OUT = "/nix/store/0mf91qf95w6blv7m6f2mx6f86shilqzw-non-utf8"
WEIRD = b"a\xffb\xc3("
DRV = (
    b'Derive([("out","/nix/store/0mf91qf95w6blv7m6f2mx6f86shilqzw-non-utf8","","")],[],[],"x86_64-linux",'
    b'"/bin/sh",["-c","printf %s \\"$weird\\" > $out"],[("builder","/bin/sh"),("name","non-utf8"),'
    b'("out","/nix/store/0mf91qf95w6blv7m6f2mx6f86shilqzw-non-utf8"),("system","x86_64-linux"),'
    b'("weird","' + WEIRD + b'")])'
)


async def _nothing(_drv_path: str) -> None:
    return None


def test_the_text_round_trips_byte_for_byte() -> None:
    parsed = parse_drv(drv_text(DRV))

    assert drv_bytes(parsed.env["weird"]) == WEIRD
    assert drv_bytes(parsed.unparse()) == DRV


@pytest.mark.anyio
async def test_the_hash_names_the_path_nix_wrote() -> None:
    hashes = await output_hashes(parse_drv(drv_text(DRV)), _nothing, cache={})

    assert hashes is not None
    assert _make_output_path("out", binascii.unhexlify(hashes["out"]), "non-utf8") == OUT


@pytest.mark.anyio
async def test_build_derivation_carries_the_bytes_on_the_wire() -> None:
    request = BuildDerivationRequest(
        drv_path=StorePath(path=DRV_PATH),
        derivation=BasicDerivation(
            platform="x86_64-linux",
            builder="/bin/sh",
            args=["-c", drv_text(b"printf " + WEIRD)],
            env={"weird": drv_text(WEIRD)},
        ),
        build_mode=BuildMode.NORMAL,
    )
    writer = BytesWriter("test")
    await request.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    data = writer.get_bytes()

    assert WEIRD in data
    decoded = await BuildDerivationRequest.from_reader(
        ReadContext(reader=BytesReader(data[8:], "test"), version=PROTOCOL_VERSION)
    )
    assert drv_bytes(decoded.derivation.env["weird"]) == WEIRD
    assert drv_bytes(decoded.derivation.args[1]) == b"printf " + WEIRD
