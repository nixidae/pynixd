"""A self-referencing output's content address, as Nix 2.34 and Nix 2.35 each compute it (`add.HashModulo`).

The answers are what `nix build` of `CA_TOP` (`tests/functional/test_local_build_parity.py`) recorded in a
fresh store of each version, with no daemon: `ca` of `parity-ca-top-dev`, whose file holds its own path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pynixd_store.add import content_hash
from pynixd_store.content_address import ContentAddress, Method
from pynixd_store.engine import hashes_self_reference_positions
from pynixd_store.nar_fs import dump_path
from pynixd_store.utils import nix32_encode

if TYPE_CHECKING:
    from pathlib import Path

# The final hash part each version gave the output, and the `ca` it recorded.
NIX_2_34 = ("2hljykyl928r711d7lh82hvrlyasx4p7", "07g735gm2xdvpbhgng407bd2c2jd61dfls3xfqizh5mvfzxddixq")
NIX_2_35 = ("g830s11x7yllailvpk21r1i2plvmydqa", "1vz3b8zjz5lll6vj4m51ilh04cpism68lkzwc3zvscjv9dqxixy4")


def _ca(tmp_path: Path, hash_part: str, *, positions: bool) -> str:
    output = tmp_path / str(positions) / hash_part
    output.parent.mkdir()
    output.write_text(f"/nix/store/{hash_part}-parity-ca-top-dev\n")
    output.chmod(0o444)
    return nix32_encode(
        content_hash(ContentAddress(Method.NAR, "sha256", b""), output, hash_part, dump_path, positions=positions)
    )


def test_nix_2_35_hashes_the_offset_of_each_self_reference(tmp_path: Path) -> None:
    part, ca = NIX_2_35
    assert _ca(tmp_path, part, positions=True) == ca


def test_nix_2_34_hashes_none(tmp_path: Path) -> None:
    part, ca = NIX_2_34
    assert _ca(tmp_path, part, positions=False) == ca
    assert _ca(tmp_path, part, positions=True) == NIX_2_35[1]


def test_the_version_decides() -> None:
    assert not hashes_self_reference_positions("2.34.8")
    assert hashes_self_reference_positions("2.35.2")
    assert hashes_self_reference_positions("2.36.0pre20261101_abcdef")
    assert hashes_self_reference_positions("")
