"""One store path, one key, and the two signatures over it.

Nix signs a path with `ValidPathInfo::sign`, which signs
`ValidPathInfo::fingerprint` at `path-info.cc:48`. pynixd signs the same
string, or it signs something else and every signature it makes is false. A
unit test cannot tell those apart, because it states the fingerprint that
pynixd builds and then checks that pynixd built it.

So this asks Nix. `nix store sign` puts a signature on a path, and pynixd
signs the same path with the same key. Ed25519 is deterministic, so the two
strings are equal or the fingerprints differ.

It found the difference it was written for. The fingerprint carries
`sha256:<base-32 digest>`, the wire carries the base-16 digest with no name of
an algorithm at `worker-protocol.cc:356`, and pynixd converted nothing.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import pytest

from nix_daemon_protocol.nar_hash import NARHash
from pynixd_store.signing import SecretKey, fingerprint

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

NIX = shutil.which("nix")

pytestmark = pytest.mark.skipif(NIX is None, reason="this test needs `nix` on the PATH")

# Nix refuses a store when a parent of it is a symbolic link, and `/tmp` is one
# on Darwin.
TEMP_ROOT = Path("/private/tmp") if Path("/private/tmp").is_dir() else Path("/tmp")
ROOT = TEMP_ROOT / "pynixd-signature-parity"


async def _nix(*words: str, stdin: str | None = None) -> str:
    """Run `nix` against the store of this test, and answer its output."""
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(ROOT),
        "NIX_STORE_DIR": str(ROOT / "store"),
        "NIX_STATE_DIR": str(ROOT / "var/nix"),
        "NIX_REMOTE": "",
    }
    done = await anyio.run_process(
        [str(NIX), *words],
        env=env,
        input=None if stdin is None else stdin.encode(),
        check=True,
    )
    return done.stdout.decode()


@pytest.fixture
async def store() -> AsyncIterator[Path]:
    shutil.rmtree(ROOT, ignore_errors=True)
    for name in ("store", "var/nix"):
        await anyio.Path(ROOT / name).mkdir(parents=True)
    yield ROOT
    shutil.rmtree(ROOT, ignore_errors=True)


async def test_pynixd_signs_what_nix_signs(store: Path) -> None:
    sample = store / "f.txt"
    await anyio.Path(sample).write_text("hello signing\n")
    secret = store / "key"
    await anyio.Path(secret).write_text((await _nix("key", "generate-secret", "--key-name", "probe")).strip())

    path = (await _nix("store", "add-file", "--name", "f.txt", str(sample))).strip()
    await _nix("store", "sign", "--key-file", str(secret), path)

    answer = json.loads(await _nix("path-info", "--json", path))
    info = answer[0] if isinstance(answer, list) else next(iter(answer.values()))
    # `path-info --json` prints the NAR hash in the SRI form, and the wire
    # carries base 16. pynixd reads the wire, so the test gives it that form.
    wire_hash = (await _nix("hash", "convert", "--to", "base16", info["narHash"])).strip()

    key = SecretKey.from_string(await anyio.Path(secret).read_text())
    ours = key.sign_fingerprint(fingerprint(path, NARHash(wire_hash), info["narSize"], info["references"]))

    assert ours in info["signatures"], (
        f"Nix signed {info['signatures']} and pynixd signed {ours}. "
        "Ed25519 is deterministic, so the two fingerprints differ."
    )
