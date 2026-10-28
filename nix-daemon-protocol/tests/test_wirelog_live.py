"""Record a real client against a real daemon, and compare two runs.

The other tests of `wirelog` build a recording from the codecs of this
package, so they cannot find a place where those codecs disagree with Nix.
This one puts a real `nix` client against a real `nix daemon` and reads what
passed between them.

It skips when there is no `nix` on the PATH, which is the case inside the
build sandbox that runs `checks.nix-daemon-protocol`. It runs in the dev
shell. Issue nixidae/nixidae#214.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from nix_daemon_protocol.wirelog import compare, decode

pytestmark = pytest.mark.skipif(shutil.which("nix") is None, reason="this test needs a nix daemon to record")

# Nix refuses a store when a parent of it is a symbolic link, and `/tmp` is one
# on Darwin. The socket also has to fit in `sun_path`, which is 104 bytes.
TEMP_ROOT = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"
SOCKET_WAIT = 30.0


def record_run(root: Path, out: Path) -> None:
    """Run one client against one daemon, with the recorder between them."""
    root.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    outer = root / "outer.sock"
    inner = root / "inner.sock"
    sample = root / "f.txt"
    sample.write_text("hello wirelog\n")

    recorder = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "nix_daemon_protocol.wirelog",
            "record",
            "--listen",
            str(outer),
            "--connect",
            str(inner),
            "--out",
            str(out),
            "--",
            "nix",
            "daemon",
            "--store",
            str(root),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + SOCKET_WAIT
        while not outer.exists():
            if time.monotonic() > deadline:
                raise AssertionError(f"the recorder made no socket at {outer}")
            time.sleep(0.02)

        env = dict(os.environ, NIX_REMOTE=f"unix://{outer}")
        for command in (
            ["nix", "store", "info"],
            ["nix", "store", "add-file", "--name", "f.txt", str(sample)],
            ["nix", "path-info", "--json", str(sample)],
        ):
            # `path-info` of a path that is not in the store fails, and that
            # failure is a fine thing to record: the daemon answers the same
            # way in both runs.
            subprocess.run(command, env=env, capture_output=True, check=False)
    finally:
        recorder.terminate()
        recorder.wait(timeout=30)


@pytest.fixture
def workspace():
    path = Path(tempfile.mkdtemp(prefix=f"{TEMP_ROOT}/nixwire-live-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.mark.anyio
async def test_two_runs_against_the_same_daemon_agree(workspace) -> None:
    """The floor of the comparison. Two equal runs must report nothing.

    A false difference here would report every test of the functional suite as
    a regression of pynixd, so this is the test that makes the gate usable.
    """
    record_run(workspace / "run1", workspace / "rec1")
    record_run(workspace / "run2", workspace / "rec2")

    first = sorted((workspace / "rec1").glob("conn-*.wire"))
    second = sorted((workspace / "rec2").glob("conn-*.wire"))
    assert first, "the first run recorded no connection"
    assert len(first) == len(second)

    for one, two in zip(first, second, strict=True):
        control = await decode(one)
        candidate = await decode(two)
        assert control.problem is None, control.problem
        assert candidate.problem is None, candidate.problem
        assert compare(control, candidate) == []


@pytest.mark.anyio
async def test_the_real_operations_decode(workspace) -> None:
    """The decoder reads what a real client really sends.

    `AddToStoreNar` carries a framed source, so this states that the decoder
    finds the end of a request whose model covers its header alone.
    """
    record_run(workspace / "run1", workspace / "rec1")

    names: list[str] = []
    for path in sorted((workspace / "rec1").glob("conn-*.wire")):
        session = await decode(path)
        assert session.problem is None, session.problem
        assert session.handshake is not None
        assert session.handshake.nix_version
        names.extend(op.name for op in session.operations)

    assert "SetOptions" in names
    assert any(name.startswith("AddToStore") for name in names), names


@pytest.mark.anyio
async def test_a_run_that_did_less_is_a_difference(workspace) -> None:
    """The comparison must find a real change, and not only agree."""
    record_run(workspace / "run1", workspace / "rec1")

    # A second daemon that the client only greets. Every operation of the
    # first run is missing from this one.
    root = workspace / "run2"
    out = workspace / "rec2"
    root.mkdir(parents=True)
    out.mkdir(parents=True)
    outer = root / "outer.sock"
    recorder = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "nix_daemon_protocol.wirelog",
            "record",
            "--listen",
            str(outer),
            "--connect",
            str(root / "inner.sock"),
            "--out",
            str(out),
            "--",
            "nix",
            "daemon",
            "--store",
            str(root),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + SOCKET_WAIT
        while not outer.exists():
            if time.monotonic() > deadline:
                raise AssertionError(f"the recorder made no socket at {outer}")
            time.sleep(0.02)
        subprocess.run(
            ["nix", "store", "info"],
            env=dict(os.environ, NIX_REMOTE=f"unix://{outer}"),
            capture_output=True,
            check=False,
        )
    finally:
        recorder.terminate()
        recorder.wait(timeout=30)

    control = await decode(max((workspace / "rec1").glob("conn-*.wire")))
    candidate = await decode(max(out.glob("conn-*.wire")))
    assert compare(control, candidate) != []
