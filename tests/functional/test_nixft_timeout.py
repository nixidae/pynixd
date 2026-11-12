"""Nix's `timeout.sh`, which skips on `needLocalStore` ("see #4813"), held to three arms (`_nixft.py`).

Each case names the line of `timeout.sh` it stands for.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import anyio
import pytest

from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, arm_store
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["_keep_store_dirs"]


# `timeout.nix`, with the builder's shell alone: no store path supplies `touch` or `sleep` here. A
# fifo opened for reading and writing never ends a `read`, so the builder waits in silence.
BUILDS = """{ fifo }: let
  mk = name: script: derivation {
    inherit name;
    system = builtins.currentSystem;
    builder = "/bin/sh";
    args = [ "-c" script ];
  };
in {
  infiniteLoop = mk "timeout" ''
    echo > $out
    echo "'timeout' builder entering an infinite loop"
    while :; do printf .; done
  '';
  silent = mk "silent" ''
    echo > $out
    read x <> ${fifo}
  '';
  closeLog = mk "silent" ''
    echo > $out
    exec > /dev/null 2>&1
    read x <> ${fifo}
  '';
}"""

# (`timeout.sh` line, command, attribute, extra arguments, exit code, stderr phrase)
CASES = (
    (11, "nix-build", "infiniteLoop", ("-Q", "--timeout", "2"), 101, "timed out"),
    (
        14,
        "nix-build",
        "infiniteLoop",
        ("-Q", "--max-build-log-size", "100"),
        1,
        "killed after writing more than 100 bytes of log output",
    ),
    (16, "nix-build", "silent", ("--max-silent-time", "2"), 101, "timed out after 2 seconds"),
    (18, "nix-build", "closeLog", (), 100, "builder failed due to signal"),
    (20, "nix", "silent", ("--max-silent-time", "2"), 1, "timed out after 2 seconds"),
)


@pytest.mark.parametrize("arm", list(Arm))
@pytest.mark.parametrize(
    ("line", "command", "attr", "extra", "rc", "phrase"), CASES, ids=[f"line{c[0]}" for c in CASES]
)
async def test_a_build_limit_ends_a_build_as_a_local_store_does(
    tmp_path: Path, arm: Arm, line: int, command: str, attr: str, extra: tuple[str, ...], rc: int, phrase: str
) -> None:
    """`timeout.sh:11`, `timeout.sh:14`, `timeout.sh:16`, `timeout.sh:18`, `timeout.sh:20`.

    `--timeout`, `--max-build-log-size`, `--max-silent-time` and a builder that closes its log.
    """
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    expr = f'({BUILDS}) {{ fifo = "{fifo}"; }}'
    if command == "nix-build":
        cmd = [NIX_BIN.with_name("nix-build"), "--no-out-link", "-E", expr, "-A", attr]
    else:
        cmd = [NIX_BIN, "build", "--impure", "--no-link", "--expr", f"({expr}).{attr}"]
    config = for_test_store(substituters=())
    config.sandbox = "false"
    async with arm_store(tmp_path, arm, config) as store:
        # Each limit ends its build within seconds; a limit nobody applies leaves it running.
        with anyio.fail_after(60):
            got, _out, err, _both = await run_subproc(
                [*cmd, "--store", store.url, *extra],
                expected_retcode=None,
                env=dict(store.env),
                nix_config=store.config,
            )
    if arm is not Arm.local:
        # NIX-DEFECT (#23): from protocol 1.26 the daemon writes an error with `operator<<(Sink &, const Error &)`
        # (`serialise.cc:564`), which drops `info.status`; the frame before 1.26 carried it. So `nix-build`, which
        # sends `BuildPaths` (`remote-store.cc:574`), exits 1 through any daemon where a local store gives 100 or
        # 101 (NixOS/nix#4813, the reason `timeout.sh:8` skips). pynixd cannot send the status under its 1.35
        # floor; the fix is a field in the frame. Lillecarl/nix#427.
        rc = 1
    assert (got, phrase in err) == (rc, True), err
