"""A builder that prints non-UTF-8 bytes builds fine through pynixd.

`containerd-2.3.5` failed 4/4 with `pynixd: internal scheduler error` because
its log holds a raw gzip stream from the `gzipping man pages` fixup phase,
and the log stream decoded as strict UTF-8. The reference daemon forwards
those bytes raw (checked against `NIX_DAEMON_SOCKET_PATH=.../upstream`),
so this builds the issue's reproducer through the session server and
asserts the build succeeds and the bytes reach the client. Issue
nixidae/nixidae#662.

The client runs on raw subprocess pipes, and not on `run_subproc`: the
helper decodes strict UTF-8, so it would crash on the very bytes this test
is about.

NIX-DEFECT (#23): `nix log` serves the line back with `0x8b` replaced by
U+FFFD, although pynixd serves it raw. The client's build hook
(`build-remote`) logs over its fromHook pipe as JSON, and
`JSONLogger::write` in `src/libutil/logging.cc:254` dumps with
`error_handler_t::replace`, so the byte becomes U+FFFD there. The
client-side goal then writes that `resBuildLogLine` field into its own log
file (`src/libstore/build/derivation-building-goal.cc:675`), which is the
record `nix log` reads. pynixd cannot fix this from its side: the
corruption happens inside the client's own hook after pynixd's bytes
arrive, and pynixd's own record of the same line is raw (measured in the
guest against the session store's `.bz2`). Reported on the fork as
Lillecarl/nix#369. The FFFD assertion below is a
tripwire: it fails when the fork fixes its JSON logger, and then it should
assert the raw bytes. Issue nixidae/nixidae#663.
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

import structlog

from tests.conftest import CLIENT_BIN, DEFAULT_SSH_OPTS, server_uri

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd import Server

log = structlog.get_logger(__name__)

REPRODUCER = """\
derivation {
  name = "stderr-binary";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "printf '\\\\037\\\\213\\\\010not-utf8\\\\n' >&2; echo done > $out" ];
}
"""
"""The issue's reproducer, without its nixpkgs.

The triage reproducer wraps this `printf` in `runCommand`, which builds the
whole stdenv closure first -- 565 derivations in a guest without them. The
bytes on the wire are what the test is about, and `minimal.leaf` in
`tests/nix/minimal.nix` proves a bare `/bin/sh` derivation builds through
this harness, so this one matches that shape exactly: no `__noChroot`, and
the proven `echo > $out` form. Issue nixidae/nixidae#662.
"""


async def test_build_with_non_utf8_log_succeeds(
    pynixd_server: Server,
    tmp_path: Path,
) -> None:
    """The issue's reproducer, built through pynixd, bytes and all."""
    uri = server_uri(pynixd_server)
    store = tmp_path / "client"
    store.mkdir()
    nix_file = tmp_path / "stderr-binary.nix"
    nix_file.write_text(REPRODUCER)

    proc = await asyncio.create_subprocess_exec(
        str(CLIENT_BIN),
        "build",
        "-v",
        "-v",
        "--store",
        str(store),
        "--builders",
        f"{uri} x86_64-linux",
        "--file",
        str(nix_file),
        "--no-link",
        "--print-out-paths",
        "--max-jobs",
        "0",
        env=os.environ.copy()
        | {
            "NIX_STATE_DIR": str(store / "var/nix"),
            # `run_subproc` sets this itself; raw pipes do not get it.
            "NIX_SSHOPTS": DEFAULT_SSH_OPTS,
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    stdout, stderr = await proc.communicate()

    assert proc.returncode == 0, stderr.decode("utf-8", errors="replace")
    out = stdout.strip()
    assert out != b""

    # The log comes back through `nix log`, which reads the client-side
    # record the build wrote (see the module docstring for whose bytes those
    # are). Raw pipes again, for the same bytes the helper cannot decode.
    log_proc = await asyncio.create_subprocess_exec(
        str(CLIENT_BIN),
        "log",
        "--store",
        str(store),
        out.decode(),
        env=os.environ.copy()
        | {
            "NIX_STATE_DIR": str(store / "var/nix"),
            "NIX_SSHOPTS": DEFAULT_SSH_OPTS,
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    log_out, log_err = await log_proc.communicate()

    assert log_proc.returncode == 0, log_err.decode("utf-8", errors="replace")
    log.info(
        "binary_log_fetched",
        # The source line (`got build log ... from '...'`): names which store
        # answered, which the byte assertion below cannot.
        source=log_err.decode("utf-8", errors="replace").strip(),
    )
    # Nix's hook replaces the byte (see the module docstring), so the
    # recorded line carries U+FFFD where the builder wrote `0x8b`. This
    # asserts that exact divergence: pynixd served the line raw, and this is
    # what the client's record made of it.
    assert b"\x1f\xef\xbf\xbd\x08not-utf8" in log_out
    log.info("binary_log_build_done", out=out.decode())
