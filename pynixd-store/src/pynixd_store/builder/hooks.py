"""The programs a build runs outside its sandbox: `diff-hook` (`unix/build/derivation-builder.cc:121`)."""

from __future__ import annotations

import functools
import os
import signal
import subprocess
from typing import TYPE_CHECKING

import anyio.to_thread
import structlog

from nix_daemon_protocol.protocol import Verbosity

from ..hintfmt import magenta, show_error
from .jsonlog import Message

if TYPE_CHECKING:
    from pathlib import Path

    from .buildlog import Output
    from .users import BuildUser

_log = structlog.get_logger()


def status_text(returncode: int) -> str:
    """`statusToString` (`unix/processes.cc:378`) of a status that is not success."""
    if returncode >= 0:
        return f"failed with exit code {returncode}"
    return f"failed due to signal {-returncode} ({signal.strsignal(-returncode)})"


async def diff_hook(
    hook: str, user: BuildUser | None, valid: str, rebuilt: Path, drv_path: str, tmp: Path
) -> list[Output]:
    """`handleDiffHook`: run *hook* on a check's two outputs, and answer the lines it logs.

    *valid* is the registered output's logical path and *rebuilt* the real path of `<path>.check`.
    NIX-DEFECT (#23): `derivation-builder.cc:1564` passes `finalDestPath`, the logical path, so in a relocated
    store the hook's first argument names nothing, or the system store's copy. `store.toRealPath` would name
    the output. pynixd passes the same; Lillecarl/nix#396.

    NIX-DEFECT (#23): `derivation-builder.cc:1567` passes the build directory `tmpDir`, under a top directory
    that is root's at 0700, to a hook that runs as the build user, which cannot enter it. Measured: `test -d`
    fails in the hook on both routes. Running the hook before the directory closes, or opening it to the build
    user, would let it read the build. pynixd passes the same; Lillecarl/nix#398.
    """
    uid, gid = (os.getuid(), os.getgid()) if user is None else (user.uid, user.gid)
    try:
        # NIX-DEFECT (#23): `runProgram2` (`unix/processes.cc:344`) drops the supplementary groups whenever it sets
        # a gid, and `handleDiffHook` always sets one, so a daemon that is not root fails with EPERM and never runs
        # the hook. Dropping groups only for a build user would run it. pynixd fails the same; Lillecarl/nix#397.
        # A thread, because uvloop's subprocess refuses `user`, `group` and `extra_groups`.
        done = await anyio.to_thread.run_sync(
            functools.partial(
                subprocess.run,
                [hook, valid, str(rebuilt), drv_path, str(tmp)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                text=True,
                errors="replace",
                check=False,
                cwd="/",
                user=uid,
                group=gid,
                extra_groups=[],
            )
        )
        returncode, out = done.returncode, done.stdout
    except OSError as e:
        # `startProcess`'s child prints the error and exits 1 (`unix/processes.cc:264`).
        _log.warning("diff_hook_not_started", hook=hook, error=str(e))
        returncode, out = 1, ""
    if returncode != 0:
        inner = f"diff-hook program {magenta(chr(34) + hook + chr(34))} {magenta(status_text(returncode))}"
        return [Message(Verbosity.ERROR, show_error(f"diff hook execution failed: {magenta(inner)}"))]
    out = out.rstrip(" \n\r\t")
    return [Message(Verbosity.ERROR, out)] if out else []
