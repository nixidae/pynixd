"""The programs a build runs outside its sandbox: `diff-hook` (`unix/build/derivation-builder.cc:121`) and
`post-build-hook` (`build/derivation-building-goal.cc:1105`)."""

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
    from collections.abc import Awaitable, Callable, Iterable, Mapping
    from pathlib import Path

    from nix_daemon_protocol.store_path import StorePath

    from .buildlog import Output
    from .users import BuildUser

_log = structlog.get_logger()


def status_text(returncode: int) -> str:
    """`statusToString` (`unix/processes.cc:378`) of a status that is not success."""
    if returncode >= 0:
        return f"failed with exit code {returncode}"
    return f"failed due to signal {-returncode} ({signal.strsignal(-returncode)})"


async def diff_hook(
    hook: str, user: BuildUser | None, valid: StorePath, rebuilt: Path, drv_path: StorePath, tmp: Path
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
                [hook, str(valid), str(rebuilt), str(drv_path), str(tmp)],
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


async def post_build_hook(
    hook: str,
    env: Mapping[str, str],
    drv_path: StorePath,
    out_paths: Iterable[StorePath],
    config: str,
    *,
    on_line: Callable[[str], Awaitable[None]],
) -> int:
    """`runPostBuildHook`: run *hook* with stdout and stderr in one pipe, and answer its exit status.

    *on_line* gets each line, as `LogSink` cuts them (`derivation-building-goal.cc:80`): a last line with no
    line feed ends in one.
    """
    env = dict(env)
    # NIX-DEFECT (#23): `derivation-building-goal.cc:1123` adds the three with `emplace`, which keeps a value the
    # daemon's own environment holds, so a daemon started with `OUT_PATHS` set hands every hook those paths.
    # Assigning them would always be right. pynixd keeps the inherited value too; Lillecarl/nix#399.
    env.setdefault("DRV_PATH", str(drv_path))
    env.setdefault("OUT_PATHS", " ".join(sorted(map(str, out_paths))).rstrip(" \n\r\t"))
    env.setdefault("NIX_CONFIG", config)
    line = b""
    try:
        async with await anyio.open_process(
            [hook], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env
        ) as process:
            if process.stdout is None:
                raise RuntimeError("the post-build-hook has no stdout pipe")
            async for chunk in process.stdout:
                *whole, line = (line + chunk).split(b"\n")
                for piece in whole:
                    await on_line(piece.decode(errors="replace"))
            returncode = await process.wait()
    except OSError as e:
        # `startProcess`'s child prints the error and exits 1 (`unix/processes.cc:264`).
        _log.warning("post_build_hook_not_started", hook=hook, error=str(e))
        returncode = 1
    if line:
        await on_line(line.decode(errors="replace") + "\n")
    return returncode
