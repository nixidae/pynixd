"""Filesystem and subprocess helpers for tests."""

from __future__ import annotations

import asyncio
import contextlib
import glob as glob_module
import os
import shlex
import shutil
import signal
import stat
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from pynixd.nix_config import NixConfig
from pynixd.serde import NarFromPathRequest, StorePath
from pynixd.serde.context import WriteContext
from tests._conftest.constants import DEFAULT_NIX_CONFIG, DEFAULT_SSH_OPTS
from tests._conftest.nix_config import host_daemon_substituters

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from pynixd.store import DaemonStore

log = structlog.get_logger(__name__)


def rmtree_robust(path: str | Path) -> None:
    """Recursively remove a directory or file, unsetting read-only bits as needed."""
    path = Path(path)
    if not path.exists():
        return

    if path.is_dir():

        def handle_errors(func: Callable[..., object], path: str, _exc: BaseException) -> None:
            # `func` is whichever call failed, and `shutil` uses four of them:
            # `os.open`, `os.scandir`, `os.unlink` and `os.rmdir`. Only the
            # last two accept a path alone, so a blind retry of `func(path)`
            # raises `TypeError: open() missing required argument 'flags'`.
            # That exception is not an `OSError`, so it left the handler and
            # aborted the whole session with an INTERNALERROR. One root-owned
            # overlayfs work directory in the tree was enough to do it.
            with contextlib.suppress(OSError):
                Path(path).chmod(stat.S_IRWXU)
            if func in (os.unlink, os.rmdir):
                with contextlib.suppress(OSError):
                    func(path)

        shutil.rmtree(path, onexc=handle_errors)
    else:
        try:
            path.unlink()
        except PermissionError:
            try:
                path.chmod(stat.S_IWRITE | stat.S_IREAD)
                path.unlink()
            except OSError:
                pass  # Best-effort cleanup; ignore failures
        except OSError:
            pass  # Best-effort cleanup; ignore failures


def rmtree_robust_glob(pattern: str) -> None:
    """Remove all directories matching a glob pattern."""
    for path_str in glob_module.glob(pattern):  # noqa: PTH207
        rmtree_robust(Path(path_str))


def serde_path(path: object) -> StorePath:
    """Convert domain/test path objects to the wire StorePath model."""
    return StorePath(path=str(path))


def serde_path_set(paths: Iterable[object]) -> set[StorePath]:
    """Convert a path iterable to the wire StorePath set used by request models."""
    return {serde_path(path) for path in paths}


async def read_nar_from_store(store: DaemonStore, path: object, nar_size: int) -> bytes:
    """Read a NAR stream from a store using the streaming NarFromPath protocol."""
    async with store.transfer_conn() as conn:
        await NarFromPathRequest(path=serde_path(path)).to_writer(WriteContext.from_conn(conn))
        await conn.w.drain()
        await conn.r.drain_stderr()
        return await conn.r.readexactly(nar_size)


async def run_subproc(
    cmd: Sequence[object],
    verbose: bool = True,
    expected_retcode: int | None = 0,
    nix_config: NixConfig | dict[str, str] | None = None,
    env: dict[str, str] | None = None,
) -> tuple[int, str, str, str]:
    """Run a command, streaming stdout/stderr through structlog in real-time.

    Args:
        cmd: Command and arguments to run.
        verbose: If True, stream output to structlog in real-time.
        expected_retcode: If not None, raise if return code doesn't match. Defaults to 0.
        nix_config: NixConfig object or dict for NIX_CONFIG env var.
        env: Extra environment variables. Updated in place with NIX_SSHOPTS and NIX_CONFIG.

    Returns:
        tuple of (returncode, stdout, stderr, combined)
    """
    run_env = {} if env is None else env
    if "NIX_SSHOPTS" not in run_env:
        run_env["NIX_SSHOPTS"] = DEFAULT_SSH_OPTS

    if isinstance(nix_config, NixConfig):
        config_str = nix_config.to_nix_config_env()
    elif nix_config is not None:
        # The host's daemon only when the host has one. See
        # `nix_config.host_daemon_substituters`: named on a machine without
        # that socket, it is a substituter that answers every query with
        # `cannot connect to socket`. Issue #47.
        default_config = {
            "substituters": " ".join(("https://nixkube.cachix.org", *host_daemon_substituters())),
            "trusted-public-keys": "nixkube.cachix.org-1:H8UE0jlI9pxHexK/NhDmEoLDarJXp1WTymQrsajlh7M=",
        }
        merged = default_config | nix_config
        config_str = "\n".join(f"{k} = {v}" for k, v in merged.items())
    else:
        config_str = DEFAULT_NIX_CONFIG.to_nix_config_env()

    if "NIX_CONFIG" in run_env:
        run_env["NIX_CONFIG"] = f"{run_env['NIX_CONFIG']}\n{config_str}"
    else:
        run_env["NIX_CONFIG"] = config_str

    str_cmd = [str(c) for c in cmd]
    log.debug("run_subproc", cmd=shlex.join(str_cmd), env=run_env)
    proc = await asyncio.create_subprocess_exec(
        *str_cmd,
        env=os.environ.copy() | run_env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )

    stdout: list[str] = []
    stderr: list[str] = []
    stdboth: list[str] = []

    async def stream(name: str, accumulator: list[str], pipe: asyncio.StreamReader) -> None:
        while True:
            line = await pipe.readline()
            # Lossy on purpose: this streams display text to the log, and a
            # builder can print bytes no UTF-8 decoder accepts (a gzip
            # stream in a containerd build log did exactly that). The bytes
            # themselves travel the wire untouched. Issue nixidae/nixidae#662.
            decoded_line = line.decode(errors="replace")
            accumulator.append(decoded_line)
            stdboth.append(decoded_line)
            if not line:
                break
            if verbose:
                log.info(name, message=decoded_line.rstrip())

    assert proc.stdout is not None
    assert proc.stderr is not None
    try:
        await asyncio.gather(
            stream("stdout", stdout, proc.stdout),
            stream("stderr", stderr, proc.stderr),
        )
        await proc.wait()
    except asyncio.CancelledError:
        log.warning("run_subproc_cancelled", cmd=shlex.join(str_cmd), pid=proc.pid)
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGKILL)
                await proc.wait()
        raise
    rc = proc.returncode if proc.returncode is not None else 0
    if expected_retcode is not None and rc != expected_retcode:
        raise RuntimeError(
            f"Command failed with rc={rc} (expected {expected_retcode}):\n{''.join(stdboth)}",
        )
    return (rc, "".join(stdout), "".join(stderr), "".join(stdboth))
