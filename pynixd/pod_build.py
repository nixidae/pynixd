"""`pynixd build-one`: a build pod's pynixd, which builds one derivation for central and ends. nixidae/nixidae#1560, spike.

The pod is a client of central only. Central's Secret gives it a job and an
SSH key that central granted this derivation (`build_grant.py`). The pod
copies the inputs in from central, builds in its sibling sandbox, copies the
outputs back, and writes the `BuildResult` as its termination message.
Central reads that message from the pod's status. It never connects to the
pod.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Final

import anyio
import structlog
import uvloop

from nix_daemon_protocol.build_derivation import BuildDerivationResponse
from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from nix_daemon_protocol.logs import LogNext, LogResult
from nix_daemon_protocol.protocol import ResultType
from nix_daemon_protocol.store_path import StorePath
from pynixd_store.builder.settings import NotBuiltHere

from .build_job import (
    JOB_NAME,
    KEY_NAME,
    POD_SECONDS,
    Job,
    PodPart,
    decode_request,
    result_line,
    result_message,
    seconds_line,
    usage_line,
)
from .build_result import failure, result_succeeded
from .cli.base import load_settings, setup_logging
from .config import LocalSocketStoreSpec, SSHSubprocessStoreSpec
from .execution import without_error_prefix
from .instance import Server
from .recursive import restricted_server
from .serde.protocol import Verbosity
from .store.daemon import DaemonStore
from .store.local_db import LocalDBStore
from .store.transfer import stream_paths_store_to_store

if TYPE_CHECKING:
    from nix_daemon_protocol.logs import LogMessage

    from .config import PynixdSettings

log = structlog.get_logger(__name__)

CENTRAL: Final = StoreId("central")
BUILD_USER: Final = "pynixd-build"
"""The local name of a build pod's uid that has none: central's SSH server reads the username `pod`, not this."""


BUILDER_LINES: Final = frozenset({ResultType.BUILD_LOG_LINE, ResultType.POST_BUILD_LOG_LINE})


def shown(message: LogMessage) -> str:
    """The line of the pod's log for *message*: the text of a log line or of a line the builder wrote, else the
    message's kind. A builder's line is a result, as `buildWithHook` reads it (`derivation-building-goal.cc:671`)."""
    if isinstance(message, LogNext):
        return message.text
    if isinstance(message, LogResult) and message.result_type in BUILDER_LINES:
        return next((field.valstr for field in message.fields if field.valstr is not None), "")
    return type(message).__name__


async def _show(message: LogMessage) -> None:
    # Central follows the container's log for liveness (`last_output_at`); the text is for a person.
    text = shown(message)
    sys.stdout.write(text if text.endswith("\n") else text + "\n")
    sys.stdout.flush()


class PodClock:
    """The seconds of each part of the pod's work, in order (nixidae/nixidae#1560: the fixed cost, part by part)."""

    def __init__(self, started: float) -> None:
        self._last = started
        self.parts: dict[PodPart, float] = {}

    def mark(self, part: PodPart) -> None:
        """*part* ends now."""
        now = time.monotonic()
        self.parts[part] = now - self._last
        self._last = now


def process_started() -> float:
    """When this process started, on `time.monotonic`'s clock: Python's own clocks start after the imports.

    The kernel counts a process's start in clock ticks since boot (`/proc/self/stat`, field 22), which
    `CLOCK_BOOTTIME` also counts from.
    """
    after_comm = Path("/proc/self/stat").read_text().rpartition(")")[2].split()
    started = int(after_comm[19]) / os.sysconf("SC_CLK_TCK")
    return time.monotonic() - (time.clock_gettime(time.CLOCK_BOOTTIME) - started)


async def build_one(job_dir: Path, result_path: Path, clock: PodClock) -> int:
    """Build the job in *job_dir*; 0 when the pod did its part, whatever the build's outcome."""
    job = Job.model_validate_json(await anyio.Path(job_dir / JOB_NAME).read_text())
    request = await decode_request(job.request)
    settings = load_settings()
    without_probe(settings)
    settings.stores[str(CENTRAL)] = SSHSubprocessStoreSpec(
        host=job.central_host,
        port=job.central_port,
        username="pod",
        known_hosts=None,
        host_public_key=job.central_host_key,
        client_keys=[str(job_dir / KEY_NAME)],
        no_schedule=True,
        monitor=False,
    )
    server = Server(stores=settings.to_stores(), settings=settings)
    async with server:
        scheduler = server.ctx.scheduler
        if scheduler is not None and scheduler.stores_opened is not None:
            await scheduler.stores_opened.wait()
        clock.mark(PodPart.SERVER)
        local = server.ctx.stores[LOCAL_STORE_ID]
        central = server.ctx.stores[CENTRAL]
        if not isinstance(local, LocalDBStore):
            raise SystemExit("pynixd build-one: the local store is not one pynixd builds into")
        if not isinstance(central, DaemonStore):
            raise SystemExit("pynixd build-one: central is not a daemon store")
        # A nested recursive-nix build: central's key would refuse whatever the pod built for it.
        local.recursive_server = restricted_server(server.ctx, builds_at=central)
        inputs = {StorePath(path=str(p)) for p in request.derivation.input_srcs}
        moved = await stream_paths_store_to_store(central, local, inputs)
        log.info("build_one_inputs", paths=len(inputs), bytes=moved)
        clock.mark(PodPart.INPUTS)
        build_settings = await local.local_build_settings(request, None)
        if isinstance(build_settings, NotBuiltHere):
            resp = BuildDerivationResponse(result=failure(f"pynixd build-one: {build_settings}"))
        else:
            if job.build_cores is not None:
                build_settings = replace(build_settings, build_cores=job.build_cores)
            if job.runner is not None:
                build_settings = replace(
                    build_settings, build_cores=job.runner.build_cores, impure_env=job.runner.impure_env
                )
            resp = without_error_prefix(await local.build_here(request, build_settings, _show, Verbosity.INFO))
        clock.mark(PodPart.BUILD)
        if result_succeeded(resp.result):
            outputs = {p for p in request.derivation.output_paths().values() if p != StorePath("")}
            # A floating output's path is known from the result alone.
            outputs |= {
                StorePath(str(r.out_path)).with_store_prefix()
                for r in resp.result.realised_outputs().values()
                if r.out_path is not None
            }
            moved = await stream_paths_store_to_store(local, central, outputs)
            log.info("build_one_outputs", paths=len(outputs), bytes=moved)
        clock.mark(PodPart.OUTPUTS)
    clock.mark(PodPart.SHUTDOWN)
    # Central reads it from the log and tells the client; the result has no room for it.
    await _show(LogNext(text=seconds_line(POD_SECONDS, clock.parts)))
    if resp.result.resources.measured:
        await _show(LogNext(text=usage_line(job.result_token, resp.result.resources)))
    message = await result_message(resp)
    await _show(LogNext(text=result_line(job.result_token, message)))
    await anyio.Path(result_path).write_text(message)
    return 0


def without_probe(settings: PynixdSettings) -> None:
    """The pod's store builds the one derivation central placed here, so it proves nothing by probe builds.

    `probe_own_builder` builds a probe for each system of nix.conf: 1.59 s of the pod's 2.19 s server start (run
    `nkunsb1`). It guards nothing here: a pod whose probe fails answers MISC_FAILURE, as a failed build does.
    """
    local = settings.stores.get(str(LOCAL_STORE_ID))
    if local is not None:
        settings.stores[str(LOCAL_STORE_ID)] = local.model_copy(update={"probe": False})


def _own_name() -> None:
    """asyncssh asks `getpass.getuser()` on each connection, which fails for a uid with no passwd entry and no USER.

    Measured in a nixkube farm pod as uid 1000: Python 3.14 raises `OSError` there, which asyncssh's
    `SSHClientConnectionOptions.prepare` does not catch, so the dial to central fails before it starts.
    """
    try:
        getpass.getuser()
    except (OSError, KeyError):
        os.environ["USER"] = BUILD_USER


def _own_home(settings: PynixdSettings) -> None:
    """Nix takes HOME only when this user owns it, and asks passwd else; a pod's image may have no entry for its uid.

    Measured in a farm pod as uid 1000 with no passwd: "cannot determine user's home directory". A `HOME` that
    is not there yet is made, so this user owns it. Else the home goes under the local store's root, which the
    pod writes: a build pod may have no writable `/tmp` (measured).
    """
    home = os.environ.get("HOME")
    if home and not os.path.lexists(home):
        Path(home).mkdir(parents=True)
    if home and Path(home).is_dir() and Path(home).stat().st_uid == os.geteuid():
        return
    local = settings.stores.get(str(LOCAL_STORE_ID))
    # A store at `/` is the machine's, whose root is no place for a home.
    if not isinstance(local, LocalSocketStoreSpec) or local.store_path == Path("/"):
        return
    own = Path(local.store_path) / "home"
    own.mkdir(exist_ok=True)
    os.environ["HOME"] = str(own)


def build_one_main(args: argparse.Namespace) -> None:
    clock = PodClock(process_started())
    clock.mark(PodPart.PYTHON)
    settings = load_settings()
    _own_name()
    _own_home(settings)
    setup_logging(settings)
    code = anyio.run(
        build_one,
        Path(args.job),
        Path(args.result),
        clock,
        backend="asyncio",
        backend_options={"loop_factory": uvloop.new_event_loop},
    )
    raise SystemExit(code)


def register(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = sub.add_parser("build-one", help="Build one derivation for central, as a build pod (spike)")
    parser.add_argument("job", help="the directory of central's Secret: job.json and key")
    parser.add_argument("--result", default="/dev/termination-log", help="where the BuildResult goes")
    parser.set_defaults(func=build_one_main)
