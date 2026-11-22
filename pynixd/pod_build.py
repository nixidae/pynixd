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
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Final

import anyio
import structlog
import uvloop

from nix_daemon_protocol.build_derivation import BuildDerivationResponse
from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from nix_daemon_protocol.logs import LogNext
from nix_daemon_protocol.store_path import StorePath
from pynixd_store.builder.settings import NotBuiltHere

from .build_job import JOB_NAME, KEY_NAME, Job, decode_request, result_message
from .build_result import failure, result_succeeded
from .cli.base import load_settings, setup_logging
from .config import SSHSubprocessStoreSpec
from .execution import without_error_prefix
from .instance import Server
from .recursive import restricted_server
from .serde.protocol import Verbosity
from .store.daemon import DaemonStore
from .store.local_db import LocalDBStore
from .store.transfer import stream_paths_store_to_store

if TYPE_CHECKING:
    from nix_daemon_protocol.logs import LogMessage

log = structlog.get_logger(__name__)

CENTRAL: Final = StoreId("central")


async def _show(message: LogMessage) -> None:
    # Central follows the container's log for liveness (`last_output_at`); the text is for a person.
    text = message.text if isinstance(message, LogNext) else type(message).__name__
    sys.stdout.write(text if text.endswith("\n") else text + "\n")
    sys.stdout.flush()


async def build_one(job_dir: Path, result_path: Path) -> int:
    """Build the job in *job_dir*; 0 when the pod did its part, whatever the build's outcome."""
    job = Job.model_validate_json(await anyio.Path(job_dir / JOB_NAME).read_text())
    request = await decode_request(job.request)
    settings = load_settings()
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
        build_settings = await local.local_build_settings(request, None)
        if isinstance(build_settings, NotBuiltHere):
            resp = BuildDerivationResponse(result=failure(f"pynixd build-one: {build_settings}"))
        else:
            resp = without_error_prefix(await local.build_here(request, build_settings, _show, Verbosity.INFO))
        if result_succeeded(resp.result):
            outputs = {p for p in request.derivation.output_paths().values() if p != StorePath("")}
            moved = await stream_paths_store_to_store(local, central, outputs)
            log.info("build_one_outputs", paths=len(outputs), bytes=moved)
    await anyio.Path(result_path).write_text(await result_message(resp))
    return 0


def build_one_main(args: argparse.Namespace) -> None:
    settings = load_settings()
    setup_logging(settings)
    code = anyio.run(
        build_one,
        Path(args.job),
        Path(args.result),
        backend="asyncio",
        backend_options={"loop_factory": uvloop.new_event_loop},
    )
    raise SystemExit(code)


def register(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = sub.add_parser("build-one", help="Build one derivation for central, as a build pod (spike)")
    parser.add_argument("job", help="the directory of central's Secret: job.json and key")
    parser.add_argument("--result", default="/dev/termination-log", help="where the BuildResult goes")
    parser.set_defaults(func=build_one_main)
