"""Entry point: python -m pynixd

Subcommands:
  daemon    Start the pynixd daemon server
  gc        Trigger garbage collection on stores
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import tracemalloc

import anyio
import structlog
import uvloop

from pynixd_store.limits import bump_file_limit
from pynixd_store.mount_namespace import enter_private as enter_private_mount_namespace

from .cli.base import load_settings, setup_logging
from .cli.gc import register as register_gc
from .cli.gha_cache import register as register_gha_cache
from .cli.reapi_runner import register as register_reapi_runner
from .cli.roots import register as register_roots
from .cli.state import register as register_state
from .handover import Inherited, Offer, hand_over, restart_instead, say_ready, take_ready_fd
from .instance import Server
from .pod_build import register as register_build_one
from .systemd import notify

log = structlog.get_logger(__name__)


async def async_daemon_main() -> None:
    """Async entry point: load settings, create server, and run until shutdown."""
    settings = load_settings()
    stores = settings.to_stores()
    inherited = Inherited.from_env(os.environ)
    ready_fd = take_ready_fd(os.environ)

    server = Server(stores=stores, settings=settings, inherited=inherited)
    shutdown_event = anyio.Event()

    async def _handle_signals() -> None:
        with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
            async for _sig in signals:
                if shutdown_event.is_set():
                    log.info("forced_shutdown")
                    raise SystemExit(1)
                log.info("shutdown_signal_received")
                shutdown_event.set()

    async def _drained() -> None:
        await server.drained.wait()
        shutdown_event.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(_handle_signals)
        tg.start_soon(_drained)
        async with server:
            # After every listener is bound: in `replace` mode the daemon
            # socket is one, and `Type=notify` holds back what is ordered
            # after us. A successor takes the main pid in the same message.
            notify(f"MAINPID={os.getpid()}\nREADY=1" if inherited is not None else "READY=1")
            if ready_fd is not None:
                say_ready(ready_fd)
            await shutdown_event.wait()
            # A process that handed over is no longer the unit's main: under `NotifyAccess=all` its
            # STOPPING=1 would stop the successor's unit.
            if not server.draining:
                notify("STOPPING=1")
        tg.cancel_scope.cancel()


def daemon_main(_args: argparse.Namespace) -> None:
    """CLI entry point for ``pynixd daemon`` — sets up logging and runs the async loop."""
    if sys.platform == "linux":
        bump_file_limit()
    # First, while the process has one thread: a store on a read-only mount is remounted in it.
    enter_private_mount_namespace()
    settings = load_settings()
    setup_logging(settings)

    if os.environ.get("PYNIXD_BENCH"):
        # Allocation census for the growth benchmark. Off unless asked:
        # tracing costs throughput and keeps a traceback per allocation.
        tracemalloc.start(5)

    anyio.run(async_daemon_main, backend="asyncio", backend_options={"loop_factory": uvloop.new_event_loop})


def handover_main(args: argparse.Namespace) -> None:
    """CLI entry point for ``pynixd handover``: the unit's `ExecReload=`."""
    settings = load_settings()
    setup_logging(settings)
    if settings.handover_path is None:
        raise SystemExit(anyio.run(restart_instead, args.unit, "the new settings turn the hand-over off"))
    command = list(args.command)
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        raise SystemExit("pynixd handover: name the successor's command after --")
    raise SystemExit(anyio.run(lambda: hand_over(settings.handover_path, command, Offer.of(settings), unit=args.unit)))


def main() -> None:
    """Top-level CLI entry point: parse args and dispatch to subcommand handlers."""
    parser = argparse.ArgumentParser(description="pynixd — Nix daemon protocol proxy")
    root_sub = parser.add_subparsers(dest="subcommand", required=True)

    daemon_parser = root_sub.add_parser("daemon", help="Start the pynixd daemon server")
    daemon_parser.set_defaults(func=daemon_main)

    handover_parser = root_sub.add_parser(
        "handover", help="Start a successor with the running daemon's listeners, then drain the running one"
    )
    handover_parser.add_argument("--unit", help="Restart this unit when the running daemon cannot hand over")
    handover_parser.add_argument("command", nargs=argparse.REMAINDER, help="-- the successor's command")
    handover_parser.set_defaults(func=handover_main)

    register_gc(root_sub)
    register_gha_cache(root_sub)
    register_reapi_runner(root_sub)
    register_roots(root_sub)
    register_state(root_sub)
    register_build_one(root_sub)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
